mod error;
mod kafka_producer;
mod util;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail};
use futures::TryStreamExt;
use lazy_static::lazy_static;

use postgres_protocol::message::backend::{
    LogicalReplicationMessage, ReplicationMessage, TupleData,
};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tokio_postgres::error::SqlState;
use tokio_postgres::replication::LogicalReplicationStream;
use tokio_postgres::types::PgLsn;
use tokio_postgres::Client;

use error::ReplicationError;
use kafka_producer::{KafkaProducer, KafkaProducerMessage};
use util::{TableInfo, TopicInfo};

lazy_static! {
    /// Postgres epoch is 2000-01-01T00:00:00Z
    static ref PG_EPOCH: SystemTime = UNIX_EPOCH + Duration::from_secs(946_684_800);
}

macro_rules! try_fatal {
    ($expr:expr $(,)?) => {
        match $expr {
            Ok(val) => val,
            Err(err) => return Err(ReplicationError::Fatal(err.into())),
        }
    };
}

macro_rules! try_recoverable {
    ($expr:expr $(,)?) => {
        match $expr {
            Ok(val) => val,
            Err(err) => return Err(ReplicationError::Recoverable(err.into())),
        }
    };
}

/// Information required to sync data from Postgres
pub struct Producer {
    connection_string: String,
    kafka_brokers: String,
    slot_name: String,
    publication_name: String,
    topic_map: HashMap<String, TopicInfo>,
}

#[derive(Serialize, Deserialize)]
pub struct ReplicationOp {
    pub rel_id: u32,
    pub lsn: u64,
    pub seq_id: u64,
    pub op: Op,
}

#[derive(Serialize, Deserialize)]
pub enum Op {
    Insert(Row),
    Update((Row, Row)),
    Delete(Row),
}

pub type Row = Vec<Option<String>>;

struct SlotMetadata {
    confirmed_flush_lsn: u64,
    active_pid: Option<i32>,
}

impl Producer {
    /// Constructs a new instance
    pub fn new(
        connection_string: String,
        kafka_brokers: String,
        slot_name: String,
        publication_name: String,
        topic_map: HashMap<String, TopicInfo>,
    ) -> Self {
        Self {
            connection_string,
            kafka_brokers,
            slot_name,
            publication_name,
            topic_map,
        }
    }

    pub async fn start(&mut self) -> Result<(), anyhow::Error> {
        loop {
            match self.start_replication().await {
                Err(ReplicationError::Recoverable(e)) => {
                    tracing::warn!(
                        "replication for slot {} interrupted, retrying: {}",
                        &self.slot_name,
                        e
                    )
                }
                Err(ReplicationError::Fatal(e)) => return Err(e),
                Ok(_) => unreachable!("replication stream cannot exit without an error"),
            }

            tokio::time::sleep(Duration::from_secs(3)).await;
            tracing::info!("resuming replication for slot {}", &self.slot_name);
        }
    }

    async fn start_replication(&mut self) -> Result<(), ReplicationError> {
        let client = try_recoverable!(util::connect_replication(&self.connection_string).await);

        ensure_replication_slot(&client, &self.slot_name).await?;

        let slot_meta_data =
            fetch_slot_metadata(&client, &self.slot_name, Duration::from_secs(3)).await?;

        let publication_tables =
            util::publication_info(&client, &self.publication_name, &self.topic_map).await?;

        let kafka_producer = KafkaProducer::new(self.kafka_brokers.clone());
        let (replication_op_tx, mut committed_lsn_rx) = kafka_producer.produce()?;

        let confirmed_flush_lsn = Arc::new(RwLock::new(slot_meta_data.confirmed_flush_lsn));
        let confirmed_flush_lsn_clone = confirmed_flush_lsn.clone();

        tokio::spawn(async move {
            while let Some(committed_lsn) = committed_lsn_rx.recv().await {
                if committed_lsn > *confirmed_flush_lsn_clone.read().await {
                    let mut confirmed_flush_lsn = confirmed_flush_lsn_clone.write().await;
                    *confirmed_flush_lsn = committed_lsn
                }
            }
        });

        // kill any active pid that is already bound to the slot before attempting to start replication
        kill_active_pid(&client, slot_meta_data.active_pid).await;

        let query = format!(
            r#"START_REPLICATION SLOT "{name}" LOGICAL {lsn}
            ("proto_version" '1', "publication_names" '{publication}')"#,
            name = &self.slot_name,
            lsn = *confirmed_flush_lsn.read().await,
            publication = self.publication_name
        );
        let copy_stream = try_recoverable!(client.copy_both_simple(&query).await);

        let stream = LogicalReplicationStream::new(copy_stream);
        tokio::pin!(stream);

        let mut last_keepalive = Instant::now();

        let mut seq_id = 0;
        let mut tx_in_progress = false;
        let mut prev_lsn = *confirmed_flush_lsn.read().await;
        let mut lsn = prev_lsn;

        while let Some(item) = stream.try_next().await? {
            use ReplicationMessage::*;
            // The upstream will periodically request keepalive responses by setting the reply field
            // to 1. However, we cannot rely on these messages arriving on time. For example, when
            // the upstream is sending a big transaction its keepalive messages are queued and can
            // be delayed arbitrarily.  Therefore, we also make sure to send a proactive keepalive
            // every 30 seconds.
            //
            // See: https://www.postgresql.org/message-id/CAMsr+YE2dSfHVr7iEv1GSPZihitWX-PMkD9QALEGcTYa+sdsgg@mail.gmail.com
            if matches!(item, PrimaryKeepAlive(ref k) if k.reply() == 1)
                || last_keepalive.elapsed() > Duration::from_secs(30)
            {
                let ts: i64 = PG_EPOCH
                    .elapsed()
                    .expect("system clock set earlier than year 2000!")
                    .as_micros()
                    .try_into()
                    .expect("software more than 200k years old, consider updating");

                let lsn = *confirmed_flush_lsn.read().await;
                let lsn = lsn.into();

                try_recoverable!(
                    stream
                        .as_mut()
                        .standby_status_update(lsn, lsn, lsn, ts, 0)
                        .await
                );

                last_keepalive = Instant::now();
            }

            match item {
                XLogData(xlog_data) => {
                    use LogicalReplicationMessage::*;
                    match xlog_data.data() {
                        Begin(begin) => {
                            if tx_in_progress {
                                return Err(ReplicationError::Fatal(anyhow!("received a begin before commit for the previous transaction! this is a bug!")));
                            }

                            tx_in_progress = true;
                            seq_id = 0;
                            prev_lsn = lsn;
                            lsn = begin.final_lsn();
                        }
                        Insert(insert) => {
                            let rel_id = insert.rel_id();
                            let new_tuple = insert.tuple().tuple_data();
                            let row = try_fatal!(row_from_tuple_data(rel_id, new_tuple));
                            let op = Op::Insert(row);

                            let message = message_from_op(
                                rel_id,
                                lsn,
                                prev_lsn,
                                seq_id,
                                op,
                                &publication_tables,
                            )?;

                            try_recoverable!(replication_op_tx.send(message).await.map_err(
                                |_| anyhow!(
                                    "unable to produce replication message to producer tx channel"
                                )
                            ));

                            seq_id += 1;
                        }
                        Update(update) => {
                            let rel_id = update.rel_id();

                            let old_tuple = try_fatal!(update
                                            .old_tuple()
                                            .ok_or_else(|| anyhow!("Old row missing from replication stream for table with OID = {}. \
                                                    Did you forget to set REPLICA IDENTITY to FULL for your table?", rel_id)))
                                        .tuple_data();

                            // If the new tuple contains unchanged toast values, reuse the ones
                            // from the old tuple
                            let new_tuple = update
                                .new_tuple()
                                .tuple_data()
                                .iter()
                                .zip(old_tuple.iter())
                                .map(|(new, old)| match new {
                                    TupleData::UnchangedToast => old,
                                    _ => new,
                                });

                            let old_row = try_fatal!(row_from_tuple_data(rel_id, old_tuple));
                            let new_row = try_fatal!(row_from_tuple_data(rel_id, new_tuple));
                            let op = Op::Update((old_row, new_row));

                            let message = message_from_op(
                                rel_id,
                                lsn,
                                prev_lsn,
                                seq_id,
                                op,
                                &publication_tables,
                            )?;

                            try_recoverable!(replication_op_tx.send(message).await.map_err(
                                |_| anyhow!(
                                    "unable to produce replication message to producer tx channel"
                                )
                            ));

                            seq_id += 1;
                        }
                        Delete(delete) => {
                            let rel_id = delete.rel_id();

                            let old_tuple = try_fatal!(delete
                                            .old_tuple()
                                            .ok_or_else(|| anyhow!("Old row missing from replication stream for table with OID = {}. \
                                                    Did you forget to set REPLICA IDENTITY to FULL for your table?", rel_id)))
                                        .tuple_data();

                            let row = try_fatal!(row_from_tuple_data(rel_id, old_tuple));
                            let op = Op::Delete(row);

                            let message = message_from_op(
                                rel_id,
                                lsn,
                                prev_lsn,
                                seq_id,
                                op,
                                &publication_tables,
                            )?;

                            try_recoverable!(replication_op_tx.send(message).await.map_err(
                                |_| anyhow!(
                                    "unable to produce replication message to producer tx channel"
                                )
                            ));

                            seq_id += 1;
                        }
                        Commit(_) => {
                            tx_in_progress = false;
                        }
                        Origin(_) | Relation(_) | Type(_) => {
                            // Ignored
                        }
                        Truncate(_) => {
                            return Err(ReplicationError::Fatal(anyhow!(
                                "source table got truncated"
                            )))
                        }
                        // The enum is marked as non_exaustive. Better to be conservative here in
                        // case a new message is relevant to the semantics of our source
                        _ => {
                            return Err(ReplicationError::Fatal(anyhow!(
                                "unexpected logical replication message"
                            )))
                        }
                    }
                }
                // Handled above
                PrimaryKeepAlive(_) => {}
                // The enum is marked non_exaustive, better be conservative
                _ => {
                    return Err(ReplicationError::Fatal(anyhow!(
                        "Unexpected replication message"
                    )))
                }
            }
        }

        Err(ReplicationError::Recoverable(anyhow!(
            "replication stream ended"
        )))
    }
}

async fn ensure_replication_slot(client: &Client, slot: &str) -> Result<(), ReplicationError> {
    match client
        .execute(
            "CREATE_REPLICATION_SLOT $1 LOGICAL \"pgoutput\" NOEXPORT_SNAPSHOT",
            &[&slot],
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(err) if err.code() == Some(&SqlState::DUPLICATE_OBJECT) => Ok(()),
        Err(_) => Err(ReplicationError::Recoverable(anyhow!(
            "transient postgres error"
        ))),
    }
}

async fn fetch_slot_metadata(
    client: &Client,
    slot: &str,
    interval: Duration,
) -> Result<SlotMetadata, ReplicationError> {
    loop {
        let query = "SELECT active_pid, confirmed_flush_lsn
                FROM pg_replication_slots WHERE slot_name = $1";

        let Some(row) = client.query_opt(query, &[&slot]).await? else {
            return Err(ReplicationError::Recoverable(anyhow!(
                "missing replication slot"
            )));
        };

        match row.get::<_, Option<PgLsn>>("confirmed_flush_lsn") {
            Some(lsn) => {
                return Ok(SlotMetadata {
                    confirmed_flush_lsn: lsn.into(),
                    active_pid: row.get("active_pid"),
                })
            }
            // It can happen that confirmed_flush_lsn is NULL as the slot initializes
            // This could probably be a `tokio::time::interval`, but its only is called twice,
            // so its fine like this.
            None => tokio::time::sleep(interval).await,
        };
    }
}

async fn kill_active_pid(client: &Client, active_pid: Option<i32>) {
    // We're the only application that should be using this replication
    // slot. The only way that there can be another connection using
    // this slot under normal operation is if there's a stale TCP
    // connection from a prior incarnation of the source holding on to
    // the slot. We don't want to wait for the WAL sender timeout and/or
    // TCP keepalives to time out that connection, because these values
    // are generally under the control of the DBA and may not time out
    // the connection for multiple minutes, or at all. Instead we just
    // force kill the connection that's using the slot.
    //
    // Note that there's a small risk that *we're* the zombie cluster
    // that should not be using the replication slot. Kubernetes cannot
    // 100% guarantee that only one cluster is alive at a time. However,
    // this situation should not last long, and the worst that can
    // happen is a bit of transient thrashing over ownership of the
    // replication slot.
    if let Some(active_pid) = active_pid {
        tracing::warn!(
            %active_pid,
            "replication slot already in use; will attempt to kill existing connection",
        );

        match client
            .execute("SELECT pg_terminate_backend($1)", &[&active_pid])
            .await
        {
            Ok(_) => {
                tracing::info!(
                    "successfully killed existing connection; \
                    starting replication is likely to succeed"
                );
                // Note that `pg_terminate_backend` does not wait for
                // the termination of the targeted connection to
                // complete. We may try to start replication before the
                // targeted connection has cleaned up its state. That's
                // okay. If that happens we'll just try again from the
                // top via the suspend-and-restart flow.
            }
            Err(e) => {
                tracing::warn!(
                    %e,
                    "failed to kill existing replication connection; \
                    replication will likely fail to start"
                );
                // Continue on anyway, just in case the replication slot
                // is actually available. Maybe PostgreSQL has some
                // staleness when it reports `active_pid`, for example.
            }
        }
    }
}

fn row_from_tuple_data<'a, T>(rel_id: u32, tuple_data: T) -> Result<Row, anyhow::Error>
where
    T: IntoIterator<Item = &'a TupleData>,
{
    let mut data = vec![];

    for val in tuple_data.into_iter() {
        let datum = match val {
            TupleData::Null => None,
            TupleData::UnchangedToast => bail!(
                "Missing TOASTed value from table with OID = {}. \
                Did you forget to set REPLICA IDENTITY to FULL for your table?",
                rel_id
            ),
            TupleData::Text(b) => std::str::from_utf8(&b)?.to_string().into(),
        };

        data.push(datum);
    }

    Ok(data)
}

fn message_from_op(
    rel_id: u32,
    lsn: u64,
    prev_lsn: u64,
    seq_id: u64,
    op: Op,
    publication_tables: &HashMap<u32, TableInfo>,
) -> Result<KafkaProducerMessage<ReplicationOp>, ReplicationError> {
    let table_info = try_fatal!(publication_tables.get(&rel_id).ok_or_else(|| anyhow!(
        "table info for the received rel_id does not exisit in publication"
    )));

    let topic = table_info.topic.clone();

    let partition_key = try_fatal!(match &op {
        Op::Insert(row) => table_info.extract_partition_key(row),
        Op::Update((_, row)) => table_info.extract_partition_key(row),
        Op::Delete(row) => table_info.extract_partition_key(row),
    })
    .join("");

    Ok(KafkaProducerMessage {
        topic,
        partition_key,
        prev_lsn,
        payload: ReplicationOp {
            rel_id,
            lsn,
            seq_id,
            op,
        },
    })
}
