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
use tokio_postgres::replication::LogicalReplicationStream;
use tokio_postgres::SimpleQueryMessage;

use error::ReplicationError;
use kafka_producer::KafkaProducer;
use util::TopicInfo;

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
    consistent_point: Arc<RwLock<u64>>,
}

pub struct ReplicationOp {
    pub rel_id: u32,
    pub seq_id: u32,
    pub lsn: u64,
    pub prev_lsn: u64,
    pub op: Op,
}

#[derive(Serialize, Deserialize)]
pub enum Op {
    Insert(Row),
    Update((Row, Row)),
    Delete(Row),
}

pub type Row = Vec<Option<String>>;

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
            consistent_point: Arc::new(RwLock::new(0)),
        }
    }

    pub async fn start(&mut self) -> Result<(), ReplicationError> {
        self.create_slot().await?;

        loop {
            match self.start_replication().await {
                Err(ReplicationError::Recoverable(e)) => {
                    tracing::warn!(
                        "replication for slot {} interrupted, retrying: {}",
                        &self.slot_name,
                        e
                    )
                }
                Err(ReplicationError::Fatal(e)) => return Err(ReplicationError::Fatal(e)),
                Ok(_) => unreachable!("replication stream cannot exit without an error"),
            }

            tokio::time::sleep(Duration::from_secs(3)).await;
            tracing::info!("resuming replication for slot {}", &self.slot_name);
        }
    }

    async fn create_slot(&mut self) -> Result<(), ReplicationError> {
        let client = try_recoverable!(util::connect_replication(&self.connection_string).await);

        // Start a transaction and immediatelly create a replication slot with the USE SNAPSHOT
        // directive. This makes the starting point of the slot and the snapshot of the transaction
        // identical.
        client
            .simple_query("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ;")
            .await?;

        // retrieve the list of replication slots
        let get_slots_query =
            "SELECT slot_name, slot_type, plugin, restart_lsn FROM pg_replication_slots";

        let slot_rows = client
            .simple_query(get_slots_query)
            .await?
            .into_iter()
            .filter_map(|msg| match msg {
                SimpleQueryMessage::Row(row) => Some(row),
                _ => None,
            });

        for slot_row in slot_rows {
            let slot_name = try_recoverable!(slot_row
                .get("slot_name")
                .ok_or_else(|| anyhow!("missing expected column: `slot_name`")));

            if slot_name != self.slot_name {
                continue;
            }

            // we've found an existing slot by the same name. check that it is a logical pgoutput slot
            let slot_type = try_recoverable!(slot_row
                .get("slot_type")
                .ok_or_else(|| anyhow!("missing expected column: `slot_type`")));

            let plugin = try_recoverable!(slot_row
                .get("plugin")
                .ok_or_else(|| anyhow!("missing expected column: `plugin`")));

            if slot_type != "logical" {
                return Err(ReplicationError::Fatal(anyhow!("a replication slot by the same name already exists, but it's type is not logical and therefore cannot be used for replication by this programme")));
            }

            if plugin != "pgoutput" {
                return Err(ReplicationError::Fatal(anyhow!("a logical replication slot by the same name already exists, but it's plugin is not pgoutput and therefore cannot be used for replication by this programme")));
            }

            let consistent_point = try_recoverable!(slot_row
                .get("restart_lsn")
                .ok_or_else(|| anyhow!("missing expected column: `restart_lsn`")));

            let consistent_point: u64 = try_fatal!(consistent_point
                .parse()
                .or_else(|_| Err(anyhow!("invalid lsn"))));

            self.consistent_point = Arc::new(RwLock::new(consistent_point));

            client.simple_query("COMMIT;").await?;

            return Ok(());
        }

        let create_slot_query = format!(
            r#"CREATE_REPLICATION_SLOT {:?} LOGICAL "pgoutput" USE_SNAPSHOT"#,
            &self.slot_name
        );

        let created_slot_row = client
            .simple_query(&create_slot_query)
            .await?
            .into_iter()
            .next()
            .and_then(|msg| match msg {
                SimpleQueryMessage::Row(row) => Some(row),
                _ => None,
            })
            .ok_or_else(|| {
                ReplicationError::Recoverable(anyhow!(
                    "empty result after creating replication slot"
                ))
            })?;

        // Store the lsn at which we will need to start the replication stream from
        let consistent_point = try_recoverable!(created_slot_row
            .get("consistent_point")
            .ok_or_else(|| anyhow!("missing expected column: `consistent_point`")));

        let consistent_point: u64 = try_fatal!(consistent_point
            .parse()
            .or_else(|_| Err(anyhow!("invalid lsn"))));

        self.consistent_point = Arc::new(RwLock::new(consistent_point));

        client.simple_query("COMMIT;").await?;

        Ok(())
    }

    async fn start_replication(&mut self) -> Result<(), ReplicationError> {
        use ReplicationError::*;

        let publication_tables = try_recoverable!(
            util::publication_info(
                &self.connection_string,
                &self.publication_name,
                &self.topic_map
            )
            .await
        );

        let kafka_producer = KafkaProducer::new(self.kafka_brokers.clone());
        let (replication_op_tx, mut committed_lsn_rx) = kafka_producer.produce(publication_tables);

        let consistent_point = self.consistent_point.clone();

        tokio::spawn(async move {
            while let Some(committed_lsn) = committed_lsn_rx.recv().await {
                if committed_lsn > *consistent_point.read().await {
                    let mut consistent_point = consistent_point.write().await;
                    *consistent_point = committed_lsn
                }
            }
        });

        // todo(umran): pass configuration via dep injection
        let client = try_recoverable!(util::connect_replication(&self.connection_string).await);

        let query = format!(
            r#"START_REPLICATION SLOT "{name}" LOGICAL {lsn}
            ("proto_version" '1', "publication_names" '{publication}')"#,
            name = &self.slot_name,
            lsn = *self.consistent_point.read().await,
            publication = self.publication_name
        );
        let copy_stream = try_recoverable!(client.copy_both_simple(&query).await);

        let stream = LogicalReplicationStream::new(copy_stream);
        tokio::pin!(stream);

        let mut last_keepalive = Instant::now();

        let mut seq_id = 0;
        let mut tx_in_progress = false;
        let mut prev_lsn = *self.consistent_point.read().await;
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

                let lsn = *self.consistent_point.read().await;
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
                                return Err(Fatal(anyhow!("received a begin before commit for the previous transaction! this is a bug!")));
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

                            try_fatal!(replication_op_tx
                                .send(ReplicationOp {
                                    rel_id,
                                    lsn,
                                    prev_lsn,
                                    seq_id,
                                    op
                                })
                                .await
                                .map_err(|_| anyhow!(
                                    "unable to produce replication message to producer tx channel"
                                )));

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

                            try_fatal!(replication_op_tx
                                .send(ReplicationOp {
                                    rel_id,
                                    lsn,
                                    prev_lsn,
                                    seq_id,
                                    op
                                })
                                .await
                                .map_err(|_| anyhow!(
                                    "unable to produce replication message to producer tx channel"
                                )));

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

                            try_fatal!(replication_op_tx
                                .send(ReplicationOp {
                                    rel_id,
                                    lsn,
                                    prev_lsn,
                                    seq_id,
                                    op
                                })
                                .await
                                .map_err(|_| anyhow!(
                                    "unable to produce replication message to producer tx channel"
                                )));

                            seq_id += 1;
                        }
                        Commit(_) => {
                            tx_in_progress = false;
                        }
                        Origin(_) | Relation(_) | Type(_) => {
                            // Ignored
                        }
                        Truncate(_) => return Err(Fatal(anyhow!("source table got truncated"))),
                        // The enum is marked as non_exaustive. Better to be conservative here in
                        // case a new message is relevant to the semantics of our source
                        _ => return Err(Fatal(anyhow!("unexpected logical replication message"))),
                    }
                }
                // Handled above
                PrimaryKeepAlive(_) => {}
                // The enum is marked non_exaustive, better be conservative
                _ => return Err(Fatal(anyhow!("Unexpected replication message"))),
            }
        }

        Err(Recoverable(anyhow!("replication stream ended")))
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
