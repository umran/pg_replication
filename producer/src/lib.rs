use std::error::Error;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail};
use async_trait::async_trait;
use futures::TryStreamExt;
use lazy_static::lazy_static;

use postgres_protocol::message::backend::{
    LogicalReplicationMessage, ReplicationMessage, TupleData,
};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::runtime::Handle;
use tokio_postgres::error::{DbError, Severity, SqlState};
use tokio_postgres::replication::LogicalReplicationStream;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{NoTls, SimpleQueryMessage};

use tokio::sync::{mpsc, Mutex};

mod util;

lazy_static! {
    /// Postgres epoch is 2000-01-01T00:00:00Z
    static ref PG_EPOCH: SystemTime = UNIX_EPOCH + Duration::from_secs(946_684_800);
}

/// Information required to sync data from Postgres
pub struct PostgresSourceReader {
    slot_name: String,
    publication_name: String,
    /// Our cursor into the WAL
    lsn: Arc<Mutex<PgLsn>>,
}

trait ErrorExt {
    fn is_recoverable(&self) -> bool;
}

impl ErrorExt for tokio_postgres::Error {
    fn is_recoverable(&self) -> bool {
        match self.source() {
            Some(err) => {
                match err.downcast_ref::<DbError>() {
                    Some(db_err) => {
                        use Severity::*;
                        // Connection and non-fatal errors
                        db_err.code() == &SqlState::CONNECTION_EXCEPTION
                            || db_err.code() == &SqlState::CONNECTION_DOES_NOT_EXIST
                            || db_err.code() == &SqlState::CONNECTION_FAILURE
                            || db_err.code() == &SqlState::TOO_MANY_CONNECTIONS
                            || db_err.code() == &SqlState::CANNOT_CONNECT_NOW
                            || db_err.code() == &SqlState::ADMIN_SHUTDOWN
                            || db_err.code() == &SqlState::CRASH_SHUTDOWN
                            || !matches!(
                                db_err.parsed_severity(),
                                Some(Error) | Some(Fatal) | Some(Panic)
                            )
                    }
                    // IO errors
                    None => err.is::<std::io::Error>(),
                }
            }
            // We have no information about what happened, it might be a fatal error or
            // it might not. Unexpected errors can happen if the upstream crashes for
            // example in which case we should retry.
            //
            // Therefore, we adopt a "recoverable unless proven otherwise" policy and
            // keep retrying in the event of unexpected errors.
            None => true,
        }
    }
}

enum ReplicationError {
    Recoverable(anyhow::Error),
    Fatal(anyhow::Error),
}

impl<E: ErrorExt + Into<anyhow::Error>> From<E> for ReplicationError {
    fn from(err: E) -> Self {
        if err.is_recoverable() {
            Self::Recoverable(err.into())
        } else {
            Self::Fatal(err.into())
        }
    }
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

pub struct Transaction {
    end_lsn: PgLsn,
    ops: Vec<Op>,
}

pub enum Op {
    Insert(Row),
    Update(Row),
    Delete(Row),
}

pub struct Row {
    pub rel_id: u32,
    pub data: Vec<Datum>,
}

pub enum Datum {
    Null,
    Text(String),
}

fn row_from_tuple<'a, T>(rel_id: u32, tuple_data: T) -> Result<Row, anyhow::Error>
where
    T: IntoIterator<Item = &'a TupleData>,
{
    for val in tuple_data.into_iter() {
        let datum = match val {
            TupleData::Null => Datum::Null,
            TupleData::UnchangedToast => bail!(
                "Missing TOASTed value from table with OID = {}. \
                Did you forget to set REPLICA IDENTITY to FULL for your table?",
                rel_id
            ),
            TupleData::Text(b) => Datum::Text(std::str::from_utf8(&b)?.into()),
        };
    }

    todo!()
}

#[derive(Debug)]
pub enum ConsumerError {
    Fatal(String),
}

#[async_trait]
pub trait ReplicationConsumer {
    async fn consume(&mut self, transaction: Transaction) -> Result<Option<PgLsn>, ConsumerError>;
}

impl PostgresSourceReader {
    /// Constructs a new instance
    pub fn new(slot_name: String, publication_name: String) -> Self {
        Self {
            slot_name,
            publication_name,
            lsn: Arc::new(Mutex::new(0.into())),
        }
    }

    async fn produce_replication<T>(
        &mut self,
        mut replication_consumer: T,
    ) -> Result<(), ReplicationError>
    where
        T: ReplicationConsumer + Send + 'static,
    {
        use ReplicationError::*;

        let (producer, mut consumer) = mpsc::channel(1);
        let lsn_copy = self.lsn.clone();

        tokio::spawn(async move {
            while let Some(tx) = consumer.recv().await {
                if let Some(confirmed_lsn) = replication_consumer.consume(tx).await.unwrap() {
                    let mut lsn = lsn_copy.lock().await;
                    *lsn = confirmed_lsn;
                }
            }
        });

        // todo(umran): pass configuration via dep injection
        let client = try_recoverable!(util::connect_replication("").await);

        let query = format!(
            r#"START_REPLICATION SLOT "{name}" LOGICAL {lsn}
            ("proto_version" '1', "publication_names" '{publication}')"#,
            name = &self.slot_name,
            lsn = self.lsn.lock().await,
            publication = self.publication_name
        );
        let copy_stream = try_recoverable!(client.copy_both_simple(&query).await);

        let stream = LogicalReplicationStream::new(copy_stream);
        tokio::pin!(stream);

        let mut last_keepalive = Instant::now();
        let mut ops: Vec<Op> = vec![];

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

                let lsn = self.lsn.lock().await;

                try_recoverable!(
                    stream
                        .as_mut()
                        .standby_status_update(*lsn, *lsn, *lsn, ts, 0)
                        .await
                );
                last_keepalive = Instant::now();
            }
            match item {
                XLogData(xlog_data) => {
                    use LogicalReplicationMessage::*;

                    match xlog_data.data() {
                        Begin(_) => {
                            if !ops.is_empty() {
                                return Err(Fatal(anyhow!(
                                    "got BEGIN statement after uncommitted data"
                                )));
                            }
                        }
                        Insert(insert) => {
                            let rel_id = insert.rel_id();
                            let new_tuple = insert.tuple().tuple_data();

                            let row = try_fatal!(row_from_tuple(rel_id, new_tuple));
                            ops.push(Op::Insert(row));
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

                            let new_row = try_fatal!(row_from_tuple(rel_id, new_tuple));
                            ops.push(Op::Update(new_row));
                        }
                        Delete(delete) => {
                            let rel_id = delete.rel_id();
                            let old_tuple = try_fatal!(delete
                                            .old_tuple()
                                            .ok_or_else(|| anyhow!("Old row missing from replication stream for table with OID = {}. \
                                                    Did you forget to set REPLICA IDENTITY to FULL for your table?", rel_id)))
                                        .tuple_data();

                            let row = try_fatal!(row_from_tuple(rel_id, old_tuple));
                            ops.push(Op::Delete(row));
                        }
                        Commit(commit) => {
                            // let tx = timestamper.start_tx().await;

                            // for row in deletes.drain(..) {
                            //     try_fatal!(tx.delete(row).await);
                            // }
                            // for row in inserts.drain(..) {
                            //     try_fatal!(tx.insert(row).await);
                            // }

                            let tx = Transaction {
                                end_lsn: commit.end_lsn().into(),
                                ops: ops.drain(..).collect(),
                            };

                            producer.send(tx).await.map_err(|_| {
                                ReplicationError::Fatal(anyhow::Error::msg(
                                    "failed to send producer message, this is an unexpected error",
                                ))
                            })?;
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

    // async fn produce_snapshot<W: AsyncWrite + Unpin>(
    //     &mut self,
    //     snapshot_tx: &mut SourceTransaction<'_>,
    //     buffer: &mut W,
    // ) -> Result<(), ReplicationError> {
    //     let client =
    //         try_recoverable!(postgres_util::connect_replication(&self.connector.conn).await);

    //     // We're initialising this source so any previously existing slot must be removed and
    //     // re-created. Once we have data persistence we will be able to reuse slots across restarts
    //     let _ = client
    //         .simple_query(&format!(
    //             "DROP_REPLICATION_SLOT {:?}",
    //             &self.connector.slot_name
    //         ))
    //         .await;

    //     // Get all the relevant tables for this publication
    //     let publication_tables = try_recoverable!(
    //         postgres_util::publication_info(&self.connector.conn, &self.connector.publication)
    //             .await
    //     );

    //     // Start a transaction and immediatelly create a replication slot with the USE SNAPSHOT
    //     // directive. This makes the starting point of the slot and the snapshot of the transaction
    //     // identical.
    //     client
    //         .simple_query("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ;")
    //         .await?;

    //     let slot_query = format!(
    //         r#"CREATE_REPLICATION_SLOT {:?} LOGICAL "pgoutput" USE_SNAPSHOT"#,
    //         &self.connector.slot_name
    //     );
    //     let slot_row = client
    //         .simple_query(&slot_query)
    //         .await?
    //         .into_iter()
    //         .next()
    //         .and_then(|msg| match msg {
    //             SimpleQueryMessage::Row(row) => Some(row),
    //             _ => None,
    //         })
    //         .ok_or_else(|| {
    //             ReplicationError::Recoverable(anyhow!(
    //                 "empty result after creating replication slot"
    //             ))
    //         })?;

    //     // Store the lsn at which we will need to start the replication stream from
    //     let consistent_point = try_recoverable!(slot_row
    //         .get("consistent_point")
    //         .ok_or_else(|| anyhow!("missing expected column: `consistent_point`")));
    //     self.lsn = try_fatal!(consistent_point
    //         .parse()
    //         .or_else(|_| Err(anyhow!("invalid lsn"))));
    //     for info in publication_tables {
    //         // TODO(petrosagg): use a COPY statement here for more efficient network transfer
    //         let data = client
    //             .simple_query(&format!(
    //                 "SELECT * FROM {:?}.{:?}",
    //                 info.namespace, info.name
    //             ))
    //             .await?;
    //         for msg in data {
    //             if let SimpleQueryMessage::Row(row) = msg {
    //                 let mut mz_row = Row::default();
    //                 let rel_id: Datum = (info.rel_id as i32).into();
    //                 mz_row.push(rel_id);
    //                 mz_row.push_list((0..row.len()).map(|n| {
    //                     let a: Datum = row.get(n).into();
    //                     a
    //                 }));
    //                 try_recoverable!(snapshot_tx.insert(mz_row.clone()).await);
    //                 try_fatal!(buffer.write(&try_fatal!(bincode::serialize(&mz_row))).await);
    //             }
    //         }
    //         self.metrics.tables.inc();
    //     }
    //     self.metrics.lsn.set(self.lsn.into());
    //     client.simple_query("COMMIT;").await?;
    //     Ok(())
    // }
}

// #[async_trait]
// impl SimpleSource for PostgresSourceReader {
//     /// The top-level control of the state machine and retry logic
//     async fn start(mut self, timestamper: &Timestamper) -> Result<(), SourceError> {
//         // Buffer rows from snapshot to retract and retry, if initial snapshot fails.
//         // Postgres sources cannot proceed without a successful snapshot.
//         {
//             let mut snapshot_tx = timestamper.start_tx().await;
//             loop {
//                 let file =
//                     tokio::fs::File::from_std(tempfile::tempfile().map_err(|e| SourceError {
//                         source_name: self.source_name.clone(),
//                         error: SourceErrorDetails::FileIO(e.to_string()),
//                     })?);
//                 let mut writer = tokio::io::BufWriter::new(file);
//                 match self.produce_snapshot(&mut snapshot_tx, &mut writer).await {
//                     Ok(_) => {
//                         tracing::info!(
//                             "replication snapshot for source {} succeeded",
//                             &self.source_name
//                         );
//                         break;
//                     }
//                     Err(ReplicationError::Recoverable(e)) => {
//                         writer.flush().await.map_err(|e| SourceError {
//                             source_name: self.source_name.clone(),
//                             error: SourceErrorDetails::Initialization(e.to_string()),
//                         })?;
//                         tracing::warn!(
//                             "replication snapshot for source {} failed, retrying: {}",
//                             &self.source_name,
//                             e
//                         );
//                         let reader = BufReader::new(writer.into_inner().into_std().await);
//                         self.revert_snapshot(&mut snapshot_tx, reader)
//                             .await
//                             .map_err(|e| SourceError {
//                                 source_name: self.source_name.clone(),
//                                 error: SourceErrorDetails::FileIO(e.to_string()),
//                             })?;
//                     }
//                     Err(ReplicationError::Fatal(e)) => {
//                         return Err(SourceError {
//                             source_name: self.source_name,
//                             error: SourceErrorDetails::Initialization(e.to_string()),
//                         })
//                     }
//                 }

//                 // TODO(petrosagg): implement exponential back-off
//                 tokio::time::sleep(Duration::from_secs(3)).await;
//             }
//         }

//         loop {
//             match self.produce_replication(timestamper).await {
//                 Err(ReplicationError::Recoverable(e)) => {
//                     tracing::warn!(
//                         "replication for source {} interrupted, retrying: {}",
//                         &self.source_name,
//                         e
//                     )
//                 }
//                 Err(ReplicationError::Fatal(e)) => {
//                     return Err(SourceError {
//                         source_name: self.source_name,
//                         error: SourceErrorDetails::FileIO(e.to_string()),
//                     })
//                 }
//                 Ok(_) => unreachable!("replication stream cannot exit without an error"),
//             }

//             // TODO(petrosagg): implement exponential back-off
//             tokio::time::sleep(Duration::from_secs(3)).await;
//             tracing::info!("resuming replication for source {}", &self.source_name);
//         }
//     }
// }
