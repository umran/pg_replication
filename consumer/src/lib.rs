mod db;
pub mod kafka_consumer;
mod state;

use std::collections::HashMap;

use anyhow::anyhow;
use db::Lock;
use model::Crud;
use producer::{error::ReplicationError, ReplicationOp};
use sqlx::{PgConnection, PgPool, Postgres, Transaction};
use state::State;
use tokio::sync::RwLock;

use kafka_consumer::{Handler, HandlerMessage};

/// Consumer processes messages from a given set of topics in partition order. In order to ensure idempotency of
/// message processing, the consumer (defined by the consumer_group_id) must keep track of the latest
/// lsn and seq_no processed per topic per partition. In order to preserve idempotency, this state
/// must be read from and written to within the same transaction as any writes that occur as a result of side-effects from processing the message
pub struct Consumer {
    /// the id of the consumer group to which this consumer belongs
    pub group_id: String,
    /// the kafka topics this consumer should subscribe to
    pub topics: Vec<String>,

    pool: PgPool,
    partition_state: RwLock<HashMap<(String, i32), (u64, u64)>>,
}

impl Consumer {
    pub fn new(group_id: &str, topics: Vec<&str>, pool: PgPool) -> Self {
        Self {
            group_id: group_id.into(),
            topics: topics.into_iter().map(|t| t.into()).collect(),
            pool,
            partition_state: RwLock::new(HashMap::new()),
        }
    }

    async fn process_payload(
        &self,
        _tx: &mut Transaction<'_, Postgres>,
        _op: &ReplicationOp,
    ) -> Result<(), ReplicationError> {
        todo!()
    }

    async fn get_cached_partition_state(&self, topic: &str, partition: i32) -> Option<(u64, u64)> {
        self.partition_state
            .read()
            .await
            .get(&(topic.to_string(), partition))
            .map(|state| *state)
    }

    async fn set_cached_partition_state(&self, topic: &str, partition: i32, lsn: u64, seq_id: u64) {
        self.partition_state
            .write()
            .await
            .insert((topic.to_string(), partition), (lsn, seq_id).into());
    }

    async fn delete_cached_partition_state(&self, topic: &str, partition: i32) {
        self.partition_state
            .write()
            .await
            .remove(&(topic.to_string(), partition));
    }

    fn bulk_delete_cached_partition_states_blocking(&self, keys: Vec<(&str, i32)>) {
        let mut state = self.partition_state.blocking_write();

        for (topic, partition) in keys.iter() {
            state.remove(&(topic.to_string(), *partition));
        }
    }

    async fn get_partition_state(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        topic: &str,
        partition: i32,
    ) -> Result<Option<(u64, u64)>, ReplicationError> {
        let state = State::select()
            .by_field("group_id", self.group_id.clone())
            .by_field("topic", topic.to_string())
            .by_field("partition", partition)
            .fetch_optional(tx)
            .await
            .map_err(|err| {
                tracing::warn!("db error: query to select consumer state didn't succeed, will consider this a transiet error: {}", err);
                ReplicationError::Recoverable(anyhow!("db error: query to select consumer state failed"))
            })?;

        if let Some(state) = state {
            // now we parse the lsn and seq_id from the returned row
            let lsn = state.lsn.parse().map_err(|_| {
                tracing::error!("Failed to decode lsn returned by database. This is a bug!");
                ReplicationError::Fatal(anyhow!(
                    "Failed to decode lsn returned by database. This is a bug!"
                ))
            })?;

            let seq_id = state.seq_id.parse().map_err(|_| {
                tracing::error!("Failed to decode seq_id returned by database. This is a bug!");
                ReplicationError::Fatal(anyhow!(
                    "Failed to decode seq_id returned by database. This is a bug!"
                ))
            })?;

            return Ok((lsn, seq_id).into());
        }

        // there is no exisiting cursor for this (group_id, topic, partition) combination on this database
        return Ok(None);
    }

    async fn update_partition_state(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        topic: &str,
        partition: i32,
        lsn: u64,
        seq_id: u64,
    ) -> Result<(), ReplicationError> {
        sqlx::query("UPDATE replication_consumer_state SET lsn = $4, seq_id = $5 WHERE group_id = $1 AND topic = $2 AND partition = $3")
            .bind(&self.group_id)
            .bind(topic)
            .bind(partition)
            .bind(lsn.to_string())
            .bind(seq_id.to_string())
            .execute(tx as &mut PgConnection)
            .await
            .map_err(|err| {
                tracing::warn!("db error: query to select consumer state didn't succeed, will consider this a transiet error: {}", err);
                ReplicationError::Recoverable(anyhow!("db error: query to select consumer state failed"))
            })?;

        Ok(())
    }

    async fn create_partition_state(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        topic: &str,
        partition: i32,
        lsn: u64,
        seq_id: u64,
    ) -> Result<(), ReplicationError> {
        let state = State::new(
            &self.group_id,
            topic,
            partition,
            lsn.to_string(),
            seq_id.to_string(),
        );

        state.create()
            .execute(tx)
            .await
            .map_err(|err| {
                tracing::warn!("db error: query to select consumer state didn't succeed, will consider this a transiet error: {}", err);
                ReplicationError::Recoverable(anyhow!("db error: query to select consumer state failed"))
            })?;

        Ok(())
    }

    async fn begin_transaction(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<Transaction<'_, Postgres>, ReplicationError> {
        let mut tx = self.pool.begin().await.map_err(|err| {
            tracing::warn!("db error: failed to being transaction: {}", err);
            ReplicationError::Recoverable(anyhow!("db error: failed to begin transaction"))
        })?;

        let key = format!("{}_{}_{}", self.group_id, topic, partition);

        Lock::new(key).acquire(&mut tx).await?;

        Ok(tx)
    }

    async fn commit_partition_state(
        &self,
        tx: Transaction<'_, Postgres>,
        topic: &str,
        partition: i32,
        lsn: u64,
        seq_id: u64,
    ) -> Result<(), ReplicationError> {
        match tx.commit().await {
            Ok(_) => {
                self.set_cached_partition_state(topic, partition, lsn, seq_id)
                    .await;
                Ok(())
            }
            Err(err) => {
                tracing::warn!("transaction commit failure. the database partition state is unknown, invalidating local cache: {}", err);
                self.delete_cached_partition_state(topic, partition).await;

                Err(ReplicationError::Recoverable(anyhow!(
                    "db commit failure, treating this as a recoverable error: {}",
                    err
                )))
            }
        }
    }
}

#[async_trait::async_trait]
impl Handler for Consumer {
    type Payload = ReplicationOp;

    async fn handle_message(
        &self,
        msg: HandlerMessage<'_, Self::Payload>,
    ) -> Result<(), ReplicationError> {
        let topic = msg.topic;
        let partition = msg.partition;

        if let Some((lsn, seq_id)) = self.get_cached_partition_state(topic, partition).await {
            if msg.payload.lsn < lsn {
                return Ok(());
            }

            if msg.payload.lsn == lsn && msg.payload.seq_id <= seq_id {
                return Ok(());
            }

            // start a tx and process the message
            let mut tx = self.begin_transaction(topic, partition).await?;
            self.process_payload(&mut tx, &msg.payload).await?;

            // update and commit partition state
            self.update_partition_state(
                &mut tx,
                topic,
                partition,
                msg.payload.lsn,
                msg.payload.seq_id,
            )
            .await?;

            self.commit_partition_state(tx, topic, partition, msg.payload.lsn, msg.payload.seq_id)
                .await?;

            return Ok(());
        }

        // state is not present in cache, so we lookup the database
        let mut tx = self.begin_transaction(topic, partition).await?;

        if let Some((lsn, seq_id)) = self.get_partition_state(&mut tx, topic, partition).await? {
            self.set_cached_partition_state(topic, partition, lsn, seq_id)
                .await;

            if msg.payload.lsn < lsn {
                return Ok(());
            }

            if msg.payload.lsn == lsn && msg.payload.seq_id <= seq_id {
                return Ok(());
            }

            self.process_payload(&mut tx, &msg.payload).await?;

            // update and commit partition state
            self.update_partition_state(
                &mut tx,
                topic,
                partition,
                msg.payload.lsn,
                msg.payload.seq_id,
            )
            .await?;

            self.commit_partition_state(tx, topic, partition, msg.payload.lsn, msg.payload.seq_id)
                .await?;

            return Ok(());
        }

        // state is not present in database
        self.process_payload(&mut tx, &msg.payload).await?;

        // create and commit the partition state
        self.create_partition_state(
            &mut tx,
            topic,
            partition,
            msg.payload.lsn,
            msg.payload.seq_id,
        )
        .await?;

        self.commit_partition_state(tx, topic, partition, msg.payload.lsn, msg.payload.seq_id)
            .await?;

        Ok(())
    }

    fn assign_partitions(&self, partitions: Vec<(&str, i32)>) {
        self.bulk_delete_cached_partition_states_blocking(partitions)
    }

    fn revoke_partitions(&self, partitions: Vec<(&str, i32)>) {
        self.bulk_delete_cached_partition_states_blocking(partitions)
    }
}
