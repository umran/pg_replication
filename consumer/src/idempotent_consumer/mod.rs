mod state;

use std::sync::Arc;
use std::{collections::HashMap, sync::RwLock};

use anyhow::anyhow;

use model::{Crud, Migration};
use producer::{error::ReplicationError, ReplicationOp};
use sqlx::{PgConnection, PgPool, Postgres, Transaction};

use crate::db::Lock;
use crate::kafka_consumer::{Handler, HandlerMessage};

use state::State;

use super::Application;

/// IdempotentConsumer processes messages from a given set of topics in partition order. In order to ensure idempotency of
/// message processing, the consumer (defined by the consumer_group_id) must keep track of the latest
/// lsn and seq_no processed per topic per partition. In order to preserve idempotency, this state
/// must be read from and written to within the same transaction as any writes that occur as a result of side-effects from processing the message
pub struct IdempotentConsumer<App: Application> {
    /// the id of the consumer group to which this consumer belongs
    pub group_id: String,

    pool: PgPool,
    partition_state: RwLock<HashMap<(String, i32), (u64, u64)>>,
    app: Arc<App>,
}

impl<App: Application> IdempotentConsumer<App> {
    pub async fn new(
        group_id: &str,
        connection_string: &str,
        app: Arc<App>,
    ) -> Result<Self, ReplicationError> {
        let pool = PgPool::connect(connection_string).await.map_err(|_| {
            ReplicationError::Recoverable(anyhow!("db error: treating as recoverable"))
        })?;

        // run migration for state table
        let mut tx = pool.begin().await.map_err(|_| {
            ReplicationError::Recoverable(anyhow!("db error: begin error, treating as recoverable"))
        })?;

        State::migrate(&mut tx).await.map_err(|_| {
            ReplicationError::Recoverable(anyhow!(
                "db error: migration error, treating as recoverable"
            ))
        })?;

        tx.commit().await.map_err(|_| {
            ReplicationError::Recoverable(anyhow!(
                "db error: commit error, treating as recoverable"
            ))
        })?;

        let consumer = Self {
            group_id: group_id.into(),
            pool,
            partition_state: RwLock::new(HashMap::new()),
            app,
        };

        Ok(consumer)
    }

    fn get_cached_partition_state(&self, topic: &str, partition: i32) -> Option<(u64, u64)> {
        self.partition_state
            .read()
            .unwrap()
            .get(&(topic.to_string(), partition))
            .map(|state| *state)
    }

    fn set_cached_partition_state(&self, topic: &str, partition: i32, lsn: u64, seq_id: u64) {
        self.partition_state
            .write()
            .unwrap()
            .insert((topic.to_string(), partition), (lsn, seq_id).into());
    }

    fn delete_cached_partition_state(&self, topic: &str, partition: i32) {
        self.partition_state
            .write()
            .unwrap()
            .remove(&(topic.to_string(), partition));
    }

    fn bulk_delete_cached_partition_states(&self, keys: Vec<(&str, i32)>) {
        let mut state = self.partition_state.write().unwrap();

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
            .for_update()
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
        current_lsn: u64,
        current_seq_id: u64,
        lsn: u64,
        seq_id: u64,
    ) -> Result<(), ReplicationError> {
        let result = sqlx::query("UPDATE replication_consumer_state SET lsn = $6, seq_id = $7 WHERE group_id = $1 AND topic = $2 AND partition = $3 AND lsn = $4 AND seq_id = $5")
            .bind(&self.group_id)
            .bind(topic)
            .bind(partition)
            .bind(current_lsn.to_string())
            .bind(current_seq_id.to_string())
            .bind(lsn.to_string())
            .bind(seq_id.to_string())
            .execute(tx as &mut PgConnection)
            .await
            .map_err(|err| {
                tracing::warn!("db error: query to update selected consumer state didn't succeed, will consider this a transiet error: {}", err);
                ReplicationError::Recoverable(anyhow!("db error: query to select consumer state failed"))
            })?;

        // expect the result to have affected exactly 1 row or else there is a synchronization issue.
        // in this case we emit a recoverable error which will restart the consumer with fresh state
        if result.rows_affected() != 1 {
            tracing::warn!("Stale consumer state detected, will consider this a recoverable error");
            return Err(ReplicationError::Recoverable(anyhow!(
                "Stale consumer state detected"
            )));
        }

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
                self.set_cached_partition_state(topic, partition, lsn, seq_id);
                Ok(())
            }
            Err(err) => {
                tracing::warn!("transaction commit failure. the database partition state is unknown, invalidating local cache: {}", err);
                self.delete_cached_partition_state(topic, partition);

                Err(ReplicationError::Recoverable(anyhow!(
                    "db commit failure, treating this as a recoverable error: {}",
                    err
                )))
            }
        }
    }
}

#[async_trait::async_trait]
impl<App: Application> Handler for IdempotentConsumer<App> {
    type Payload = ReplicationOp;

    async fn handle_message(
        &self,
        msg: &HandlerMessage<'_, Self::Payload>,
    ) -> Result<(), ReplicationError> {
        let topic = msg.topic;
        let partition = msg.partition;

        tracing::info!(
            "received message for topic {} and partition {}",
            topic,
            partition
        );

        if let Some((lsn, seq_id)) = self.get_cached_partition_state(topic, partition) {
            if is_processed_payload(&msg.payload, lsn, seq_id) {
                tracing::info!("skipping already processed message: {}_{}", lsn, seq_id);

                return Ok(());
            }

            // start a tx and process the message
            let mut tx = self.begin_transaction(topic, partition).await?;
            self.app.handle_message(&mut tx, &msg.payload).await?;

            // update and commit partition state
            self.update_partition_state(
                &mut tx,
                topic,
                partition,
                lsn,
                seq_id,
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
            self.set_cached_partition_state(topic, partition, lsn, seq_id);

            if is_processed_payload(&msg.payload, lsn, seq_id) {
                tracing::info!("skipping already processed message: {}_{}", lsn, seq_id);

                return Ok(());
            }

            self.app.handle_message(&mut tx, &msg.payload).await?;

            // update and commit partition state
            self.update_partition_state(
                &mut tx,
                topic,
                partition,
                lsn,
                seq_id,
                msg.payload.lsn,
                msg.payload.seq_id,
            )
            .await?;

            self.commit_partition_state(tx, topic, partition, msg.payload.lsn, msg.payload.seq_id)
                .await?;

            return Ok(());
        }

        // state is not present in database
        self.app.handle_message(&mut tx, &msg.payload).await?;

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
        self.bulk_delete_cached_partition_states(partitions)
    }

    fn revoke_partitions(&self, partitions: Vec<(&str, i32)>) {
        self.bulk_delete_cached_partition_states(partitions)
    }
}

fn is_processed_payload(payload: &ReplicationOp, lsn: u64, seq_id: u64) -> bool {
    if payload.lsn < lsn {
        return true;
    }

    if payload.lsn == lsn && payload.seq_id <= seq_id {
        return true;
    }

    false
}
