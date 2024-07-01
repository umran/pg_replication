use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use idempotent_consumer::IdempotentConsumer;
use kafka_consumer::KafkaConsumer;
use producer::{error::ReplicationError, ReplicationOp};
use sqlx::{Postgres, Transaction};

mod db;
pub mod idempotent_consumer;
pub mod kafka_consumer;

pub struct Consumer<App: Application> {
    pub group_id: String,
    pub topics: Vec<String>,
    pub brokers: String,
    pub connection_string: String,

    app: Arc<App>,
}

#[async_trait]
pub trait Application: Send + Sync + 'static {
    async fn handle_message(
        &self,
        _tx: &mut Transaction<'_, Postgres>,
        _op: &ReplicationOp,
    ) -> Result<(), ReplicationError>;
}

impl<App: Application> Consumer<App> {
    pub fn new(
        group_id: &str,
        topics: Vec<&str>,
        brokers: &str,
        connection_string: &str,
        app: App,
    ) -> Self {
        Self {
            group_id: group_id.into(),
            topics: topics.iter().map(|t| t.to_string()).collect(),
            brokers: brokers.into(),
            connection_string: connection_string.into(),
            app: Arc::new(app),
        }
    }

    pub async fn start(self) -> Result<(), anyhow::Error> {
        loop {
            match self.start_consumer().await {
                Err(ReplicationError::Recoverable(_)) => {
                    tracing::warn!("failed to create idempotent consumer, will retry");
                }
                Err(ReplicationError::Fatal(err)) => {
                    tracing::error!("encountered a fatal error, please investigate: {}", err);
                    return Err(err);
                }
                Ok(_) => unreachable!("consumer cannot exit without an error"),
            }

            tokio::time::sleep(Duration::from_secs(3)).await;
            tracing::info!("resuming consumer for group_id {}", &self.group_id);
        }
    }

    async fn start_consumer(&self) -> Result<(), ReplicationError> {
        let idempotent_consumer =
            IdempotentConsumer::new(&self.group_id, &self.connection_string, self.app.clone())
                .await?;

        let kafka_consumer = KafkaConsumer::new(
            &self.group_id,
            &self.brokers,
            &self.topics,
            idempotent_consumer,
        )?;
        kafka_consumer.consume().await
    }
}
