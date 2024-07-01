use std::{sync::Arc, time::Duration};

use anyhow::anyhow;
use async_trait::async_trait;
use rdkafka::{
    config::RDKafkaLogLevel,
    consumer::{Consumer, ConsumerContext, Rebalance, StreamConsumer},
    message::BorrowedMessage,
    ClientConfig, ClientContext, Message,
};
use serde::Deserialize;

use producer::error::ReplicationError;

pub struct KafkaConsumer<T: Handler> {
    consumer: Arc<StreamConsumer<KafkaConsumerContext<T>>>,
    handler: Arc<T>,
}

struct KafkaConsumerContext<T: Handler> {
    handler: Arc<T>,
}

#[async_trait]
pub trait Handler: Sync + Send + 'static {
    type Payload: for<'de> Deserialize<'de> + Send;

    async fn handle_message(
        &self,
        message: &HandlerMessage<'_, Self::Payload>,
    ) -> Result<(), ReplicationError>;

    fn assign_partitions(&self, _: Vec<(&str, i32)>) {}
    fn revoke_partitions(&self, _: Vec<(&str, i32)>) {}
}

pub struct HandlerMessage<'a, T> {
    pub topic: &'a str,
    pub partition: i32,
    pub payload: T,
}

impl<T: Handler> ClientContext for KafkaConsumerContext<T> {}

impl<T: Handler> ConsumerContext for KafkaConsumerContext<T> {
    fn post_rebalance<'a>(&self, rebalance: &rdkafka::consumer::Rebalance<'a>) {
        match rebalance {
            Rebalance::Assign(assignments) => {
                let ass = assignments.elements();
                let ass = ass
                    .iter()
                    .map(|e| (e.topic(), e.partition()))
                    .collect::<Vec<_>>();

                self.handler.assign_partitions(ass);
            }
            Rebalance::Revoke(revocations) => {
                let rev = revocations.elements();
                let rev = rev
                    .iter()
                    .map(|e| (e.topic(), e.partition()))
                    .collect::<Vec<_>>();

                self.handler.revoke_partitions(rev);
            }
            Rebalance::Error(err) => {
                tracing::warn!("kafka rebalancing error: {}", err)
            }
        }
    }
}

impl<T: Handler> KafkaConsumer<T> {
    pub fn new(
        group_id: &str,
        brokers: &str,
        topics: &Vec<String>,
        handler: T,
    ) -> Result<Self, ReplicationError> {
        let handler = Arc::new(handler);

        let context = KafkaConsumerContext {
            handler: handler.clone(),
        };

        let consumer: StreamConsumer<KafkaConsumerContext<T>> = ClientConfig::new()
            .set("group.id", group_id)
            .set("bootstrap.servers", brokers)
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            // Commit automatically every 5 seconds
            .set("enable.auto.commit", "true")
            .set("auto.commit.interval.ms", "5000")
            // but only commit the offsets explicitly stored via `consumer.store_offset`.
            .set("enable.auto.offset.store", "false")
            .set_log_level(RDKafkaLogLevel::Debug)
            .create_with_context(context)?;

        let topics = topics
            .iter()
            .map(|topic| topic.as_str())
            .collect::<Vec<_>>();

        consumer.subscribe(&topics)?;

        let consumer = Arc::new(consumer);

        Ok(Self { consumer, handler })
    }

    pub async fn consume(&self) -> Result<(), ReplicationError> {
        loop {
            tracing::info!("polling for messages");
            match self.consumer.recv().await {
                Ok(msg) => self.handle_message(msg).await?,
                Err(err) => {
                    // potentially fatal error
                    tracing::error!("Potentially fatal kafka error: {}", err);

                    return Err(ReplicationError::Fatal(anyhow!(
                        "Potentially fatal kafka error"
                    )));
                }
            }
        }
    }

    async fn handle_message(&self, msg: BorrowedMessage<'_>) -> Result<(), ReplicationError> {
        let payload = self.extract_payload(&msg)?;

        let message = HandlerMessage {
            topic: msg.topic(),
            partition: msg.partition(),
            payload,
        };

        loop {
            match self.handler.handle_message(&message).await {
                Ok(()) => {
                    self.consumer.store_offset_from_message(&msg).map_err(|err| {
                        tracing::warn!("unable to set offset in store, this is a recoverable error, will retry: {}", err);
                        ReplicationError::Recoverable(anyhow!("Failed to set offset in store, this is a recoverable error: {}", err))
                    })?;

                    break;
                }
                Err(ReplicationError::Recoverable(err)) => {
                    tracing::warn!(
                        "Handler reported a recoverable error: {}, we will simply retry the message after a delay",
                        err
                    );

                    // this will block the current task
                    tokio::time::sleep(Duration::from_secs(3)).await;
                }
                Err(ReplicationError::Fatal(err)) => {
                    // an unrecoverable error has occurred downstream
                    // this implies either there is a bug in the downstream application
                    // or the producer of the message is invalid. Either case warrants a sev_1 investigation
                    tracing::error!("Handler reported an unrecoverable error in processing the message, will not process any more messages from this partition, {}", err);
                    return Err(ReplicationError::Fatal(anyhow!("Handler reported an unrecoverable error in processing the message, will not process any more messages from this partition, {}", err)));
                }
            }
        }

        Ok(())
    }

    fn extract_payload(&self, msg: &BorrowedMessage) -> Result<T::Payload, ReplicationError> {
        match msg.payload_view::<str>() {
            Some(Ok(payload)) => serde_json::from_str(payload).map_err(|_| {
                tracing::error!("Unable to deserialize payload into expected handler payload type");
                ReplicationError::Fatal(anyhow!(
                    "Unable to deserialize payload into expected handler payload type"
                ))
            }),
            Some(Err(err)) => {
                tracing::error!(
                    "Failed to parse message into intermediate utf8. This is a fatal error: {}",
                    err
                );
                return Err(ReplicationError::Fatal(anyhow!("Invalid payload")));
            }
            None => {
                tracing::error!("No payload received. This is a fatal error");
                return Err(ReplicationError::Fatal(anyhow!("Invalid payload")));
            }
        }
    }
}
