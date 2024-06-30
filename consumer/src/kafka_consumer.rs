use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use rdkafka::{
    config::RDKafkaLogLevel,
    consumer::{Consumer, ConsumerContext, Rebalance, StreamConsumer},
    ClientConfig, ClientContext, Message,
};
use serde::Deserialize;

use producer::error::ReplicationError;

pub struct KafkaConsumer {
    /// the id of the consumer group to which this consumer belongs
    pub group_id: String,
    /// the kafka brokers to connect to
    pub brokers: String,
    /// the kafka topics this consumer should subscribe to
    pub topics: Vec<String>,
}

struct KafkaConsumerContext<T: Handler> {
    handler: Arc<T>,
}

#[async_trait]
pub trait Handler: Sync + Send + 'static {
    type Payload: for<'de> Deserialize<'de> + Send;

    async fn handle_message(
        &self,
        message: HandlerMessage<'_, Self::Payload>,
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

impl KafkaConsumer {
    pub fn new(group_id: String, brokers: String, topics: Vec<String>) -> Self {
        Self {
            group_id,
            brokers,
            topics,
        }
    }

    pub async fn consume<T: Handler>(&self, handler: T) -> Result<(), ReplicationError> {
        let handler = Arc::new(handler);

        let context = KafkaConsumerContext {
            handler: handler.clone(),
        };

        let consumer: StreamConsumer<KafkaConsumerContext<T>> = ClientConfig::new()
            .set("group.id", &self.group_id)
            .set("bootstrap.servers", &self.brokers)
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "false")
            .set_log_level(RDKafkaLogLevel::Debug)
            .create_with_context(context)?;

        let topics: &[&str] = &self
            .topics
            .iter()
            .map(|topic| topic.as_str())
            .collect::<Vec<_>>();

        consumer.subscribe(topics)?;

        loop {
            match consumer.recv().await {
                Ok(msg) => {
                    match msg.payload_view::<str>() {
                        Some(Ok(payload)) => {
                            let payload: T::Payload = serde_json::from_str(payload).map_err(|_| {
                                tracing::error!("Unable to deserialize payload into expected handler payload type");
                                ReplicationError::Fatal(anyhow!(
                                    "Unable to deserialize payload into expected handler payload type"
                                ))
                            })?;

                            let message = HandlerMessage {
                                topic: msg.topic(),
                                partition: msg.partition(),
                                payload,
                            };

                            match handler.handle_message(message).await {
                                Ok(()) => consumer
                                    .commit_message(&msg, rdkafka::consumer::CommitMode::Async)?,
                                Err(ReplicationError::Recoverable(err)) => {
                                    tracing::warn!("Handler reported a recoverable error: {}", err);
                                }
                                Err(ReplicationError::Fatal(err)) => {
                                    // an unrecoverable error has occurred downstream
                                    // this implies either there is a bug in the downstream application
                                    // or the message is invalid. Either case warrants a sev_1 investigation

                                    tracing::error!("Handler reported an unrecoverable error in processing the message, will not process any more messages from this partition, {}", err);
                                    return Err(ReplicationError::Fatal(anyhow!("Handler reported an unrecoverable error in processing the message, will not process any more messages from this partition, {}", err)));
                                }
                            }
                        }
                        _ => {
                            tracing::error!(
                                "Invalid payload received. Topic considered corrupted, will exit"
                            );

                            return Err(ReplicationError::Fatal(anyhow!("Invalid payload")));
                        }
                    }
                }
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
}
