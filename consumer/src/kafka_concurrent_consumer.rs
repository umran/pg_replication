use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::anyhow;
use rdkafka::{
    config::RDKafkaLogLevel,
    consumer::{Consumer, ConsumerContext, Rebalance, StreamConsumer},
    message::BorrowedMessage,
    ClientConfig, ClientContext, Message,
};

use producer::error::ReplicationError;
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    RwLock,
};

use crate::kafka_consumer::{Handler, HandlerMessage};

pub struct KafkaConcurrentConsumer<T: Handler> {
    consumer: Arc<StreamConsumer<KafkaConcurrentConsumerContext<T>>>,
    partition_senders: Arc<RwLock<HashMap<(String, i32), Sender<(T::Payload, i64)>>>>,
    offset_rx: Receiver<(String, i32, i64)>,
}

struct KafkaConcurrentConsumerContext<T: Handler> {
    handler: Arc<T>,
    partition_senders: Arc<RwLock<HashMap<(String, i32), Sender<(T::Payload, i64)>>>>,
    offset_tx: Sender<(String, i32, i64)>,
}

impl<T: Handler> ClientContext for KafkaConcurrentConsumerContext<T> {}

impl<T: Handler> ConsumerContext for KafkaConcurrentConsumerContext<T> {
    fn post_rebalance<'a>(&self, rebalance: &rdkafka::consumer::Rebalance<'a>) {
        match rebalance {
            Rebalance::Assign(assignments) => {
                if assignments.capacity() == 0 {
                    return;
                }

                let ass = assignments.elements();
                let ass = ass
                    .iter()
                    .map(|e| (e.topic(), e.partition()))
                    .collect::<Vec<_>>();

                self.handler.assign_partitions(ass);

                // spawn new tasks to handle the assigned partitions and put the tx channels in partition_senders
                let handler = self.handler.clone();
                let offset_tx = self.offset_tx.clone();
                let partition_senders = self.partition_senders.clone();

                tokio::task::block_in_place(move || {
                    let mut partition_senders = partition_senders.blocking_write();
                    for ass in assignments.elements().iter() {
                        let (payload_tx, payload_rx) = mpsc::channel(1);

                        handle_partition_messages(
                            ass.topic().to_string(),
                            ass.partition(),
                            handler.clone(),
                            payload_rx,
                            offset_tx.clone(),
                        );
                        partition_senders
                            .insert((ass.topic().to_string(), ass.partition()), payload_tx);
                    }
                });
            }
            Rebalance::Revoke(revocations) => {
                if revocations.capacity() == 0 {
                    return;
                }

                let rev = revocations.elements();
                let rev = rev
                    .iter()
                    .map(|e| (e.topic(), e.partition()))
                    .collect::<Vec<_>>();

                self.handler.revoke_partitions(rev);

                // remove the partition_senders for the revoked partitions
                let partition_senders = self.partition_senders.clone();
                tokio::task::block_in_place(move || {
                    let mut partition_senders = partition_senders.blocking_write();
                    for rev in revocations.elements().iter() {
                        partition_senders.remove(&(rev.topic().to_string(), rev.partition()));
                    }
                });
            }
            Rebalance::Error(err) => {
                tracing::warn!("kafka rebalancing error: {}", err)
            }
        }
    }
}

impl<T: Handler> KafkaConcurrentConsumer<T> {
    pub fn new(
        group_id: &str,
        brokers: &str,
        topics: &Vec<String>,
        handler: T,
    ) -> Result<Self, ReplicationError> {
        let handler = Arc::new(handler);
        let partition_senders = Arc::new(RwLock::new(HashMap::new()));
        let (offset_tx, offset_rx) = mpsc::channel(1);

        let context = KafkaConcurrentConsumerContext {
            handler: handler.clone(),
            partition_senders: partition_senders.clone(),
            offset_tx,
        };

        let consumer: StreamConsumer<KafkaConcurrentConsumerContext<T>> = ClientConfig::new()
            .set("group.id", group_id)
            // .set("auto.offset.reset", "latest")
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

        Ok(Self {
            consumer,
            partition_senders,
            offset_rx,
        })
    }

    pub async fn consume(&mut self) -> Result<(), ReplicationError> {
        loop {
            tracing::info!("polling for messages or storing offsets");
            tokio::select! {
                res = self.consumer.recv() => match res {
                    Ok(msg) => self.route_message(&msg).await?,
                    Err(err) => {
                        tracing::error!("Fatal kafka error: {}", err);

                        return Err(ReplicationError::Fatal(anyhow!("Fatal kafka error: {}", err)));
                    }
                },
                Some((topic, partition, offset)) = self.offset_rx.recv() => {
                    self.consumer.store_offset(&topic, partition, offset).map_err(|err| {
                        tracing::warn!("unable to set offset in store, this is a recoverable error, will retry: {}", err);
                        ReplicationError::Recoverable(anyhow!("Failed to set offset in store, this is a recoverable error: {}", err))
                    })?;
                }
            }
        }
    }

    async fn route_message(&self, msg: &BorrowedMessage<'_>) -> Result<(), ReplicationError> {
        let payload = self.extract_payload(msg)?;

        // determine the partition to send the message to
        let partition_senders = self.partition_senders.read().await;
        let tx = partition_senders.get(&(msg.topic().to_string(), msg.partition())).ok_or_else(|| {
            tracing::warn!("received message for partition that is not assigned to this consumer, will consider this a recoverable error");
            ReplicationError::Recoverable(anyhow!("received message for unassigned partition"))
        })?;

        tx.send((payload, msg.offset())).await.map_err(|err| {
            tracing::error!("send to partition handler channel failed. this is because the receiver has been closed by the handler, which would have been due to a fatal error downstream");
            ReplicationError::Fatal(anyhow!("send failed due to a closed partion handler channel: {}", err))
        })?;

        Ok(())
    }

    fn extract_payload(&self, msg: &BorrowedMessage) -> Result<T::Payload, ReplicationError> {
        match msg.payload_view::<str>() {
            Some(Ok(payload)) => {
                tracing::info!("received the following payload");
                tracing::info!("{}", payload);

                serde_json::from_str(payload).map_err(|_| {
                    tracing::error!(
                        "Unable to deserialize payload into expected handler payload type"
                    );
                    ReplicationError::Fatal(anyhow!(
                        "Unable to deserialize payload into expected handler payload type"
                    ))
                })
            }
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

fn handle_partition_messages<T: Handler>(
    topic: String,
    partition: i32,
    handler: Arc<T>,
    mut payload_rx: Receiver<(T::Payload, i64)>,
    offset_tx: Sender<(String, i32, i64)>,
) {
    tokio::spawn(async move {
        'outer: while let Some((payload, offset)) = payload_rx.recv().await {
            let message = HandlerMessage {
                topic: &topic,
                partition,
                payload,
            };

            loop {
                match handler.handle_message(&message).await {
                    Ok(()) => {
                        // we don't care about the result of this send because it can only fail if the main task has exited
                        let _ = offset_tx.send((topic.to_string(), partition, offset));

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
                        payload_rx.close();

                        break 'outer;
                    }
                }
            }
        }
    });
}
