use std::thread;
use std::time::Duration;

use rdkafka::error::KafkaError;
use rdkafka::producer::BaseRecord;
use rdkafka::producer::ProducerContext;
use rdkafka::producer::ThreadedProducer;
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::ClientConfig;
use rdkafka::ClientContext;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;

use crate::error::ReplicationError;

#[derive(Clone)]
pub struct KafkaProducer {
    brokers: String,
}

#[derive(Clone)]
struct KafkaProducerContext {
    committed_lsn_tx: Sender<u64>,
}

#[derive(Debug)]
pub struct KafkaProducerMessage {
    pub topic: String,
    pub partition_key: String,
    pub prev_lsn: u64,
    pub payload: String,
}

impl ClientContext for KafkaProducerContext {}

impl ProducerContext for KafkaProducerContext {
    type DeliveryOpaque = Box<u64>;

    fn delivery(
        &self,
        delivery_result: &rdkafka::message::DeliveryResult<'_>,
        delivery_opaque: Self::DeliveryOpaque,
    ) {
        match delivery_result {
            Ok(_) => {
                // We don't care if the send fails here as it can only fail if the receiver has been dropped.
                // If the receiver has been dropped it means the main task has exited
                let _ = self.committed_lsn_tx.blocking_send(*delivery_opaque);
            }
            _ => {
                // Since we set enable.gapless.guarantee to true, this case would ensure that attempting to process
                // any messages queued after the failed message will result in a fatal error. Therefore, we can
                // ignore this error and simply wait for a fatal error, which is handled in the main producer loop

                tracing::error!(
                    "message delivery failed for message with prev_lsn = {}",
                    *delivery_opaque
                );
            }
        }
    }
}

impl KafkaProducer {
    pub fn new(brokers: String) -> Self {
        Self { brokers }
    }

    pub fn produce(
        &self,
    ) -> Result<(Sender<KafkaProducerMessage>, Receiver<u64>), ReplicationError> {
        let (msg_tx, mut msg_rx): (Sender<KafkaProducerMessage>, Receiver<KafkaProducerMessage>) =
            mpsc::channel(1);

        let (committed_lsn_tx, committed_lsn_rx) = mpsc::channel(1);

        let context = KafkaProducerContext { committed_lsn_tx };

        let producer: ThreadedProducer<_> = ClientConfig::new()
            .set("bootstrap.servers", &self.brokers)
            .set("delivery.timeout.ms", "0")
            .set("retries", "2147483647")
            .set("max.in.flight.requests.per.connection", "5")
            .set("enable.idempotence", "true")
            .set("enable.gapless.guarantee", "true")
            .set("acks", "all")
            .set("linger.ms", "5")
            .set("compression.type", "lz4")
            .create_with_context(context)?;

        tokio::task::spawn_blocking(move || {
            'outer: while let Some(msg) = msg_rx.blocking_recv() {
                let mut record = BaseRecord::with_opaque_to(&msg.topic, Box::new(msg.prev_lsn))
                    .key(&msg.partition_key)
                    .payload(&msg.payload);

                loop {
                    match producer.send(record) {
                        Ok(()) => {
                            break;
                        }
                        Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), rec)) => {
                            tracing::warn!("Send queue full, will retry");

                            record = rec;
                            thread::sleep(Duration::from_millis(500));
                        }
                        Err((e, _)) => {
                            tracing::error!(
                                "Potentially unrecoverable kafka error encountered, will not attempt to produce new messages"
                            );

                            tracing::error!("{:?}", e);

                            // breaking 'outer will cause the current task to exit, causing
                            // the receiver end of the message channel to be dropped,
                            // causing future sends to the message channel from the main task to fail
                            // at which point a recoverable error will be emitted, which will cause
                            // the programme to restart
                            break 'outer;
                        }
                    }
                }
            }
        });

        Ok((msg_tx, committed_lsn_rx))
    }
}
