use std::collections::HashMap;
use std::thread;
use std::time::Duration;

use async_trait::async_trait;
use rdkafka::error::KafkaError;
use rdkafka::message::Header;
use rdkafka::message::OwnedHeaders;
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
use crate::util::TableInfo;
use crate::Op;
use crate::ReplicationOp;
use crate::ReplicationProducer;

#[derive(Clone)]
pub struct KafkaProducer {
    brokers: String,
}

#[derive(Clone)]
struct KafkaProducerContext {
    committed_lsn_tx: Sender<u64>,
}

impl ClientContext for KafkaProducerContext {}

impl ProducerContext for KafkaProducerContext {
    type DeliveryOpaque = Box<ReplicationOp>;

    fn delivery(
        &self,
        delivery_result: &rdkafka::message::DeliveryResult<'_>,
        delivery_opaque: Self::DeliveryOpaque,
    ) {
        if let Ok(_) = delivery_result {
            self.committed_lsn_tx
                .blocking_send(delivery_opaque.prev_lsn)
                .unwrap();
        }
    }
}

impl KafkaProducer {
    pub async fn new(brokers: &str) -> Result<Self, ReplicationError> {
        Ok(Self {
            brokers: brokers.into(),
        })
    }
}

#[async_trait]
impl ReplicationProducer for KafkaProducer {
    fn produce(
        &self,
        publication_tables: HashMap<u32, TableInfo>,
    ) -> (Sender<ReplicationOp>, Receiver<u64>) {
        let (msg_tx, mut msg_rx): (Sender<ReplicationOp>, Receiver<ReplicationOp>) =
            mpsc::channel(1);

        let (committed_lsn_tx, committed_lsn_rx) = mpsc::channel(1);

        let context = KafkaProducerContext { committed_lsn_tx };

        let producer: ThreadedProducer<_> = ClientConfig::new()
            .set("bootstrap.servers", &self.brokers)
            .set("message.timeout.ms", "5000")
            .create_with_context(context)
            .unwrap();

        tokio::task::spawn_blocking(move || {
            while let Some(msg) = msg_rx.blocking_recv() {
                let table_info = publication_tables
                    .get(&msg.rel_id)
                    .expect("table with requested oid not present in publication");

                let topic_name = format!("{}.{}", table_info.namespace, table_info.name);

                let primary_key = match &msg.op {
                    Op::Insert(row) => table_info.extract_primary_key(row).unwrap(),
                    Op::Update((row, _)) => table_info.extract_primary_key(row).unwrap(),
                    Op::Delete(row) => table_info.extract_primary_key(row).unwrap(),
                }
                .join("");

                let idempotency_key = format!("{}-{}", msg.lsn, msg.seq_id);

                let headers = OwnedHeaders::new().insert(Header {
                    key: "idempotency_key",
                    value: Some(idempotency_key.as_bytes()),
                });

                let payload = serde_json::to_string(&msg.op).unwrap();

                let mut record = BaseRecord::with_opaque_to(topic_name.as_str(), Box::new(msg))
                    .key(&primary_key)
                    .headers(headers)
                    .payload(&payload);

                loop {
                    match producer.send(record) {
                        Ok(()) => break,
                        Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), rec)) => {
                            // Retry after 500ms
                            record = rec;
                            thread::sleep(Duration::from_millis(500));
                        }
                        Err((e, _)) => {
                            panic!("Failed to publish on kafka {:?}", e);
                        }
                    }
                }
            }
        });

        (msg_tx, committed_lsn_rx)
    }
}
