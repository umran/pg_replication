use producer::{KafkaProducer, KafkaProducerMessage};

const BROKERS: &'static str = "localhost:9092";

#[tokio::test]
async fn it_produces_messages() {
    let n: u64 = 1000;

    let producer = KafkaProducer::new(BROKERS.into());
    let (tx, mut rx) = producer.produce().unwrap();

    let payload: String = std::iter::repeat('c').take(144).collect();

    // clone tx to ensure the sender is not dropped when the
    // messages are all buffered and the task exits.
    let tx_clone = tx.clone();
    tokio::spawn(async move {
        tracing::info!("queueing {} 144 byte payloads", n);

        for offset in 1..=n {
            tx_clone
                .send(KafkaProducerMessage {
                    topic: "test".into(),
                    partition_key: "some_key".into(),
                    offset,
                    payload: payload.clone(),
                })
                .await
                .unwrap();
        }
    });

    let mut delivered = 0;

    while let Some(_) = rx.recv().await {
        delivered += 1;
        if delivered == n {
            break;
        }
    }

    assert_eq!(delivered, n)
}
