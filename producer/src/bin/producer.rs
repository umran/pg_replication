use std::collections::HashMap;

use producer::{Producer, TopicInfo};

#[tokio::main]
async fn main() {
    let subscriber = tracing_subscriber::FmtSubscriber::new();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    start_producer().await
}

async fn start_producer() {
    let topic_map = serde_json::json!({
        "inventory": {
            "name": "inventory",
            "partition_key": ["organization_id"]
        },
        "room_type": {
            "name": "inventory",
            "partition_key": ["organization_id"]
        },
        "rate_plan": {
            "name": "inventory",
            "partition_key": ["organization_id"]
        }
    });

    let topic_map: HashMap<String, TopicInfo> = serde_json::from_value(topic_map).unwrap();

    let producer = Producer::new(
        "postgresql://kanko:kanko@localhost:5432/kanko".into(),
        "localhost:9092".into(),
        "kanko".into(),
        "kanko".into(),
        topic_map,
    );

    tracing::info!("starting producer");
    producer.start().await.unwrap()
}
