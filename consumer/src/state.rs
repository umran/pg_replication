use model::Model;
use serde::{Deserialize, Serialize};
use sqlx::prelude::FromRow;
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize, FromRow, Model)]
#[model(table_name = "replication_consumer_state")]
pub struct State {
    #[model(id)]
    pub id: Uuid,
    #[model(primary_key)]
    pub group_id: String,
    #[model(primary_key)]
    pub topic: String,
    #[model(primary_key)]
    pub partition: i32,
    pub lsn: String,
    pub seq_id: String,
}

impl State {
    pub fn new(group_id: &str, topic: &str, partition: i32, lsn: String, seq_id: String) -> Self {
        Self {
            id: Uuid::new_v4(),
            group_id: group_id.to_string(),
            topic: topic.to_string(),
            partition,
            lsn,
            seq_id,
        }
    }
}
