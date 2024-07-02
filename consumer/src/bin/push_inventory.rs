use model::{Crud, Model};
use serde::{Deserialize, Serialize};
use sqlx::{prelude::FromRow, PgPool};
use uuid::Uuid;

#[derive(Clone, Serialize, Deserialize, FromRow, Model)]
#[model(table_name = "inventory")]
struct Inventory {
    #[model(id, primary_key)]
    id: Uuid,
    organization_id: Uuid,
}

#[tokio::main]
async fn main() {
    push_inventory().await
}

async fn push_inventory() {
    let pool = PgPool::connect("postgresql://kanko:kanko@localhost:5432/kanko")
        .await
        .unwrap();

    let mut conn = pool.acquire().await.unwrap();

    let inventory = Inventory {
        id: Uuid::new_v4(),
        organization_id: Uuid::max(),
    };

    Inventory::create(&inventory)
        .execute(&mut conn)
        .await
        .unwrap();
}
