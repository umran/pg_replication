// use anyhow::anyhow;
use async_trait::async_trait;
use consumer::{Consumer, IdempotentApplication};
use producer::{error::ReplicationError, ReplicationOp};
use sqlx::{Postgres, Transaction};

struct LoggerApp {}

#[async_trait]
impl IdempotentApplication for LoggerApp {
    async fn handle_message(
        &self,
        _tx: &mut Transaction<'_, Postgres>,
        op: &ReplicationOp,
    ) -> Result<(), ReplicationError> {
        tracing::info!("handling message");
        tracing::info!("{:?}", op);

        // Err(ReplicationError::Recoverable(anyhow!(
        //     "HIIII I JUST WANT TO PROCESS THIS MESSAGE AGAIN AND AGAIN AND AGAIN"
        // )))

        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let subscriber = tracing_subscriber::FmtSubscriber::new();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    start_idempotent_consumer().await
}

async fn start_idempotent_consumer() {
    let app = LoggerApp {};
    let consumer = Consumer::new("bravo", vec!["inventory"], "localhost:9092", app);

    tracing::info!("starting idempotent consumer");
    consumer
        .start_idempotent("postgresql://kanko:kanko@localhost:5432/kanko")
        .await
        .unwrap();
}
