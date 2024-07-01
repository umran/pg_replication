use async_trait::async_trait;
use consumer::{BasicApplication, Consumer};
use producer::{error::ReplicationError, ReplicationOp};

struct BasicApp {}

#[async_trait]
impl BasicApplication for BasicApp {
    async fn handle_message(&self, op: &ReplicationOp) -> Result<(), ReplicationError> {
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

    start_basic_consumer().await
}

async fn start_basic_consumer() {
    let app = BasicApp {};
    let consumer = Consumer::new("alpha", vec!["inventory"], "localhost:9092", app);

    tracing::info!("starting basic consumer");
    consumer.start_basic().await.unwrap();
}
