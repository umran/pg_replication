use std::sync::Arc;

use async_trait::async_trait;
use producer::{error::ReplicationError, ReplicationOp};

use crate::{
    kafka_consumer::{Handler, HandlerMessage},
    BasicApplication,
};

pub struct BasicConsumer<App: BasicApplication> {
    app: Arc<App>,
}

impl<App: BasicApplication> BasicConsumer<App> {
    pub fn new(app: Arc<App>) -> Self {
        Self { app }
    }
}

#[async_trait]
impl<App: BasicApplication> Handler for BasicConsumer<App> {
    type Payload = ReplicationOp;

    async fn handle_message(
        &self,
        message: &HandlerMessage<'_, Self::Payload>,
    ) -> Result<(), ReplicationError> {
        tracing::info!("handling basic consumer message");
        self.app.handle_message(&message.payload).await
    }
}
