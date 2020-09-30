//! NATS implementation of the `MessageBus` connecting mayastor to the control
//! plane components.

use super::{Channel, MessageBus};
use async_trait::async_trait;
use nats::asynk::Connection;
use once_cell::sync::OnceCell;
use serde::Serialize;
use smol::io;

pub(super) static NATS_MSG_BUS: OnceCell<NatsMessageBus> = OnceCell::new();
pub(super) fn message_bus_init(server: String) {
    NATS_MSG_BUS.get_or_init(|| {
        // Waits for the message bus to become ready
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async { NatsMessageBus::new(&server).await })
    });
}

// Would we want to have both sync and async clients?
pub struct NatsMessageBus {
    connection: Connection,
}
impl NatsMessageBus {
    pub async fn connect(server: &str) -> Connection {
        info!("Connecting to the nats server {}...", server);
        // We retry in a loop until successful. Once connected the nats
        // library will handle reconnections for us.
        let interval = std::time::Duration::from_millis(500);
        let mut log_error = true;
        loop {
            match nats::asynk::connect(server).await {
                Ok(connection) => {
                    info!(
                        "Successfully connected to the nats server {}",
                        server
                    );
                    return connection;
                }
                Err(error) => {
                    if log_error {
                        warn!(
                            "Error connection: {}. Quietly retrying...",
                            error
                        );
                        log_error = false;
                    }
                    smol::Timer::after(interval).await;
                    continue;
                }
            }
        }
    }

    async fn new(server: &str) -> Self {
        Self {
            connection: Self::connect(server).await,
        }
    }
}

#[async_trait]
impl MessageBus for NatsMessageBus {
    async fn publish(
        &self,
        channel: Channel,
        message: impl Serialize
            + std::marker::Send
            + std::marker::Sync
            + 'async_trait,
    ) -> std::io::Result<()> {
        let payload = serde_json::to_vec(&message)?;
        self.connection
            .publish(&channel.to_string(), &payload)
            .await
    }

    async fn send(
        &self,
        _channel: Channel,
        _message: impl Serialize
            + std::marker::Send
            + std::marker::Sync
            + 'async_trait,
    ) -> Result<(), ()> {
        unimplemented!()
    }

    async fn request(
        &self,
        _channel: Channel,
        _message: impl Serialize
            + std::marker::Send
            + std::marker::Sync
            + 'async_trait,
    ) -> Result<Vec<u8>, ()> {
        unimplemented!()
    }

    async fn flush(&self) -> io::Result<()> {
        self.connection.flush().await
    }
}
