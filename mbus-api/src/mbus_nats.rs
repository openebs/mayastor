use super::*;
use log::{info, warn};
use nats::asynk::Connection;
use once_cell::sync::OnceCell;
use smol::io;

static NATS_MSG_BUS: OnceCell<NatsMessageBus> = OnceCell::new();
/// Initialise the Nats Message Bus with the current tokio runtime
/// (the runtime MUST be setup already or we will panic)
pub fn message_bus_init_tokio(server: String) {
    NATS_MSG_BUS.get_or_init(|| {
        // Waits for the message bus to become ready
        tokio::runtime::Handle::current().block_on(async {
            NatsMessageBus::new(&server, BusOptions::new()).await
        })
    });
}
/// Initialise the Nats Message Bus
pub async fn message_bus_init(server: String) {
    let nc = NatsMessageBus::new(&server, BusOptions::new()).await;
    NATS_MSG_BUS
        .set(nc)
        .ok()
        .expect("Expect to be initialised only once");
}

/// Get the static `NatsMessageBus` as a boxed `MessageBus`
pub fn bus() -> DynBus {
    Box::new(
        NATS_MSG_BUS
            .get()
            .expect("Should be initialised before use")
            .clone(),
    )
}

// Would we want to have both sync and async clients?
#[derive(Clone)]
struct NatsMessageBus {
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
            match BusOptions::new()
                .max_reconnects(None)
                .connect_async(server)
                .await
            {
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

    async fn new(server: &str, _options: BusOptions) -> Self {
        Self {
            connection: Self::connect(server).await,
        }
    }
}

#[async_trait]
impl Bus for NatsMessageBus {
    async fn publish(
        &self,
        channel: Channel,
        message: &[u8],
    ) -> std::io::Result<()> {
        self.connection.publish(&channel.to_string(), message).await
    }

    async fn send(&self, _channel: Channel, _message: &[u8]) -> io::Result<()> {
        unimplemented!()
    }

    async fn request(
        &self,
        channel: Channel,
        message: &[u8],
    ) -> io::Result<BusMessage> {
        self.connection.request(&channel.to_string(), message).await
    }

    async fn flush(&self) -> io::Result<()> {
        self.connection.flush().await
    }

    async fn subscribe(&self, channel: Channel) -> io::Result<BusSubscription> {
        self.connection.subscribe(&channel.to_string()).await
    }
}
