//! NATS message bus connecting mayastor to control plane (moac).
//!
//! It is designed to make sending events to control plane easy in the future.
//! That's the reason for global sender protected by the mutex, that normally
//! would not be needed and currently is used only to terminate the message bus.

use std::{
    env,
    io::Error as IoError,
    net::SocketAddr,
    str::FromStr,
    sync::Mutex,
    time::Duration,
};

use futures::{channel::mpsc, select, FutureExt, StreamExt};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use tokio::{net::lookup_host, time::delay_for};
use tokio_nats::{
    connect,
    Error as TokioNatsError,
    NatsClient,
    NatsConfigBuilder,
};

/// Mayastor sends registration messages in this interval (kind of heart-beat)
const HB_INTERVAL: u64 = 10;

/// The end of channel used to send messages to or terminate the NATS client.
static SENDER: Lazy<Mutex<Option<mpsc::Sender<()>>>> =
    Lazy::new(|| Mutex::new(None));

/// Errors for pool operations.
///
/// Note: The types here that would be normally used as source for snafu errors
/// do not implement Error trait required by Snafu. So they are renamed to
/// "cause" attribute and we use .map_err() instead of .context() when creating
/// them.
#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display(
        "Failed to resolve NATS server '{}': {}",
        server,
        source
    ))]
    ResolveServer { source: IoError, server: String },
    #[snafu(display("Failed to resolve NATS server '{}'", server))]
    ResolveServerEmpty { server: String },
    #[snafu(display(
        "Failed to build NATS client for '{}': {:?}",
        server,
        cause
    ))]
    BuildClient { cause: String, server: String },
    #[snafu(display(
        "Failed to connect to the NATS server {}: {:?}",
        server,
        cause
    ))]
    ConnectFailed {
        cause: TokioNatsError,
        server: String,
    },
    #[snafu(display(
        "Cannot issue requests if message bus hasn't been started"
    ))]
    NotStarted {},
    #[snafu(display("Failed to queue register request: {:?}", cause))]
    QueueRegister { cause: TokioNatsError },
    #[snafu(display("Failed to queue deregister request: {:?}", cause))]
    QueueDeregister { cause: TokioNatsError },
}

/// Register message payload
#[derive(Serialize, Deserialize, Debug)]
struct RegisterArgs {
    id: String,
    #[serde(rename = "grpcEndpoint")]
    grpc_endpoint: String,
}

/// Deregister message payload
#[derive(Serialize, Deserialize, Debug)]
struct DeregisterArgs {
    id: String,
}

/// Resolve a hostname or return an error.
async fn resolve(name: &str) -> Result<String, Error> {
    let mut ips = lookup_host(name).await.context(ResolveServer {
        server: name.to_owned(),
    })?;
    match ips.next() {
        None => Err(Error::ResolveServerEmpty {
            server: name.to_owned(),
        }),
        Some(ip) => Ok(ip.to_string()),
    }
}

/// Message bus implementation
struct MessageBus {
    /// NATS server endpoint
    server: String,
    /// Name of the node that mayastor is running on
    node: String,
    /// gRPC endpoint of the server provided by mayastor
    grpc_endpoint: String,
    /// NATS client
    client: Option<NatsClient>,
    /// heartbeat interval (how often the register message is sent)
    hb_interval: Duration,
}

impl MessageBus {
    /// Create message bus object with given parameters.
    pub fn new(server: &str, node: &str, grpc_endpoint: &str) -> Self {
        Self {
            server: server.to_owned(),
            node: node.to_owned(),
            grpc_endpoint: grpc_endpoint.to_owned(),
            client: None,
            hb_interval: Duration::from_secs(
                match env::var("MAYASTOR_HB_INTERVAL") {
                    Ok(val) => match val.parse::<u64>() {
                        Ok(num) => num,
                        Err(_) => HB_INTERVAL,
                    },
                    Err(_) => HB_INTERVAL,
                },
            ),
        }
    }

    /// Connect to the server and start emitting periodic register messages.
    /// Runs until the sender side of mpsc channel is closed.
    pub async fn run(
        &mut self,
        mut receiver: mpsc::Receiver<()>,
    ) -> Result<(), Error> {
        assert!(self.client.is_none());

        // We retry connect in loop until successful. Once connected the nats
        // library will handle reconnections for us.
        while self.client.is_none() {
            self.client = match self.connect().await {
                Ok(client) => Some(client),
                Err(err) => {
                    error!("{}", err);
                    delay_for(self.hb_interval).await;
                    continue;
                }
            };
        }
        info!("Connected to the NATS server {}", self.server);

        info!(
            "Registering '{}' and grpc server {} ...",
            self.node, self.grpc_endpoint
        );
        loop {
            if let Err(err) = self.register().await {
                error!("Registration failed: {:?}", err);
            };
            let _res = select! {
                () = delay_for(self.hb_interval).fuse() => (),
                msg = receiver.next() => {
                    match msg {
                        Some(_) => warn!("Messages have not been implemented yet"),
                        None => {
                            info!("Terminating the NATS client");
                            break;
                        }
                    }
                }
            };
        }

        if let Err(err) = self.deregister().await {
            error!("Deregistration failed: {:?}", err);
        };
        Ok(())
    }

    /// Try to connect to the NATS server including DNS resolution step if
    /// needed.
    async fn connect(&self) -> Result<NatsClient, Error> {
        // Resolve the hostname of the server - nats lib won't do that for us
        let server_ip = match SocketAddr::from_str(&self.server) {
            Ok(_) => self.server.clone(),
            Err(_) => resolve(&self.server).await?,
        };

        debug!("Connecting to the message bus");
        let config = NatsConfigBuilder::default()
            .reconnection_period(self.hb_interval)
            .server(&server_ip)
            .build()
            .map_err(|cause| Error::BuildClient {
                server: self.server.clone(),
                cause,
            })?;

        connect(config).await.map_err(|err| Error::ConnectFailed {
            server: self.server.clone(),
            cause: err,
        })
    }

    /// Send a register message to the NATS server.
    async fn register(&mut self) -> Result<(), Error> {
        let payload = RegisterArgs {
            id: self.node.clone(),
            grpc_endpoint: self.grpc_endpoint.clone(),
        };
        match &mut self.client {
            Some(client) => client
                .publish("register", serde_json::to_vec(&payload).unwrap())
                .await
                .map_err(|cause| Error::QueueRegister {
                    cause,
                })?,
            None => return Err(Error::NotStarted {}),
        }
        // Note that the message was only queued and we don't know if it was
        // really sent to the NATS server (limitation of the nats lib)
        debug!(
            "Registered '{}' and grpc server {}",
            self.node, self.grpc_endpoint
        );
        Ok(())
    }

    /// Send a deregister message to the NATS server.
    async fn deregister(&mut self) -> Result<(), Error> {
        let payload = DeregisterArgs {
            id: self.node.clone(),
        };
        match &mut self.client {
            Some(client) => client
                .publish("deregister", serde_json::to_vec(&payload).unwrap())
                .await
                .map_err(|cause| Error::QueueRegister {
                    cause,
                })?,
            None => return Err(Error::NotStarted {}),
        }
        info!(
            "Deregistered '{}' and grpc server {}",
            self.node, self.grpc_endpoint
        );
        Ok(())
    }
}

/// Connect to the NATS server and start emitting periodic register messages.
/// Runs until the message_bus_stop() is called.
pub async fn message_bus_run(
    server: &str,
    node: &str,
    grpc_endpoint: &str,
) -> Result<(), ()> {
    let (sender, receiver) = mpsc::channel::<()>(1);
    {
        let mut sender_maybe = SENDER.lock().unwrap();
        if sender_maybe.is_some() {
            panic!("Double initialization of message bus");
        }
        *sender_maybe = Some(sender);
    }
    let mut mbus = MessageBus::new(server, node, grpc_endpoint);
    match mbus.run(receiver).await {
        Err(err) => {
            error!("{}", err);
            Err(())
        }
        Ok(_) => Ok(()),
    }
}

/// Causes the future created by message_bus_run() to resolve.
pub fn message_bus_stop() {
    // this will free the sender and unblock the receiver waiting for a message
    let _sender_maybe = SENDER.lock().unwrap().take();
}
