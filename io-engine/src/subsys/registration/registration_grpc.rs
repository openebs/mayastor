#![warn(missing_docs)]

use futures::{select, FutureExt, StreamExt};
use http::Uri;
use once_cell::sync::OnceCell;
use rpc::registration::{
    registration_client,
    DeregisterRequest,
    RegisterRequest,
};
use std::{env, time::Duration};

/// Mayastor sends registration messages in this interval (kind of heart-beat)
const HB_INTERVAL_SEC: Duration = Duration::from_secs(5);
/// How long we wait to send a registration message before timing out
const HB_TIMEOUT_SEC: Duration = Duration::from_secs(5);
/// The http2 keep alive interval.
const HTTP_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(10);
/// The http2 keep alive TIMEOUT.
const HTTP_KEEP_ALIVE_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Clone)]
struct Configuration {
    /// Id of the node that mayastor is running on
    node: String,
    /// gRPC endpoint of the server provided by mayastor
    grpc_endpoint: String,
    /// heartbeat interval (how often the register message is sent)
    hb_interval_sec: Duration,
    /// how long we wait to send a registration message before timing out
    hb_timeout_sec: Duration,
}

/// Registration component for registering dataplane to controlplane
#[derive(Clone)]
pub struct Registration {
    /// Configuration of the registration
    config: Configuration,
    /// Registration client
    client: registration_client::RegistrationClient<tonic::transport::Channel>,
    /// Receive channel for messages and termination
    rcv_chan: async_channel::Receiver<()>,
    /// Termination channel
    fini_chan: async_channel::Sender<()>,
}

static GRPC_REGISTRATION: OnceCell<Registration> = OnceCell::new();
impl Registration {
    /// Initialise the global registration instance
    pub fn init(node: &str, grpc_endpoint: &str, registration_addr: Uri) {
        GRPC_REGISTRATION.get_or_init(|| {
            Registration::new(node, grpc_endpoint, registration_addr)
        });
    }

    /// Create a new registration instance
    pub fn new(
        node: &str,
        grpc_endpoint: &str,
        registration_addr: Uri,
    ) -> Self {
        let (msg_sender, msg_receiver) = async_channel::unbounded::<()>();
        let config = Configuration {
            node: node.to_owned(),
            grpc_endpoint: grpc_endpoint.to_owned(),
            hb_interval_sec: match env::var("MAYASTOR_HB_INTERVAL_SEC")
                .map(|v| v.parse::<u64>())
            {
                Ok(Ok(num)) => Duration::from_secs(num),
                _ => HB_INTERVAL_SEC,
            },
            hb_timeout_sec: match env::var("MAYASTOR_HB_TIMEOUT_SEC")
                .map(|v| v.parse::<u64>())
            {
                Ok(Ok(num)) => Duration::from_secs(num),
                _ => HB_TIMEOUT_SEC,
            },
        };
        let endpoint = tonic::transport::Endpoint::from(registration_addr)
            .connect_timeout(config.hb_timeout_sec)
            .timeout(config.hb_timeout_sec)
            .http2_keep_alive_interval(HTTP_KEEP_ALIVE_INTERVAL)
            .keep_alive_timeout(HTTP_KEEP_ALIVE_TIMEOUT);
        let channel = endpoint.connect_lazy().unwrap();
        Self {
            config,
            client: registration_client::RegistrationClient::new(channel),
            rcv_chan: msg_receiver,
            fini_chan: msg_sender,
        }
    }

    /// Get the global registration instance
    pub(super) fn get() -> Option<&'static Registration> {
        GRPC_REGISTRATION.get()
    }

    /// terminate and re-register
    pub(super) fn fini(&self) {
        self.fini_chan.close();
    }

    /// Register a new node over rpc
    pub async fn register(&mut self) -> Result<(), tonic::Status> {
        match self
            .client
            .register(tonic::Request::new(RegisterRequest {
                id: self.config.node.to_string(),
                grpc_endpoint: self.config.grpc_endpoint.clone(),
                instance_uuid: None,
            }))
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Deregister a node over rpc
    pub async fn deregister(&mut self) -> Result<(), tonic::Status> {
        match self
            .client
            .deregister(tonic::Request::new(DeregisterRequest {
                id: self.config.node.to_string(),
            }))
            .await
        {
            Ok(_) => {
                tracing::info!(
                    "Deregistered '{:?}' and grpc server {}",
                    self.config.node,
                    self.config.grpc_endpoint
                );
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    /// runner responsible for registering and
    /// de-registering the mayastor instance on shutdown
    pub async fn run() -> Result<(), ()> {
        if let Some(registration) = GRPC_REGISTRATION.get() {
            registration.clone().run_loop().await;
        }
        Ok(())
    }

    /// Connect to the server and start emitting periodic register
    /// requests.
    pub async fn run_loop(&mut self) {
        let mut show_error: bool = true;
        info!(
            "Registering '{:?}' with grpc server {} ...",
            self.config.node, self.config.grpc_endpoint
        );
        loop {
            match self.register().await {
                Ok(_) => {
                    if !show_error {
                        info!(
                            "Re-registered '{:?}' with grpc server {} ...",
                            self.config.node, self.config.grpc_endpoint
                        );
                    }
                    show_error = true;
                }
                Err(err) => {
                    if show_error {
                        error!("Registration failed: {:?}", err);
                        show_error = false;
                    }
                }
            };
            select! {
                _ = tokio::time::sleep(self.config.hb_interval_sec).fuse() => continue,
                msg = self.rcv_chan.next().fuse() => {
                    match msg {
                        Some(_) => info!("Messages have not been implemented yet"),
                        _ => {
                            info!("Terminating the registration handler");
                            break;
                        }
                    }
                }
            };
        }
        if let Err(err) = self.deregister().await {
            error!("Deregistration failed: {:?}", err);
        };
    }
}
