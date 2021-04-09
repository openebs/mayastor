//! Registration subsystem connecting mayastor to control plane (moac).
//! A registration message is used to let the control plane know about a
//! mayastor instance. A deregistration message is used let the control plane
//! know that a mayastor instance is going down.
//!
//! The registration messages are currently sent on an `HB_INTERVAL` by default
//! but can be overridden by the `MAYASTOR_HB_INTERVAL` environment variable.
//! containing the node name and the grpc endpoint.

use futures::{select, FutureExt, StreamExt};
use mbus_api::{v0::*, *};
use once_cell::sync::OnceCell;
use snafu::Snafu;
use std::{env, time::Duration};

/// Mayastor sends registration messages in this interval (kind of heart-beat)
const HB_INTERVAL: Duration = Duration::from_secs(5);

/// Errors for pool operations.
///
/// Note: The types here that would be normally used as source for snafu errors
/// do not implement Error trait required by Snafu. So they are renamed to
/// "cause" attribute and we use .map_err() instead of .context() when creating
/// them.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to connect to the MessageBus server {}: {:?}",
        server,
        cause
    ))]
    ConnectFailed {
        cause: std::io::Error,
        server: String,
    },
    #[snafu(display(
        "Cannot issue requests if message bus hasn't been started"
    ))]
    NotStarted {},
    #[snafu(display("Failed to queue register request: {:?}", cause))]
    QueueRegister { cause: mbus_api::Error },
    #[snafu(display("Failed to queue deregister request: {:?}", cause))]
    QueueDeregister { cause: mbus_api::Error },
}

#[derive(Clone)]
struct Configuration {
    /// Id of the node that mayastor is running on
    node: NodeId,
    /// gRPC endpoint of the server provided by mayastor
    grpc_endpoint: String,
    /// heartbeat interval (how often the register message is sent)
    hb_interval: Duration,
}

#[derive(Clone)]
pub struct Registration {
    /// Configuration of the registration
    config: Configuration,
    /// Receive channel for messages and termination
    rcv_chan: smol::channel::Receiver<()>,
    /// Termination channel
    fini_chan: smol::channel::Sender<()>,
}

static MESSAGE_BUS_REG: OnceCell<Registration> = OnceCell::new();
impl Registration {
    /// initialise the global registration instance
    pub(super) fn init(node: &str, grpc_endpoint: &str) {
        MESSAGE_BUS_REG.get_or_init(|| {
            Registration::new(&NodeId::from(node), grpc_endpoint)
        });
    }

    /// terminate and re-register
    pub(super) fn fini(&self) {
        self.fini_chan.close();
    }

    pub(super) fn get() -> &'static Registration {
        MESSAGE_BUS_REG.get().unwrap()
    }

    /// runner responsible for registering and
    /// de-registering the mayastor instance on shutdown
    pub async fn run() -> Result<(), ()> {
        if let Some(registration) = MESSAGE_BUS_REG.get() {
            registration.clone().run_loop().await;
        }
        Ok(())
    }

    fn new(node: &NodeId, grpc_endpoint: &str) -> Registration {
        let (msg_sender, msg_receiver) = smol::channel::unbounded::<()>();
        let config = Configuration {
            node: node.to_owned(),
            grpc_endpoint: grpc_endpoint.to_owned(),
            hb_interval: match env::var("MAYASTOR_HB_INTERVAL")
                .map(|v| v.parse::<u64>())
            {
                Ok(Ok(num)) => Duration::from_secs(num),
                _ => HB_INTERVAL,
            },
        };
        Self {
            config,
            rcv_chan: msg_receiver,
            fini_chan: msg_sender,
        }
    }

    /// Connect to the server and start emitting periodic register
    /// messages.
    /// Runs until the sender side of the message channel is closed
    pub async fn run_loop(&mut self) {
        info!(
            "Registering '{}' and grpc server {} ...",
            self.config.node, self.config.grpc_endpoint
        );
        loop {
            if let Err(err) = self.register().await {
                error!("Registration failed: {:?}", err);
            };

            select! {
                _ = tokio::time::sleep(self.config.hb_interval).fuse() => continue,
                msg = self.rcv_chan.next().fuse() => {
                    match msg {
                        Some(_) => log::info!("Messages have not been implemented yet"),
                        _ => {
                            log::info!("Terminating the registration handler");
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

    /// Send a register message to the MessageBus.
    async fn register(&self) -> Result<(), Error> {
        let payload = Register {
            id: self.config.node.clone(),
            grpc_endpoint: self.config.grpc_endpoint.clone(),
        };

        payload
            .publish()
            .await
            .map_err(|cause| Error::QueueRegister {
                cause,
            })?;

        // Note that the message was only queued and we don't know if it was
        // really sent to the message server
        // We could explicitly flush to make sure it reaches the server or
        // use request/reply to guarantee that it was delivered
        debug!(
            "Registered '{}' and grpc server {}",
            self.config.node, self.config.grpc_endpoint
        );
        Ok(())
    }

    /// Send a deregister message to the MessageBus.
    async fn deregister(&self) -> Result<(), Error> {
        let payload = Deregister {
            id: self.config.node.clone(),
        };

        payload
            .publish()
            .await
            .map_err(|cause| Error::QueueRegister {
                cause,
            })?;

        if let Err(e) = bus().flush().await {
            error!("Failed to explicitly flush: {}", e);
        }

        info!(
            "Deregistered '{}' and grpc server {}",
            self.config.node, self.config.grpc_endpoint
        );
        Ok(())
    }
}
