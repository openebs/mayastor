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
use snafu::{ResultExt, Snafu};
use std::{env, time::Duration};

/// Mayastor sends registration messages in this interval (kind of heart-beat)
const HB_INTERVAL_SEC: Duration = Duration::from_secs(5);
/// How long we wait to send a registration message before timing out
const HB_TIMEOUT_SEC: Duration = Duration::from_secs(5);

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
    #[snafu(display("Failed to queue register request: {:?}", source))]
    QueueRegister { source: mbus_api::Error },
    #[snafu(display("Failed to queue deregister request: {:?}", source))]
    QueueDeregister { source: mbus_api::Error },
}

#[derive(Clone)]
struct Configuration {
    /// Id of the node that mayastor is running on
    node: NodeId,
    /// gRPC endpoint of the server provided by mayastor
    grpc_endpoint: String,
    /// heartbeat interval (how often the register message is sent)
    hb_interval_sec: Duration,
    /// how long we wait to send a registration message before timing out
    hb_timeout_sec: Duration,
}

#[derive(Clone)]
pub struct Registration {
    /// Configuration of the registration
    config: Configuration,
    /// Receive channel for messages and termination
    rcv_chan: async_channel::Receiver<()>,
    /// Termination channel
    fini_chan: async_channel::Sender<()>,
}

static MESSAGE_BUS_REG: OnceCell<Registration> = OnceCell::new();
impl Registration {
    /// initialise the global registration instance
    pub fn init(node: &str, grpc_endpoint: &str) {
        MESSAGE_BUS_REG.get_or_init(|| {
            Registration::new(&NodeId::from(node), grpc_endpoint)
        });
    }

    /// terminate and re-register
    pub(super) fn fini(&self) {
        self.fini_chan.close();
    }

    pub(super) fn get() -> Option<&'static Registration> {
        MESSAGE_BUS_REG.get()
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
        Self {
            config,
            rcv_chan: msg_receiver,
            fini_chan: msg_sender,
        }
    }

    /// Connect to the server and start emitting periodic register
    /// messages.
    /// Runs until the sender side of the message channel is closed.
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

    /// Send a register message to the MessageBus.
    async fn register(&self) -> Result<(), Error> {
        let payload = Register {
            id: self.config.node.clone(),
            grpc_endpoint: self.config.grpc_endpoint.clone(),
        };

        payload.publish().await.context(QueueRegister)?;
        bus()
            .flush_timeout(self.config.hb_timeout_sec)
            .await
            .context(QueueRegister)?;

        // the message has been sent to the nats server, but we don't know
        // whether the control plane has received it or not
        // we could use request/reply to guarantee that it was delivered
        // debug!(
        //     "Registered '{}' and grpc server {}",
        //     self.config.node, self.config.grpc_endpoint
        // );
        Ok(())
    }

    /// Send a deregister message to the MessageBus.
    async fn deregister(&self) -> Result<(), Error> {
        let payload = Deregister {
            id: self.config.node.clone(),
        };

        payload.publish().await.context(QueueDeregister)?;
        bus()
            .flush_timeout(self.config.hb_timeout_sec)
            .await
            .context(QueueDeregister)?;

        info!(
            "Deregistered '{}' and grpc server {}",
            self.config.node, self.config.grpc_endpoint
        );
        Ok(())
    }
}
