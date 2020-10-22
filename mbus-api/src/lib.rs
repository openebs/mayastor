#![warn(missing_docs)]
//! All the different messages which can be sent/received to/from the control
//! plane services and mayastor
//! We could split these out further into categories when they start to grow

mod mbus_nats;
/// received message traits
pub mod receive;
/// send messages traits
pub mod send;

use async_trait::async_trait;
use dyn_clonable::clonable;
pub use mbus_nats::{bus, message_bus_init, message_bus_init_tokio};
pub use receive::*;
pub use send::*;
use serde::{Deserialize, Serialize};
use smol::io;
use snafu::Snafu;
use std::{fmt::Debug, marker::PhantomData, str::FromStr, time::Duration};

/// Available Message Bus channels
#[derive(Clone, Debug)]
pub enum Channel {
    /// Default
    Default,
    /// Registration of mayastor instances with the control plane
    Registry,
    /// Keep it In Sync Service
    Kiiss,
    /// Reply to requested Channel
    Reply(String),
}

impl FromStr for Channel {
    type Err = String;

    fn from_str(source: &str) -> Result<Self, Self::Err> {
        match source {
            "default" => Ok(Self::Default),
            "registry" => Ok(Self::Registry),
            "kiiss" => Ok(Self::Kiiss),
            _ => Err(format!("Could not parse the channel: {}", source)),
        }
    }
}

impl Default for Channel {
    fn default() -> Self {
        Channel::Default
    }
}

impl std::fmt::Display for Channel {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Channel::Default => write!(f, "default"),
            Channel::Registry => write!(f, "registry"),
            Channel::Kiiss => write!(f, "kiiss"),
            Channel::Reply(ch) => write!(f, "{}", ch),
        }
    }
}

/// Message id which uniquely identifies every type of unsolicited message
/// The solicited (replies) message do not currently carry an id as they
/// are sent to a specific requested channel
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "camelCase")]
pub enum MessageId {
    /// Default
    Default,
    /// Update Config
    ConfigUpdate,
    /// Request current Config
    ConfigGetCurrent,
    /// Register mayastor
    Register,
    /// Deregister mayastor
    Deregister,
}

/// Sender identification (eg which mayastor instance sent the message)
pub type SenderId = String;

/// Mayastor configurations
/// Currently, we have the global mayastor config and the child states config
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Hash)]
pub enum Config {
    /// Mayastor global config
    MayastorConfig,
    /// Mayastor child states config
    ChildStatesConfig,
}
impl Default for Config {
    fn default() -> Self {
        Config::MayastorConfig
    }
}

/// Config Messages

/// Update mayastor configuration
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct ConfigUpdate {
    /// type of config being updated
    pub kind: Config,
    /// actual config data
    pub data: Vec<u8>,
}
bus_impl_message_all!(ConfigUpdate, ConfigUpdate, (), Kiiss);

/// Request message configuration used by mayastor to request configuration
/// from a control plane service
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct ConfigGetCurrent {
    /// type of config requested
    pub kind: Config,
}
/// Reply message configuration returned by a controle plane service to mayastor
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct ReplyConfig {
    /// config data
    pub config: Vec<u8>,
}
bus_impl_message_all!(
    ConfigGetCurrent,
    ConfigGetCurrent,
    ReplyConfig,
    Kiiss,
    GetConfig
);

/// Registration

/// Register message payload
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct Register {
    /// id of the mayastor instance
    pub id: String,
    /// grpc_endpoint of the mayastor instance
    #[serde(rename = "grpcEndpoint")]
    pub grpc_endpoint: String,
}
bus_impl_message_all!(Register, Register, (), Registry);

/// Deregister message payload
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct Deregister {
    /// id of the mayastor instance
    pub id: String,
}
bus_impl_message_all!(Deregister, Deregister, (), Registry);

/// This trait defines all Bus Messages which must:
/// 1 - be uniquely identifiable via MessageId
/// 2 - have a default Channel on which they are sent/received
#[async_trait(?Send)]
pub trait Message {
    /// type which is sent back in response to a request
    type Reply;

    /// identification of this object according to the `MessageId`
    fn id(&self) -> MessageId;
    /// default channel where this object is sent to
    fn channel(&self) -> Channel;

    /// publish a message with no delivery guarantees
    async fn publish(&self) -> io::Result<()>;
    /// publish a message with a request for a `Self::Reply` reply
    async fn request(&self) -> io::Result<Self::Reply>;
    /// publish a message with a request for a `Self::Reply` reply
    /// and non default timeout options
    async fn request_ext(
        &self,
        options: TimeoutOptions,
    ) -> io::Result<Self::Reply>;
}

/// The preamble is used to peek into messages so allowing for them to be routed
/// by their identifier
#[derive(Serialize, Deserialize, Debug)]
struct Preamble {
    pub(crate) id: MessageId,
}

/// Unsolicited (send) messages carry the message identifier, the sender
/// identifier and finally the message payload itself
#[derive(Serialize, Deserialize)]
struct SendPayload<T> {
    pub(crate) id: MessageId,
    pub(crate) sender: SenderId,
    pub(crate) data: T,
}

/// Error type which is returned over the bus
/// todo: Use this Error not just for the "transport" but also
/// for any other operation
#[derive(Serialize, Deserialize, Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("Generic Failure, message={}", message))]
    WithMessage { message: String },
    #[snafu(display("Ill formed request when deserializing the request"))]
    InvalidFormat,
}

/// Payload returned to the sender
/// Includes an error as the operations may be fallible
#[derive(Serialize, Deserialize)]
pub struct ReplyPayload<T>(pub Result<T, Error>);

// todo: implement thin wrappers on these
/// MessageBus raw Message
pub type BusMessage = nats::asynk::Message;
/// MessageBus subscription
pub type BusSubscription = nats::asynk::Subscription;
/// MessageBus configuration options
pub type BusOptions = nats::Options;
/// Save on typing
pub type DynBus = Box<dyn Bus>;

/// Timeout for receiving a reply to a request message
/// Max number of retries until it gives up
#[derive(Clone)]
pub struct TimeoutOptions {
    /// initial request message timeout
    pub(crate) timeout: std::time::Duration,
    /// max number of retries following the initial attempt's timeout
    pub(crate) max_retries: Option<u32>,
}

impl TimeoutOptions {
    pub(crate) fn default_timeout() -> Duration {
        Duration::from_secs(6)
    }
    pub(crate) fn default_max_retries() -> u32 {
        6
    }
}

impl Default for TimeoutOptions {
    fn default() -> Self {
        Self {
            timeout: Self::default_timeout(),
            max_retries: Some(Self::default_max_retries()),
        }
    }
}

impl TimeoutOptions {
    /// New options with default values
    pub fn new() -> Self {
        Default::default()
    }

    /// Timeout after which we'll either fail the request or start retrying
    /// if max_retries is greater than 0 or None
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Specify a max number of retries before giving up
    /// None for unlimited retries
    pub fn with_max_retries(mut self, max_retries: Option<u32>) -> Self {
        self.max_retries = max_retries;
        self
    }
}

/// Messaging Bus trait with "generic" publish and request/reply semantics
#[async_trait]
#[clonable]
pub trait Bus: Clone + Send + Sync {
    /// publish a message - not guaranteed to be sent or received (fire and
    /// forget)
    async fn publish(
        &self,
        channel: Channel,
        message: &[u8],
    ) -> std::io::Result<()>;
    /// Send a message and wait for it to be received by the target component
    async fn send(&self, channel: Channel, message: &[u8]) -> io::Result<()>;
    /// Send a message and request a reply from the target component
    async fn request(
        &self,
        channel: Channel,
        message: &[u8],
        options: Option<TimeoutOptions>,
    ) -> io::Result<BusMessage>;
    /// Flush queued messages to the server
    async fn flush(&self) -> io::Result<()>;
    /// Create a subscription on the given channel which can be
    /// polled for messages until it is either explicitly closed or
    /// when the bus is closed
    async fn subscribe(&self, channel: Channel) -> io::Result<BusSubscription>;
}
