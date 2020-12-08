#![warn(missing_docs)]
//! All the different messages which can be sent/received to/from the control
//! plane services and mayastor
//! We could split these out further into categories when they start to grow

mod mbus_nats;
/// received message traits
pub mod receive;
/// send messages traits
pub mod send;
/// Version 0 of the messages
pub mod v0;

use async_trait::async_trait;
use dyn_clonable::clonable;
pub use mbus_nats::{
    bus,
    message_bus_init,
    message_bus_init_options,
    message_bus_init_tokio,
};
pub use receive::*;
pub use send::*;
use serde::{Deserialize, Serialize};
use smol::io;
use snafu::Snafu;
use std::{fmt::Debug, marker::PhantomData, str::FromStr, time::Duration};

/// Common error type for send/receive
pub type Error = io::Error;

/// Available Message Bus channels
#[derive(Clone, Debug)]
#[allow(non_camel_case_types)]
pub enum Channel {
    /// Version 0 of the Channels
    v0(v0::ChannelVs),
}

impl FromStr for Channel {
    type Err = strum::ParseError;

    fn from_str(source: &str) -> Result<Self, Self::Err> {
        match &source[0 ..= 2] {
            "v0/" => {
                let c: v0::ChannelVs = source[3 ..].parse()?;
                Ok(Self::v0(c))
            }
            _ => Err(strum::ParseError::VariantNotFound),
        }
    }
}
impl ToString for Channel {
    fn to_string(&self) -> String {
        match self {
            Self::v0(channel) => format!("v0/{}", channel.to_string()),
        }
    }
}

impl Default for Channel {
    fn default() -> Self {
        Channel::v0(v0::ChannelVs::Default)
    }
}

/// Message id which uniquely identifies every type of unsolicited message
/// The solicited (replies) message do not currently carry an id as they
/// are sent to a specific requested channel
#[derive(Debug, PartialEq, Clone)]
#[allow(non_camel_case_types)]
pub enum MessageId {
    /// Version 0
    v0(v0::MessageIdVs),
}

impl Serialize for MessageId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.to_string().as_str())
    }
}
impl<'de> Deserialize<'de> for MessageId {
    fn deserialize<D>(deserializer: D) -> Result<MessageId, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let string = String::deserialize(deserializer)?;
        match string.parse() {
            Ok(id) => Ok(id),
            Err(error) => {
                let error =
                    format!("Failed to parse into MessageId, error: {}", error);
                Err(serde::de::Error::custom(error))
            }
        }
    }
}

impl FromStr for MessageId {
    type Err = strum::ParseError;

    fn from_str(source: &str) -> Result<Self, Self::Err> {
        match &source[0 ..= 2] {
            "v0/" => {
                let id: v0::MessageIdVs = source[3 ..].parse()?;
                Ok(Self::v0(id))
            }
            _ => Err(strum::ParseError::VariantNotFound),
        }
    }
}
impl ToString for MessageId {
    fn to_string(&self) -> String {
        match self {
            Self::v0(id) => format!("v0/{}", id.to_string()),
        }
    }
}

/// Sender identification (eg which mayastor instance sent the message)
pub type SenderId = String;

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
pub enum BusError {
    #[snafu(display("Generic Failure, message={}", message))]
    WithMessage { message: String },
    #[snafu(display("Ill formed request when deserializing the request"))]
    InvalidFormat,
}

/// Payload returned to the sender
/// Includes an error as the operations may be fallible
#[derive(Serialize, Deserialize)]
pub struct ReplyPayload<T>(pub Result<T, BusError>);

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
#[derive(Clone, Debug)]
pub struct TimeoutOptions {
    /// initial request message timeout
    pub(crate) timeout: std::time::Duration,
    /// request message incremental timeout step
    pub(crate) timeout_step: std::time::Duration,
    /// max number of retries following the initial attempt's timeout
    pub(crate) max_retries: Option<u32>,
}

impl TimeoutOptions {
    pub(crate) fn default_timeout() -> Duration {
        Duration::from_secs(6)
    }
    pub(crate) fn default_timeout_step() -> Duration {
        Duration::from_secs(1)
    }
    pub(crate) fn default_max_retries() -> u32 {
        6
    }
}

impl Default for TimeoutOptions {
    fn default() -> Self {
        Self {
            timeout: Self::default_timeout(),
            timeout_step: Self::default_timeout_step(),
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

    /// Timeout multiplied at each iteration
    pub fn with_timeout_backoff(mut self, timeout: Duration) -> Self {
        self.timeout_step = timeout;
        self
    }

    /// Specify a max number of retries before giving up
    /// None for unlimited retries
    pub fn with_max_retries<M: Into<Option<u32>>>(
        mut self,
        max_retries: M,
    ) -> Self {
        self.max_retries = max_retries.into();
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
