#![warn(missing_docs)]
//! All the different messages which can be sent/received to/from the control
//! plane services and mayastor
//! We could split these out further into categories when they start to grow

mod mbus_nats;
/// Message bus client interface
pub mod message_bus;
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
use snafu::{ResultExt, Snafu};
use std::{fmt::Debug, marker::PhantomData, str::FromStr, time::Duration};

/// Result wrapper for send/receive
pub type BusResult<T> = Result<T, Error>;
/// Common error type for send/receive
#[derive(Debug, Snafu, strum_macros::AsRefStr)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("Message with wrong message id received. Received '{}' but Expected '{}'", received.to_string(), expected.to_string()))]
    WrongMessageId {
        received: MessageId,
        expected: MessageId,
    },
    #[snafu(display("Failed to serialize the publish payload on channel '{}'", channel.to_string()))]
    SerializeSend {
        source: serde_json::Error,
        channel: Channel,
    },
    #[snafu(display(
        "Failed to deserialize the publish payload: '{:?}' into type '{}'",
        payload,
        receiver
    ))]
    DeserializeSend {
        payload: Result<String, std::string::FromUtf8Error>,
        receiver: String,
        source: serde_json::Error,
    },
    #[snafu(display("Failed to serialize the reply payload for request message id '{}'", request.to_string()))]
    SerializeReply {
        request: MessageId,
        source: serde_json::Error,
    },
    #[snafu(display(
        "Failed to deserialize the reply payload '{:?}' for message: '{:?}'",
        reply,
        request
    ))]
    DeserializeReceive {
        request: Result<String, serde_json::Error>,
        reply: Result<String, std::string::FromUtf8Error>,
        source: serde_json::Error,
    },
    #[snafu(display(
        "Failed to send message '{:?}' through the message bus on channel '{}'",
        payload,
        channel
    ))]
    Publish {
        channel: String,
        payload: Result<String, std::string::FromUtf8Error>,
        source: io::Error,
    },
    #[snafu(display(
        "Timed out waiting for a reply to message '{:?}' on channel '{:?}' with options '{:?}'.",
        payload,
        channel,
        options
    ))]
    RequestTimeout {
        channel: String,
        payload: Result<String, std::string::FromUtf8Error>,
        options: TimeoutOptions,
    },
    #[snafu(display(
        "Failed to reply back to message id '{}' through the message bus",
        request.to_string()
    ))]
    Reply {
        request: MessageId,
        source: io::Error,
    },
    #[snafu(display("Failed to flush the message bus"))]
    Flush { source: io::Error },
    #[snafu(display(
        "Failed to subscribe to channel '{}' on the message bus",
        channel
    ))]
    Subscribe { channel: String, source: io::Error },
    #[snafu(display("Reply message came back with an error"))]
    ReplyWithError { source: ReplyError },
    #[snafu(display("Service error whilst handling request: {}", message))]
    ServiceError { message: String },
}

/// Report error chain
pub trait ErrorChain {
    /// full error chain as a string separated by ':'
    fn full_string(&self) -> String;
}

impl<T> ErrorChain for T
where
    T: std::error::Error,
{
    /// loops through the error chain and formats into a single string
    /// containing all the lower level errors
    fn full_string(&self) -> String {
        let mut msg = format!("{}", self);
        let mut opt_source = self.source();
        while let Some(source) = opt_source {
            msg = format!("{}: {}", msg, source);
            opt_source = source.source();
        }
        msg
    }
}

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
#[async_trait]
pub trait Message {
    /// type which is sent back in response to a request
    type Reply;

    /// identification of this object according to the `MessageId`
    fn id(&self) -> MessageId;
    /// default channel where this object is sent to
    fn channel(&self) -> Channel;

    /// publish a message with no delivery guarantees
    async fn publish(&self) -> BusResult<()>;
    /// publish a message with a request for a `Self::Reply` reply
    async fn request(&self) -> BusResult<Self::Reply>;
    /// publish a message on the given channel with a request for a
    /// `Self::Reply` reply
    async fn request_on<C: Into<Channel> + Send>(
        &self,
        channel: C,
    ) -> BusResult<Self::Reply>;
    /// publish a message with a request for a `Self::Reply` reply
    /// and non default timeout options
    async fn request_ext(
        &self,
        options: TimeoutOptions,
    ) -> BusResult<Self::Reply>;
    /// publish a message with a request for a `Self::Reply` reply
    /// and non default timeout options on the given channel
    async fn request_on_ext<C: Into<Channel> + Send>(
        &self,
        channel: C,
        options: TimeoutOptions,
    ) -> BusResult<Self::Reply>;
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
/// for any other operation
#[derive(Serialize, Deserialize, Debug, Snafu, strum_macros::AsRefStr)]
#[allow(missing_docs)]
pub enum ReplyError {
    #[snafu(display("Generic Failure, message={}", message))]
    WithMessage { message: String },
    #[snafu(display("Failed to deserialize the request: '{}'", message))]
    DeserializeReq { message: String },
    #[snafu(display("Failed to process the request: '{}'", message))]
    Process { message: String },
}

/// Payload returned to the sender
/// Includes an error as the operations may be fallible
#[derive(Serialize, Deserialize, Debug)]
pub struct ReplyPayload<T>(pub Result<T, ReplyError>);

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
    async fn publish(&self, channel: Channel, message: &[u8]) -> BusResult<()>;
    /// Send a message and wait for it to be received by the target component
    async fn send(&self, channel: Channel, message: &[u8]) -> BusResult<()>;
    /// Send a message and request a reply from the target component
    async fn request(
        &self,
        channel: Channel,
        message: &[u8],
        options: Option<TimeoutOptions>,
    ) -> BusResult<BusMessage>;
    /// Flush queued messages to the server
    async fn flush(&self) -> BusResult<()>;
    /// Create a subscription on the given channel which can be
    /// polled for messages until it is either explicitly closed or
    /// when the bus is closed
    async fn subscribe(&self, channel: Channel) -> BusResult<BusSubscription>;
}
