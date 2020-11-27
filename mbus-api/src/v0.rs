use super::*;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use strum_macros::{EnumString, ToString};

/// Versioned Channels
#[derive(Clone, Debug, EnumString, ToString)]
#[strum(serialize_all = "camelCase")]
pub enum ChannelVs {
    /// Default
    Default,
    /// Registration of mayastor instances with the control plane
    Registry,
    /// Node Service which exposes the registered mayastor instances
    Node,
    /// Keep it In Sync Service
    Kiiss,
}
impl Default for ChannelVs {
    fn default() -> Self {
        ChannelVs::Default
    }
}

impl From<ChannelVs> for Channel {
    fn from(channel: ChannelVs) -> Self {
        Channel::v0(channel)
    }
}

/// Versioned Message Id's
#[derive(Debug, PartialEq, Clone, ToString, EnumString)]
#[strum(serialize_all = "camelCase")]
pub enum MessageIdVs {
    /// Default
    Default,
    /// Liveness Probe
    Liveness,
    /// Update Config
    ConfigUpdate,
    /// Request current Config
    ConfigGetCurrent,
    /// Register mayastor
    Register,
    /// Deregister mayastor
    Deregister,
    /// Node Service
    /// Get all node information
    GetNodes,
}

// Only V0 should export this macro
// This allows the example code to use the v0 default
// Otherwise they have to impl whatever version they require
#[macro_export]
/// Use version 0 of the Message and Channel
macro_rules! impl_channel_id {
    ($I:ident, $C:ident) => {
        fn id(&self) -> MessageId {
            MessageId::v0(v0::MessageIdVs::$I)
        }
        fn channel(&self) -> Channel {
            Channel::v0(v0::ChannelVs::$C)
        }
    };
}

/// Liveness Probe
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct Liveness {}
bus_impl_message_all!(Liveness, Liveness, (), Default);

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
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Register {
    /// id of the mayastor instance
    pub id: String,
    /// grpc_endpoint of the mayastor instance
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

/// Node Service
///
/// Get all the nodes
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct GetNodes {}

/// State of the Node
#[derive(
    Serialize, Deserialize, Debug, Clone, EnumString, ToString, Eq, PartialEq,
)]
pub enum NodeState {
    /// Node has unexpectedly disappeared
    Unknown,
    /// Node is deemed online if it has not missed the
    /// registration keep alive deadline
    Online,
    /// Node is deemed offline if has missed the
    /// registration keep alive deadline
    Offline,
}

impl Default for NodeState {
    fn default() -> Self {
        Self::Unknown
    }
}

/// Node information
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Node {
    /// id of the mayastor instance
    pub id: String,
    /// grpc_endpoint of the mayastor instance
    pub grpc_endpoint: String,
    /// deemed state of the node
    pub state: NodeState,
}

bus_impl_vector_request!(Nodes, Node);
bus_impl_message_all!(GetNodes, GetNodes, Nodes, Node);
