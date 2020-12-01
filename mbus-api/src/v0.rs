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
    /// Pool Service which manages mayastor pools and replicas
    Pool,
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
    /// Pool Service
    ///
    /// Get pools with filter
    GetPools,
    /// Create Pool,
    CreatePool,
    /// Destroy Pool,
    DestroyPool,
    /// Get replicas with filter
    GetReplicas,
    /// Create Replica,
    CreateReplica,
    /// Destroy Replica,
    DestroyReplica,
    /// Share Replica,
    ShareReplica,
    /// Unshare Replica,
    UnshareReplica,
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

/// Filter Objects based on one of the following criteria
/// # Example:
/// // Get all nexuses from the node `node_id`
/// let nexuses =
///     MessageBus::get_nexuses(Filter::Node(node_id)).await.unwrap();
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Filter {
    /// All objects
    None,
    /// Filter by Node id
    Node(String),
    /// Pool filters
    ///
    /// Filter by Pool id
    Pool(String),
    /// Filter by Node and Pool id
    NodePool(String, String),
    /// Filter by Node and Replica id
    NodeReplica(String, String),
    /// Filter by Node, Pool and Replica id
    NodePoolReplica(String, String, String),
    /// Filter by Pool and Replica id
    PoolReplica(String, String),
    /// Filter by Replica id
    Replica(String),
}
impl Default for Filter {
    fn default() -> Self {
        Self::None
    }
}

/// Pool Service
/// Get all the pools from specific node or None for all nodes
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct GetPools {
    /// Filter request
    pub filter: Filter,
}

/// State of the Pool
#[derive(
    Serialize, Deserialize, Debug, Clone, EnumString, ToString, Eq, PartialEq,
)]
pub enum PoolState {
    /// unknown state
    Unknown = 0,
    /// the pool is in normal working order
    Online = 1,
    /// the pool has experienced a failure but can still function
    Degraded = 2,
    /// the pool is completely inaccessible
    Faulted = 3,
}

impl Default for PoolState {
    fn default() -> Self {
        Self::Unknown
    }
}
impl From<i32> for PoolState {
    fn from(src: i32) -> Self {
        match src {
            1 => Self::Online,
            2 => Self::Degraded,
            3 => Self::Faulted,
            _ => Self::Unknown,
        }
    }
}

/// Pool information
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Pool {
    /// id of the mayastor instance
    pub node: String,
    /// name of the pool
    pub name: String,
    /// absolute disk paths claimed by the pool
    pub disks: Vec<String>,
    /// current state of the pool
    pub state: PoolState,
    /// size of the pool in bytes
    pub capacity: u64,
    /// used bytes from the pool
    pub used: u64,
}

/// Create Pool Request
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CreatePool {
    /// id of the mayastor instance
    pub node: String,
    /// name of the pool
    pub name: String,
    /// disk device paths or URIs to be claimed by the pool
    pub disks: Vec<String>,
}
bus_impl_message_all!(CreatePool, CreatePool, Pool, Pool);

/// Destroy Pool Request
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DestroyPool {
    /// id of the mayastor instance
    pub node: String,
    /// name of the pool
    pub name: String,
}
bus_impl_message_all!(DestroyPool, DestroyPool, (), Pool);

bus_impl_vector_request!(Pools, Pool);
bus_impl_message_all!(GetPools, GetPools, Pools, Pool);

/// Get all the replicas from specific node and pool
/// or None for all nodes or all pools
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct GetReplicas {
    /// Filter request
    pub filter: Filter,
}

/// Replica information
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Replica {
    /// id of the mayastor instance
    pub node: String,
    /// uuid of the replica
    pub uuid: String,
    /// name of the pool
    pub pool: String,
    /// thin provisioning
    pub thin: bool,
    /// size of the replica in bytes
    pub size: u64,
    /// protocol used for exposing the replica
    pub share: Protocol,
    /// uri usable by nexus to access it
    pub uri: String,
}

bus_impl_vector_request!(Replicas, Replica);
bus_impl_message_all!(GetReplicas, GetReplicas, Replicas, Pool);

/// Create Replica Request
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CreateReplica {
    /// id of the mayastor instance
    pub node: String,
    /// uuid of the replica
    pub uuid: String,
    /// name of the pool
    pub pool: String,
    /// size of the replica in bytes
    pub size: u64,
    /// thin provisioning
    pub thin: bool,
    /// protocol to expose the replica over
    pub share: Protocol,
}
bus_impl_message_all!(CreateReplica, CreateReplica, Replica, Pool);

/// Destroy Replica Request
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DestroyReplica {
    /// id of the mayastor instance
    pub node: String,
    /// name of the pool
    pub pool: String,
    /// uuid of the replica
    pub uuid: String,
}
bus_impl_message_all!(DestroyReplica, DestroyReplica, (), Pool);

/// Share Replica Request
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ShareReplica {
    /// id of the mayastor instance
    pub node: String,
    /// name of the pool
    pub pool: String,
    /// uuid of the replica
    pub uuid: String,
    /// protocol used for exposing the replica
    pub protocol: Protocol,
}
bus_impl_message_all!(ShareReplica, ShareReplica, String, Pool);

/// Unshare Replica Request
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct UnshareReplica {
    /// id of the mayastor instance
    pub node: String,
    /// name of the pool
    pub pool: String,
    /// uuid of the replica
    pub uuid: String,
}
bus_impl_message_all!(UnshareReplica, UnshareReplica, (), Pool);

/// Indicates what protocol the bdev is shared as
#[derive(
    Serialize, Deserialize, Debug, Clone, EnumString, ToString, Eq, PartialEq,
)]
#[strum(serialize_all = "camelCase")]
#[serde(rename_all = "camelCase")]
pub enum Protocol {
    /// not shared by any of the variants
    Off = 0,
    /// shared as NVMe-oF TCP
    Nvmf = 1,
    /// shared as iSCSI
    Iscsi = 2,
    /// shared as NBD
    Nbd = 3,
}

impl Default for Protocol {
    fn default() -> Self {
        Self::Off
    }
}
impl From<i32> for Protocol {
    fn from(src: i32) -> Self {
        match src {
            0 => Self::Off,
            1 => Self::Nvmf,
            2 => Self::Iscsi,
            _ => Self::Off,
        }
    }
}

/// State of the Replica
#[derive(
    Serialize, Deserialize, Debug, Clone, EnumString, ToString, Eq, PartialEq,
)]
#[strum(serialize_all = "camelCase")]
#[serde(rename_all = "camelCase")]
pub enum ReplicaState {
    /// unknown state
    Unknown = 0,
    /// the pool is in normal working order
    Online = 1,
    /// the pool has experienced a failure but can still function
    Degraded = 2,
    /// the pool is completely inaccessible
    Faulted = 3,
}

impl Default for ReplicaState {
    fn default() -> Self {
        Self::Unknown
    }
}
impl From<i32> for ReplicaState {
    fn from(src: i32) -> Self {
        match src {
            1 => Self::Online,
            2 => Self::Degraded,
            3 => Self::Faulted,
            _ => Self::Unknown,
        }
    }
}
