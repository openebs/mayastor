use mbus_api::{
    message_bus::v0::BusError,
    v0::*,
    ErrorChain,
    ReplyError,
    ReplyErrorKind,
    ResourceKind,
};
use snafu::{Error, Snafu};
use tonic::Code;

/// Common error type for send/receive
#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
#[allow(missing_docs)]
pub enum SvcError {
    #[snafu(display("Failed to get node '{}' from the node agent", node))]
    BusGetNode { node: String, source: BusError },
    #[snafu(display("Failed to get nodes from the node agent"))]
    BusGetNodes { source: BusError },
    #[snafu(display("Node '{}' is not online", node))]
    NodeNotOnline { node: NodeId },
    #[snafu(display(
        "Timed out after '{:?}' attempting to connect to node '{}' via gRPC endpoint '{}'",
        timeout,
        node_id,
        endpoint
    ))]
    GrpcConnectTimeout {
        node_id: String,
        endpoint: String,
        timeout: std::time::Duration,
    },
    #[snafu(display("Failed to connect to node via gRPC"))]
    GrpcConnect { source: tonic::transport::Error },
    #[snafu(display("Node '{}' has invalid gRPC URI '{}'", node_id, uri))]
    GrpcConnectUri {
        node_id: String,
        uri: String,
        source: http::uri::InvalidUri,
    },
    #[snafu(display(
        "gRPC request '{}' for '{}' failed with '{}'",
        request,
        resource.to_string(),
        source
    ))]
    GrpcRequestError {
        resource: ResourceKind,
        request: String,
        source: tonic::Status,
    },
    #[snafu(display("Node '{}' not found", node_id))]
    NodeNotFound { node_id: NodeId },
    #[snafu(display("Pool '{}' not found", pool_id))]
    PoolNotFound { pool_id: PoolId },
    #[snafu(display("Nexus '{}' not found", nexus_id))]
    NexusNotFound { nexus_id: String },
    #[snafu(display("Replica '{}' not found", replica_id))]
    ReplicaNotFound { replica_id: ReplicaId },
    #[snafu(display("Invalid filter value: {:?}", filter))]
    InvalidFilter { filter: Filter },
    #[snafu(display("Operation failed due to insufficient resources"))]
    NotEnoughResources { source: NotEnough },
    #[snafu(display("Failed to deserialise JsonRpc response"))]
    JsonRpcDeserialise { source: serde_json::Error },
    #[snafu(display(
        "Json RPC call failed for method '{}' with parameters '{}'. Error {}",
        method,
        params,
        error,
    ))]
    JsonRpc {
        method: String,
        params: String,
        error: String,
    },
    #[snafu(display("Internal error: {}", details))]
    Internal { details: String },
    #[snafu(display("Message Bus error"))]
    MBusError { source: mbus_api::Error },
    #[snafu(display("Invalid Arguments"))]
    InvalidArguments {},
}

impl From<mbus_api::Error> for SvcError {
    fn from(source: mbus_api::Error) -> Self {
        Self::MBusError {
            source,
        }
    }
}

impl From<NotEnough> for SvcError {
    fn from(source: NotEnough) -> Self {
        Self::NotEnoughResources {
            source,
        }
    }
}

impl From<SvcError> for ReplyError {
    fn from(error: SvcError) -> Self {
        #[allow(deprecated)]
        let desc: &String = &error.description().to_string();
        match error {
            SvcError::BusGetNode {
                source, ..
            } => source,
            SvcError::BusGetNodes {
                source,
            } => source,
            SvcError::GrpcRequestError {
                source,
                request,
                resource,
            } => grpc_to_reply_error(SvcError::GrpcRequestError {
                source,
                request,
                resource,
            }),

            SvcError::InvalidArguments {
                ..
            } => ReplyError {
                kind: ReplyErrorKind::InvalidArgument,
                resource: ResourceKind::Unknown,
                source: desc.to_string(),
                extra: error.full_string(),
            },

            SvcError::NodeNotOnline {
                ..
            } => ReplyError {
                kind: ReplyErrorKind::FailedPrecondition,
                resource: ResourceKind::Node,
                source: desc.to_string(),
                extra: error.full_string(),
            },

            SvcError::GrpcConnectTimeout {
                ..
            } => ReplyError {
                kind: ReplyErrorKind::Timeout,
                resource: ResourceKind::Unknown,
                source: desc.to_string(),
                extra: error.full_string(),
            },

            SvcError::GrpcConnectUri {
                ..
            } => ReplyError {
                kind: ReplyErrorKind::Internal,
                resource: ResourceKind::Unknown,
                source: desc.to_string(),
                extra: error.full_string(),
            },

            SvcError::GrpcConnect {
                source,
            } => ReplyError {
                kind: ReplyErrorKind::Internal,
                resource: ResourceKind::Unknown,
                source: desc.to_string(),
                extra: source.to_string(),
            },

            SvcError::NotEnoughResources {
                ..
            } => ReplyError {
                kind: ReplyErrorKind::ResourceExhausted,
                resource: ResourceKind::Unknown,
                source: desc.to_string(),
                extra: error.full_string(),
            },
            SvcError::JsonRpcDeserialise {
                ..
            } => ReplyError {
                kind: ReplyErrorKind::Internal,
                resource: ResourceKind::JsonGrpc,
                source: desc.to_string(),
                extra: error.full_string(),
            },
            SvcError::JsonRpc {
                ..
            } => ReplyError {
                kind: ReplyErrorKind::Internal,
                resource: ResourceKind::JsonGrpc,
                source: desc.to_string(),
                extra: error.full_string(),
            },
            SvcError::NodeNotFound {
                ..
            } => ReplyError {
                kind: ReplyErrorKind::NotFound,
                resource: ResourceKind::Node,
                source: desc.to_string(),
                extra: error.full_string(),
            },
            SvcError::PoolNotFound {
                ..
            } => ReplyError {
                kind: ReplyErrorKind::NotFound,
                resource: ResourceKind::Pool,
                source: desc.to_string(),
                extra: error.full_string(),
            },
            SvcError::ReplicaNotFound {
                ..
            } => ReplyError {
                kind: ReplyErrorKind::NotFound,
                resource: ResourceKind::Replica,
                source: desc.to_string(),
                extra: error.full_string(),
            },
            SvcError::NexusNotFound {
                ..
            } => ReplyError {
                kind: ReplyErrorKind::NotFound,
                resource: ResourceKind::Nexus,
                source: desc.to_string(),
                extra: error.full_string(),
            },
            SvcError::InvalidFilter {
                ..
            } => ReplyError {
                kind: ReplyErrorKind::Internal,
                resource: ResourceKind::Unknown,
                source: desc.to_string(),
                extra: error.full_string(),
            },
            SvcError::Internal {
                ..
            } => ReplyError {
                kind: ReplyErrorKind::Internal,
                resource: ResourceKind::Unknown,
                source: desc.to_string(),
                extra: error.full_string(),
            },
            SvcError::MBusError {
                source,
            } => source.into(),
        }
    }
}

fn grpc_to_reply_error(error: SvcError) -> ReplyError {
    match error {
        SvcError::GrpcRequestError {
            source,
            request,
            resource,
        } => {
            let kind = match source.code() {
                Code::Ok => ReplyErrorKind::Internal,
                Code::Cancelled => ReplyErrorKind::Internal,
                Code::Unknown => ReplyErrorKind::Internal,
                Code::InvalidArgument => ReplyErrorKind::InvalidArgument,
                Code::DeadlineExceeded => ReplyErrorKind::DeadlineExceeded,
                Code::NotFound => ReplyErrorKind::NotFound,
                Code::AlreadyExists => ReplyErrorKind::AlreadyExists,
                Code::PermissionDenied => ReplyErrorKind::PermissionDenied,
                Code::ResourceExhausted => ReplyErrorKind::ResourceExhausted,
                Code::FailedPrecondition => ReplyErrorKind::FailedPrecondition,
                Code::Aborted => ReplyErrorKind::Aborted,
                Code::OutOfRange => ReplyErrorKind::OutOfRange,
                Code::Unimplemented => ReplyErrorKind::Unimplemented,
                Code::Internal => ReplyErrorKind::Internal,
                Code::Unavailable => ReplyErrorKind::Unavailable,
                Code::DataLoss => ReplyErrorKind::Internal,
                Code::Unauthenticated => ReplyErrorKind::Unauthenticated,
                Code::__NonExhaustive => ReplyErrorKind::Internal,
            };
            let extra = format!("{}::{}", request, source.to_string());
            ReplyError {
                kind,
                resource,
                source: "SvcError::GrpcRequestError".to_string(),
                extra,
            }
        }
        _ => unreachable!("Expected a GrpcRequestError!"),
    }
}

/// Not enough resources available
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum NotEnough {
    #[snafu(display(
        "Not enough suitable pools available, {}/{}",
        have,
        need
    ))]
    OfPools { have: u64, need: u64 },
    #[snafu(display("Not enough replicas available, {}/{}", have, need))]
    OfReplicas { have: u64, need: u64 },
    #[snafu(display("Not enough nexuses available, {}/{}", have, need))]
    OfNexuses { have: u64, need: u64 },
}
