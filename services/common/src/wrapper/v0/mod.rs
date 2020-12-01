//! Implementation of a service backend which interacts with
//! mayastor instances via gRPC and with the other services via the
//! message bus.

mod registry;

pub use pool::NodeWrapperPool;
pub use registry::Registry;

use async_trait::async_trait;
use dyn_clonable::clonable;
use mbus_api::{
    message_bus::v0::{BusError, MessageBus, MessageBusTrait},
    v0::*,
};
use rpc::mayastor::{mayastor_client::MayastorClient, Null};
use snafu::{ResultExt, Snafu};
use std::{
    collections::HashMap,
    fmt::Debug,
    marker::PhantomData,
    str::FromStr,
    sync::Arc,
};
use tokio::sync::Mutex;
use tonic::transport::Channel;

/// Common error type for send/receive
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum SvcError {
    #[snafu(display("Failed to get nodes from the node service"))]
    BusGetNodes { source: BusError },
    #[snafu(display("Failed to get node '{}' from the node service", node))]
    BusGetNode { source: BusError, node: String },
    #[snafu(display("Node '{}' is not online", node))]
    NodeNotOnline { node: String },
    #[snafu(display("Failed to connect to node via gRPC"))]
    GrpcConnect { source: tonic::transport::Error },
    #[snafu(display("Failed to list pools via gRPC"))]
    GrpcListPools { source: tonic::Status },
    #[snafu(display("Failed to create pool via gRPC"))]
    GrpcCreatePool { source: tonic::Status },
    #[snafu(display("Failed to destroy pool via gRPC"))]
    GrpcDestroyPool { source: tonic::Status },
    #[snafu(display("Failed to list replicas via gRPC"))]
    GrpcListReplicas { source: tonic::Status },
    #[snafu(display("Failed to create replica via gRPC"))]
    GrpcCreateReplica { source: tonic::Status },
    #[snafu(display("Failed to destroy replica via gRPC"))]
    GrpcDestroyReplica { source: tonic::Status },
    #[snafu(display("Failed to share replica via gRPC"))]
    GrpcShareReplica { source: tonic::Status },
    #[snafu(display("Failed to unshare replica via gRPC"))]
    GrpcUnshareReplica { source: tonic::Status },
    #[snafu(display("Node not found"))]
    BusNodeNotFound { node_id: String },
    #[snafu(display("Pool not found"))]
    BusPoolNotFound { pool_id: String },
    #[snafu(display("Invalid filter for pools"))]
    InvalidFilter { filter: Filter },
    #[snafu(display("Failed to list nexuses via gRPC"))]
    GrpcListNexuses { source: tonic::Status },
    #[snafu(display("Failed to create nexus via gRPC"))]
    GrpcCreateNexus { source: tonic::Status },
    #[snafu(display("Failed to destroy nexus via gRPC"))]
    GrpcDestroyNexus { source: tonic::Status },
    #[snafu(display("Failed to share nexus via gRPC"))]
    GrpcShareNexus { source: tonic::Status },
    #[snafu(display("Failed to unshare nexus via gRPC"))]
    GrpcUnshareNexus { source: tonic::Status },
    #[snafu(display("Failed to volume due to insufficient resources"))]
    NotEnoughResources {},
    #[snafu(display("Invalid arguments"))]
    InvalidArguments {},
    #[snafu(display("Not implemented"))]
    NotImplemented {},
}

mod node_traits;
mod pool;
