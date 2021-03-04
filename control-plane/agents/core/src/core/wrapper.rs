use super::{super::node::watchdog::Watchdog, grpc::GrpcContext};
use common::{
    errors::{GrpcRequestError, SvcError},
    v0::msg_translation::{MessageBusToRpc, RpcToMessageBus},
};
use mbus_api::{
    v0::{
        CreatePool,
        CreateReplica,
        DestroyPool,
        DestroyReplica,
        Node,
        NodeId,
        NodeState,
        Pool,
        PoolId,
        PoolState,
        Protocol,
        Replica,
        ReplicaId,
        ShareReplica,
        UnshareReplica,
    },
    ResourceKind,
};
use rpc::mayastor::Null;
use snafu::ResultExt;
use std::{cmp::Ordering, collections::HashMap};

/// Wrapper over a `Node` plus a few useful methods/properties. Includes:
/// all pools and replicas from the node
/// a watchdog to keep track of the node's liveness
/// a lock to serialize mutating gRPC calls
#[derive(Debug, Clone)]
pub(crate) struct NodeWrapper {
    /// inner Node value
    node: Node,
    /// watchdog to track the node state
    watchdog: Watchdog,
    /// gRPC CRUD lock
    lock: Arc<tokio::sync::Mutex<()>>,
    /// pools part of the node
    pools: HashMap<PoolId, PoolWrapper>,
    /// nexuses part of the node
    nexuses: HashMap<NexusId, Nexus>,
}

impl NodeWrapper {
    /// Create a new wrapper for a `Node` with a `deadline` for its watchdog
    pub(crate) fn new(node: &Node, deadline: std::time::Duration) -> Self {
        tracing::debug!("Creating new node {:?}", node);
        Self {
            node: node.clone(),
            watchdog: Watchdog::new(&node.id, deadline),
            pools: Default::default(),
            nexuses: Default::default(),
            lock: Default::default(),
        }
    }

    /// Get `GrpcClient` for this node
    async fn grpc_client(&self) -> Result<GrpcClient, SvcError> {
        GrpcClient::new(&GrpcContext::new(
            self.lock.clone(),
            &self.id,
            &self.node.grpc_endpoint,
        )?)
        .await
    }

    /// Get `GrpcContext` for this node
    pub(crate) fn grpc_context(&self) -> Result<GrpcContext, SvcError> {
        GrpcContext::new(self.lock.clone(), &self.id, &self.node.grpc_endpoint)
    }

    /// Whether the watchdog deadline has expired
    pub(crate) fn registration_expired(&self) -> bool {
        self.watchdog.timestamp().elapsed() > self.watchdog.deadline()
    }

    /// On_register callback when the node is registered with the registry
    pub(crate) async fn on_register(&mut self) {
        self.watchdog.pet().await.ok();
        self.set_state(NodeState::Online);
    }

    /// Update the node state based on the watchdog
    pub(crate) fn update(&mut self) {
        if self.registration_expired() {
            self.set_state(NodeState::Offline);
        }
    }

    /// Set the node state
    pub(crate) fn set_state(&mut self, state: NodeState) {
        if self.node.state != state {
            tracing::info!(
                "Node '{}' changing from {} to {}",
                self.node.id,
                self.node.state.to_string(),
                state.to_string(),
            );
            self.node.state = state;
            if self.node.state == NodeState::Unknown {
                self.watchdog.disarm()
            }
            for (_, pool) in self.pools.iter_mut() {
                pool.set_unknown();
            }
        }
    }

    /// Get a mutable reference to the node's watchdog
    pub(crate) fn watchdog_mut(&mut self) -> &mut Watchdog {
        &mut self.watchdog
    }
    /// Get the inner node
    pub(crate) fn node(&self) -> &Node {
        &self.node
    }
    /// Get all pools
    pub(crate) fn pools(&self) -> Vec<PoolWrapper> {
        self.pools.values().cloned().collect()
    }
    /// Get pool from `pool_id` or None
    pub(crate) fn pool(&self, pool_id: &PoolId) -> Option<&PoolWrapper> {
        self.pools.get(pool_id)
    }
    /// Get all replicas
    pub(crate) fn replicas(&self) -> Vec<Replica> {
        let replicas = self.pools.iter().map(|p| p.1.replicas());
        replicas.flatten().collect()
    }
    /// Get all nexuses
    fn nexuses(&self) -> Vec<Nexus> {
        self.nexuses.values().cloned().collect()
    }
    /// Get nexus
    fn nexus(&self, nexus_id: &NexusId) -> Option<&Nexus> {
        self.nexuses.get(nexus_id)
    }
    /// Get replica from `replica_id`
    pub(crate) fn replica(&self, replica_id: &ReplicaId) -> Option<&Replica> {
        self.pools
            .iter()
            .find_map(|p| p.1.replicas.iter().find(|r| &r.uuid == replica_id))
    }
    /// Is the node online
    pub(crate) fn is_online(&self) -> bool {
        self.node.state == NodeState::Online
    }

    /// Reload the node by fetching information from mayastor
    pub(super) async fn reload(&mut self) -> Result<(), SvcError> {
        if self.is_online() {
            tracing::trace!("Reloading node '{}'", self.id);

            let replicas = self.fetch_replicas().await?;
            let pools = self.fetch_pools().await?;
            let nexuses = self.fetch_nexuses().await?;

            self.pools.clear();
            for pool in &pools {
                let replicas = replicas
                    .iter()
                    .filter(|r| r.pool == pool.id)
                    .cloned()
                    .collect::<Vec<_>>();
                self.add_pool_with_replicas(pool, &replicas);
            }
            self.nexuses.clear();
            for nexus in &nexuses {
                self.add_nexus(nexus);
            }
            Ok(())
        } else {
            tracing::trace!(
                "Skipping reload of node '{}' since it's '{:?}'",
                self.id,
                self.state
            );
            Err(SvcError::NodeNotOnline {
                node: self.id.to_owned(),
            })
        }
    }

    /// Add pool with replicas
    fn add_pool_with_replicas(&mut self, pool: &Pool, replicas: &[Replica]) {
        self.pools
            .insert(pool.id.clone(), PoolWrapper::new(&pool, replicas));
    }
    /// Remove pool from node
    fn remove_pool(&mut self, pool: &PoolId) {
        self.pools.remove(&pool);
    }
    /// Add replica
    fn add_replica(&mut self, replica: &Replica) {
        match self.pools.iter_mut().find(|(id, _)| id == &&replica.pool) {
            None => {
                tracing::error!("Can't add replica '{} to pool '{}' because the pool does not exist", replica.uuid, replica.pool);
            }
            Some((_, pool)) => {
                pool.add_replica(replica);
            }
        };
    }
    /// Remove replica from pool
    fn remove_replica(&mut self, pool: &PoolId, replica: &ReplicaId) {
        match self.pools.iter_mut().find(|(id, _)| id == &pool) {
            None => (),
            Some((_, pool)) => {
                pool.remove_replica(replica);
            }
        };
    }
    /// Update a replica's share uri and protocol
    fn share_replica(
        &mut self,
        share: &Protocol,
        uri: &str,
        pool: &PoolId,
        replica: &ReplicaId,
    ) {
        match self.pools.iter_mut().find(|(id, _)| id == &pool) {
            None => (),
            Some((_, pool)) => {
                pool.update_replica(replica, share, uri);
            }
        };
    }
    /// Unshare a replica by removing its share protocol and uri
    fn unshare_replica(&mut self, pool: &PoolId, replica: &ReplicaId) {
        self.share_replica(&Protocol::Off, "", pool, replica);
    }
    /// Add a new nexus to the node
    fn add_nexus(&mut self, nexus: &Nexus) {
        self.nexuses.insert(nexus.uuid.clone(), nexus.clone());
    }
    /// Remove nexus from the node
    fn remove_nexus(&mut self, nexus: &NexusId) {
        self.nexuses.remove(nexus);
    }
    /// Update a nexus share uri
    fn share_nexus(&mut self, uri: &str, nexus: &NexusId) {
        match self.nexuses.get_mut(nexus) {
            None => (),
            Some(nexus) => {
                nexus.device_uri = uri.to_string();
            }
        }
    }
    /// Unshare a nexus by removing its share uri
    fn unshare_nexus(&mut self, nexus: &NexusId) {
        self.share_nexus("", nexus);
    }
    /// Add a Child to the nexus
    fn add_child(&mut self, nexus: &NexusId, child: &Child) {
        match self.nexuses.get_mut(nexus) {
            None => (),
            Some(nexus) => {
                nexus.children.push(child.clone());
            }
        }
    }
    /// Remove child from the nexus
    fn remove_child(&mut self, nexus: &NexusId, child: &ChildUri) {
        match self.nexuses.get_mut(nexus) {
            None => (),
            Some(nexus) => {
                nexus.children.retain(|c| &c.uri == child);
            }
        }
    }

    /// Fetch all replicas from this node via gRPC
    async fn fetch_replicas(&self) -> Result<Vec<Replica>, SvcError> {
        let mut ctx = self.grpc_client().await?;
        let rpc_replicas = ctx.client.list_replicas(Null {}).await.context(
            GrpcRequestError {
                resource: ResourceKind::Replica,
                request: "list_replicas",
            },
        )?;
        let rpc_replicas = &rpc_replicas.get_ref().replicas;
        let pools = rpc_replicas
            .iter()
            .map(|p| rpc_replica_to_bus(p, &self.id))
            .collect();
        Ok(pools)
    }
    /// Fetch all pools from this node via gRPC
    async fn fetch_pools(&self) -> Result<Vec<Pool>, SvcError> {
        let mut ctx = self.grpc_client().await?;
        let rpc_pools =
            ctx.client
                .list_pools(Null {})
                .await
                .context(GrpcRequestError {
                    resource: ResourceKind::Pool,
                    request: "list_pools",
                })?;
        let rpc_pools = &rpc_pools.get_ref().pools;
        let pools = rpc_pools
            .iter()
            .map(|p| rpc_pool_to_bus(p, &self.id))
            .collect();
        Ok(pools)
    }
    /// Fetch all nexuses from the node via gRPC
    async fn fetch_nexuses(&self) -> Result<Vec<Nexus>, SvcError> {
        let mut ctx = self.grpc_client().await?;
        let rpc_nexuses =
            ctx.client
                .list_nexus(Null {})
                .await
                .context(GrpcRequestError {
                    resource: ResourceKind::Nexus,
                    request: "list_nexus",
                })?;
        let rpc_nexuses = &rpc_nexuses.get_ref().nexus_list;
        let nexuses = rpc_nexuses
            .iter()
            .map(|n| rpc_nexus_to_bus(n, &self.id))
            .collect();
        Ok(nexuses)
    }
}

impl std::ops::Deref for NodeWrapper {
    type Target = Node;
    fn deref(&self) -> &Self::Target {
        &self.node
    }
}

use crate::core::grpc::{GrpcClient, GrpcClientLocked};
use async_trait::async_trait;
use mbus_api::v0::{
    AddNexusChild,
    Child,
    ChildUri,
    CreateNexus,
    DestroyNexus,
    Nexus,
    NexusId,
    RemoveNexusChild,
    ShareNexus,
    UnshareNexus,
};
use std::{ops::Deref, sync::Arc};

/// CRUD Operations on a locked mayastor `NodeWrapper` such as:
/// pools, replicas, nexuses and their children
#[async_trait]
pub trait ClientOps {
    async fn create_pool(&self, request: &CreatePool)
        -> Result<Pool, SvcError>;
    /// Destroy a pool on the node via gRPC
    async fn destroy_pool(&self, request: &DestroyPool)
        -> Result<(), SvcError>;
    /// Create a replica on the pool via gRPC
    async fn create_replica(
        &self,
        request: &CreateReplica,
    ) -> Result<Replica, SvcError>;
    /// Share a replica on the pool via gRPC
    async fn share_replica(
        &self,
        request: &ShareReplica,
    ) -> Result<String, SvcError>;
    /// Unshare a replica on the pool via gRPC
    async fn unshare_replica(
        &self,
        request: &UnshareReplica,
    ) -> Result<(), SvcError>;
    /// Destroy a replica on the pool via gRPC
    async fn destroy_replica(
        &self,
        request: &DestroyReplica,
    ) -> Result<(), SvcError>;

    /// Create a nexus on a node via gRPC or MBUS
    async fn create_nexus(
        &self,
        request: &CreateNexus,
    ) -> Result<Nexus, SvcError>;
    /// Destroy a nexus on a node via gRPC or MBUS
    async fn destroy_nexus(
        &self,
        request: &DestroyNexus,
    ) -> Result<(), SvcError>;
    /// Share a nexus on the node via gRPC
    async fn share_nexus(
        &self,
        request: &ShareNexus,
    ) -> Result<String, SvcError>;
    /// Unshare a nexus on the node via gRPC
    async fn unshare_nexus(
        &self,
        request: &UnshareNexus,
    ) -> Result<(), SvcError>;
    /// Add a child to a nexus via gRPC
    async fn add_child(
        &self,
        request: &AddNexusChild,
    ) -> Result<Child, SvcError>;
    /// Remove a child from its parent nexus via gRPC
    async fn remove_child(
        &self,
        request: &RemoveNexusChild,
    ) -> Result<(), SvcError>;
}

/// Internal Operations on a mayastor locked `NodeWrapper` for the implementor
/// of the `ClientOps` trait and the `Registry` itself
#[async_trait]
pub(crate) trait InternalOps {
    /// Get the grpc lock and client pair
    async fn grpc_client_locked(&self) -> Result<GrpcClientLocked, SvcError>;
    /// Get the inner lock, typically used to sync mutating gRPC operations
    async fn grpc_lock(&self) -> Arc<tokio::sync::Mutex<()>>;
}

/// Getter operations on a mayastor locked `NodeWrapper` to get copies of its
/// resources, such as pools, replicas and nexuses
#[async_trait]
pub(crate) trait GetterOps {
    async fn pools(&self) -> Vec<PoolWrapper>;
    async fn pool(&self, pool_id: &PoolId) -> Option<PoolWrapper>;

    async fn replicas(&self) -> Vec<Replica>;
    async fn replica(&self, replica: &ReplicaId) -> Option<Replica>;

    async fn nexuses(&self) -> Vec<Nexus>;
    async fn nexus(&self, nexus_id: &NexusId) -> Option<Nexus>;
}

#[async_trait]
impl GetterOps for Arc<tokio::sync::Mutex<NodeWrapper>> {
    async fn pools(&self) -> Vec<PoolWrapper> {
        let node = self.lock().await;
        node.pools()
    }
    async fn pool(&self, pool_id: &PoolId) -> Option<PoolWrapper> {
        let node = self.lock().await;
        node.pool(pool_id).cloned()
    }
    async fn replicas(&self) -> Vec<Replica> {
        let node = self.lock().await;
        node.replicas()
    }
    async fn replica(&self, replica: &ReplicaId) -> Option<Replica> {
        let node = self.lock().await;
        node.replica(replica).cloned()
    }
    async fn nexuses(&self) -> Vec<Nexus> {
        let node = self.lock().await;
        node.nexuses()
    }
    async fn nexus(&self, nexus_id: &NexusId) -> Option<Nexus> {
        let node = self.lock().await;
        node.nexus(nexus_id).cloned()
    }
}

#[async_trait]
impl InternalOps for Arc<tokio::sync::Mutex<NodeWrapper>> {
    async fn grpc_client_locked(&self) -> Result<GrpcClientLocked, SvcError> {
        let ctx = self.lock().await.grpc_context()?;
        let client = ctx.connect_locked().await?;
        Ok(client)
    }
    async fn grpc_lock(&self) -> Arc<tokio::sync::Mutex<()>> {
        self.lock().await.lock.clone()
    }
}

#[async_trait]
impl ClientOps for Arc<tokio::sync::Mutex<NodeWrapper>> {
    async fn create_pool(
        &self,
        request: &CreatePool,
    ) -> Result<Pool, SvcError> {
        let mut ctx = self.grpc_client_locked().await?;
        let rpc_pool = ctx.client.create_pool(request.to_rpc()).await.context(
            GrpcRequestError {
                resource: ResourceKind::Pool,
                request: "create_pool",
            },
        )?;
        let pool = rpc_pool_to_bus(&rpc_pool.into_inner(), &request.node);

        self.lock().await.add_pool_with_replicas(&pool, &[]);
        Ok(pool)
    }
    /// Destroy a pool on the node via gRPC
    async fn destroy_pool(
        &self,
        request: &DestroyPool,
    ) -> Result<(), SvcError> {
        let mut ctx = self.grpc_client_locked().await?;
        let _ = ctx.client.destroy_pool(request.to_rpc()).await.context(
            GrpcRequestError {
                resource: ResourceKind::Pool,
                request: "destroy_pool",
            },
        )?;
        self.lock().await.remove_pool(&request.id);
        Ok(())
    }

    /// Create a replica on the pool via gRPC
    async fn create_replica(
        &self,
        request: &CreateReplica,
    ) -> Result<Replica, SvcError> {
        let mut ctx = self.grpc_client_locked().await?;
        let rpc_replica =
            ctx.client.create_replica(request.to_rpc()).await.context(
                GrpcRequestError {
                    resource: ResourceKind::Replica,
                    request: "create_replica",
                },
            )?;

        let replica =
            rpc_replica_to_bus(&rpc_replica.into_inner(), &request.node);
        self.lock().await.add_replica(&replica);
        Ok(replica)
    }

    /// Share a replica on the pool via gRPC
    async fn share_replica(
        &self,
        request: &ShareReplica,
    ) -> Result<String, SvcError> {
        let mut ctx = self.grpc_client_locked().await?;
        let share = ctx
            .client
            .share_replica(request.to_rpc())
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Replica,
                request: "share_replica",
            })?
            .into_inner()
            .uri;
        self.lock().await.share_replica(
            &request.protocol,
            &share,
            &request.pool,
            &request.uuid,
        );
        Ok(share)
    }

    /// Unshare a replica on the pool via gRPC
    async fn unshare_replica(
        &self,
        request: &UnshareReplica,
    ) -> Result<(), SvcError> {
        let mut ctx = self.grpc_client_locked().await?;
        let _ = ctx.client.share_replica(request.to_rpc()).await.context(
            GrpcRequestError {
                resource: ResourceKind::Replica,
                request: "unshare_replica",
            },
        )?;
        self.lock()
            .await
            .unshare_replica(&request.pool, &request.uuid);
        Ok(())
    }

    /// Destroy a replica on the pool via gRPC
    async fn destroy_replica(
        &self,
        request: &DestroyReplica,
    ) -> Result<(), SvcError> {
        let mut ctx = self.grpc_client_locked().await?;
        let _ = ctx.client.destroy_replica(request.to_rpc()).await.context(
            GrpcRequestError {
                resource: ResourceKind::Replica,
                request: "destroy_replica",
            },
        )?;
        self.lock()
            .await
            .remove_replica(&request.pool, &request.uuid);
        Ok(())
    }

    /// Create a nexus on the node via gRPC
    async fn create_nexus(
        &self,
        request: &CreateNexus,
    ) -> Result<Nexus, SvcError> {
        let mut ctx = self.grpc_client_locked().await?;
        let rpc_nexus =
            ctx.client.create_nexus(request.to_rpc()).await.context(
                GrpcRequestError {
                    resource: ResourceKind::Nexus,
                    request: "create_nexus",
                },
            )?;
        let nexus = rpc_nexus_to_bus(&rpc_nexus.into_inner(), &request.node);
        self.lock().await.add_nexus(&nexus);
        Ok(nexus)
    }

    /// Destroy a nexus on the node via gRPC
    async fn destroy_nexus(
        &self,
        request: &DestroyNexus,
    ) -> Result<(), SvcError> {
        let mut ctx = self.grpc_client_locked().await?;
        let _ = ctx.client.destroy_nexus(request.to_rpc()).await.context(
            GrpcRequestError {
                resource: ResourceKind::Nexus,
                request: "destroy_nexus",
            },
        )?;
        self.lock().await.remove_nexus(&request.uuid);
        Ok(())
    }

    /// Share a nexus on the node via gRPC
    async fn share_nexus(
        &self,
        request: &ShareNexus,
    ) -> Result<String, SvcError> {
        let mut ctx = self.grpc_client_locked().await?;
        let share = ctx.client.publish_nexus(request.to_rpc()).await.context(
            GrpcRequestError {
                resource: ResourceKind::Nexus,
                request: "publish_nexus",
            },
        )?;
        let share = share.into_inner().device_uri;
        self.lock().await.share_nexus(&share, &request.uuid);
        Ok(share)
    }

    /// Unshare a nexus on the node via gRPC
    async fn unshare_nexus(
        &self,
        request: &UnshareNexus,
    ) -> Result<(), SvcError> {
        let mut ctx = self.grpc_client_locked().await?;
        let _ = ctx.client.unpublish_nexus(request.to_rpc()).await.context(
            GrpcRequestError {
                resource: ResourceKind::Nexus,
                request: "unpublish_nexus",
            },
        )?;
        self.lock().await.unshare_nexus(&request.uuid);
        Ok(())
    }

    /// Add a child to a nexus via gRPC
    async fn add_child(
        &self,
        request: &AddNexusChild,
    ) -> Result<Child, SvcError> {
        let mut ctx = self.grpc_client_locked().await?;
        let rpc_child =
            ctx.client.add_child_nexus(request.to_rpc()).await.context(
                GrpcRequestError {
                    resource: ResourceKind::Child,
                    request: "add_child_nexus",
                },
            )?;
        let child = rpc_child.into_inner().to_mbus();
        self.lock().await.add_child(&request.nexus, &child);
        Ok(child)
    }

    /// Remove a child from its parent nexus via gRPC
    async fn remove_child(
        &self,
        request: &RemoveNexusChild,
    ) -> Result<(), SvcError> {
        let mut ctx = self.grpc_client_locked().await?;
        let _ = ctx
            .client
            .remove_child_nexus(request.to_rpc())
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Child,
                request: "remove_child_nexus",
            })?;
        self.lock().await.remove_child(&request.nexus, &request.uri);
        Ok(())
    }
}

/// convert rpc pool to a message bus pool
fn rpc_pool_to_bus(rpc_pool: &rpc::mayastor::Pool, id: &NodeId) -> Pool {
    let mut pool = rpc_pool.to_mbus();
    pool.node = id.clone();
    pool
}

/// convert rpc replica to a message bus replica
fn rpc_replica_to_bus(
    rpc_replica: &rpc::mayastor::Replica,
    id: &NodeId,
) -> Replica {
    let mut replica = rpc_replica.to_mbus();
    replica.node = id.clone();
    replica
}

fn rpc_nexus_to_bus(rpc_nexus: &rpc::mayastor::Nexus, id: &NodeId) -> Nexus {
    let mut nexus = rpc_nexus.to_mbus();
    nexus.node = id.clone();
    nexus
}

/// Wrapper over the message bus `Pool` which includes all the replicas
/// and Ord traits to aid pool selection for volume replicas
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PoolWrapper {
    pool: Pool,
    replicas: Vec<Replica>,
}

impl Deref for PoolWrapper {
    type Target = Pool;
    fn deref(&self) -> &Self::Target {
        &self.pool
    }
}

impl PoolWrapper {
    /// New Pool wrapper with the pool and replicas
    pub fn new(pool: &Pool, replicas: &[Replica]) -> Self {
        Self {
            pool: pool.clone(),
            replicas: replicas.into(),
        }
    }

    /// Get the pool's replicas
    pub fn replicas(&self) -> Vec<Replica> {
        self.replicas.clone()
    }
    /// Get replica from the pool
    pub fn replica(&self, replica: &ReplicaId) -> Option<&Replica> {
        self.replicas.iter().find(|r| &r.uuid == replica)
    }

    /// Get the free space
    pub fn free_space(&self) -> u64 {
        if self.pool.capacity >= self.pool.used {
            self.pool.capacity - self.pool.used
        } else {
            // odd, let's report no free space available
            tracing::error!(
                "Pool '{}' has a capacity of '{} B' but is using '{} B'",
                self.pool.id,
                self.pool.capacity,
                self.pool.used
            );
            0
        }
    }

    /// Set pool state as unknown
    pub fn set_unknown(&mut self) {
        self.pool.state = PoolState::Unknown;
    }

    /// Add replica to list
    pub fn add_replica(&mut self, replica: &Replica) {
        self.replicas.push(replica.clone())
    }
    /// Remove replica from list
    pub fn remove_replica(&mut self, uuid: &ReplicaId) {
        self.replicas.retain(|replica| &replica.uuid != uuid)
    }
    /// update replica from list
    pub fn update_replica(
        &mut self,
        uuid: &ReplicaId,
        share: &Protocol,
        uri: &str,
    ) {
        if let Some(replica) = self
            .replicas
            .iter_mut()
            .find(|replica| &replica.uuid == uuid)
        {
            replica.share = share.clone();
            replica.uri = uri.to_string();
        }
    }
}

impl From<&NodeWrapper> for Node {
    fn from(node: &NodeWrapper) -> Self {
        node.node.clone()
    }
}
impl From<NodeWrapper> for Vec<Replica> {
    fn from(node: NodeWrapper) -> Self {
        node.pools
            .values()
            .map(Vec::<Replica>::from)
            .flatten()
            .collect()
    }
}
impl From<NodeWrapper> for Vec<PoolWrapper> {
    fn from(node: NodeWrapper) -> Self {
        node.pools.values().cloned().collect()
    }
}

impl From<PoolWrapper> for Pool {
    fn from(pool: PoolWrapper) -> Self {
        pool.pool
    }
}
impl From<&PoolWrapper> for Pool {
    fn from(pool: &PoolWrapper) -> Self {
        pool.pool.clone()
    }
}
impl From<PoolWrapper> for Vec<Replica> {
    fn from(pool: PoolWrapper) -> Self {
        pool.replicas
    }
}
impl From<&PoolWrapper> for Vec<Replica> {
    fn from(pool: &PoolWrapper) -> Self {
        pool.replicas.clone()
    }
}

// 1. state ( online > degraded )
// 2. smaller n replicas
// (here we should have pool IO stats over time so we can pick less active
// pools rather than the number of replicas which is useless if the volumes
// are not active)
impl PartialOrd for PoolWrapper {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.pool.state.partial_cmp(&other.pool.state) {
            Some(Ordering::Greater) => Some(Ordering::Greater),
            Some(Ordering::Less) => Some(Ordering::Less),
            Some(Ordering::Equal) => {
                match self.replicas.len().cmp(&other.replicas.len()) {
                    Ordering::Greater => Some(Ordering::Greater),
                    Ordering::Less => Some(Ordering::Less),
                    Ordering::Equal => {
                        Some(self.free_space().cmp(&other.free_space()))
                    }
                }
            }
            None => None,
        }
    }
}

impl Ord for PoolWrapper {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.pool.state.partial_cmp(&other.pool.state) {
            Some(Ordering::Greater) => Ordering::Greater,
            Some(Ordering::Less) => Ordering::Less,
            Some(Ordering::Equal) => {
                match self.replicas.len().cmp(&other.replicas.len()) {
                    Ordering::Greater => Ordering::Greater,
                    Ordering::Less => Ordering::Less,
                    Ordering::Equal => {
                        self.free_space().cmp(&other.free_space())
                    }
                }
            }
            None => Ordering::Equal,
        }
    }
}
