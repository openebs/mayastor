use super::{node_traits::*, *};

/// Implementation of the trait NodeWrapperPool for the pool service
#[derive(Debug, Default, Clone)]
pub struct NodeWrapperPool {
    node: Node,
    pools: HashMap<PoolId, PoolWrapper>,
}

#[async_trait]
impl NodePoolTrait for NodeWrapperPool {
    /// Fetch all pools from this node via gRPC
    async fn fetch_pools(&self) -> Result<Vec<Pool>, SvcError> {
        let mut ctx = self.grpc_client().await?;
        let rpc_pools = ctx
            .client
            .list_pools(Null {})
            .await
            .context(GrpcListPools {})?;
        let rpc_pools = &rpc_pools.get_ref().pools;
        let pools = rpc_pools
            .iter()
            .map(|p| rpc_pool_to_bus(p, self.node.id.clone()))
            .collect();
        Ok(pools)
    }

    /// Create a pool on the node via gRPC
    async fn create_pool(
        &self,
        request: &CreatePool,
    ) -> Result<Pool, SvcError> {
        let mut ctx = self.grpc_client().await?;
        let rpc_pool = ctx
            .client
            .create_pool(bus_pool_to_rpc(&request))
            .await
            .context(GrpcCreatePool {})?;

        Ok(rpc_pool_to_bus(&rpc_pool.into_inner(), self.id()))
    }

    /// Destroy a pool on the node via gRPC
    async fn destroy_pool(
        &self,
        request: &DestroyPool,
    ) -> Result<(), SvcError> {
        let mut ctx = self.grpc_client().await?;
        let _ = ctx
            .client
            .destroy_pool(bus_pool_destroy_to_rpc(request))
            .await
            .context(GrpcDestroyPool {})?;

        Ok(())
    }

    async fn on_create_pool(&mut self, pool: &Pool, replicas: &[Replica]) {
        self.pools
            .insert(pool.id.clone(), PoolWrapper::new_from(&pool, replicas));
    }

    fn on_destroy_pool(&mut self, pool: &PoolId) {
        self.pools.remove(pool);
    }
}

#[async_trait]
impl NodeReplicaTrait for NodeWrapperPool {
    /// Fetch all replicas from this node via gRPC
    async fn fetch_replicas(&self) -> Result<Vec<Replica>, SvcError> {
        let mut ctx = self.grpc_client().await?;
        let rpc_pools = ctx
            .client
            .list_replicas(Null {})
            .await
            .context(GrpcListPools {})?;
        let rpc_pools = &rpc_pools.get_ref().replicas;
        let pools = rpc_pools
            .iter()
            .map(|p| rpc_replica_to_bus(p, self.node.id.clone()))
            .collect();
        Ok(pools)
    }

    /// Create a replica on the pool via gRPC
    async fn create_replica(
        &self,
        request: &CreateReplica,
    ) -> Result<Replica, SvcError> {
        let mut ctx = self.grpc_client().await?;
        let rpc_replica = ctx
            .client
            .create_replica(bus_replica_to_rpc(request))
            .await
            .context(GrpcCreateReplica {})?;

        Ok(rpc_replica_to_bus(&rpc_replica.into_inner(), self.id()))
    }

    /// Share a replica on the pool via gRPC
    async fn share_replica(
        &self,
        request: &ShareReplica,
    ) -> Result<String, SvcError> {
        let mut ctx = self.grpc_client().await?;
        let share = ctx
            .client
            .share_replica(bus_replica_share_to_rpc(request))
            .await
            .context(GrpcShareReplica {})?;

        Ok(share.into_inner().uri)
    }

    /// Unshare a replica on the pool via gRPC
    async fn unshare_replica(
        &self,
        request: &UnshareReplica,
    ) -> Result<(), SvcError> {
        let mut ctx = self.grpc_client().await?;
        let _ = ctx
            .client
            .share_replica(bus_replica_unshare_to_rpc(request))
            .await
            .context(GrpcUnshareReplica {})?;

        Ok(())
    }

    /// Destroy a replica on the pool via gRPC
    async fn destroy_replica(
        &self,
        request: &DestroyReplica,
    ) -> Result<(), SvcError> {
        let mut ctx = self.grpc_client().await?;
        let _ = ctx
            .client
            .destroy_replica(bus_replica_destroy_to_rpc(request))
            .await
            .context(GrpcDestroyReplica {})?;

        Ok(())
    }

    fn on_create_replica(&mut self, replica: &Replica) {
        if let Some(pool) = self.pools.get_mut(&replica.pool) {
            pool.added_replica(replica);
        }
    }

    fn on_destroy_replica(&mut self, pool: &PoolId, replica: &ReplicaId) {
        if let Some(pool) = self.pools.get_mut(pool) {
            pool.removed_replica(replica)
        }
    }

    fn on_update_replica(
        &mut self,
        pool: &PoolId,
        replica: &ReplicaId,
        share: &Protocol,
        uri: &str,
    ) {
        if let Some(pool) = self.pools.get_mut(pool) {
            pool.updated_replica(replica, share, uri);
        }
    }
}

#[async_trait]
impl NodeWrapperTrait for NodeWrapperPool {
    async fn new(node: &NodeId) -> Result<NodeWrapper, SvcError> {
        Ok(Box::new(Self::new_wrapper(node).await?))
    }

    fn id(&self) -> NodeId {
        self.node.id.clone()
    }
    fn node(&self) -> Node {
        self.node.clone()
    }
    fn pools(&self) -> Vec<Pool> {
        self.pools.values().map(|p| p.pool()).collect()
    }
    fn pools_wrapper(&self) -> Vec<PoolWrapper> {
        self.pools.values().cloned().collect()
    }
    fn replicas(&self) -> Vec<Replica> {
        self.pools
            .values()
            .map(|p| p.replicas())
            .flatten()
            .collect()
    }
    fn is_online(&self) -> bool {
        self.node.state == NodeState::Online
    }

    async fn update(&mut self) {
        match Self::new_wrapper(&self.node.id).await {
            Ok(node) => {
                let old_state = self.node.state.clone();
                *self = node;
                if old_state != self.node.state {
                    tracing::error!(
                        "Node '{}' changed state from '{}' to '{}'",
                        self.node.id,
                        old_state.to_string(),
                        self.node.state.to_string()
                    )
                }
            }
            Err(error) => {
                tracing::error!(
                    "Failed to update the node '{}', error: {}",
                    self.node.id,
                    error
                );
                self.set_state(NodeState::Unknown);
            }
        }
    }
    fn set_state(&mut self, state: NodeState) {
        if self.node.state != state {
            tracing::info!(
                "Node '{}' state is now {}",
                self.node.id,
                state.to_string()
            );
            self.node.state = state;
            for (_, pool) in self.pools.iter_mut() {
                pool.set_unknown();
            }
        }
    }
}

impl NodeWrapperPool {
    /// Fetch node via the message bus
    async fn fetch_node(node: &NodeId) -> Result<Node, SvcError> {
        MessageBus::get_node(node).await.context(BusGetNode {
            node,
        })
    }

    /// New node wrapper for the pool service containing
    /// a list of pools and replicas
    async fn new_wrapper(node: &NodeId) -> Result<NodeWrapperPool, SvcError> {
        let mut node = Self {
            // if we can't even fetch the node, then no point in proceeding
            node: NodeWrapperPool::fetch_node(node).await?,
            ..Default::default()
        };

        // if the node is not online, don't even bother trying to connect
        if node.is_online() {
            let pools = node.fetch_pools().await?;
            let replicas = node.fetch_replicas().await?;

            for pool in &pools {
                let replicas = replicas
                    .iter()
                    .filter(|r| r.pool == pool.id)
                    .cloned()
                    .collect::<Vec<_>>();
                node.on_create_pool(pool, &replicas).await;
            }
        }
        // we've got a node, but we might not have the full picture if it's
        // offline
        Ok(node)
    }
}

impl_no_nexus_child!(NodeWrapperPool);
impl_no_nexus!(NodeWrapperPool);

/// Helper methods to convert between the message bus types and the
/// mayastor gRPC types

/// convert rpc pool to a message bus pool
fn rpc_pool_to_bus(rpc_pool: &rpc::mayastor::Pool, id: NodeId) -> Pool {
    let rpc_pool = rpc_pool.clone();
    Pool {
        node: id,
        id: rpc_pool.name.into(),
        disks: rpc_pool.disks.clone(),
        state: rpc_pool.state.into(),
        capacity: rpc_pool.capacity,
        used: rpc_pool.used,
    }
}

/// convert rpc replica to a message bus replica
fn rpc_replica_to_bus(
    rpc_replica: &rpc::mayastor::Replica,
    id: NodeId,
) -> Replica {
    let rpc_replica = rpc_replica.clone();
    Replica {
        node: id,
        uuid: rpc_replica.uuid.into(),
        pool: rpc_replica.pool.into(),
        thin: rpc_replica.thin,
        size: rpc_replica.size,
        share: rpc_replica.share.into(),
        uri: rpc_replica.uri,
    }
}

/// convert a message bus replica to an rpc replica
fn bus_replica_to_rpc(
    request: &CreateReplica,
) -> rpc::mayastor::CreateReplicaRequest {
    let request = request.clone();
    rpc::mayastor::CreateReplicaRequest {
        uuid: request.uuid.into(),
        pool: request.pool.into(),
        thin: request.thin,
        size: request.size,
        share: request.share as i32,
    }
}

/// convert a message bus replica share to an rpc replica share
fn bus_replica_share_to_rpc(
    request: &ShareReplica,
) -> rpc::mayastor::ShareReplicaRequest {
    let request = request.clone();
    rpc::mayastor::ShareReplicaRequest {
        uuid: request.uuid.into(),
        share: request.protocol as i32,
    }
}

/// convert a message bus replica unshare to an rpc replica unshare
fn bus_replica_unshare_to_rpc(
    request: &UnshareReplica,
) -> rpc::mayastor::ShareReplicaRequest {
    let request = request.clone();
    rpc::mayastor::ShareReplicaRequest {
        uuid: request.uuid.into(),
        share: Protocol::Off as i32,
    }
}

/// convert a message bus pool to an rpc pool
fn bus_pool_to_rpc(request: &CreatePool) -> rpc::mayastor::CreatePoolRequest {
    let request = request.clone();
    rpc::mayastor::CreatePoolRequest {
        name: request.id.into(),
        disks: request.disks,
    }
}

/// convert a message bus replica destroy to an rpc replica destroy
fn bus_replica_destroy_to_rpc(
    request: &DestroyReplica,
) -> rpc::mayastor::DestroyReplicaRequest {
    rpc::mayastor::DestroyReplicaRequest {
        uuid: request.uuid.clone().into(),
    }
}

/// convert a message bus pool destroy to an rpc pool destroy
fn bus_pool_destroy_to_rpc(
    request: &DestroyPool,
) -> rpc::mayastor::DestroyPoolRequest {
    rpc::mayastor::DestroyPoolRequest {
        name: request.id.clone().into(),
    }
}
