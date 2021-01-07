use super::{node_traits::*, *};
use mbus_api::Message;

/// Implementation of the trait NodeWrapperVolume for the pool service
#[derive(Debug, Default, Clone)]
pub struct NodeWrapperVolume {
    node: Node,
    pools: HashMap<PoolId, PoolWrapper>,
    nexuses: HashMap<NexusId, Nexus>,
}

#[async_trait]
impl NodePoolTrait for NodeWrapperVolume {
    /// Fetch all pools from this node via MBUS
    async fn fetch_pools(&self) -> Result<Vec<Pool>, SvcError> {
        MessageBus::get_pools(Filter::Node(self.id()))
            .await
            .context(BusGetNodes {})
    }

    /// Create a pool on the node via gRPC
    async fn create_pool(
        &self,
        request: &CreatePool,
    ) -> Result<Pool, SvcError> {
        request.request().await.context(BusCreatePool {})
    }

    /// Destroy a pool on the node via gRPC
    async fn destroy_pool(
        &self,
        request: &DestroyPool,
    ) -> Result<(), SvcError> {
        request.request().await.context(BusCreatePool {})
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
impl NodeReplicaTrait for NodeWrapperVolume {
    /// Fetch all replicas from this node via gRPC
    async fn fetch_replicas(&self) -> Result<Vec<Replica>, SvcError> {
        GetReplicas {
            filter: Filter::Node(self.id()),
        }
        .request()
        .await
        .context(BusGetReplicas {})
        .map(|r| r.0)
    }

    /// Create a replica on the pool via gRPC
    async fn create_replica(
        &self,
        request: &CreateReplica,
    ) -> Result<Replica, SvcError> {
        request.request().await.context(BusGetReplicas {})
    }

    /// Share a replica on the pool via gRPC
    async fn share_replica(
        &self,
        request: &ShareReplica,
    ) -> Result<String, SvcError> {
        request.request().await.context(BusGetReplicas {})
    }

    /// Unshare a replica on the pool via gRPC
    async fn unshare_replica(
        &self,
        request: &UnshareReplica,
    ) -> Result<(), SvcError> {
        request.request().await.context(BusGetReplicas {})
    }

    /// Destroy a replica on the pool via gRPC
    async fn destroy_replica(
        &self,
        request: &DestroyReplica,
    ) -> Result<(), SvcError> {
        request.request().await.context(BusGetReplicas {})
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
impl NodeNexusTrait for NodeWrapperVolume {
    fn nexuses(&self) -> Vec<Nexus> {
        self.nexuses.values().cloned().collect()
    }

    /// Fetch all nexuses from the node via gRPC
    async fn fetch_nexuses(&self) -> Result<Vec<Nexus>, SvcError> {
        let mut ctx = self.grpc_client().await?;
        let rpc_nexuses = ctx
            .client
            .list_nexus(Null {})
            .await
            .context(GrpcListNexuses {})?;
        let rpc_nexuses = &rpc_nexuses.get_ref().nexus_list;
        let nexuses = rpc_nexuses
            .iter()
            .map(|n| rpc_nexus_to_bus(n, self.node.id.clone()))
            .collect();
        Ok(nexuses)
    }

    /// Create a nexus on the node via gRPC
    async fn create_nexus(
        &self,
        request: &CreateNexus,
    ) -> Result<Nexus, SvcError> {
        let mut ctx = self.grpc_client().await?;
        let rpc_nexus = ctx
            .client
            .create_nexus(bus_nexus_to_rpc(request))
            .await
            .context(GrpcCreateNexus {})?;
        Ok(rpc_nexus_to_bus(
            &rpc_nexus.into_inner(),
            self.node.id.clone(),
        ))
    }

    /// Destroy a nexus on the node via gRPC
    async fn destroy_nexus(
        &self,
        request: &DestroyNexus,
    ) -> Result<(), SvcError> {
        let mut ctx = self.grpc_client().await?;
        let _ = ctx
            .client
            .destroy_nexus(bus_nexus_destroy_to_rpc(request))
            .await
            .context(GrpcDestroyNexus {})?;
        Ok(())
    }

    /// Share a nexus on the node via gRPC
    async fn share_nexus(
        &self,
        request: &ShareNexus,
    ) -> Result<String, SvcError> {
        let mut ctx = self.grpc_client().await?;
        let share = ctx
            .client
            .publish_nexus(bus_nexus_share_to_rpc(request))
            .await
            .context(GrpcShareNexus {})?;
        Ok(share.into_inner().device_uri)
    }

    /// Unshare a nexus on the node via gRPC
    async fn unshare_nexus(
        &self,
        request: &UnshareNexus,
    ) -> Result<(), SvcError> {
        let mut ctx = self.grpc_client().await?;
        let _ = ctx
            .client
            .unpublish_nexus(bus_nexus_unshare_to_rpc(request))
            .await
            .context(GrpcUnshareNexus {})?;
        Ok(())
    }

    fn on_create_nexus(&mut self, nexus: &Nexus) {
        self.nexuses.insert(nexus.uuid.clone(), nexus.clone());
    }

    fn on_update_nexus(&mut self, nexus: &NexusId, uri: &str) {
        if let Some(nexus) = self.nexuses.get_mut(nexus) {
            nexus.device_uri = uri.to_string();
        }
    }

    fn on_destroy_nexus(&mut self, nexus: &NexusId) {
        self.nexuses.remove(nexus);
    }
}

#[async_trait]
impl NodeNexusChildTrait for NodeWrapperVolume {
    async fn fetch_children(&self) -> Result<Vec<Child>, SvcError> {
        unimplemented!()
    }

    /// Add a child to a nexus via gRPC
    async fn add_child(
        &self,
        request: &AddNexusChild,
    ) -> Result<Child, SvcError> {
        let mut ctx = self.grpc_client().await?;
        let rpc_child = ctx
            .client
            .add_child_nexus(bus_nexus_child_add_to_rpc(request))
            .await
            .context(GrpcDestroyNexus {})?;
        Ok(rpc_child_to_bus(&rpc_child.into_inner()))
    }

    /// Remove a child from its parent nexus via gRPC
    async fn remove_child(
        &self,
        request: &RemoveNexusChild,
    ) -> Result<(), SvcError> {
        let mut ctx = self.grpc_client().await?;
        let _ = ctx
            .client
            .remove_child_nexus(bus_nexus_child_remove_to_rpc(request))
            .await
            .context(GrpcDestroyNexus {})?;
        Ok(())
    }

    fn on_add_child(&mut self, nexus: &NexusId, child: &Child) {
        if let Some(nexus) = self.nexuses.get_mut(nexus) {
            nexus.children.push(child.clone());
        }
    }

    fn on_remove_child(&mut self, request: &RemoveNexusChild) {
        if let Some(nexus) = self.nexuses.get_mut(&request.nexus) {
            nexus.children.retain(|replica| replica.uri != request.uri)
        }
    }
}

#[async_trait]
impl NodeWrapperTrait for NodeWrapperVolume {
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

impl NodeWrapperVolume {
    /// Fetch node via the message bus
    async fn fetch_node(node: &NodeId) -> Result<Node, SvcError> {
        MessageBus::get_node(node).await.context(BusGetNode {
            node,
        })
    }

    /// New node wrapper for the pool service containing
    /// a list of pools and replicas
    async fn new_wrapper(node: &NodeId) -> Result<NodeWrapperVolume, SvcError> {
        let mut node = Self {
            // if we can't even fetch the node, then no point in proceeding
            node: NodeWrapperVolume::fetch_node(node).await?,
            ..Default::default()
        };

        // if the node is not online, don't even bother trying to connect
        if node.is_online() {
            let pools = node.fetch_pools().await?;
            let replicas = node.fetch_replicas().await?;
            let nexuses = node.fetch_nexuses().await?;

            for pool in &pools {
                let replicas = replicas
                    .iter()
                    .filter(|r| r.pool == pool.id)
                    .cloned()
                    .collect::<Vec<_>>();
                node.on_create_pool(pool, &replicas).await;
            }

            for nexus in &nexuses {
                node.on_create_nexus(nexus);
            }
        }
        // we've got a node, but we might not have the full picture if it's
        // offline
        Ok(node)
    }
}

fn rpc_nexus_to_bus(rpc_nexus: &rpc::mayastor::Nexus, id: NodeId) -> Nexus {
    let rpc_nexus = rpc_nexus.clone();
    Nexus {
        node: id,
        uuid: rpc_nexus.uuid.into(),
        size: rpc_nexus.size,
        state: NexusState::from(rpc_nexus.state),
        children: rpc_nexus
            .children
            .iter()
            .map(|c| rpc_child_to_bus(&c))
            .collect(),
        device_uri: rpc_nexus.device_uri.clone(),
        rebuilds: rpc_nexus.rebuilds,
    }
}
fn rpc_child_to_bus(rpc_child: &rpc::mayastor::Child) -> Child {
    let rpc_child = rpc_child.clone();
    Child {
        uri: rpc_child.uri.into(),
        state: ChildState::from(rpc_child.state),
        rebuild_progress: if rpc_child.rebuild_progress >= 0 {
            Some(rpc_child.rebuild_progress)
        } else {
            None
        },
    }
}
fn bus_nexus_to_rpc(
    request: &CreateNexus,
) -> rpc::mayastor::CreateNexusRequest {
    let request = request.clone();
    rpc::mayastor::CreateNexusRequest {
        uuid: request.uuid.into(),
        size: request.size,
        children: request.children.iter().map(|c| c.to_string()).collect(),
    }
}
fn bus_nexus_share_to_rpc(
    request: &ShareNexus,
) -> rpc::mayastor::PublishNexusRequest {
    let request = request.clone();
    rpc::mayastor::PublishNexusRequest {
        uuid: request.uuid.into(),
        key: request.key.clone().unwrap_or_default(),
        share: request.protocol as i32,
    }
}
fn bus_nexus_unshare_to_rpc(
    request: &UnshareNexus,
) -> rpc::mayastor::UnpublishNexusRequest {
    rpc::mayastor::UnpublishNexusRequest {
        uuid: request.uuid.clone().into(),
    }
}
fn bus_nexus_destroy_to_rpc(
    request: &DestroyNexus,
) -> rpc::mayastor::DestroyNexusRequest {
    rpc::mayastor::DestroyNexusRequest {
        uuid: request.uuid.clone().into(),
    }
}
fn bus_nexus_child_add_to_rpc(
    request: &AddNexusChild,
) -> rpc::mayastor::AddChildNexusRequest {
    let request = request.clone();
    rpc::mayastor::AddChildNexusRequest {
        uuid: request.nexus.into(),
        uri: request.uri.into(),
        norebuild: !request.auto_rebuild,
    }
}
fn bus_nexus_child_remove_to_rpc(
    request: &RemoveNexusChild,
) -> rpc::mayastor::RemoveChildNexusRequest {
    let request = request.clone();
    rpc::mayastor::RemoveChildNexusRequest {
        uuid: request.nexus.into(),
        uri: request.uri.into(),
    }
}
