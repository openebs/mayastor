use super::{node_traits::*, *};

/// When operating on a resource which is not found, determines whether to
/// Ignore/Fail the operation or try and fetch the latest version, if possible
#[derive(Clone, Debug, Eq, PartialEq)]
enum NotFoundPolicy {
    #[allow(dead_code)]
    Ignore,
    Fetch,
}

impl Default for NotFoundPolicy {
    fn default() -> Self {
        NotFoundPolicy::Fetch
    }
}

/// Registry with NodeWrapperTrait which allows us to get the resources either
/// via gRPC or message bus in a service specific way.
/// Event propagation from mayastor/services would be useful to avoid thrashing
/// mayastor instances with gRPC and services with message bus requests. For now
/// we update the the registry:
/// every `N` seconds as it queries the node service
/// for changes for every request that reaches the instances, it updates itself
/// with the result.
/// `T` is the specific type of the NodeWrapperTrait which allocates Node helper
/// Wrappers.
/// List operations list what the object has been built with or what the cache
/// has. Fetch operations make use of the node wrapper trait to fetch from
/// mayastor nodes/other services.
#[derive(Clone, Default, Debug)]
pub struct Registry<T> {
    nodes: Arc<Mutex<HashMap<NodeId, NodeWrapper>>>,
    update_period: std::time::Duration,
    not_found: NotFoundPolicy,
    _t: PhantomData<T>,
}

impl<T: NodeWrapperTrait + Default + 'static + Clone> Registry<T> {
    /// Create a new registry with the `period` for updates
    pub fn new(period: std::time::Duration) -> Self {
        Self {
            update_period: period,
            ..Default::default()
        }
    }
    /// Start thread which updates the registry
    pub fn start(&self) {
        let registry = self.clone();
        tokio::spawn(async move {
            registry.poller().await;
        });
    }

    /// List all cached node wrappers
    async fn list_nodes_wrapper(&self) -> Vec<NodeWrapper> {
        let nodes = self.nodes.lock().await;
        nodes.values().cloned().collect()
    }

    /// List all cached nodes
    pub async fn list_nodes(&self) -> Vec<Node> {
        let nodes = self.list_nodes_wrapper().await;
        nodes.iter().map(|n| n.node()).collect()
    }

    /// List all cached pool wrappers
    pub async fn list_pools_wrapper(&self) -> Vec<PoolWrapper> {
        let nodes = self.nodes.lock().await;
        nodes
            .values()
            .map(|node| node.pools_wrapper())
            .flatten()
            .collect()
    }

    /// Fetch all pools wrapper
    pub async fn fetch_pools_wrapper(&self) -> Vec<PoolWrapper> {
        match T::fetch_nodes().await {
            Ok(mut nodes) => {
                for node in &mut nodes {
                    self.found_node(node).await;
                }
            }
            Err(error) => {
                tracing::error!(
                    "Failed to fetch the latest node information, '{}'",
                    error
                );
            }
        };

        self.list_pools_wrapper().await
    }

    /// List all cached pools
    pub async fn list_pools(&self) -> Vec<Pool> {
        let nodes = self.nodes.lock().await;
        nodes.values().map(|node| node.pools()).flatten().collect()
    }

    /// List all cached pools from node
    pub async fn list_node_pools(&self, node: &NodeId) -> Vec<Pool> {
        let nodes = self.list_nodes_wrapper().await;
        if let Some(node) = nodes.iter().find(|&n| &n.id() == node) {
            node.pools()
        } else {
            // or return error, node not found?
            vec![]
        }
    }

    /// List all cached replicas
    pub async fn list_replicas(&self) -> Vec<Replica> {
        let nodes = self.nodes.lock().await;
        nodes
            .values()
            .map(|node| node.replicas())
            .flatten()
            .collect()
    }

    /// List all cached replicas from node
    pub async fn list_node_replicas(&self, node: &NodeId) -> Vec<Replica> {
        let nodes = self.list_nodes_wrapper().await;
        if let Some(node) = nodes.iter().find(|&n| &n.id() == node) {
            node.replicas()
        } else {
            // or return error, node not found?
            vec![]
        }
    }

    /// Create pool
    pub async fn create_pool(
        &self,
        request: &CreatePool,
    ) -> Result<Pool, SvcError> {
        let pool = self
            .get_node(&request.node)
            .await?
            .create_pool(request)
            .await?;
        self.on_pool_created(&pool).await;
        Ok(pool)
    }

    /// Get current list of known nodes
    async fn get_known_nodes(&self, node_id: &NodeId) -> Option<NodeWrapper> {
        let nodes = self.nodes.lock().await;
        nodes.get(node_id).cloned()
    }
    /// Get node `node_id`
    async fn get_node(
        &self,
        node_id: &NodeId,
    ) -> Result<NodeWrapper, SvcError> {
        let mut nodes = self.nodes.lock().await;
        let node = match nodes.get(node_id) {
            Some(node) => node.clone(),
            None => {
                if self.not_found == NotFoundPolicy::Fetch {
                    let node = T::new(node_id).await;
                    if let Ok(node) = node {
                        nodes.insert(node.id(), node.clone());
                        node
                    } else {
                        return Err(SvcError::BusNodeNotFound {
                            node_id: node_id.into(),
                        });
                    }
                } else {
                    return Err(SvcError::BusNodeNotFound {
                        node_id: node_id.into(),
                    });
                }
            }
        };
        Ok(node)
    }
    /// Registry events on crud operations
    async fn on_pool_created(&self, pool: &Pool) {
        if let Ok(node) = self.get_node(&pool.node).await {
            // most likely no replicas, but in case it's an "import"
            // let's go ahead and fetch them
            let replicas = node.fetch_replicas().await.unwrap_or_default();
            {
                let mut nodes = self.nodes.lock().await;
                let node = nodes.get_mut(&pool.node);
                if let Some(node) = node {
                    node.on_create_pool(pool, &replicas).await;
                }
            }
        }
    }
    async fn on_pool_destroyed(&self, request: &DestroyPool) {
        let mut nodes = self.nodes.lock().await;
        let node = nodes.get_mut(&request.node);
        if let Some(node) = node {
            node.on_destroy_pool(&request.id)
        }
    }
    async fn on_replica_added(&self, replica: &Replica) {
        let mut nodes = self.nodes.lock().await;
        let node = nodes.get_mut(&replica.node);
        if let Some(node) = node {
            node.on_create_replica(replica);
        }
    }
    async fn on_replica_removed(&self, request: &DestroyReplica) {
        let mut nodes = self.nodes.lock().await;
        let node = nodes.get_mut(&request.node);
        if let Some(node) = node {
            node.on_destroy_replica(&request.pool, &request.uuid);
        }
    }
    async fn reg_update_replica(
        &self,
        node: &NodeId,
        pool: &PoolId,
        id: &ReplicaId,
        share: &Protocol,
        uri: &str,
    ) {
        let mut nodes = self.nodes.lock().await;
        let node = nodes.get_mut(node);
        if let Some(node) = node {
            node.on_update_replica(pool, id, share, uri);
        }
    }

    /// Destroy pool and update registry
    pub async fn destroy_pool(
        &self,
        request: &DestroyPool,
    ) -> Result<(), SvcError> {
        let node = self.get_node(&request.node).await?;
        node.destroy_pool(&request).await?;
        self.on_pool_destroyed(&request).await;
        Ok(())
    }

    /// Create replica and update registry
    pub async fn create_replica(
        &self,
        request: &CreateReplica,
    ) -> Result<Replica, SvcError> {
        let node = self.get_node(&request.node).await?;
        let replica = node.create_replica(&request).await?;
        self.on_replica_added(&replica).await;
        Ok(replica)
    }

    /// Destroy replica and update registry
    pub async fn destroy_replica(
        &self,
        request: &DestroyReplica,
    ) -> Result<(), SvcError> {
        let node = self.get_node(&request.node).await?;
        node.destroy_replica(request).await?;
        self.on_replica_removed(request).await;
        Ok(())
    }

    /// Share replica and update registry
    pub async fn share_replica(
        &self,
        request: &ShareReplica,
    ) -> Result<String, SvcError> {
        let node = self.get_node(&request.node).await?;
        let share = node.share_replica(request).await?;
        self.reg_update_replica(
            &request.node,
            &request.pool,
            &request.uuid,
            &request.protocol,
            &share,
        )
        .await;
        Ok(share)
    }

    /// Unshare replica and update registry
    pub async fn unshare_replica(
        &self,
        request: &UnshareReplica,
    ) -> Result<(), SvcError> {
        let node = self.get_node(&request.node).await?;
        node.unshare_replica(request).await?;
        self.reg_update_replica(
            &request.node,
            &request.pool,
            &request.uuid,
            &Protocol::Off,
            "",
        )
        .await;
        Ok(())
    }

    async fn on_create_nexus(&self, nexus: &Nexus) {
        let mut nodes = self.nodes.lock().await;
        let node = nodes.get_mut(&nexus.node);
        if let Some(node) = node {
            node.on_create_nexus(nexus);
        }
    }
    async fn on_destroy_nexus(&self, request: &DestroyNexus) {
        let mut nodes = self.nodes.lock().await;
        let node = nodes.get_mut(&request.node);
        if let Some(node) = node {
            node.on_destroy_nexus(&request.uuid);
        }
    }
    async fn on_add_nexus_child(
        &self,
        node: &NodeId,
        nexus: &NexusId,
        child: &Child,
    ) {
        let mut nodes = self.nodes.lock().await;
        let node = nodes.get_mut(node);
        if let Some(node) = node {
            node.on_add_child(nexus, child);
        }
    }
    async fn on_remove_nexus_child(&self, request: &RemoveNexusChild) {
        let mut nodes = self.nodes.lock().await;
        let node = nodes.get_mut(&request.node);
        if let Some(node) = node {
            node.on_remove_child(request);
        }
    }
    async fn on_update_nexus(&self, node: &NodeId, nexus: &NexusId, uri: &str) {
        let mut nodes = self.nodes.lock().await;
        let node = nodes.get_mut(node);
        if let Some(node) = node {
            node.on_update_nexus(nexus, uri);
        }
    }

    /// List all cached nexuses
    pub async fn list_nexuses(&self) -> Vec<Nexus> {
        let nodes = self.nodes.lock().await;
        nodes
            .values()
            .map(|node| node.nexuses())
            .flatten()
            .collect()
    }

    /// List all cached nexuses from node
    pub async fn list_node_nexuses(&self, node: &NodeId) -> Vec<Nexus> {
        let nodes = self.list_nodes_wrapper().await;
        if let Some(node) = nodes.iter().find(|&n| &n.id() == node) {
            node.nexuses()
        } else {
            // hmm, or return error, node not found?
            vec![]
        }
    }

    /// Create nexus
    pub async fn create_nexus(
        &self,
        request: &CreateNexus,
    ) -> Result<Nexus, SvcError> {
        let node = self.get_node(&request.node).await?;
        let nexus = node.create_nexus(request).await?;
        self.on_create_nexus(&nexus).await;
        Ok(nexus)
    }

    /// Destroy nexus
    pub async fn destroy_nexus(
        &self,
        request: &DestroyNexus,
    ) -> Result<(), SvcError> {
        let node = self.get_node(&request.node).await?;
        node.destroy_nexus(request).await?;
        self.on_destroy_nexus(request).await;
        Ok(())
    }

    /// Share nexus
    pub async fn share_nexus(
        &self,
        request: &ShareNexus,
    ) -> Result<String, SvcError> {
        let node = self.get_node(&request.node).await?;
        let share = node.share_nexus(request).await?;
        self.on_update_nexus(&request.node, &request.uuid, &share)
            .await;
        Ok(share)
    }

    /// Unshare nexus
    pub async fn unshare_nexus(
        &self,
        request: &UnshareNexus,
    ) -> Result<(), SvcError> {
        let node = self.get_node(&request.node).await?;
        node.unshare_nexus(request).await?;
        self.on_update_nexus(&request.node, &request.uuid, "").await;
        Ok(())
    }

    /// Add nexus child
    pub async fn add_nexus_child(
        &self,
        request: &AddNexusChild,
    ) -> Result<Child, SvcError> {
        let node = self.get_node(&request.node).await?;
        let child = node.add_child(request).await?;
        self.on_add_nexus_child(&request.node, &request.nexus, &child)
            .await;
        Ok(child)
    }

    /// Remove nexus child
    pub async fn remove_nexus_child(
        &self,
        request: &RemoveNexusChild,
    ) -> Result<(), SvcError> {
        let node = self.get_node(&request.node).await?;
        node.remove_child(request).await?;
        self.on_remove_nexus_child(request).await;
        Ok(())
    }

    /// Found this node via the node service
    /// Update its resource list or add it to the registry if not there yet
    async fn found_node(&self, node: &Node) {
        match &node.state {
            NodeState::Online => {
                self.add_or_update_node(node).await;
            }
            state => {
                // if not online, then only update the node state if it already
                // exists in the registry, and don't even try to
                // add it
                let mut registry = self.nodes.lock().await;
                if let Some((_, existing_node)) =
                    registry.iter_mut().find(|(id, _)| id == &&node.id)
                {
                    existing_node.set_state(state.clone());
                }
            }
        }
    }

    /// Mark nodes as missing if they are no longer discoverable by the node
    /// service
    async fn mark_missing_nodes(&self, live_nodes: &[Node]) {
        let mut registry = self.nodes.lock().await;
        for (name, node) in registry.iter_mut() {
            let found = live_nodes.iter().find(|n| &n.id == name);
            // if a node from the registry is not found then mark it as missing
            if found.is_none() {
                node.set_state(NodeState::Unknown);
            }
        }
    }

    /// Update node from the registry
    async fn update_node(&self, mut node: NodeWrapper) {
        // update all resources from the node: nexus, pools, etc...
        // note this is done this way to avoid holding the lock whilst
        // we're doing gRPC requests
        node.update().await;
        let mut registry = self.nodes.lock().await;
        registry.insert(node.id(), node.clone());
    }

    /// Add new node to the registry
    async fn add_node(&self, node: &Node) {
        match T::new(&node.id).await {
            Ok(node) => {
                let mut registry = self.nodes.lock().await;
                registry.insert(node.id(), node.clone());
            }
            Err(error) => {
                tracing::error!(
                    "Error when adding node '{}': {}",
                    node.id,
                    error
                );
            }
        }
    }

    /// Add or update a node (depending on whether the registry it's already in
    /// the registry or not)
    async fn add_or_update_node(&self, node: &Node) {
        let existing_node = self.get_known_nodes(&node.id).await;
        if let Some(node) = existing_node {
            self.update_node(node).await;
        } else {
            self.add_node(node).await;
        }
    }

    /// Poll the node service for the current nodes it knows about
    /// and update our view of their resources by querying the specific
    /// mayastor instances themselves
    async fn poller(&self) {
        loop {
            // collect all the nodes from the node service and then collect
            // all the nexus and pool information from the nodes themselves
            // (depending on the specific trait implementations of T)
            let found_nodes = T::fetch_nodes().await;
            if let Ok(found_nodes) = found_nodes {
                self.mark_missing_nodes(&found_nodes).await;

                for node in &found_nodes {
                    // todo: add "last seen online" kind of thing to the node to
                    // avoid retrying to connect to a crashed/missed node over
                    // and over again when the node service
                    // is not aware of this yet.
                    self.found_node(node).await;
                }
            }

            self.trace_all().await;
            tokio::time::delay_for(self.update_period).await;
        }
    }

    async fn trace_all(&self) {
        let registry = self.nodes.lock().await;
        tracing::trace!("Registry update: {:?}", registry);
    }
}
