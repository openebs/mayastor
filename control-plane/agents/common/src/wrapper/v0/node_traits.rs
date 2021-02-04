use super::*;

/// Context with the gRPC clients
pub struct GrpcContext {
    pub client: MayaClient,
}
pub type MayaClient = MayastorClient<Channel>;
impl GrpcContext {
    pub async fn new(endpoint: String) -> Result<GrpcContext, SvcError> {
        let uri = format!("http://{}", endpoint);
        let uri = http::uri::Uri::from_str(&uri).unwrap();
        let endpoint = tonic::transport::Endpoint::from(uri)
            .timeout(std::time::Duration::from_secs(1));
        let client = MayaClient::connect(endpoint)
            .await
            .context(GrpcConnect {})?;

        Ok(Self {
            client,
        })
    }
}

/// Trait for a Node Replica which can be implemented to interact with mayastor
/// node replicas either via gRPC or MBUS or with a service via MBUS
#[async_trait]
#[clonable]
pub trait NodeReplicaTrait: Send + Sync + Debug + Clone {
    /// Fetch replicas on all pools via gRPC or MBUS
    async fn fetch_replicas(&self) -> Result<Vec<Replica>, SvcError>;

    /// Create a replica on a pool via gRPC or MBUS
    async fn create_replica(
        &self,
        request: &CreateReplica,
    ) -> Result<Replica, SvcError>;

    /// Share a replica on a pool via gRPC or MBUS
    async fn share_replica(
        &self,
        request: &ShareReplica,
    ) -> Result<String, SvcError>;

    /// Unshare a replica on a pool via gRPC or MBUS
    async fn unshare_replica(
        &self,
        request: &UnshareReplica,
    ) -> Result<(), SvcError>;

    /// Destroy a replica on a pool via gRPC or MBUS
    async fn destroy_replica(
        &self,
        request: &DestroyReplica,
    ) -> Result<(), SvcError>;

    /// Update internal replica list following a create
    fn on_create_replica(&mut self, replica: &Replica);
    /// Update internal replica list following a destroy
    fn on_destroy_replica(&mut self, pool: &PoolId, replica: &ReplicaId);
    /// Update internal replica list following an update
    fn on_update_replica(
        &mut self,
        pool: &PoolId,
        replica: &ReplicaId,
        share: &Protocol,
        uri: &str,
    );
}

/// Trait for a Node Pool which can be implemented to interact with mayastor
/// node pools either via gRPC or MBUS or with a service via MBUS
#[async_trait]
#[clonable]
pub trait NodePoolTrait: Send + Sync + Debug + Clone {
    /// Fetch all pools via gRPC or MBUS
    async fn fetch_pools(&self) -> Result<Vec<Pool>, SvcError>;

    /// Create a pool on a node via gRPC or MBUS
    async fn create_pool(&self, request: &CreatePool)
        -> Result<Pool, SvcError>;

    /// Destroy a pool on a node via gRPC or MBUS
    async fn destroy_pool(&self, request: &DestroyPool)
        -> Result<(), SvcError>;

    /// Update internal pool list following a create
    async fn on_create_pool(&mut self, pool: &Pool, replicas: &[Replica]);
    /// Update internal pool list following a destroy
    fn on_destroy_pool(&mut self, pool: &PoolId);
}

/// Trait for a Node Nexus which can be implemented to interact with mayastor
/// node nexuses either via gRPC or MBUS or with a service via MBUS
#[async_trait]
#[clonable]
#[allow(unused_variables)]
pub trait NodeNexusTrait: Send + Sync + Debug + Clone {
    /// Get the internal nexuses
    fn nexuses(&self) -> Vec<Nexus> {
        vec![]
    }

    /// Fetch all nexuses via gRPC or MBUS
    async fn fetch_nexuses(&self) -> Result<Vec<Nexus>, SvcError> {
        Err(SvcError::NotImplemented {})
    }

    /// Create a nexus on a node via gRPC or MBUS
    async fn create_nexus(
        &self,
        request: &CreateNexus,
    ) -> Result<Nexus, SvcError> {
        Err(SvcError::NotImplemented {})
    }

    /// Destroy a nexus on a node via gRPC or MBUS
    async fn destroy_nexus(
        &self,
        request: &DestroyNexus,
    ) -> Result<(), SvcError> {
        Err(SvcError::NotImplemented {})
    }

    /// Share a nexus on the node via gRPC
    async fn share_nexus(
        &self,
        request: &ShareNexus,
    ) -> Result<String, SvcError> {
        Err(SvcError::NotImplemented {})
    }

    /// Unshare a nexus on the node via gRPC
    async fn unshare_nexus(
        &self,
        request: &UnshareNexus,
    ) -> Result<(), SvcError> {
        Err(SvcError::NotImplemented {})
    }

    /// Update internal nexus list following a create
    fn on_create_nexus(&mut self, nexus: &Nexus) {}
    /// Update internal nexus following a share/unshare
    fn on_update_nexus(&mut self, nexus: &NexusId, uri: &str) {}
    /// Update internal nexus list following a destroy
    fn on_destroy_nexus(&mut self, nexus: &NexusId) {}
}

/// Trait for a Node Nexus Children which can be implemented to interact with
/// mayastor node nexus children either via gRPC or MBUS or with a service via
/// MBUS
#[async_trait]
#[clonable]
#[allow(unused_variables)]
pub trait NodeNexusChildTrait: Send + Sync + Debug + Clone {
    /// Fetch all children via gRPC or MBUS
    async fn fetch_children(&self) -> Result<Vec<Child>, SvcError> {
        Err(SvcError::NotImplemented {})
    }

    /// Add a child to a nexus via gRPC or MBUS
    async fn add_child(
        &self,
        request: &AddNexusChild,
    ) -> Result<Child, SvcError> {
        Err(SvcError::NotImplemented {})
    }

    /// Remove a child from a nexus via gRPC or MBUS
    async fn remove_child(
        &self,
        request: &RemoveNexusChild,
    ) -> Result<(), SvcError> {
        Err(SvcError::NotImplemented {})
    }

    /// Update internal nexus children following a create
    fn on_add_child(&mut self, nexus: &NexusId, child: &Child) {}
    /// Update internal nexus children following a remove
    fn on_remove_child(&mut self, request: &RemoveNexusChild) {}
}

/// Trait for a Node which can be implemented to interact with mayastor
/// node replicas either via gRPC or MBUS or with a service via MBUS
#[async_trait]
#[clonable]
pub trait NodeWrapperTrait:
    Send
    + Sync
    + Debug
    + Clone
    + NodeReplicaTrait
    + NodePoolTrait
    + NodeNexusTrait
    + NodeNexusChildTrait
{
    /// New NodeWrapper for the node
    #[allow(clippy::new_ret_no_self)]
    async fn new(node: &NodeId) -> Result<NodeWrapper, SvcError>
    where
        Self: Sized;
    /// Fetch all nodes via the message bus
    async fn fetch_nodes() -> Result<Vec<Node>, SvcError>
    where
        Self: Sized,
    {
        MessageBus::get_nodes().await.context(BusGetNodes {})
    }

    /// Get the internal id
    fn id(&self) -> NodeId;
    /// Get the internal node
    fn node(&self) -> Node;
    /// Get the internal pools
    fn pools(&self) -> Vec<Pool>;
    /// Get the internal pools wrapper
    fn pools_wrapper(&self) -> Vec<PoolWrapper>;
    /// Get the internal replicas
    fn replicas(&self) -> Vec<Replica>;

    /// Check if the node is online
    fn is_online(&self) -> bool;
    /// Fallible Result used by operations that should only proceed with the
    /// node online
    fn online_only(&self) -> Result<(), SvcError> {
        if !self.is_online() {
            Err(SvcError::NodeNotOnline {
                node: self.node().id,
            })
        } else {
            Ok(())
        }
    }

    /// Update this node with the latest information from the message bus and
    /// mayastor
    async fn update(&mut self);
    /// Set the node state
    fn set_state(&mut self, state: NodeState);

    /// Get the gRPC context with the mayastor proto handle
    async fn grpc_client(&self) -> Result<GrpcContext, SvcError> {
        self.online_only()?;
        GrpcContext::new(self.node().grpc_endpoint.clone()).await
    }
}
/// Handy Boxed NodeWrapperTrait
pub type NodeWrapper = Box<dyn NodeWrapperTrait>;

/// Wrapper over the message bus Pools
/// With the respective node and pool replicas
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PoolWrapper {
    pool: Pool,
    replicas: Vec<Replica>,
}

impl PoolWrapper {
    /// New Pool wrapper with the pool and replicas
    pub fn new_from(pool: &Pool, replicas: &[Replica]) -> Self {
        Self {
            pool: pool.clone(),
            replicas: replicas.into(),
        }
    }

    /// Get the internal pool
    pub fn pool(&self) -> Pool {
        self.pool.clone()
    }
    /// Get the pool uuid
    pub fn uuid(&self) -> PoolId {
        self.pool.id.clone()
    }
    /// Get the pool node name
    pub fn node(&self) -> NodeId {
        self.pool.node.clone()
    }
    /// Get the pool state
    pub fn state(&self) -> PoolState {
        self.pool.state.clone()
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

    /// Get all replicas from this pool
    pub fn replicas(&self) -> Vec<Replica> {
        self.replicas.clone()
    }

    /// Add replica to list
    pub fn added_replica(&mut self, replica: &Replica) {
        self.replicas.push(replica.clone())
    }
    /// Remove replica from list
    pub fn removed_replica(&mut self, uuid: &ReplicaId) {
        self.replicas.retain(|replica| &replica.uuid != uuid)
    }
    /// update replica from list
    pub fn updated_replica(
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
