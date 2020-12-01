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
    fn on_destroy_replica(&mut self, pool: &str, replica: &str);
    /// Update internal replica list following an update
    fn on_update_replica(
        &mut self,
        pool: &str,
        replica: &str,
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
    fn on_destroy_pool(&mut self, pool: &str);
}

/// Trait for a Node which can be implemented to interact with mayastor
/// node replicas either via gRPC or MBUS or with a service via MBUS
#[async_trait]
#[clonable]
pub trait NodeWrapperTrait:
    Send + Sync + Debug + Clone + NodeReplicaTrait + NodePoolTrait
{
    /// New NodeWrapper for the node
    #[allow(clippy::new_ret_no_self)]
    async fn new(node: &str) -> Result<NodeWrapper, SvcError>
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
    fn id(&self) -> String;
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
    pub fn uuid(&self) -> String {
        self.pool.name.clone()
    }
    /// Get the pool node name
    pub fn node(&self) -> String {
        self.pool.node.clone()
    }
    /// Get the pool state
    pub fn state(&self) -> PoolState {
        self.pool.state.clone()
    }

    /// Get the free space
    pub fn free_space(&self) -> u64 {
        if self.pool.capacity > self.pool.used {
            self.pool.capacity - self.pool.used
        } else {
            // odd, let's report no free space available
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
    pub fn removed_replica(&mut self, uuid: &str) {
        self.replicas.retain(|replica| replica.uuid != uuid)
    }
    /// update replica from list
    pub fn updated_replica(&mut self, uuid: &str, share: &Protocol, uri: &str) {
        if let Some(replica) = self
            .replicas
            .iter_mut()
            .find(|replica| replica.uuid == uuid)
        {
            replica.share = share.clone();
            replica.uri = uri.to_string();
        }
    }
}
