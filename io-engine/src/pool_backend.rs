use crate::replica_backend::ReplicaOps;

/// PoolArgs is used to translate the input for the grpc
/// Create/Import requests which contains name, uuid & disks.
/// This help us avoid importing grpc structs in the actual lvs mod
#[derive(Clone, Debug, Default)]
pub struct PoolArgs {
    pub name: String,
    pub disks: Vec<String>,
    pub uuid: Option<String>,
    pub cluster_size: Option<u32>,
    pub backend: PoolBackend,
}

/// PoolBackend is the type of pool underneath Lvs, Lvm, etc
#[derive(Copy, Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub enum PoolBackend {
    #[default]
    Lvs,
    Lvm,
}

/// Arguments for replica creation.
pub struct ReplicaArgs {
    pub(crate) name: String,
    pub(crate) size: u64,
    pub(crate) uuid: String,
    pub(crate) thin: bool,
    pub(crate) entity_id: Option<String>,
}

/// This interface defines the high level operations which can be done on a
/// pool. Pool-Specific details should be hidden away in the implementation as
/// much as possible, though we can allow for extra pool specific options
/// to be passed as parameters.
#[async_trait::async_trait(?Send)]
pub trait PoolOps {
    type Replica: ReplicaOps + std::fmt::Debug;
    type Error: Into<tonic::Status> + std::fmt::Display;

    /// List all replicas which exist on this pool.
    async fn replicas(&self) -> Result<Vec<Self::Replica>, Self::Error>;
    /// Create a replica on this pool with the given arguments.
    async fn create_repl(
        &self,
        args: ReplicaArgs,
    ) -> Result<Self::Replica, Self::Error>;
    /// Destroy the pool itself along with all its replicas.
    async fn destroy(self) -> Result<(), Self::Error>;
    /// Exports the volume group by unloading all logical volumes.
    /// The pool will no longer be listable until it is imported again.
    async fn export(self) -> Result<(), Self::Error>;
}
