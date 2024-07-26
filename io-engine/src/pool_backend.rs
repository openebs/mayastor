use crate::{
    core::{BdevStater, BdevStats, ToErrno},
    replica_backend::ReplicaOps,
};
use nix::errno::Errno;

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

#[derive(Debug, snafu::Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("{source}"))]
    Lvs { source: crate::lvs::LvsError },
    #[snafu(display("{source}"))]
    Lvm { source: crate::lvm::Error },
}
impl From<crate::lvs::LvsError> for Error {
    fn from(source: crate::lvs::LvsError) -> Self {
        Self::Lvs {
            source,
        }
    }
}
impl From<crate::lvm::Error> for Error {
    fn from(source: crate::lvm::Error) -> Self {
        Self::Lvm {
            source,
        }
    }
}
impl From<Error> for tonic::Status {
    fn from(e: Error) -> Self {
        match e {
            Error::Lvs {
                source,
            } => source.into(),
            Error::Lvm {
                source,
            } => source.into(),
        }
    }
}
impl ToErrno for Error {
    fn to_errno(self) -> Errno {
        match self {
            Error::Lvs {
                source,
            } => source.to_errno(),
            Error::Lvm {
                source,
            } => source.to_errno(),
        }
    }
}

/// This interface defines the high level operations which can be done on a
/// pool. Pool-Specific details should be hidden away in the implementation as
/// much as possible, though we can allow for extra pool specific options
/// to be passed as parameters.
#[async_trait::async_trait(?Send)]
pub trait PoolOps:
    IPoolProps + BdevStater<Stats = BdevStats> + std::fmt::Debug
{
    /// Create a replica on this pool with the given arguments.
    async fn create_repl(
        &self,
        args: ReplicaArgs,
    ) -> Result<Box<dyn ReplicaOps>, Error>;
    /// Destroy the pool itself along with all its replicas.
    async fn destroy(self: Box<Self>) -> Result<(), Error>;
    /// Exports the volume group by unloading all logical volumes.
    /// The pool will no longer be listable until it is imported again.
    async fn export(self: Box<Self>) -> Result<(), Error>;
}

/// Interface for a pool factory which can be used for various
/// pool creation and listings, for a specific backend type.
#[async_trait::async_trait(?Send)]
pub trait PoolFactory {
    /// Create a pool using the provided arguments.
    async fn create(&self, args: PoolArgs) -> Result<Box<dyn PoolOps>, Error>;
    /// Import a pool (do not create it!) using the provided arguments.
    async fn import(&self, args: PoolArgs) -> Result<Box<dyn PoolOps>, Error>;
    /// Find the pool which matches the given arguments.
    /// # Note: the disks are not currently matched.
    async fn find(
        &self,
        args: &FindPoolArgs,
    ) -> Result<Option<Box<dyn PoolOps>>, Error>;
    /// List all pools from this `PoolBackend`.
    async fn list(
        &self,
        args: &ListPoolArgs,
    ) -> Result<Vec<Box<dyn PoolOps>>, Error>;
    /// The pool backend type.
    fn backend(&self) -> PoolBackend;
}

/// List pools using filters.
#[derive(Default, Debug)]
pub struct ListPoolArgs {
    /// Filter using the pool name.
    pub name: Option<String>,
    /// Filter using the pool backend type.
    pub backend: Option<PoolBackend>,
    /// Filter using the pool uuid.
    pub uuid: Option<String>,
}
impl ListPoolArgs {
    /// A new `Self` with only the name specified.
    pub fn new_named(name: Option<String>) -> Self {
        Self {
            name,
            ..Default::default()
        }
    }
}
/// Probe for pools using these criteria.
#[derive(Debug, Clone)]
pub enum FindPoolArgs {
    Uuid(String),
    UuidOrName(String),
    NameUuid { name: String, uuid: Option<String> },
}
impl From<&PoolArgs> for FindPoolArgs {
    fn from(value: &PoolArgs) -> Self {
        Self::NameUuid {
            name: value.name.to_owned(),
            uuid: value.uuid.to_owned(),
        }
    }
}
impl FindPoolArgs {
    /// Find pools by name and optional uuid.
    pub fn name_uuid(name: &str, uuid: &Option<String>) -> Self {
        Self::NameUuid {
            name: name.to_owned(),
            uuid: uuid.to_owned(),
        }
    }
    /// Find pools by uuid.
    pub fn uuid(uuid: &String) -> Self {
        Self::Uuid(uuid.to_string())
    }
    /// Back compat which finds pools by uuid and fallback to name.
    pub fn uuid_or_name(id: &String) -> Self {
        Self::UuidOrName(id.to_string())
    }
}

/// Various properties from a pool.
pub trait IPoolProps {
    fn name(&self) -> &str;
    fn uuid(&self) -> String;
    fn disks(&self) -> Vec<String>;
    fn used(&self) -> u64;
    fn capacity(&self) -> u64;
    fn committed(&self) -> u64;
    fn pool_type(&self) -> PoolBackend;
    fn cluster_size(&self) -> u32;
}
