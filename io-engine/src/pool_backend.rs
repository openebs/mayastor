use crate::{
    core::{BdevStater, BdevStats, ToErrno},
    replica_backend::ReplicaOps,
};
use nix::errno::Errno;
use std::ops::Deref;

/// PoolArgs is used to translate the input for the grpc
/// Create/Import requests which contains name, uuid & disks.
/// This helps us avoid importing grpc structs in the actual lvs mod
#[derive(Clone, Debug, Default)]
pub struct PoolArgs {
    pub name: String,
    pub disks: Vec<String>,
    pub uuid: Option<String>,
    pub cluster_size: Option<u32>,
    pub md_args: Option<PoolMetadataArgs>,
    pub backend: PoolBackend,
}

/// Pool metadata args.
#[derive(Clone, Debug, Default)]
pub struct PoolMetadataArgs {
    pub md_resv_ratio: Option<f32>,
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
    pub name: String,
    pub size: u64,
    pub uuid: String,
    pub thin: bool,
    pub entity_id: Option<String>,
    pub use_extent_table: Option<bool>,
}

/// Generic Errors shared by all backends.
/// todo: most common errors should be moved here.
#[derive(Debug, snafu::Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum GenericError {
    #[snafu(display("{message}"))]
    NotFound { message: String },
}
impl From<GenericError> for tonic::Status {
    fn from(e: GenericError) -> Self {
        match e {
            GenericError::NotFound {
                message,
            } => tonic::Status::not_found(message),
        }
    }
}
impl ToErrno for GenericError {
    fn to_errno(self) -> Errno {
        match self {
            GenericError::NotFound {
                ..
            } => Errno::ENODEV,
        }
    }
}

/// Aggregated errors for all backends.
#[derive(Debug, snafu::Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("{source}"))]
    Lvs { source: crate::lvs::LvsError },
    #[snafu(display("{source}"))]
    Lvm { source: crate::lvm::Error },
    #[snafu(display("{source}"))]
    Gen { source: GenericError },
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
impl From<GenericError> for Error {
    fn from(source: GenericError) -> Self {
        Self::Gen {
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
            Error::Gen {
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
            Error::Gen {
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

    /// Grows the given pool by filling the entire underlying device(s).
    async fn grow(&self) -> Result<(), Error>;
}

/// Interface for a pool factory which can be used for various
/// pool creation and listings, for a specific backend type.
#[async_trait::async_trait(?Send)]
pub trait IPoolFactory {
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
    pub fn name_uuid(name: String, uuid: &Option<String>) -> Self {
        Self::NameUuid {
            name,
            uuid: uuid.to_owned(),
        }
    }
    /// Find pools by uuid.
    pub fn uuid(uuid: String) -> Self {
        Self::Uuid(uuid)
    }
    /// Back compat which finds pools by uuid and fallback to name.
    pub fn uuid_or_name(id: &String) -> Self {
        Self::UuidOrName(id.to_string())
    }
}

/// Pool metadata properties/statistics.
pub struct PoolMetadataInfo {
    pub md_page_size: u32,
    pub md_pages: u64,
    pub md_used_pages: u64,
}

/// Various properties from a pool.
pub trait IPoolProps {
    fn pool_type(&self) -> PoolBackend;
    fn name(&self) -> &str;
    fn uuid(&self) -> String;
    fn disks(&self) -> Vec<String>;
    fn disk_capacity(&self) -> u64;
    fn cluster_size(&self) -> u32;
    fn page_size(&self) -> Option<u32>;
    fn capacity(&self) -> u64;
    fn used(&self) -> u64;
    fn committed(&self) -> u64;
    fn md_props(&self) -> Option<PoolMetadataInfo>;
}

/// A pool factory helper.
pub struct PoolFactory(Box<dyn IPoolFactory>);
impl PoolFactory {
    /// Get all available backends.
    pub fn all_backends() -> Vec<PoolBackend> {
        vec![PoolBackend::Lvm, PoolBackend::Lvs]
    }
    /// Get all **enabled** backends.
    pub fn backends() -> Vec<PoolBackend> {
        let backends = Self::all_backends().into_iter();
        backends.filter(|b| b.enabled().is_ok()).collect()
    }
    /// Get factories for all **enabled** backends.
    pub fn factories() -> Vec<Self> {
        Self::backends().into_iter().map(Self::new).collect()
    }
    /// Returns the factory for the given backend kind.
    pub fn new(backend: PoolBackend) -> Self {
        Self(match backend {
            PoolBackend::Lvs => {
                Box::<crate::lvs::PoolLvsFactory>::default() as _
            }
            PoolBackend::Lvm => {
                Box::<crate::lvm::PoolLvmFactory>::default() as _
            }
        })
    }
    /// Probe backends for the given name and/or uuid and return the right one.
    pub async fn find<I: Into<FindPoolArgs>>(
        args: I,
    ) -> Result<Box<dyn PoolOps>, Error> {
        let args = args.into();
        let mut error = None;

        for factory in Self::factories() {
            match factory.0.find(&args).await {
                Ok(Some(pool)) => {
                    return Ok(pool);
                }
                Ok(None) => {}
                Err(err) => {
                    error = Some(err);
                }
            }
        }
        Err(error.unwrap_or_else(|| Error::Gen {
            source: GenericError::NotFound {
                message: format!("Pool {args:?} not found"),
            },
        }))
    }
    /// Get the inner factory interface.
    pub fn as_factory(&self) -> &dyn IPoolFactory {
        self.0.deref()
    }
}
