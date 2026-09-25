use crate::{
    bdev::crypto::EncryptionKey,
    core::{BdevStater, BdevStats, CoreError, DeviceHealth, Reactors, ToErrno},
    replica_backend::ReplicaOps,
};
use futures::channel::oneshot;
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
    pub enc_key: Option<EncryptionKey>,
    pub crypto_vbdev_name: Option<String>,
    /// Set to false if you don't want the pool and its replicas to be managed by spdk.
    /// Applicable for non LvsBlobstore pools, such as the LVM.
    pub no_spdk: bool,
}

impl PoolArgs {
    pub fn with_encryption(mut self, encryption_args: Option<EncryptionKey>) -> Self {
        self.crypto_vbdev_name = encryption_args
            .as_ref()
            .map(|_| format!("crypto_{}", self.name));
        self.enc_key = encryption_args;

        self
    }
}

/// Pool metadata args.
#[derive(Clone, Debug, Default)]
pub struct PoolMetadataArgs {
    pub max_expansion: Option<String>,
}

/// PoolBackend is the type of pool underneath Lvs, Lvm, etc
#[derive(Copy, Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub enum PoolBackend {
    #[default]
    Lvs,
    Lvm,
}

/// Arguments for replica creation.
#[derive(Default)]
pub struct ReplicaArgs {
    pub name: String,
    pub size: u64,
    pub uuid: String,
    pub thin: bool,
    pub entity_id: Option<String>,
    pub use_extent_table: Option<bool>,
    pub wipe_super: bool,
}
impl ReplicaArgs {
    /// Create [`ReplicaArgs`] with the given name and size.
    pub fn new<S: Into<String>>(name: S, size: u64) -> Self {
        Self {
            name: name.into(),
            size,
            ..Default::default()
        }
    }
    /// Specify the `wipe_super` argument.
    pub fn wipe_super(mut self, wipe_super: bool) -> Self {
        self.wipe_super = wipe_super;
        self
    }
    /// Specify the `thin` argument.
    pub fn thin(mut self, thin: bool) -> Self {
        self.thin = thin;
        self
    }
}

/// Generic Errors shared by all backends.
/// todo: most common errors should be moved here.
#[derive(Debug, snafu::Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum GenericError {
    #[snafu(display("{message}"))]
    NotFound { message: String },
    #[snafu(display("Failed to reset the stats: {errno}"))]
    StatsReset { errno: Errno },
}
impl From<GenericError> for tonic::Status {
    fn from(e: GenericError) -> Self {
        match e {
            GenericError::NotFound { message } => tonic::Status::not_found(message),
            GenericError::StatsReset { .. } => tonic::Status::internal(e.to_string()),
        }
    }
}
impl ToErrno for GenericError {
    fn to_errno(&self) -> Errno {
        match self {
            GenericError::NotFound { .. } => Errno::ENODEV,
            GenericError::StatsReset { errno } => *errno,
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
        Self::Lvs { source }
    }
}
impl From<crate::lvm::Error> for Error {
    fn from(source: crate::lvm::Error) -> Self {
        Self::Lvm { source }
    }
}
impl From<GenericError> for Error {
    fn from(source: GenericError) -> Self {
        Self::Gen { source }
    }
}
impl From<Error> for tonic::Status {
    fn from(e: Error) -> Self {
        match e {
            Error::Lvs { source } => source.into(),
            Error::Lvm { source } => source.into(),
            Error::Gen { source } => source.into(),
        }
    }
}
impl ToErrno for Error {
    fn to_errno(&self) -> Errno {
        match self {
            Error::Lvs { source } => source.to_errno(),
            Error::Lvm { source } => source.to_errno(),
            Error::Gen { source } => source.to_errno(),
        }
    }
}

/// This interface defines the high level operations which can be done on a
/// pool. Pool-Specific details should be hidden away in the implementation as
/// much as possible, though we can allow for extra pool specific options
/// to be passed as parameters.
#[async_trait::async_trait(?Send)]
pub trait PoolOps: IPoolProps + BdevStater<Stats = BdevStats> + std::fmt::Debug {
    /// Create a replica on this pool with the given arguments.
    async fn create_repl(&self, args: ReplicaArgs) -> Result<Box<dyn ReplicaOps>, Error>;

    /// Destroy the pool itself along with all its replicas.
    async fn destroy(self: Box<Self>) -> Result<(), Error>;

    /// Exports the volume group by unloading all logical volumes.
    /// The pool will no longer be listable until it is imported again.
    async fn export(self: Box<Self>) -> Result<(), Error>;

    /// Rescan the pool, refreshing the file handles (if any).
    /// This can be used for 2 purposes:
    /// 1. to detect any hot-removals without ongoing I/O
    /// 2. to detect disk resize (we can then grow the pool)
    fn rescan(&self) -> Result<(), Error>;

    /// Grows the given pool by filling the entire underlying device(s).
    async fn grow(&self) -> Result<(), Error>;

    /// Reset the error stats of the pool disks.
    async fn reset_errors(&self) -> Result<(), Error>;

    /// Reset stall transitions for the pool.
    async fn reset_stall_transitions(&self) -> Result<(), Error>;

    /// Read SMART/health info for one of this pool's disks, addressed by the
    /// URI/path as returned by `disks()`. The default implementation reads
    /// the disk as a plain kernel device path via smartctl -- correct for
    /// any backend (e.g. LVM) whose `disks()` are plain paths, since those
    /// are never registered as SPDK bdevs. A backend whose `disks()` are
    /// bdev URIs instead (e.g. LVS) overrides this to resolve the disk to
    /// its registered bdev and query it directly.
    async fn read_device_health(&self, disk: &str) -> Result<DeviceHealth, CoreError> {
        crate::core::device_health::read_device_health(disk).await
    }
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
    async fn find(&self, args: &FindPoolArgs) -> Result<Option<Box<dyn PoolOps>>, Error>;
    /// List all pools from this `PoolBackend`.
    async fn list(&self, args: &ListPoolArgs) -> Result<Vec<Box<dyn PoolOps>>, Error>;
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
    fn encrypted(&self) -> bool;
    fn max_expandable_size(&self) -> Option<u64>;
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
            PoolBackend::Lvs => Box::<crate::lvs::PoolLvsFactory>::default() as _,
            PoolBackend::Lvm => Box::<crate::lvm::PoolLvmFactory>::default() as _,
        })
    }
    /// Probe backends for the given name and/or uuid and return the right one.
    pub async fn find<I: Into<FindPoolArgs>>(args: I) -> Result<Box<dyn PoolOps>, Error> {
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

async fn pool_rescanner_() {
    // We typically would have to wait for the rescan to complete, or at least delay the next
    // rescan until the previous one is done, but the rescan itself is a blocking operation and so
    // We're sure it won't be executed concurrently.
    //
    // So we can just spawn it and mostly forget about it (we'll await the reactor down below).
    Reactors::master().send_future(async move {
        for factory in PoolFactory::factories() {
            let pools = match factory.0.list(&ListPoolArgs::default()).await {
                Ok(pools) => pools,
                Err(e) => {
                    tracing::error!("Failed to rescan pools: {e}");
                    continue;
                }
            };

            for pool in pools {
                if let Err(e) = pool.rescan() {
                    tracing::error!("Failed to rescan pool {}: {e}", pool.name());
                }
            }
        }
    });

    // Wait for the reactor to process all the rescan requests before returning.
    wait_reactor().await;
}

async fn wait_reactor() {
    let (s, r) = oneshot::channel::<()>();
    Reactors::master().send_future(async move {
        s.send(()).ok();
    });
    r.await.ok();
}

/// Rescan all pools from all backends.
///
/// This is used to detect any changes in the underlying devices, such as size changes or
/// devices being hot-removed.
///
/// Device removal is not always handled automatically by the pool device layer:
///
/// 1. PCIe processes hot-removal events and will trigger hot-removal of the underlying device and the pool
///
/// 2. AIO/URING devices need IO to be submitted to detect hot-removal, so if the device is not being used, it will not be detected as removed.
///    Rescan on these devices inspects the file handles and will trigger a hot-removal event if the file handle is no longer valid.
///
/// # Note
///
/// Pools are not currently grown automatically, but this would expose a new size of the underlying device to the pool, if it's extended.
///
pub async fn pool_rescanner(period: humantime::Duration) {
    tracing::info!(?period, "Pool handle rescanner is enabled");

    let interval = period.into();
    loop {
        // we could use interval to ensure precise timing, but there's not much point in doing that
        // also if the reactor is busy we don't want to keep pushing rescans...
        tokio::time::sleep(interval).await;

        pool_rescanner_().await;
    }
}
