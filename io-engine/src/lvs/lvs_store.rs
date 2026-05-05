use byte_unit::Byte;
use core::f64;
use events_api::event::EventAction;
use futures::channel::oneshot;
use nix::errno::Errno;
use parking_lot::RwLock;
use pin_utils::core_reexport::fmt::Formatter;
use std::{
    convert::TryFrom, fmt::Debug, os::raw::c_void, pin::Pin, ptr::NonNull, str::FromStr,
    time::Instant,
};
use url::Url;

use spdk_rs::libspdk::{
    bdev_aio_rescan, bdev_uring_rescan, spdk_bdev_io, spdk_bdev_update_bs_blockcnt,
    spdk_blob_store, spdk_bs_free_cluster_count, spdk_bs_get_cluster_size,
    spdk_bs_get_max_growable_size, spdk_bs_get_md_len, spdk_bs_get_page_size, spdk_bs_get_used_md,
    spdk_bs_read_super, spdk_bs_total_data_cluster_count, spdk_lvol, spdk_lvol_opts,
    spdk_lvol_opts_init, spdk_lvol_store, spdk_lvs_grow_live, vbdev_get_lvol_store_by_name,
    vbdev_get_lvol_store_by_uuid, vbdev_get_lvs_bdev_by_lvs, vbdev_lvol_create_with_opts,
    vbdev_lvs_bs_bdev_reset, vbdev_lvs_create_ext, vbdev_lvs_create_with_uuid, vbdev_lvs_destruct,
    vbdev_lvs_import, vbdev_lvs_set_timeout, vbdev_lvs_unload, LVOL_CLEAR_WITH_NONE,
    LVOL_CLEAR_WITH_UNMAP, LVS_CLEAR_WITH_NONE,
};

use super::{BsError, ImportErrorReason, Lvol, LvsError, LvsIter, PropName, PropValue};

use crate::{
    bdev::{
        crypto::{create_crypto_vbdev_on_base_bdev, destroy_crypto_vbdev},
        uri, BdevCreateDestroy, PtplFileOps,
    },
    bdev_api::{bdev_destroy, BdevError},
    core::{
        logical_volume::LogicalVolume, snapshot::LvolSnapshotOps, Bdev, IoType,
        MayastorEnvironment, NvmfShareProps, Protocol, Reactors, Share, UnshareProps, UntypedBdev,
    },
    eventing::Event,
    ffihelper::{cb_arg, pair, AsStr, ErrnoResult, FfiResult, IntoCString},
    lvs::{
        lvs_lvol::{LvsLvol, WIPE_SUPER_LEN},
        LvolSnapshotDescriptor,
    },
    pool_backend::{PoolArgs, ReplicaArgs},
    pool_information::{pool_info_write, PoolInfo},
    sleep::mayastor_sleep,
};

static ROUND_TO_MB: u32 = 1024 * 1024;
/// Default spdk cluster size is 4MiB.
static DEFAULT_CLUSTER_SIZE: u32 = 4 * 1024 * 1024;
/// Maximum spdk cluster size can be considered as 1GiB.
static MAX_CLUSTER_SIZE: u32 = 1024 * 1024 * 1024;

impl Debug for Lvs {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let bdev = self.base_bdev_opt();
        let (name, uuid) = match &bdev {
            Some(bdev) => (bdev.name(), bdev.uuid().to_string()),
            None => ("bdev~removing", "~".into()),
        };
        write!(
            f,
            "Lvs '{}' [{}/{}] ({:.2}/{:.2})",
            self.name(),
            name,
            uuid,
            Byte::from(self.available()).get_appropriate_unit(byte_unit::UnitType::Binary),
            Byte::from(self.capacity()).get_appropriate_unit(byte_unit::UnitType::Binary)
        )
    }
}

struct LvsBackendBdevs {
    /// The pool arguments.
    args: PoolArgs,

    /// The bdev ops for the base pool disk.
    bdev_ops: Box<dyn BdevCreateDestroy<Error = BdevError>>,
    /// The name of the base bdev.
    bdev_name: String,
    /// Whether we've created the bdev in this attempt.
    /// If so, then we should cleanup.
    created_bdev: bool,

    /// Whether we've created the crypto bdev in this attempt.
    created_crypto: bool,

    /// The lvs backing bdev, which is either the crypto of the base bdev name.
    pool_bdev_name: String,
}

/// Logical Volume Store (LVS) stores the lvols
#[derive(Clone)]
pub struct Lvs {
    inner: NonNull<spdk_lvol_store>,
}

impl Lvs {
    /// TODO
    pub(super) fn from_inner_ptr(ptr: *mut spdk_lvol_store) -> Self {
        Self {
            inner: NonNull::new(ptr).unwrap(),
        }
    }

    /// TODO
    #[inline(always)]
    pub fn as_inner_ptr(&self) -> *mut spdk_lvol_store {
        self.inner.as_ptr()
    }

    /// TODO
    #[inline(always)]
    pub(super) fn as_inner_ref(&self) -> &spdk_lvol_store {
        unsafe { self.inner.as_ref() }
    }

    /// TODO
    #[inline(always)]
    pub fn blob_store(&self) -> *mut spdk_blob_store {
        self.as_inner_ref().blobstore
    }

    /// generic lvol store callback
    extern "C" fn lvs_cb(sender_ptr: *mut c_void, lvs: *mut spdk_lvol_store, errno: i32) {
        let sender = unsafe { Box::from_raw(sender_ptr as *mut oneshot::Sender<ErrnoResult<Lvs>>) };

        if errno == 0 {
            sender
                .send(Ok(Lvs::from_inner_ptr(lvs)))
                .expect("receiver gone");
        } else {
            sender
                .send(Err(Errno::from_raw(errno.abs())))
                .expect("receiver gone");
        }
    }

    /// callback when operation has been performed on lvol
    extern "C" fn lvs_op_cb(sender: *mut c_void, errno: i32) {
        let sender = unsafe { Box::from_raw(sender as *mut oneshot::Sender<i32>) };
        sender.send(errno).unwrap();
    }

    /// returns a new iterator over all lvs
    pub fn iter() -> LvsIter {
        LvsIter::new(false)
    }

    /// Returns a new iterator over all lvs, including ones which are removing.
    pub fn iter_all() -> LvsIter {
        LvsIter::new(true)
    }

    /// export all LVS instances
    pub async fn export_all() {
        for pool in Self::iter() {
            let _ = pool.export().await;
        }
    }

    /// lookup a lvol store by its name
    pub fn lookup(name: &str) -> Option<Self> {
        let name = name.into_cstring();

        let lvs = unsafe { vbdev_get_lvol_store_by_name(name.as_ptr()) };
        if lvs.is_null() {
            None
        } else {
            Some(Lvs::from_inner_ptr(lvs))
        }
    }

    /// lookup a lvol store by its uuid
    pub fn lookup_by_uuid(uuid: &str) -> Option<Self> {
        let uuid = uuid.into_cstring();

        let lvs = unsafe { vbdev_get_lvol_store_by_uuid(uuid.as_ptr()) };
        if lvs.is_null() {
            None
        } else {
            Some(Lvs::from_inner_ptr(lvs))
        }
    }

    /// Lookup an [`Lvol`] by its string uuid.
    pub fn lookup_lvol_by_uuid_str(&self, uuid: &str) -> Option<Lvol> {
        let uuid = uuid::Uuid::parse_str(uuid).ok()?;
        let uuid = crate::spdk_rs::Uuid::from(uuid);

        let lvol = unsafe {
            spdk_rs::libspdk::spdk_lvs_lvol_get_by_uuid(self.as_inner_ptr(), &uuid.into_raw())
        };
        if lvol.is_null() {
            return None;
        }
        Some(Lvol::from_inner_ptr(lvol))
    }

    /// return the name of the current store
    pub fn name(&self) -> &str {
        self.as_inner_ref().name.as_str()
    }

    /// returns the total capacity of the store
    pub fn capacity(&self) -> u64 {
        let blobs = self.blob_store();
        unsafe { spdk_bs_get_cluster_size(blobs) * spdk_bs_total_data_cluster_count(blobs) }
    }

    /// returns the available capacity
    pub fn available(&self) -> u64 {
        let blobs = self.blob_store();
        unsafe { spdk_bs_get_cluster_size(blobs) * spdk_bs_free_cluster_count(blobs) }
    }

    /// returns the used capacity
    pub fn used(&self) -> u64 {
        self.capacity() - self.available()
    }

    /// returns committed size
    pub fn committed(&self) -> u64 {
        self.lvols().fold(0, |acc, r| acc + r.committed())
    }

    /// returns the base bdev of this lvs
    pub fn base_bdev_(&self) -> UntypedBdev {
        let p = unsafe { (*vbdev_get_lvs_bdev_by_lvs(self.as_inner_ptr())).bdev };
        Bdev::checked_from_ptr(p).unwrap()
    }

    /// returns the base bdev of this lvs
    pub fn base_bdev_name(&self) -> String {
        self.base_bdev_opt()
            .map(|b| b.name().to_string())
            .unwrap_or_else(|| "~removing".to_string())
    }

    /// Returns the base bdev of this lvs if not pending removal.
    pub fn base_bdev_opt(&self) -> Option<UntypedBdev> {
        let lvs = unsafe { vbdev_get_lvs_bdev_by_lvs(self.as_inner_ptr()) };
        if lvs.is_null() {
            return None;
        }
        Bdev::checked_from_ptr(unsafe { (*lvs).bdev })
    }

    /// Returns the base bdev of this lvs if not pending removal.
    pub fn base_bdev(&self) -> Result<UntypedBdev, LvsError> {
        match self.base_bdev_opt() {
            Some(bdev) => Ok(bdev),
            None => Err(LvsError::Invalid {
                source: BsError::LvsRemoving {},
                msg: self.name().to_string(),
            }),
        }
    }

    /// Is the Lvs/pool encrypted.
    pub fn encrypted(&self) -> bool {
        let base = self.base_bdev_opt();
        let driver = base.as_ref().map(|b| b.driver());
        driver == Some("crypto")
    }

    /// Returns blobstore cluster size.
    pub fn blob_cluster_size(&self) -> u64 {
        let blobs = self.blob_store();
        unsafe { spdk_bs_get_cluster_size(blobs) }
    }

    /// Returns blobstore page size.
    pub fn page_size(&self) -> u64 {
        unsafe { spdk_bs_get_page_size(self.blob_store()) }
    }

    /// TODO
    pub fn md_pages(&self) -> u64 {
        unsafe { spdk_bs_get_md_len(self.blob_store()) }
    }

    /// TODO
    pub fn md_used_pages(&self) -> u64 {
        unsafe { spdk_bs_get_used_md(self.blob_store()) }
    }

    /// Size upto which blobstore can be expanded.
    pub fn max_expandable_size(&self) -> Option<u64> {
        let s = unsafe { spdk_bs_get_max_growable_size(self.blob_store()) };
        Some(s)
    }

    /// returns the UUID of the lvs
    pub fn uuid(&self) -> String {
        let t = unsafe { self.as_inner_ref().uuid.u.raw };
        uuid::Uuid::from_bytes(t).to_string()
    }

    /// Adds pool information to in memory cache.
    pub fn add_info(&self) {
        let mut cache = pool_info_write();
        cache.insert(self.name().to_string(), RwLock::new(PoolInfo::default()));
    }

    /// Removes pool information from in memory cache.
    pub fn remove_info(pool: &str) {
        let mut cache = pool_info_write();
        cache.remove(pool);
    }

    /// Enable stall detection on a given Pool.
    fn enable_stall_detection(&self) {
        let environ = MayastorEnvironment::global();
        self.enable_stall_detection_(&environ)
    }

    /// Enable stall detection on a given Pool.
    fn enable_stall_detection_(&self, environ: &MayastorEnvironment) {
        let secs = environ.pool_args.io_stall_deadline.as_secs();
        let rc = unsafe {
            vbdev_lvs_set_timeout(self.as_inner_ptr(), secs, Some(Self::lvstore_timeout_cb))
        };
        if rc != 0 {
            let error = Errno::from_raw(rc);
            error!("{self:?}: failed to enable I/O stall detection: {error:?}");
        } else {
            info!("{self:?}: enabled I/O stall detection @{secs}s");
        }
    }

    /// Disable stall detection on a given Pool if reset is submitted successfully.
    /// This is to stop receiving flood of callbacks as SPDK sends notification
    /// for all IOs which are stuck on the Pool in loop.
    fn disable_stall_detection(&self) {
        if !self.is_stalled() {
            // we only disable the detection when we're already stalled
            return;
        }

        let rc = unsafe { vbdev_lvs_set_timeout(self.as_inner_ptr(), 0, None) };
        if rc != 0 {
            // This can't fail when timeout_in_sec is 0.
            let error = Errno::from_raw(rc.abs());
            error!("{self:?}: failed to disable stall detection: {error}");
        } else {
            info!("{self:?}: disabled I/O stall detection");
        }
    }

    /// Check if the pool metadata indicates we're stalled.
    fn is_stalled(&self) -> bool {
        PoolInfo::get(self.name())
            .map(|guard| guard.read().io_stalled)
            .unwrap_or_default()
    }
    /// Update the pool metadata to indicate we're stalled.
    fn set_stalled(&self) -> Option<bool> {
        PoolInfo::get(self.name()).map(|guard| {
            let mut info = guard.write();
            if !info.io_stalled {
                warn!("{self:?}: detected I/O stall");
                info.io_stalled = true;
                true
            } else {
                false
            }
        })
    }
    /// Update the pool metadata to indicate we're not stalled.
    fn clear_stalled(&self) {
        let Some(pool_lock) = PoolInfo::get(self.name()) else {
            return;
        };
        pool_lock.write().io_stalled = false;
    }

    /// Attempts to reset the pool facing IO stall and disables stall detection
    /// to avoid flood of callbacks if reset is submitted successfully.
    fn mark_io_stalled(&self) {
        let Some(set_stalled) = self.set_stalled() else {
            tracing::error!("{self:?}: not found in cache");
            return;
        };

        // When multiple I/Os are stalled, we get callbacks for each I/O.
        // In this case, we proceed only for the stalled I/O which has set the pool as stalled.
        if !set_stalled {
            return;
        }

        let p = std::ptr::null_mut();
        let rc = unsafe {
            vbdev_lvs_bs_bdev_reset(self.as_inner_ptr(), Some(Self::lvstore_reset_cb), p)
        };
        if rc != 0 {
            // For some reason we've failed to trigger the reset, so we clear the stalled flag
            // since we currently have no way of clearing it otherwise.
            // todo: add out of band way of clearing the stall to handle this corner case.
            self.clear_stalled();
            let error = nix::Error::from_raw(rc.abs());
            tracing::warn!("{self:?}: clearing I/O stall due to reset failure: {error}");
            return;
        }

        let name = self.name().to_string();
        Reactors::master().send_future(async move {
            if let Some(lvs) = Lvs::lookup(&name) {
                // we now disable the stall detection since we don't need to get notified
                // of the stall until such time we recover (ie when the reset completes)
                lvs.disable_stall_detection();
            }
        });
    }

    /// Marks the pool as recovered in the cache, update transition timestamp and re-enables stall detection.
    fn mark_io_resumed(&self) {
        let Some(pool_guard) = PoolInfo::get(self.name()) else {
            return;
        };
        let mut pool_info = pool_guard.write();
        if !pool_info.io_stalled {
            return;
        }

        pool_info.io_stalled = false;
        info!("{self:?}: recovered from I/O stall");

        let environ = MayastorEnvironment::global();
        let max_entries = environ.pool_args.io_stall_transition_threshold * 10;
        if pool_info.transition_timestamps.len() == max_entries as usize {
            let _ = pool_info.transition_timestamps.pop_front();
        }
        pool_info.transition_timestamps.push_back(Instant::now());

        self.enable_stall_detection_(&environ);
    }

    // checks for the disks length and parses to correct format
    pub fn parse_disk(disks: &[String]) -> Result<String, LvsError> {
        let disk = match disks.first() {
            Some(disk) if disks.len() == 1 => {
                if Url::parse(disk).is_err() {
                    format!("aio://{disk}")
                } else {
                    disk.clone()
                }
            }
            _ => {
                return Err(LvsError::Invalid {
                    source: BsError::InvalidArgument {},
                    msg: format!("invalid number {} of devices {:?}", disks.len(), disks),
                })
            }
        };
        Ok(disk)
    }

    /// imports a pool based on its name and base bdev name
    pub async fn import(name: &str, bdev: &str) -> Result<Lvs, LvsError> {
        let (sender, receiver) = pair::<ErrnoResult<Lvs>>();

        debug!("Trying to import lvs '{name}' from '{bdev}'...");

        let mut bdev = UntypedBdev::lookup_by_name(bdev).ok_or(LvsError::InvalidBdev {
            source: BdevError::BdevNotFound {
                name: bdev.to_string(),
            },
            name: name.to_string(),
        })?;

        // examining a bdev that is in-use by an lvs, will hang to avoid this
        // we will determine the usage of the bdev prior to examining it.

        if bdev.is_claimed() {
            return Err(LvsError::Import {
                source: BsError::VolBusy {},
                name: bdev.name().to_string(),
                reason: ImportErrorReason::None,
            });
        }

        let rc = unsafe {
            // EXISTS is SHOULD be returned when we import a lvs with different
            // names this however is not the case.
            vbdev_lvs_import(
                bdev.unsafe_inner_mut_ptr(),
                Some(Self::lvs_cb),
                cb_arg(sender),
            )
        };

        if rc != 0 {
            // as of now, vbdev_lvs_import fails only with -1, even when hitting enomem
            debug_assert_eq!(rc, -1, "Unexpected error for vbdev_lvs_import");
            return Err(LvsError::Import {
                source: BsError::InvalidArgument {},
                name: name.to_string(),
                reason: ImportErrorReason::None,
            });
        }

        // when no pool name can be determined the or failed to compare to the
        // desired pool name EILSEQ is returned
        let lvs = receiver
            .await
            .expect("Cancellation is not supported")
            .map_err(|err| LvsError::Import {
                source: BsError::from_errno(err),
                name: name.into(),
                reason: if err == nix::Error::EILSEQ {
                    ImportErrorReason::NotFound
                } else {
                    ImportErrorReason::IoError
                },
            })?;

        if name != lvs.name() {
            warn!(
                "No lvs with name '{name}' found on this device: '{bdev}'; found lvs: '{}'",
                lvs.name()
            );
            let pool_name = lvs.name().to_string();
            lvs.export().await?;
            Err(LvsError::Import {
                source: BsError::InvalidArgument {},
                name: name.to_string(),
                reason: ImportErrorReason::NameMismatch { name: pool_name },
            })
        } else {
            lvs.share_all().await;
            info!("{:?}: existing lvs imported successfully", lvs);
            lvs.add_info();
            lvs.enable_stall_detection();
            Ok(lvs)
        }
    }

    /// Imports a pool based on its name, uuid and base bdev name.
    #[tracing::instrument(level = "debug", err)]
    pub async fn import_from_args(args: PoolArgs) -> Result<Lvs, LvsError> {
        let backend = LvsBackendBdevs::new(args, false).await?;
        match Self::import_from_args_(&backend).await {
            Ok(lvs) => Ok(lvs),
            Err(error) => {
                backend.undo().await;
                Err(error)
            }
        }
    }

    /// Imports a pool based on its name, uuid and base bdev name.
    async fn import_from_args_(backend: &LvsBackendBdevs) -> Result<Lvs, LvsError> {
        let pool = Self::import(&backend.args.name, &backend.pool_bdev_name).await?;
        // Try to destroy the pending snapshots without catching the error.
        Lvol::destroy_pending_discarded_snapshot().await;
        // if the uuid is provided for the import request check
        // for the pool uuid to make sure it is the correct one
        if let Some(ref uuid) = backend.args.uuid {
            let pool_uuid = pool.uuid();
            if &pool_uuid == uuid {
                Ok(pool)
            } else {
                pool.export().await?;
                Err(LvsError::Import {
                    source: BsError::InvalidArgument {},
                    name: backend.args.name.clone(),
                    reason: ImportErrorReason::UuidMismatch { uuid: pool_uuid },
                })
            }
        } else {
            Ok(pool)
        }
    }

    /// Derive num_md_pages_per_cluster_ratio from max_expansion which can be a factor or absolute size.
    fn md_resv_ratio(args: &PoolArgs, capacity: u64) -> Result<u32, LvsError> {
        let param = match args.md_args.as_ref().and_then(|p| p.max_expansion.clone()) {
            Some(p) => p,
            None => return Ok(100),
        };
        let factor = if let Some(stripped) = param.strip_suffix('x') {
            stripped
                .parse::<f64>()
                .map_err(|error| LvsError::MaxExpansionParse {
                    msg: format!(
                        "Failed to parse factor max_expansion {stripped} as float: {error}"
                    ),
                })?
        } else if param.ends_with('B') {
            let expand_bytes =
                Byte::from_str(&param).map_err(|error| LvsError::MaxExpansionParse {
                    msg: format!("Failed to parse max_expansion {param} as bytes: {error}"),
                })?;
            (expand_bytes.as_u64() as f64 / capacity as f64).ceil()
        } else {
            return Err(LvsError::MaxExpansionParse {
                msg: format!("Max expansion factor {param} does not end with x or B"),
            });
        };
        // The Blobstore ensures that we have enough pages in used_cluster_mask to track the current device size.
        // So, If maxExpansion results is < 1x it still shouldn't matter. Its same as having
        // default reservation. However, it does impact how many md pages per cluster are reserved.
        // For ex. if the factor turns out to be 0.5 then blobstore reserves 1 page per 2 clusters.
        // So lets ensures that we pass at least default reservation.
        Ok((factor.max(1.0) * 100.0) as u32)
    }

    /// Creates a pool on base bdev.
    /// The caller must ensure the base bdev exists.
    /// This function is made public for tests purposes.
    pub async fn create_from_args_inner(args: PoolArgs) -> Result<Lvs, LvsError> {
        assert_eq!(args.disks.len(), 1);
        let bdev_name = args.disks[0].clone();

        let pool_name = args.name.clone().into_cstring();

        let cluster_size = if let Some(cluster_size) = args.cluster_size {
            if cluster_size % ROUND_TO_MB == 0 {
                cluster_size
            } else {
                return Err(LvsError::InvalidClusterSize {
                    name: args.name,
                    msg: format!("{cluster_size}, not multiple of 1MiB"),
                });
            }
        } else {
            DEFAULT_CLUSTER_SIZE
        };

        if cluster_size > MAX_CLUSTER_SIZE {
            return Err(LvsError::InvalidClusterSize {
                name: args.name,
                msg: format!("{cluster_size}, larger than max limit {MAX_CLUSTER_SIZE}"),
            });
        }
        let bdev = UntypedBdev::lookup_by_name(&bdev_name).ok_or(LvsError::InvalidBdev {
            source: BdevError::BdevNotFound {
                name: bdev_name.to_string(),
            },
            name: bdev_name.to_string(),
        })?;
        let bdev_name = bdev_name.into_cstring();
        let bdev_capacity = bdev.num_blocks() * bdev.block_len() as u64;
        let mdp_ratio = Self::md_resv_ratio(&args, bdev_capacity)?;
        let (sender, receiver) = pair::<ErrnoResult<Lvs>>();
        unsafe {
            if let Some(uuid) = &args.uuid {
                let cuuid = uuid.clone().into_cstring();
                vbdev_lvs_create_with_uuid(
                    bdev_name.as_ptr(),
                    pool_name.as_ptr(),
                    cuuid.as_ptr(),
                    cluster_size,
                    // We used to clear a pool with UNMAP but that takes
                    // awfully long time on large SSDs (~
                    // can take an hour). Clearing the pool
                    // is not necessary. Clearing the lvol must be done, but
                    // lvols tend to be small so there the overhead is
                    // acceptable.
                    LVS_CLEAR_WITH_NONE,
                    mdp_ratio,
                    0,
                    Some(Self::lvs_cb),
                    cb_arg(sender),
                )
            } else {
                vbdev_lvs_create_ext(
                    bdev_name.as_ptr(),
                    pool_name.as_ptr(),
                    cluster_size,
                    // We used to clear a pool with UNMAP but that takes
                    // awfully long time on large SSDs (~
                    // can take an hour). Clearing the pool
                    // is not necessary. Clearing the lvol must be done, but
                    // lvols tend to be small so there the overhead is
                    // acceptable.
                    LVS_CLEAR_WITH_NONE,
                    mdp_ratio,
                    0,
                    Some(Self::lvs_cb),
                    cb_arg(sender),
                )
            }
        }
        .to_result(|e| LvsError::PoolCreate {
            source: BsError::from_i32(e),
            name: args.name.clone(),
        })?;

        receiver
            .await
            .expect("Cancellation is not supported")
            .map_err(|err| LvsError::PoolCreate {
                source: BsError::from_errno(err),
                name: args.name.clone(),
            })?;

        match Self::lookup(&args.name) {
            Some(pool) => {
                info!("{pool:?}: new lvs created successfully");
                pool.add_info();
                pool.enable_stall_detection();
                Ok(pool)
            }
            None => Err(LvsError::PoolCreate {
                source: BsError::LvolNotFound {},
                name: args.name.clone(),
            }),
        }
    }

    /// Callback function called by SPDK when IO stalls beyond stall deadline period.
    extern "C" fn lvstore_timeout_cb(ctx: *mut c_void, _bdev_io: *mut spdk_bdev_io) {
        let lv_store = ctx as *mut spdk_lvol_store;
        let lvs: Lvs = Lvs::from_inner_ptr(lv_store);
        lvs.mark_io_stalled();
    }

    /// Callback function called by SPDK when reset completes.
    extern "C" fn lvstore_reset_cb(lvs: *mut spdk_lvol_store, success: bool, _ctx: *mut c_void) {
        let lvs: Lvs = Lvs::from_inner_ptr(lvs);
        let lvs_name = lvs.name().to_string();
        if let Ok(bdev) = lvs.base_bdev() {
            let driver = bdev.driver();
            info!("{lvs:?}: reset completed with success={success}, bdev_type={driver}");
        }
        Reactors::master().send_future(async move {
            if let Some(lvs) = Lvs::lookup(&lvs_name) {
                let bs = lvs.blob_store();
                let lvs_ptr = lvs.as_inner_ptr();
                unsafe {
                    spdk_bs_read_super(bs, Some(Self::bs_super_read_cb), lvs_ptr as *mut c_void)
                }
            }
        });
    }

    /// Callback function called by SPDK when superblock read completes.
    extern "C" fn bs_super_read_cb(ctx: *mut c_void, _errno: i32) {
        let lvs_raw = ctx as *mut spdk_lvol_store;
        let lvs: Lvs = Lvs::from_inner_ptr(lvs_raw);
        lvs.mark_io_resumed();
    }

    /// Imports the pool if it exists, otherwise tries to create a new pool.
    /// This function creates the underlying bdev if it does not exist.
    #[tracing::instrument(level = "debug", err)]
    pub async fn create_or_import(args: PoolArgs) -> Result<Lvs, LvsError> {
        let backend = LvsBackendBdevs::new(args, true).await?;

        match Self::import_from_args_(&backend).await {
            Ok(pool) => Ok(pool),
            Err(LvsError::Import {
                source: BsError::CannotImportLvs {},
                ..
            }) => {
                // No pool found, so we try to create it.
                match Self::create_from_args_inner(backend.args.clone()).await {
                    Err(create) => {
                        backend.undo().await;
                        Err(create)
                    }
                    Ok(pool) => {
                        pool.event(EventAction::Create).generate();
                        Ok(pool)
                    }
                }
            }
            Err(e) => {
                backend.undo().await;
                Err(e)
            }
        }
    }

    /// export the given lvs
    #[tracing::instrument(level = "debug", err)]
    pub async fn export(self) -> Result<(), LvsError> {
        let self_str = format!("{self:?}");

        info!("{self_str}: exporting lvs...");

        let pool = self.name().to_string();
        let base_bdev = self.base_bdev()?.name().to_string();
        let (s, r) = pair::<i32>();

        self.unshare_all().await;

        unsafe { vbdev_lvs_unload(self.as_inner_ptr(), Some(Self::lvs_op_cb), cb_arg(s)) };

        let result = r
            .await
            .expect("callback gone while destroying lvs")
            .to_result(|e| LvsError::Export {
                source: BsError::from_i32(e),
                name: pool.clone(),
            });

        if Lvs::lookup(&pool).is_none() {
            Lvs::remove_info(&pool);
        }

        // todo: if result is EIO error, then delete the bdevs as well here?
        //  note that in this case we'd have to re-lookup the bdevs again as
        //  they may have been hot-removed!
        if let Err(error) = result {
            if Lvs::lookup(&pool).is_none() {
                Self::lvs_cleanup(&base_bdev, "export").await?;
            }
            return Err(error);
        }

        info!("{self_str}: lvs exported successfully. base bdev: {base_bdev}");

        Self::lvs_cleanup(&base_bdev, "export").await?;

        Ok(())
    }

    /// unshare all lvols prior to export or destroy
    async fn unshare_all(&self) {
        for l in self.lvols() {
            // notice we dont use the unshare impl of the bdev
            // here. we do this to avoid the on disk persistence
            let mut bdev = l.as_bdev();
            if let Err(e) = Pin::new(&mut bdev)
                .unshare(Some(UnshareProps::new(false)))
                .await
            {
                error!("{:?}: failed to unshare: {}", l, e.to_string())
            }
        }
    }

    /// share all lvols who have the shared property set, this is implicitly
    /// shared over nvmf
    async fn share_all(&self) {
        let lvols = self.lvols().collect::<Vec<_>>();
        let mut share_lvols = Vec::with_capacity(lvols.len());

        for mut l in lvols {
            // First we unshare to ensure we clean up resources on re-import when the backend
            // is hot-removed and then hot-attached again.
            let unshare = Some(UnshareProps::new(false));
            Pin::new(&mut l).unshare(unshare).await.ok();

            match l.get(PropName::Shared).await {
                Ok(PropValue::Shared(true)) => {
                    share_lvols.push(l);
                }
                Ok(PropValue::Shared(false)) => {
                    debug!("{l:?} not shared on disk")
                }
                _ => {}
            }
        }

        let start = Instant::now();
        loop {
            let lvols = std::mem::take(&mut share_lvols);
            for mut l in lvols {
                // Unsharing is completing asynchronously, but whilst that is happening we can't
                // reshare, and must wait until the bdev is fully unshared.
                // In this case, we add push the share back to the list, and will retry later.
                if crate::core::is_shared(&l.as_bdev()) == Some(Protocol::Nvmf) {
                    share_lvols.push(l);
                    continue;
                }

                let allowed_hosts = match l.get(PropName::AllowedHosts).await {
                    Ok(PropValue::AllowedHosts(hosts)) => hosts,
                    _ => vec![],
                };
                let props = NvmfShareProps::new()
                    .with_allowed_hosts(allowed_hosts)
                    .with_ptpl(l.ptpl().create().unwrap_or_default());
                if let Err(e) = Pin::new(&mut l).share_nvmf(Some(props)).await {
                    error!("failed to share {l:?}: {e}");
                }
            }
            if start.elapsed().as_secs() > 2 {
                let lvol_count = share_lvols.len();
                tracing::warn!("{self:?} failed to auto share {lvol_count} lvols on import");
                break;
            }
            let sleep_for = std::time::Duration::from_millis(50);
            mayastor_sleep(sleep_for).await.ok();
        }
    }

    /// destroys the given pool deleting the on disk super blob before doing so,
    /// un share all targets
    #[tracing::instrument(level = "debug", err)]
    pub async fn destroy(self) -> Result<(), LvsError> {
        let self_str = format!("{self:?}");
        info!("{}: destroying lvs...", self_str);

        let ptpl = self.ptpl();
        let pool = self.name().to_string();
        let (s, r) = pair::<i32>();

        // when destroying a pool unshare all volumes
        self.unshare_all().await;

        let base_bdev = self.base_bdev()?.name().to_string();

        let evt = self.event(EventAction::Delete);

        unsafe { vbdev_lvs_destruct(self.as_inner_ptr(), Some(Self::lvs_op_cb), cb_arg(s)) };

        let result = r
            .await
            .expect("callback gone while destroying lvs")
            .to_result(|e| LvsError::Export {
                source: BsError::from_i32(e),
                name: pool.clone(),
            });

        if Lvs::lookup(&pool).is_none() {
            Lvs::remove_info(&pool);
        }

        if let Err(error) = result {
            if Lvs::lookup(&pool).is_none() {
                Self::lvs_cleanup(&base_bdev, "destroy").await?;
            }
            return Err(error);
        }

        info!("{self_str}: lvs destroyed successfully. base_bdev: {base_bdev}");
        evt.generate();

        Self::lvs_cleanup(&base_bdev, "destroy").await?;

        if let Err(error) = ptpl.destroy() {
            tracing::error!(
                "{self_str}: Failed to clean up persistence through power loss for pool: {error}",
            );
        }

        Ok(())
    }

    async fn lvs_cleanup(base_bdev: &str, op: &str) -> Result<(), LvsError> {
        tracing::debug!(base_bdev, op, "Cleanup of lvs backing storage");

        let Some(mut base_bdev) = UntypedBdev::lookup_by_name(base_bdev) else {
            return Ok(());
        };

        // If the base_bdev is a crypto vbdev then we need to destroy both - the crypto vbdev and it's base.
        if base_bdev.driver() == "crypto" {
            let cbdev = base_bdev.crypto_base_bdev();
            let cbdev_name = cbdev.map(|c| c.name().to_string());
            let name = base_bdev.name();

            if let Err(e) = destroy_crypto_vbdev(name.to_string(), None).await {
                error!("failed to delete crypto vbdev {name} during lvs {op}: {e}");
            }

            // A None cbdev here is highly unlikely as the vbdev can't exist in thin air.
            // If cbdev is somehow None anyway, then the following bdev_destroy will likely
            // fail, and we can let it.
            if let Some(c) = cbdev_name.and_then(|c| UntypedBdev::lookup_by_name(&c)) {
                base_bdev = c;
            }
        }
        debug!(
            "Deleting bdev {}, uri {:?}",
            base_bdev.name(),
            base_bdev.bdev_uri_original_str()
        );
        if let Some(u) = base_bdev.bdev_uri_original_str() {
            bdev_destroy(&u).await.map_err(|e| LvsError::Destroy {
                source: e,
                name: base_bdev.name().to_string(),
            })?;
        }

        Ok(())
    }

    /// Rescans the bdev and triggers live LVS grow i.e. without closing the blobs and unloading the blobstore.
    #[tracing::instrument(level = "debug", err)]
    pub async fn grow(&self) -> Result<(), LvsError> {
        info!("{self:?}: growing lvs...");
        let lvs_name = self.name();

        let base_bdev = self.base_bdev()?;
        let disk_bdev = base_bdev
            .crypto_base_bdev()
            .map(Bdev::new)
            .unwrap_or_else(|| base_bdev);

        let uri_str = disk_bdev.bdev_uri_str().unwrap_or_default();
        let url = Url::parse(&uri_str).map_err(|source| LvsError::InvalidBdev {
            source: BdevError::UriParseFailed {
                source,
                uri: uri_str.to_string(),
            },
            name: lvs_name.to_string(),
        })?;

        let bdev = disk_bdev.name().into_cstring();
        info!("Attempting to rescan bdev: {uri_str} part of lvs {lvs_name}");

        // Performs a rescan only for uring or aio devices, this is a no-op for other device types.
        let errno = match url.scheme() {
            "uring" => unsafe { bdev_uring_rescan(bdev.as_ptr().cast()) },
            "aio" => unsafe { bdev_aio_rescan(bdev.as_ptr().cast()) },
            _ => 0,
        };

        if errno != 0 {
            return Err(LvsError::BdevRescanFailed {
                source: BsError::from_i32(errno),
                name: self.base_bdev_name(),
            });
        }

        if self.encrypted() && !self.crypto_vbdev_resized().await {
            error!("crypto bdev {} has not resized", self.base_bdev_name());
            return Err(LvsError::CryptoBdevNotResized {
                name: self.base_bdev_name(),
            });
        }

        let capacity_before_grow = self.capacity();

        let (s, r) = pair::<i32>();

        unsafe {
            let lvs = self.as_inner_ptr();

            // Update block count on spdk_bs_bdev.
            spdk_bdev_update_bs_blockcnt((*lvs).bs_dev);

            // Grow the LVS.
            spdk_lvs_grow_live(lvs, Some(Self::lvs_op_cb), cb_arg(s));
        }

        r.await
            .expect("callback gone while growing lvs")
            .to_result(|e| LvsError::Grow {
                source: BsError::from_i32(e),
                name: lvs_name.to_string(),
            })?;

        if self.capacity() == capacity_before_grow {
            return Err(LvsError::BdevNotExtended {
                name: self.base_bdev_name(),
            });
        }

        info!("{self:?}: lvs has been grown successfully");

        Ok(())
    }

    /// When the underlying AIO bdev is resized, crypto bdev receives SPDK_BDEV_EVENT_RESIZE
    /// event. The crypto bdev then adjusts its block count accordingly. To ensure that this resize
    /// operation completes before proceeding, we wait briefly for the crypto bdev to update.
    /// This delay gives the crypto bdev time to process the resize event asynchronously.
    async fn crypto_vbdev_resized(&self) -> bool {
        for _i in 1..=30 {
            let Some(base_bdev) = self.base_bdev_opt() else {
                return false;
            };
            let disk_bdev = base_bdev
                .crypto_base_bdev()
                .map(Bdev::new)
                .unwrap_or_else(|| base_bdev);
            let disk_bdev_size = disk_bdev.num_blocks() * disk_bdev.block_len() as u64;
            let crypto_bdev_size = base_bdev.num_blocks() * base_bdev.block_len() as u64;
            if crypto_bdev_size == disk_bdev_size {
                return true;
            } else {
                let rx = mayastor_sleep(std::time::Duration::from_millis(100));
                if rx.await.is_err() {
                    error!("failed to wait for mayastor_sleep");
                }
            }
        }
        false
    }

    /// return an iterator for enumerating all snapshots that reside on the pool
    pub fn snapshots(&self) -> Option<impl Iterator<Item = LvolSnapshotDescriptor>> {
        if let Some(bdev) = UntypedBdev::bdev_first() {
            let pool_name = format!("{}/", self.name());
            Some(
                bdev.into_iter()
                    .filter(move |b| {
                        b.driver() == "lvol" && b.aliases().iter().any(|a| a.contains(&pool_name))
                    })
                    .filter_map(|b| {
                        Lvol::try_from(b).ok().and_then(|l| {
                            if l.is_snapshot() {
                                l.lvol_snapshot_descriptor(None)
                            } else {
                                None
                            }
                        })
                    }),
            )
        } else {
            None
        }
    }

    /// return an iterator that filters out all bdevs that patch the pool
    /// signature
    pub fn lvols(&self) -> impl Iterator<Item = Lvol> {
        super::lvol_iter::LvsLvolIter::new(self)
    }

    /// create a new lvol on this pool
    pub async fn create_lvol(
        &self,
        name: &str,
        size: u64,
        uuid: Option<&str>,
        thin: bool,
        entity_id: Option<String>,
    ) -> Result<Lvol, LvsError> {
        self.create_lvol_with_opts(ReplicaArgs {
            name: name.to_owned(),
            size,
            uuid: uuid.unwrap_or("").to_string(),
            thin,
            entity_id,
            wipe_super: true,
            ..Default::default()
        })
        .await
    }

    /// create a new lvol on this pool
    pub async fn create_lvol_with_opts(&self, opts: ReplicaArgs) -> Result<Lvol, LvsError> {
        let clear_method = if self.base_bdev()?.io_type_supported(IoType::Unmap) {
            LVOL_CLEAR_WITH_UNMAP
        } else {
            LVOL_CLEAR_WITH_NONE
        };

        if !opts.uuid.is_empty() && self.lookup_lvol_by_uuid_str(&opts.uuid).is_some() {
            return Err(LvsError::RepExists {
                source: BsError::VolAlreadyExists {},
                name: opts.uuid,
            });
        }

        if UntypedBdev::lookup_by_name(&opts.name).is_some() {
            return Err(LvsError::RepExists {
                source: BsError::VolAlreadyExists {},
                name: opts.name,
            });
        };

        if clear_method != spdk_rs::libspdk::LVS_CLEAR_WITH_UNMAP
            && WIPE_SUPER_LEN > self.available()
        {
            return Err(LvsError::RepCreate {
                source: BsError::NoSpace {},
                name: opts.name,
            });
        }

        // As it stands lvs pools can't grow, so limit the max replica size to
        // the pool capacity.
        if opts.size > self.capacity() {
            return Err(LvsError::RepCreate {
                source: BsError::CapacityOverflow {},
                name: opts.name,
            });
        }

        let (s, r) = pair::<ErrnoResult<*mut spdk_lvol>>();

        let cname = opts.name.clone().into_cstring();
        let cuuid = opts.uuid.clone().into_cstring();

        unsafe {
            let mut lvol_opts: spdk_lvol_opts = std::mem::zeroed();
            spdk_lvol_opts_init(&mut lvol_opts as *mut _);
            lvol_opts.name = cname.as_ptr();
            lvol_opts.size = opts.size;
            lvol_opts.thin_provision = opts.thin;
            if let Some(v) = opts.use_extent_table {
                lvol_opts.use_extent_table = v;
            }
            lvol_opts.clear_method = clear_method;

            if !cuuid.is_empty() {
                lvol_opts.uuid = cuuid.as_ptr();
            }

            vbdev_lvol_create_with_opts(
                self.as_inner_ptr(),
                &lvol_opts as *const _,
                Some(Lvol::lvol_cb),
                cb_arg(s),
            )
        }
        .to_result(|e| LvsError::RepCreate {
            source: BsError::from_i32(e),
            name: opts.name.clone(),
        })?;

        let mut lvol = r
            .await
            .expect("lvol creation callback dropped")
            .map_err(|e| LvsError::RepCreate {
                source: BsError::from_errno(e),
                name: opts.name.clone(),
            })
            .map(Lvol::from_inner_ptr)?;

        if let Some(id) = opts.entity_id {
            if let Err(error) = Pin::new(&mut lvol).set(PropValue::EntityId(id)).await {
                let lvol_uuid = lvol.uuid();
                if let Err(error) = lvol.destroy().await {
                    warn!(
                        "uuid/{lvol_uuid}: failed to destroy lvol after failing to set entity id: {error:?}",
                    );
                }
                return Err(error);
            }
        }

        if opts.wipe_super {
            info!("{lvol:?}: wiping super");

            if let Err(error) = lvol.wipe_super().await {
                // If we fail to destroy it hopefully the control-plane will clean
                // it up, though it's possible it may attempt to use it...
                // todo: address this; with a property?
                let lvol_uuid = lvol.uuid();
                if let Err(error) = lvol.destroy().await {
                    warn!(
                        "uuid/{lvol_uuid}: failed to destroy lvol after failing to wipe super: {error:?}",
                    );
                }
                return Err(error);
            }
        }
        info!("{lvol:?}: created");

        lvol.event(EventAction::Create).generate();
        Ok(lvol)
    }

    /// Get a `PtplFileOps` from `&self`.
    pub(crate) fn ptpl(&self) -> impl PtplFileOps {
        LvsPtpl::from(self)
    }
}

impl LvsBackendBdevs {
    async fn new(mut args: PoolArgs, create: bool) -> Result<Self, LvsError> {
        let disk = Lvs::parse_disk(&args.disks)?;
        let name = &args.name;
        let enc = if args.crypto_vbdev_name.is_some() {
            "encrypted"
        } else {
            "non-encrypted"
        };
        if create {
            info!("Creating or importing {enc} lvs '{name}' from '{disk}'...");
        } else {
            info!("Importing {enc} lvs '{name}' from '{disk}'...");
        }

        let bdev_ops = uri::parse(&disk).map_err(|e| LvsError::InvalidBdev {
            source: e,
            name: args.name.clone(),
        })?;
        let bdev_name = bdev_ops.get_name();

        // If we are requesting for an encrypted pool, then we should lookup existing pool(if any)
        // by pool's bdev name as crypto bdev name.
        let pool_bdev_name = args.crypto_vbdev_name.clone().unwrap_or(bdev_name.clone());

        if let Some(pool) = Lvs::lookup(&args.name) {
            let source = if pool.base_bdev()?.name() == pool_bdev_name {
                // todo: this error makes no sense
                BsError::VolAlreadyExists {}
            } else {
                BsError::InvalidArgument {}
            };
            return if create {
                Err(LvsError::PoolCreate {
                    source,
                    name: args.name.clone(),
                })
            } else {
                let pool_name = pool.base_bdev()?.name().to_string();
                Err(LvsError::Import {
                    source,
                    name: args.name.clone(),
                    reason: ImportErrorReason::NameClash { name: pool_name },
                })
            };
        }

        let created_bdev = UntypedBdev::lookup_by_name(&bdev_name).is_none();

        // Create the underlying bdev.
        let bdev_name = match bdev_ops.create().await {
            Err(e) => match e {
                BdevError::BdevExists { .. } => Ok(bdev_ops.get_name()),
                BdevError::CreateBdevInvalidParams {
                    source: Errno::EEXIST,
                    ..
                } => Ok(bdev_ops.get_name()),
                _ => {
                    tracing::error!("Failed to create pool bdev: {e:?}");
                    Err(LvsError::InvalidBdev {
                        source: e,
                        name: bdev_ops.get_name(),
                    })
                }
            },
            Ok(name) => Ok(name),
        }?;

        // Create crypto bdev now if required.
        let mut created_crypto = false;
        if let Some(ref cname) = args.crypto_vbdev_name {
            if let Some(ref e) = args.enc_key {
                if UntypedBdev::lookup_by_name(cname).is_none() {
                    if let Err(error) = create_crypto_vbdev_on_base_bdev(cname, &bdev_name, e) {
                        let _ = bdev_ops.destroy().await.map_err(|e| {
                            error!(
                                "failed to delete base_bdev {bdev_name} after failed crypto vbdev creation. {e:?}"
                            );
                        });
                        return Err(LvsError::PoolCreate {
                            source: BsError::LvsCryptoVbdev {
                                source: match error {
                                    BdevError::CreateBdevFailed { source, .. } => source,
                                    _ => Errno::EINVAL,
                                },
                            },
                            name: args.name.clone(),
                        });
                    }
                    created_crypto = true;
                }
            }
        }

        // override disks with the bdev name of the top-level bdev
        args.disks = vec![pool_bdev_name.clone()];

        Ok(LvsBackendBdevs {
            args,
            bdev_name: bdev_ops.get_name(),
            bdev_ops,
            created_bdev,
            created_crypto,
            pool_bdev_name,
        })
    }

    /// Cleans up any created bdev.
    async fn undo(self) {
        tracing::info!(
            name = self.args.name,
            bdev_name = self.bdev_name,
            pool_bdev_name = self.pool_bdev_name,
            "Undoing created bdevs for failed pool create/import"
        );

        // destroy crypto vbdev first.
        if self.created_crypto {
            if let Some(c) = self.args.crypto_vbdev_name.as_ref() {
                let key_name = self.args.enc_key.as_ref().map(|e| e.key_name.clone());
                let _ = destroy_crypto_vbdev(c.clone(), key_name)
                    .await
                    .map_err(|e| {
                        error!("failed to delete crypto vbdev {c} after failed pool creation. {e}");
                    });
            }
        }

        if self.created_bdev {
            let bdev_name = self.bdev_name;
            let _ = self.bdev_ops.destroy().await.map_err(|error| {
                // we failed to delete the base_bdev be loud about it
                // there is not much we can do about it here, likely
                // some desc is still holding on to it or something.
                error!(
                    %error,
                    bdev_name,
                    "failed to delete base_bdev after failed pool creation",
                );
            });
        }
    }
}

/// Persist through power loss implementation for an LvsStore (pool).
pub(super) struct LvsPtpl {
    uuid: String,
}
impl LvsPtpl {
    fn uuid(&self) -> &str {
        &self.uuid
    }
}
impl From<&Lvs> for LvsPtpl {
    fn from(lvs: &Lvs) -> Self {
        Self { uuid: lvs.uuid() }
    }
}
impl PtplFileOps for LvsPtpl {
    fn destroy(&self) -> Result<(), std::io::Error> {
        if let Some(path) = self.path() {
            if path.exists() {
                std::fs::remove_dir_all(path)?;
            }
        }
        Ok(())
    }

    fn subpath(&self) -> std::path::PathBuf {
        std::path::PathBuf::from("pool/").join(self.uuid())
    }
}
