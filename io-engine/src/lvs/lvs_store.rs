use std::{
    convert::TryFrom,
    fmt::Debug,
    os::raw::c_void,
    pin::Pin,
    ptr::NonNull,
};

use byte_unit::Byte;
use events_api::event::EventAction;
use futures::channel::oneshot;
use nix::errno::Errno;
use pin_utils::core_reexport::fmt::Formatter;

use spdk_rs::libspdk::{
    spdk_bdev_update_bs_blockcnt,
    spdk_blob_store,
    spdk_bs_free_cluster_count,
    spdk_bs_get_cluster_size,
    spdk_bs_get_md_len,
    spdk_bs_get_page_size,
    spdk_bs_get_used_md,
    spdk_bs_total_data_cluster_count,
    spdk_lvol,
    spdk_lvol_opts,
    spdk_lvol_opts_init,
    spdk_lvol_store,
    spdk_lvs_grow_live,
    vbdev_get_lvol_store_by_name,
    vbdev_get_lvol_store_by_uuid,
    vbdev_get_lvs_bdev_by_lvs,
    vbdev_lvol_create_with_opts,
    vbdev_lvs_create,
    vbdev_lvs_create_with_uuid,
    vbdev_lvs_destruct,
    vbdev_lvs_import,
    vbdev_lvs_unload,
    LVOL_CLEAR_WITH_NONE,
    LVOL_CLEAR_WITH_UNMAP,
    LVS_CLEAR_WITH_NONE,
};
use url::Url;

use super::{
    BsError,
    ImportErrorReason,
    Lvol,
    LvsError,
    LvsIter,
    PropName,
    PropValue,
};

use crate::{
    bdev::{uri, PtplFileOps},
    bdev_api::{bdev_destroy, BdevError},
    core::{
        logical_volume::LogicalVolume,
        snapshot::LvolSnapshotOps,
        Bdev,
        IoType,
        NvmfShareProps,
        Share,
        UntypedBdev,
    },
    eventing::Event,
    ffihelper::{cb_arg, pair, AsStr, ErrnoResult, FfiResult, IntoCString},
    lvs::{
        lvs_lvol::{LvsLvol, WIPE_SUPER_LEN},
        LvolSnapshotDescriptor,
    },
    pool_backend::{PoolArgs, ReplicaArgs},
};

static ROUND_TO_MB: u32 = 1024 * 1024;
/// Default spdk cluster size is 4MiB.
static DEFAULT_CLUSTER_SIZE: u32 = 4 * 1024 * 1024;
/// Maximum spdk cluster size can be considered as 1GiB.
static MAX_CLUSTER_SIZE: u32 = 1024 * 1024 * 1024;

impl Debug for Lvs {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Lvs '{}' [{}/{}] ({}/{})",
            self.name(),
            self.base_bdev().name(),
            self.base_bdev().uuid(),
            Byte::from(self.available()).get_appropriate_unit(true),
            Byte::from(self.capacity()).get_appropriate_unit(true)
        )
    }
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
    fn as_inner_ref(&self) -> &spdk_lvol_store {
        unsafe { self.inner.as_ref() }
    }

    /// TODO
    #[inline(always)]
    pub fn blob_store(&self) -> *mut spdk_blob_store {
        self.as_inner_ref().blobstore
    }

    /// generic lvol store callback
    extern "C" fn lvs_cb(
        sender_ptr: *mut c_void,
        lvs: *mut spdk_lvol_store,
        errno: i32,
    ) {
        let sender = unsafe {
            Box::from_raw(sender_ptr as *mut oneshot::Sender<ErrnoResult<Lvs>>)
        };

        if errno == 0 {
            sender
                .send(Ok(Lvs::from_inner_ptr(lvs)))
                .expect("receiver gone");
        } else {
            sender
                .send(Err(Errno::from_i32(errno.abs())))
                .expect("receiver gone");
        }
    }

    /// callback when operation has been performed on lvol
    extern "C" fn lvs_op_cb(sender: *mut c_void, errno: i32) {
        let sender =
            unsafe { Box::from_raw(sender as *mut oneshot::Sender<i32>) };
        sender.send(errno).unwrap();
    }

    /// returns a new iterator over all lvols
    pub fn iter() -> LvsIter {
        LvsIter::new()
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

    /// return the name of the current store
    pub fn name(&self) -> &str {
        self.as_inner_ref().name.as_str()
    }

    /// returns the total capacity of the store
    pub fn capacity(&self) -> u64 {
        let blobs = self.blob_store();
        unsafe {
            spdk_bs_get_cluster_size(blobs)
                * spdk_bs_total_data_cluster_count(blobs)
        }
    }

    /// returns the available capacity
    pub fn available(&self) -> u64 {
        let blobs = self.blob_store();
        unsafe {
            spdk_bs_get_cluster_size(blobs) * spdk_bs_free_cluster_count(blobs)
        }
    }

    /// returns the used capacity
    pub fn used(&self) -> u64 {
        self.capacity() - self.available()
    }

    /// returns committed size
    pub fn committed(&self) -> u64 {
        self.lvols()
            .map_or(0, |vols| vols.fold(0, |acc, r| acc + r.committed()))
    }

    /// returns the base bdev of this lvs
    pub fn base_bdev(&self) -> UntypedBdev {
        let p =
            unsafe { (*vbdev_get_lvs_bdev_by_lvs(self.as_inner_ptr())).bdev };
        Bdev::checked_from_ptr(p).unwrap()
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

    /// returns the UUID of the lvs
    pub fn uuid(&self) -> String {
        let t = unsafe { self.as_inner_ref().uuid.u.raw };
        uuid::Uuid::from_bytes(t).to_string()
    }

    // checks for the disks length and parses to correct format
    pub fn parse_disk(disks: Vec<String>) -> Result<String, LvsError> {
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
                    msg: format!(
                        "invalid number {} of devices {:?}",
                        disks.len(),
                        disks,
                    ),
                })
            }
        };
        Ok(disk)
    }

    /// imports a pool based on its name and base bdev name
    pub async fn import(name: &str, bdev: &str) -> Result<Lvs, LvsError> {
        let (sender, receiver) = pair::<ErrnoResult<Lvs>>();

        debug!("Trying to import lvs '{}' from '{}'...", name, bdev);

        let mut bdev =
            UntypedBdev::lookup_by_name(bdev).ok_or(LvsError::InvalidBdev {
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
                reason: ImportErrorReason::None,
            })?;

        if name != lvs.name() {
            warn!(
                "No lvs with name '{}' found on this device: '{}'; \
                found lvs: '{}'",
                name,
                bdev,
                lvs.name()
            );
            let pool_name = lvs.name().to_string();
            lvs.export().await?;
            Err(LvsError::Import {
                source: BsError::InvalidArgument {},
                name: name.to_string(),
                reason: ImportErrorReason::NameMismatch {
                    name: pool_name,
                },
            })
        } else {
            lvs.share_all().await;
            info!("{:?}: existing lvs imported successfully", lvs);
            Ok(lvs)
        }
    }

    /// imports a pool based on its name, uuid and base bdev name
    #[tracing::instrument(level = "debug", err)]
    pub async fn import_from_args(args: PoolArgs) -> Result<Lvs, LvsError> {
        let disk = Self::parse_disk(args.disks.clone())?;

        let parsed = uri::parse(&disk).map_err(|e| LvsError::InvalidBdev {
            source: e,
            name: args.name.clone(),
        })?;

        // At any point two pools with the same name should
        // not exists so returning error
        if let Some(pool) = Self::lookup(&args.name) {
            let pool_name = pool.base_bdev().name().to_string();
            return if pool_name.as_str() == parsed.get_name() {
                Err(LvsError::Import {
                    source: BsError::VolAlreadyExists {},
                    name: args.name.clone(),
                    reason: ImportErrorReason::None,
                })
            } else {
                Err(LvsError::Import {
                    source: BsError::InvalidArgument {},
                    name: args.name.clone(),
                    reason: ImportErrorReason::NameClash {
                        name: pool_name,
                    },
                })
            };
        }

        let bdev = match parsed.create().await {
            Err(e) => match e {
                BdevError::BdevExists {
                    ..
                } => Ok(parsed.get_name()),
                BdevError::CreateBdevInvalidParams {
                    source, ..
                } if source == Errno::EEXIST => Ok(parsed.get_name()),
                _ => {
                    tracing::error!("Failed to create pool bdev: {e:?}");
                    Err(LvsError::InvalidBdev {
                        source: e,
                        name: args.disks[0].clone(),
                    })
                }
            },
            Ok(name) => Ok(name),
        }?;

        let pool = Self::import(&args.name, &bdev).await?;
        // Try to destroy the pending snapshots without catching
        // the error.
        Lvol::destroy_pending_discarded_snapshot().await;
        // if the uuid is provided for the import request check
        // for the pool uuid to make sure it is the correct one
        if let Some(uuid) = args.uuid {
            let pool_uuid = pool.uuid();
            if pool_uuid == uuid {
                Ok(pool)
            } else {
                pool.export().await?;
                Err(LvsError::Import {
                    source: BsError::InvalidArgument {},
                    name: args.name,
                    reason: ImportErrorReason::UuidMismatch {
                        uuid: pool_uuid,
                    },
                })
            }
        } else {
            Ok(pool)
        }
    }

    /// Converts floating point metadata reservation ratio into SPDK's format.
    fn mdp_ratio(args: &PoolArgs) -> Result<u32, LvsError> {
        if let Some(h) = args.md_args.as_ref().and_then(|p| p.md_resv_ratio) {
            if h > 0.0 {
                Ok((h * 100.0) as u32)
            } else {
                Err(LvsError::InvalidMetadataParam {
                    name: args.name.clone(),
                    msg: format!("bad metadata resevation ratio: {h}"),
                })
            }
        } else {
            Ok(0)
        }
    }

    /// Creates a pool on base bdev.
    /// The caller must ensure the base bdev exists.
    /// This function is made public for tests purposes.
    pub async fn create_from_args_inner(
        args: PoolArgs,
    ) -> Result<Lvs, LvsError> {
        assert_eq!(args.disks.len(), 1);
        let bdev = args.disks[0].clone();

        let pool_name = args.name.clone().into_cstring();
        let bdev_name = bdev.into_cstring();

        let cluster_size = if let Some(cluster_size) = args.cluster_size {
            if cluster_size % ROUND_TO_MB == 0 {
                cluster_size
            } else {
                return Err(LvsError::InvalidClusterSize {
                    source: BsError::InvalidArgument {},
                    name: args.name,
                    msg: format!("{cluster_size}, not multiple of 1MiB"),
                });
            }
        } else {
            DEFAULT_CLUSTER_SIZE
        };

        if cluster_size > MAX_CLUSTER_SIZE {
            return Err(LvsError::InvalidClusterSize {
                source: BsError::InvalidArgument {},
                name: args.name,
                msg: format!(
                    "{cluster_size}, larger than max limit {MAX_CLUSTER_SIZE}"
                ),
            });
        }

        let mdp_ratio = Self::mdp_ratio(&args)?;

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
                    Some(Self::lvs_cb),
                    cb_arg(sender),
                )
            } else {
                vbdev_lvs_create(
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
                info!("{:?}: new lvs created successfully", pool);
                Ok(pool)
            }
            None => Err(LvsError::PoolCreate {
                source: BsError::LvolNotFound {},
                name: args.name.clone(),
            }),
        }
    }

    /// Imports the pool if it exists, otherwise tries to create a new pool.
    /// This function creates the underlying bdev if it does not exist.
    #[tracing::instrument(level = "debug", err)]
    pub async fn create_or_import(args: PoolArgs) -> Result<Lvs, LvsError> {
        let disk = Self::parse_disk(args.disks.clone())?;

        info!(
            "Creating or importing lvs '{}' from '{}'...",
            args.name, disk
        );

        let bdev_ops =
            uri::parse(&disk).map_err(|e| LvsError::InvalidBdev {
                source: e,
                name: args.name.clone(),
            })?;

        if let Some(pool) = Self::lookup(&args.name) {
            return if pool.base_bdev().name() == bdev_ops.get_name() {
                Err(LvsError::PoolCreate {
                    source: BsError::VolAlreadyExists {},
                    name: args.name.clone(),
                })
            } else {
                Err(LvsError::PoolCreate {
                    source: BsError::InvalidArgument {},
                    name: args.name.clone(),
                })
            };
        }

        // Create the underlying ndev.
        let bdev_name = match bdev_ops.create().await {
            Err(e) => match e {
                BdevError::BdevExists {
                    ..
                } => Ok(bdev_ops.get_name()),
                BdevError::CreateBdevInvalidParams {
                    source, ..
                } if source == Errno::EEXIST => Ok(bdev_ops.get_name()),
                _ => {
                    tracing::error!("Failed to create pool bdev: {e:?}");
                    Err(LvsError::InvalidBdev {
                        source: e,
                        name: args.disks[0].clone(),
                    })
                }
            },
            Ok(name) => Ok(name),
        }?;

        match Self::import_from_args(args.clone()).await {
            Ok(pool) => Ok(pool),
            // try to create the pool
            Err(LvsError::Import {
                source, ..
            }) if matches!(source, BsError::CannotImportLvs {}) => {
                match Self::create_from_args_inner(PoolArgs {
                    disks: vec![bdev_name.clone()],
                    ..args
                })
                .await
                {
                    Err(create) => {
                        let _ = bdev_ops.destroy().await.map_err(|_e| {
                            // we failed to delete the base_bdev be loud about it
                            // there is not much we can do about it here, likely
                            // some desc is still holding on to it or something.
                            error!("failed to delete base_bdev {bdev_name} after failed pool creation");
                        });
                        Err(create)
                    }
                    Ok(pool) => {
                        pool.event(EventAction::Create).generate();
                        Ok(pool)
                    }
                }
            }
            // some other error, bubble it back up
            Err(e) => Err(e),
        }
    }

    /// export the given lvs
    #[tracing::instrument(level = "debug", err)]
    pub async fn export(self) -> Result<(), LvsError> {
        let self_str = format!("{self:?}");

        info!("{}: exporting lvs...", self_str);

        let pool = self.name().to_string();
        let base_bdev = self.base_bdev();
        let (s, r) = pair::<i32>();

        self.unshare_all().await;

        unsafe {
            vbdev_lvs_unload(
                self.as_inner_ptr(),
                Some(Self::lvs_op_cb),
                cb_arg(s),
            )
        };

        r.await
            .expect("callback gone while exporting lvs")
            .to_result(|e| LvsError::Export {
                source: BsError::from_i32(e),
                name: pool.clone(),
            })?;

        info!("{}: lvs exported successfully", self_str);

        bdev_destroy(&base_bdev.bdev_uri_original_str().unwrap_or_default())
            .await
            .map_err(|e| LvsError::Destroy {
                source: e,
                name: base_bdev.name().to_string(),
            })?;

        Ok(())
    }

    /// unshare all lvols prior to export or destroy
    async fn unshare_all(&self) {
        for l in self.lvols().unwrap() {
            // notice we dont use the unshare impl of the bdev
            // here. we do this to avoid the on disk persistence
            let mut bdev = l.as_bdev();
            if let Err(e) = Pin::new(&mut bdev).unshare().await {
                error!("{:?}: failed to unshare: {}", l, e.to_string())
            }
        }
    }

    /// share all lvols who have the shared property set, this is implicitly
    /// shared over nvmf
    async fn share_all(&self) {
        if let Some(lvols) = self.lvols() {
            for mut l in lvols {
                let allowed_hosts = match l.get(PropName::AllowedHosts).await {
                    Ok(PropValue::AllowedHosts(hosts)) => hosts,
                    _ => vec![],
                };

                if let Ok(prop) = l.get(PropName::Shared).await {
                    match prop {
                        PropValue::Shared(true) => {
                            let name = l.name().clone();
                            let props = NvmfShareProps::new()
                                .with_allowed_hosts(allowed_hosts)
                                .with_ptpl(
                                    l.ptpl().create().unwrap_or_default(),
                                );
                            if let Err(e) =
                                Pin::new(&mut l).share_nvmf(Some(props)).await
                            {
                                error!(
                                    "failed to share {} {}",
                                    name,
                                    e.to_string()
                                );
                            }
                        }
                        PropValue::Shared(false) => {
                            debug!("{} not shared on disk", l.name())
                        }
                        _ => {}
                    }
                }
            }
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

        let base_bdev = self.base_bdev();

        let evt = self.event(EventAction::Delete);

        unsafe {
            vbdev_lvs_destruct(
                self.as_inner_ptr(),
                Some(Self::lvs_op_cb),
                cb_arg(s),
            )
        };

        r.await
            .expect("callback gone while destroying lvs")
            .to_result(|e| LvsError::Export {
                source: BsError::from_i32(e),
                name: pool.clone(),
            })?;

        info!("{}: lvs destroyed successfully", self_str);

        evt.generate();

        bdev_destroy(&base_bdev.bdev_uri_original_str().unwrap())
            .await
            .map_err(|e| LvsError::Destroy {
                source: e,
                name: base_bdev.name().to_string(),
            })?;

        if let Err(error) = ptpl.destroy() {
            tracing::error!(
                "{}: Failed to clean up persistence through power loss for pool: {}",
                self_str,
                error
            );
        }

        Ok(())
    }

    /// Grows the online (live) pool.
    #[tracing::instrument(level = "debug", err)]
    pub async fn grow(&self) -> Result<(), LvsError> {
        info!("{self:?}: growing lvs...");

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
                name: self.name().to_string(),
            })?;

        info!("{self:?}: lvs has been grown successfully");

        Ok(())
    }

    /// return an iterator for enumerating all snapshots that reside on the pool
    pub fn snapshots(
        &self,
    ) -> Option<impl Iterator<Item = LvolSnapshotDescriptor>> {
        if let Some(bdev) = UntypedBdev::bdev_first() {
            let pool_name = format!("{}/", self.name());
            Some(
                bdev.into_iter()
                    .filter(move |b| {
                        b.driver() == "lvol"
                            && b.aliases()
                                .iter()
                                .any(|a| a.contains(&pool_name))
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
    pub fn lvols(&self) -> Option<impl Iterator<Item = Lvol>> {
        if let Some(bdev) = UntypedBdev::bdev_first() {
            let pool_name = format!("{}/", self.name());
            Some(
                bdev.into_iter()
                    .filter(move |b| {
                        b.driver() == "lvol"
                            && b.aliases()
                                .iter()
                                .any(|a| a.contains(&pool_name))
                    })
                    .map(|b| Lvol::try_from(b).unwrap()),
            )
        } else {
            None
        }
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
            use_extent_table: None,
        })
        .await
    }

    /// create a new lvol on this pool
    pub async fn create_lvol_with_opts(
        &self,
        opts: ReplicaArgs,
    ) -> Result<Lvol, LvsError> {
        let clear_method = if self.base_bdev().io_type_supported(IoType::Unmap)
        {
            LVOL_CLEAR_WITH_UNMAP
        } else {
            LVOL_CLEAR_WITH_NONE
        };

        if !opts.uuid.is_empty()
            && UntypedBdev::lookup_by_uuid_str(&opts.uuid).is_some()
        {
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
            if let Err(error) =
                Pin::new(&mut lvol).set(PropValue::EntityId(id)).await
            {
                let lvol_uuid = lvol.uuid();
                if let Err(error) = lvol.destroy().await {
                    warn!(
                        "uuid/{lvol_uuid}: failed to destroy lvol after failing to set entity id: {error:?}",
                    );
                }
                return Err(error);
            }
        }

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

        info!("{lvol:?}: created");
        lvol.event(EventAction::Create).generate();
        Ok(lvol)
    }

    /// Get a `PtplFileOps` from `&self`.
    pub(crate) fn ptpl(&self) -> impl PtplFileOps {
        LvsPtpl::from(self)
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
        Self {
            uuid: lvs.uuid(),
        }
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
