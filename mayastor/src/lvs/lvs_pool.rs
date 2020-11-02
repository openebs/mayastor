use std::{convert::TryFrom, fmt::Debug, os::raw::c_void, ptr::NonNull};

use futures::channel::oneshot;
use nix::errno::Errno;
use pin_utils::core_reexport::fmt::Formatter;
use tracing::instrument;

use rpc::mayastor::CreatePoolRequest;
use spdk_sys::{
    lvol_store_bdev,
    spdk_bs_free_cluster_count,
    spdk_bs_get_cluster_size,
    spdk_bs_total_data_cluster_count,
    spdk_lvol,
    spdk_lvol_store,
    vbdev_get_lvol_store_by_name,
    vbdev_get_lvs_bdev_by_lvs,
    vbdev_lvol_create,
    vbdev_lvol_store_first,
    vbdev_lvol_store_next,
    vbdev_lvs_create,
    vbdev_lvs_destruct,
    vbdev_lvs_examine,
    vbdev_lvs_unload,
    LVOL_CLEAR_WITH_UNMAP,
    LVOL_CLEAR_WITH_WRITE_ZEROES,
    LVS_CLEAR_WITH_NONE,
    SPDK_BDEV_IO_TYPE_UNMAP,
};
use url::Url;

use crate::{
    bdev::{util::uring, Uri},
    core::{Bdev, Share, Uuid},
    ffihelper::{cb_arg, pair, AsStr, ErrnoResult, FfiResult, IntoCString},
    lvs::{Error, Lvol, PropName, PropValue},
    nexus_uri::{bdev_destroy, NexusBdevError},
};

impl From<*mut spdk_lvol_store> for Lvs {
    fn from(p: *mut spdk_lvol_store) -> Self {
        Lvs(NonNull::new(p).unwrap())
    }
}

/// iterator over all lvol stores
pub struct LvsIterator(*mut lvol_store_bdev);

/// returns a new lvs iterator
impl LvsIterator {
    fn new() -> Self {
        LvsIterator(unsafe { vbdev_lvol_store_first() })
    }
}

impl Default for LvsIterator {
    fn default() -> Self {
        Self::new()
    }
}

impl Iterator for LvsIterator {
    type Item = Lvs;

    fn next(&mut self) -> Option<Self::Item> {
        if self.0.is_null() {
            None
        } else {
            let current = self.0;
            self.0 = unsafe { vbdev_lvol_store_next(current) };
            Some(Lvs::from(unsafe { (*current).lvs }))
        }
    }
}

impl IntoIterator for Lvs {
    type Item = Lvs;
    type IntoIter = LvsIterator;

    fn into_iter(self) -> Self::IntoIter {
        LvsIterator(unsafe { vbdev_lvol_store_first() })
    }
}

impl Debug for Lvs {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "name: {}, uuid: {}, base_bdev: {}",
            self.name(),
            self.uuid(),
            self.base_bdev().name()
        )
    }
}

/// Logical Volume Store (LVS) stores the lvols
pub struct Lvs(pub(crate) NonNull<spdk_lvol_store>);

impl Lvs {
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
            sender.send(Ok(Lvs::from(lvs))).expect("receiver gone");
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
    pub fn iter() -> LvsIterator {
        LvsIterator::default()
    }

    /// lookup a lvol store by its name
    pub fn lookup(name: &str) -> Option<Self> {
        let name = name.into_cstring();

        let lvs = unsafe { vbdev_get_lvol_store_by_name(name.as_ptr()) };
        if lvs.is_null() {
            None
        } else {
            Some(Lvs::from(lvs))
        }
    }

    /// return the name of the current store
    pub fn name(&self) -> &str {
        unsafe { self.0.as_ref().name.as_str() }
    }

    /// returns the total capacity of the store
    pub fn capacity(&self) -> u64 {
        let blobs = unsafe { self.0.as_ref().blobstore };
        unsafe {
            spdk_bs_get_cluster_size(blobs)
                * spdk_bs_total_data_cluster_count(blobs)
        }
    }

    /// returns the available capacity
    pub fn available(&self) -> u64 {
        let blobs = unsafe { self.0.as_ref().blobstore };
        unsafe {
            spdk_bs_get_cluster_size(blobs) * spdk_bs_free_cluster_count(blobs)
        }
    }

    /// returns the used capacity
    pub fn used(&self) -> u64 {
        self.capacity() - self.available()
    }

    /// returns the base bdev of this lvs
    pub fn base_bdev(&self) -> Bdev {
        Bdev::from(unsafe {
            (*vbdev_get_lvs_bdev_by_lvs(self.0.as_ptr())).bdev
        })
    }

    /// returns the UUID of the lvs
    pub fn uuid(&self) -> String {
        let t = unsafe { self.0.as_ref().uuid.u.raw };
        Uuid::from_bytes(t).to_string()
    }

    /// imports a pool based on its name and base bdev name
    #[instrument(level = "debug", err)]
    pub async fn import(name: &str, bdev: &str) -> Result<Lvs, Error> {
        let (sender, receiver) = pair::<ErrnoResult<Lvs>>();

        debug!("Trying to import pool {} on {}", name, bdev);

        let bdev = Bdev::lookup_by_name(bdev).ok_or(Error::InvalidBdev {
            source: NexusBdevError::BdevNotFound {
                name: bdev.to_string(),
            },
            name: name.to_string(),
        })?;

        // examining a bdev that is in-use by an lvs, will hang to avoid this
        // we will determine the usage of the bdev prior to examining it.

        if bdev.is_claimed() {
            return Err(Error::Import {
                source: Errno::EBUSY,
                name: bdev.name(),
            });
        }

        unsafe {
            // EXISTS is SHOULD be returned when we import a lvs with different
            // names this however is not the case.
            vbdev_lvs_examine(
                bdev.as_ptr(),
                Some(Self::lvs_cb),
                cb_arg(sender),
            );
        }

        // when no pool name can be determined the or failed to compare to the
        // desired pool name EILSEQ is returned
        let lvs = receiver
            .await
            .expect("Cancellation is not supported")
            .map_err(|err| Error::Import {
                source: err,
                name: name.into(),
            })?;

        if name != lvs.name() {
            warn!("no pool with name {} found on this device -- unloading the pool", name);
            lvs.export().await.unwrap();
            Err(Error::Import {
                source: Errno::EINVAL,
                name: name.into(),
            })
        } else {
            lvs.share_all().await;
            info!("The pool '{}' has been imported", name);
            Ok(lvs)
        }
    }

    #[instrument(level = "debug", err)]
    /// Create a pool on base bdev
    pub async fn create(name: &str, bdev: &str) -> Result<Lvs, Error> {
        let pool_name = name.into_cstring();
        let bdev_name = bdev.into_cstring();

        let (sender, receiver) = pair::<ErrnoResult<Lvs>>();
        unsafe {
            vbdev_lvs_create(
                bdev_name.as_ptr(),
                pool_name.as_ptr(),
                0,
                // We used to clear a pool with UNMAP but that takes awfully
                // long time on large SSDs (~ can take an hour). Clearing the
                // pool is not necessary. Clearing the lvol must be done, but
                // lvols tend to be small so there the overhead is acceptable.
                LVS_CLEAR_WITH_NONE,
                Some(Self::lvs_cb),
                cb_arg(sender),
            )
        }
        .to_result(|e| Error::Create {
            source: Errno::from_i32(e),
            name: name.to_string(),
        })?;

        receiver
            .await
            .expect("Cancellation is not supported")
            .map_err(|err| Error::Create {
                source: err,
                name: name.to_string(),
            })?;

        match Self::lookup(&name) {
            Some(pool) => {
                info!("The pool '{}' has been created on {}", name, bdev);
                Ok(pool)
            }
            None => Err(Error::Create {
                source: Errno::ENOENT,
                name: name.to_string(),
            }),
        }
    }

    /// imports the pool if it exists, otherwise try to create it
    #[instrument(level = "debug", err)]
    pub async fn create_or_import(
        args: CreatePoolRequest,
    ) -> Result<Lvs, Error> {
        if args.disks.len() != 1 {
            return Err(Error::Invalid {
                source: Errno::EINVAL,
                msg: format!(
                    "invalid number {} of devices {:?}",
                    args.disks.len(),
                    args.disks
                ),
            });
        }

        // default to uring if kernel supports it
        let disks = args
            .disks
            .iter()
            .map(|d| {
                if Url::parse(d).is_err() {
                    format!(
                        "{}://{}",
                        if uring::kernel_support() {
                            "uring"
                        } else {
                            "aio"
                        },
                        d,
                    )
                } else {
                    d.clone()
                }
            })
            .collect::<Vec<_>>();

        let parsed = Uri::parse(&disks[0]).map_err(|e| Error::InvalidBdev {
            source: e,
            name: args.name.clone(),
        })?;

        if let Some(pool) = Self::lookup(&args.name) {
            return if pool.base_bdev().name() == parsed.get_name() {
                Ok(pool)
            } else {
                Err(Error::Create {
                    source: Errno::EEXIST,
                    name: args.name.clone(),
                })
            };
        }

        let bdev = match parsed.create().await {
            Err(e) => match e {
                NexusBdevError::BdevExists {
                    ..
                } => Ok(parsed.get_name()),
                _ => Err(Error::InvalidBdev {
                    source: e,
                    name: args.disks[0].clone(),
                }),
            },
            Ok(name) => Ok(name),
        }?;

        match Self::import(&args.name, &bdev).await {
            Ok(pool) => Ok(pool),
            Err(Error::Import {
                source,
                name,
            }) if source == Errno::EINVAL => {
                // there is a pool here, but it does not match the name
                error!("pool name mismatch");
                Err(Error::Import {
                    source,
                    name,
                })
            }
            // try to create the pool
            Err(Error::Import {
                source, ..
            }) if source == Errno::EILSEQ => {
                match Self::create(&args.name, &bdev).await {
                    Err(create) => {
                        let _ = parsed.destroy().await.map_err(|_e| {
                            // we failed to delete the base_bdev be loud about it
                            // there is not much we can do about it here, likely
                            // some desc is still holding on to it or something.
                            error!("failed to delete base_bdev {} after failed pool creation", bdev);
                        });
                        Err(create)
                    }
                    Ok(pool) => Ok(pool),
                }
            }
            // some other error, bubble it back up
            Err(e) => Err(e),
        }
    }

    /// export the given lvl
    #[allow(clippy::unit_arg)] // here to silence the () argument
    #[instrument(level = "debug", err)]
    pub async fn export(self) -> Result<(), Error> {
        let pool = self.name().to_string();
        let base_bdev = self.base_bdev();
        let (s, r) = pair::<i32>();

        self.unshare_all().await;

        unsafe {
            vbdev_lvs_unload(self.0.as_ptr(), Some(Self::lvs_op_cb), cb_arg(s))
        };

        r.await
            .expect("callback gone while exporting lvs")
            .to_result(|e| Error::Export {
                source: Errno::from_i32(e),
                name: pool.clone(),
            })?;

        info!("pool {} exported successfully", pool);
        bdev_destroy(&base_bdev.bdev_uri().unwrap())
            .await
            .map_err(|e| Error::Destroy {
                source: e,
                name: base_bdev.name(),
            })?;
        Ok(())
    }

    /// unshare all lvols prior to export or destroy
    async fn unshare_all(&self) {
        for l in self.lvols().unwrap() {
            // notice we dont use the unshare impl of the bdev
            // here. we do this to avoid the on disk persistence
            let bdev = l.as_bdev();
            if let Err(e) = bdev.unshare().await {
                error!("failed to unshare lvol {} error {}", l, e.to_string())
            }
        }
    }

    /// share all lvols who have the shared property set, this is implicitly
    /// shared over nvmf
    async fn share_all(&self) {
        if let Some(lvols) = self.lvols() {
            for l in lvols {
                if let Ok(prop) = l.get(PropName::Shared).await {
                    match prop {
                        PropValue::Shared(true) => {
                            if let Err(e) = l.share_nvmf().await {
                                error!(
                                    "failed to share {} {}",
                                    l.name(),
                                    e.to_string()
                                );
                            }
                        }
                        PropValue::Shared(false) => {
                            debug!("{} not shared on disk", l.name())
                        }
                    }
                }
            }
        }
    }

    /// destroys the given pool deleting the on disk super blob before doing so,
    /// un share all targets
    #[allow(clippy::unit_arg)]
    #[instrument(level = "debug", err)]
    pub async fn destroy(self) -> Result<(), Error> {
        let pool = self.name().to_string();
        let (s, r) = pair::<i32>();

        // when destroying a pool unshare all volumes
        self.unshare_all().await;

        let base_bdev = self.base_bdev();

        unsafe {
            vbdev_lvs_destruct(
                self.0.as_ptr(),
                Some(Self::lvs_op_cb),
                cb_arg(s),
            )
        };

        r.await
            .expect("callback gone while destroying lvs")
            .to_result(|e| Error::Export {
                source: Errno::from_i32(e),
                name: pool.clone(),
            })?;

        info!("pool {} destroyed successfully", pool);

        bdev_destroy(&base_bdev.bdev_uri().unwrap())
            .await
            .map_err(|e| Error::Destroy {
                source: e,
                name: base_bdev.name(),
            })?;

        Ok(())
    }

    /// return an iterator that filters out all bdevs that patch the pool
    /// signature
    pub fn lvols(&self) -> Option<impl Iterator<Item = Lvol>> {
        if let Some(bdev) = Bdev::bdev_first() {
            let pool_name = format!("{}/", self.name().to_string());
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

    #[instrument(level = "debug", err)]
    /// create a new lvol on this pool
    pub async fn create_lvol(
        &self,
        name: &str,
        size: u64,
        thin: bool,
    ) -> Result<Lvol, Error> {
        let clear_method =
            if self.base_bdev().io_type_supported(SPDK_BDEV_IO_TYPE_UNMAP) {
                LVOL_CLEAR_WITH_UNMAP
            } else {
                LVOL_CLEAR_WITH_WRITE_ZEROES
            };

        if Bdev::lookup_by_name(name).is_some() {
            return Err(Error::RepExists {
                source: Errno::EEXIST,
                name: name.to_string(),
            });
        };

        let (s, r) = pair::<ErrnoResult<*mut spdk_lvol>>();

        let cname = name.into_cstring();
        unsafe {
            vbdev_lvol_create(
                self.0.as_ptr(),
                cname.as_ptr(),
                size,
                thin,
                clear_method,
                Some(Lvol::lvol_cb),
                cb_arg(s),
            )
        }
        .to_result(|e| Error::RepCreate {
            source: Errno::from_i32(e),
            name: name.to_string(),
        })?;

        let lvol = r
            .await
            .expect("lvol creation callback dropped")
            .map_err(|e| Error::RepCreate {
                source: e,
                name: name.to_string(),
            })
            .map(|lvol| Lvol(NonNull::new(lvol).unwrap()))?;

        info!("created {}", lvol);
        Ok(lvol)
    }
}
