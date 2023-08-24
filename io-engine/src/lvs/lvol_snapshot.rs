use crate::{
    core::{
        logical_volume::LogicalVolume,
        snapshot::{CloneParams, SnapshotDescriptor, VolumeSnapshotDescriptor},
        Bdev,
        CloneXattrs,
        SnapshotOps,
        SnapshotParams,
        SnapshotXattrs,
        UntypedBdev,
    },
    ffihelper::{cb_arg, IntoCString},
    lvs::{lvs_lvol::LvsLvol, Lvol},
    subsys::NvmfReq,
};
use async_trait::async_trait;
use chrono::Utc;
use futures::channel::oneshot;
use nix::errno::Errno;
use spdk_rs::libspdk::{
    spdk_blob,
    spdk_blob_reset_used_clusters_cache,
    spdk_lvol,
    spdk_xattr_descriptor,
    vbdev_lvol_create_clone_ext,
    vbdev_lvol_create_snapshot_ext,
};
use std::{
    convert::TryFrom,
    ffi::{c_ushort, c_void, CString},
    os::raw::c_char,
};
use strum::{EnumCount, IntoEnumIterator};

use super::Error;
use futures::future::join_all;

#[async_trait(?Send)]
trait AsyncIterator {
    type Item;
    async fn next(&mut self) -> Option<SnapshotParams>;
}

/// Iterator over Lvol Blobstore for Snapshot.
pub struct LvolSnapshotIter {
    inner: *mut spdk_blob,
    inner_lvol: Lvol,
}

impl LvolSnapshotIter {
    pub fn new(lvol: Lvol) -> Self {
        Self {
            inner: lvol.bs_iter_first(),
            inner_lvol: lvol,
        }
    }
}

#[async_trait(?Send)]
/// Iterator implementation for LvolSnapshot.
impl AsyncIterator for LvolSnapshotIter {
    type Item = SnapshotParams;
    async fn next(&mut self) -> Option<Self::Item> {
        if self.inner.is_null() {
            None
        } else {
            let current = self.inner;
            match self.inner_lvol.bs_iter_next(current).await {
                Some(next_blob) => self.inner = next_blob,
                None => self.inner = std::ptr::null_mut(),
            }
            Some(self.inner_lvol.build_snapshot_param(current))
        }
    }
}

#[async_trait(?Send)]
impl SnapshotOps for Lvol {
    type Error = Error;
    type SnapshotIter = LvolSnapshotIter;
    type Lvol = Lvol;

    /// Prepare Snapshot Config for Block/Nvmf Device, before snapshot create.
    fn prepare_snap_config(
        &self,
        snap_name: &str,
        entity_id: &str,
        txn_id: &str,
        snap_uuid: &str,
    ) -> Option<SnapshotParams> {
        // snap_name
        let snap_name = if snap_name.is_empty() {
            return None;
        } else {
            snap_name.to_string()
        };
        let entity_id = if entity_id.is_empty() {
            return None;
        } else {
            entity_id.to_string()
        };

        // txn_id
        let txn_id = if txn_id.is_empty() {
            return None;
        } else {
            txn_id.to_string()
        };
        // snapshot_uuid
        let snap_uuid: Option<String> = if snap_uuid.is_empty() {
            None
        } else {
            Some(snap_uuid.to_string())
        };
        // Current Lvol uuid is the parent for the snapshot.
        let parent_id = Some(self.uuid());
        Some(SnapshotParams::new(
            Some(entity_id),
            parent_id,
            Some(txn_id),
            Some(snap_name),
            snap_uuid,
            Some(Utc::now().to_string()),
            false,
        ))
    }
    /// Prepare snapshot xattrs.
    fn prepare_snapshot_xattrs(
        &self,
        attr_descrs: &mut [spdk_xattr_descriptor; SnapshotXattrs::COUNT],
        params: SnapshotParams,
        cstrs: &mut Vec<CString>,
    ) -> Result<(), Error> {
        for (idx, attr) in SnapshotXattrs::iter().enumerate() {
            // Get attribute value from snapshot params.
            let av = match attr {
                SnapshotXattrs::TxId => match params.txn_id() {
                    Some(v) => v,
                    None => {
                        return Err(Error::SnapshotConfigFailed {
                            name: self.as_bdev().name().to_string(),
                            msg: "txn id not provided".to_string(),
                        })
                    }
                },
                SnapshotXattrs::EntityId => match params.entity_id() {
                    Some(v) => v,
                    None => {
                        return Err(Error::SnapshotConfigFailed {
                            name: self.as_bdev().name().to_string(),
                            msg: "entity id not provided".to_string(),
                        })
                    }
                },
                SnapshotXattrs::ParentId => match params.parent_id() {
                    Some(v) => v,
                    None => {
                        return Err(Error::SnapshotConfigFailed {
                            name: self.as_bdev().name().to_string(),
                            msg: "parent id not provided".to_string(),
                        })
                    }
                },
                SnapshotXattrs::SnapshotUuid => match params.snapshot_uuid() {
                    Some(v) => v,
                    None => {
                        return Err(Error::SnapshotConfigFailed {
                            name: self.as_bdev().name().to_string(),
                            msg: "snapshot_uuid not provided".to_string(),
                        })
                    }
                },
                SnapshotXattrs::SnapshotCreateTime => {
                    match params.create_time() {
                        Some(v) => v,
                        None => {
                            return Err(Error::SnapshotConfigFailed {
                                name: self.as_bdev().name().to_string(),
                                msg: "create_time not provided".to_string(),
                            })
                        }
                    }
                }
                SnapshotXattrs::DiscardedSnapshot => {
                    params.discarded_snapshot().to_string()
                }
            };
            let attr_name = attr.name().to_string().into_cstring();
            let attr_val = av.into_cstring();
            attr_descrs[idx].name = attr_name.as_ptr() as *mut c_char;
            attr_descrs[idx].value = attr_val.as_ptr() as *mut c_void;
            attr_descrs[idx].value_len = attr_val.to_bytes().len() as c_ushort;

            cstrs.push(attr_val);
            cstrs.push(attr_name);
        }

        Ok(())
    }
    /// create replica snapshot inner function to call spdk snapshot create
    /// function.
    unsafe fn create_snapshot_inner(
        &self,
        snap_param: &SnapshotParams,
        done_cb: unsafe extern "C" fn(*mut c_void, *mut spdk_lvol, i32),
        done_cb_arg: *mut ::std::os::raw::c_void,
    ) -> Result<(), Error> {
        let mut attr_descrs: [spdk_xattr_descriptor; SnapshotXattrs::COUNT] =
            [spdk_xattr_descriptor::default(); SnapshotXattrs::COUNT];

        // Vector to keep allocated CStrings before snapshot  creation
        // is complete to guarantee validity of attribute buffers
        // stored inside CStrings.
        let mut cstrs: Vec<CString> = Vec::new();

        self.prepare_snapshot_xattrs(
            &mut attr_descrs,
            snap_param.clone(),
            &mut cstrs,
        )?;

        let c_snapshot_name = snap_param.name().unwrap().into_cstring();

        // No need to flush blob's buffers explicitly as SPDK always
        // synchronizes blob when taking a snapshot.
        unsafe {
            vbdev_lvol_create_snapshot_ext(
                self.as_inner_ptr(),
                c_snapshot_name.as_ptr(),
                attr_descrs.as_mut_ptr(),
                SnapshotXattrs::COUNT as u32,
                Some(done_cb),
                done_cb_arg,
            )
        };
        Ok(())
    }
    async fn do_create_snapshot(
        &self,
        snap_param: SnapshotParams,
        done_cb: unsafe extern "C" fn(*mut c_void, *mut spdk_lvol, i32),
        done_cb_arg: *mut ::std::os::raw::c_void,
        receiver: oneshot::Receiver<(i32, *mut spdk_lvol)>,
    ) -> Result<Lvol, Error> {
        unsafe {
            self.create_snapshot_inner(&snap_param, done_cb, done_cb_arg)?;
        }
        // Wait till operation succeeds, if requested.
        let (error, lvol_ptr) =
            receiver.await.expect("Snapshot done callback disappeared");
        match error {
            0 => Ok(Lvol::from_inner_ptr(lvol_ptr)),
            _ => Err(Error::SnapshotCreate {
                source: Errno::from_i32(error),
                msg: snap_param.name().unwrap(),
            }),
        }
    }
    async fn do_create_snapshot_remote(
        &self,
        snap_param: SnapshotParams,
        done_cb: unsafe extern "C" fn(*mut c_void, *mut spdk_lvol, i32),
        done_cb_arg: *mut ::std::os::raw::c_void,
    ) -> Result<(), Error> {
        unsafe {
            self.create_snapshot_inner(&snap_param, done_cb, done_cb_arg)?;
        }
        Ok(())
    }

    /// Prepare clone config for snapshot.
    fn prepare_clone_config(
        &self,
        clone_name: &str,
        clone_uuid: &str,
        source_uuid: &str,
    ) -> Option<CloneParams> {
        // clone_name
        let clone_name = if clone_name.is_empty() {
            return None;
        } else {
            clone_name.to_string()
        };
        // clone_uuid
        let clone_uuid = if clone_uuid.is_empty() {
            return None;
        } else {
            clone_uuid.to_string()
        };
        // source_uuid
        let source_uuid = if source_uuid.is_empty() {
            return None;
        } else {
            source_uuid.to_string()
        };
        Some(CloneParams::new(
            Some(clone_name),
            Some(clone_uuid),
            Some(source_uuid),
            Some(Utc::now().to_string()),
        ))
    }

    /// Prepare clone xattrs.
    fn prepare_clone_xattrs(
        &self,
        attr_descrs: &mut [spdk_xattr_descriptor; CloneXattrs::COUNT],
        params: CloneParams,
        cstrs: &mut Vec<CString>,
    ) -> Result<(), Error> {
        for (idx, attr) in CloneXattrs::iter().enumerate() {
            // Get attribute value from CloneParams.
            let av = match attr {
                CloneXattrs::SourceUuid => match params.source_uuid() {
                    Some(v) => v,
                    None => {
                        return Err(Error::CloneConfigFailed {
                            name: self.as_bdev().name().to_string(),
                            msg: "source uuid not provided".to_string(),
                        })
                    }
                },
                CloneXattrs::CloneCreateTime => {
                    match params.clone_create_time() {
                        Some(v) => v,
                        None => {
                            return Err(Error::CloneConfigFailed {
                                name: self.as_bdev().name().to_string(),
                                msg: "create_time not provided".to_string(),
                            })
                        }
                    }
                }
                CloneXattrs::CloneUuid => match params.clone_uuid() {
                    Some(v) => v,
                    None => {
                        return Err(Error::CloneConfigFailed {
                            name: self.as_bdev().name().to_string(),
                            msg: "clone_uuid not provided".to_string(),
                        })
                    }
                },
            };
            let attr_name = attr.name().to_string().into_cstring();
            let attr_val = av.into_cstring();
            attr_descrs[idx].name = attr_name.as_ptr() as *mut c_char;
            attr_descrs[idx].value = attr_val.as_ptr() as *mut c_void;
            attr_descrs[idx].value_len = attr_val.to_bytes().len() as c_ushort;

            cstrs.push(attr_val);
            cstrs.push(attr_name);
        }
        Ok(())
    }
    /// Create clone inner function to call spdk clone function.
    unsafe fn create_clone_inner(
        &self,
        clone_param: &CloneParams,
        done_cb: unsafe extern "C" fn(*mut c_void, *mut spdk_lvol, i32),
        done_cb_arg: *mut ::std::os::raw::c_void,
    ) -> Result<(), Error> {
        let mut attr_descrs: [spdk_xattr_descriptor; CloneXattrs::COUNT] =
            [spdk_xattr_descriptor::default(); CloneXattrs::COUNT];

        // Vector to keep allocated CStrings before snapshot  creation
        // is complete to guarantee validity of attribute buffers
        // stored inside CStrings.
        let mut cstrs: Vec<CString> = Vec::new();

        self.prepare_clone_xattrs(
            &mut attr_descrs,
            clone_param.clone(),
            &mut cstrs,
        )?;

        let c_clone_name =
            clone_param.clone_name().unwrap_or_default().into_cstring();

        unsafe {
            vbdev_lvol_create_clone_ext(
                self.as_inner_ptr(),
                c_clone_name.as_ptr(),
                attr_descrs.as_mut_ptr(),
                CloneXattrs::COUNT as u32,
                Some(done_cb),
                done_cb_arg,
            )
        };
        Ok(())
    }
    async fn do_create_clone(
        &self,
        clone_param: CloneParams,
        done_cb: unsafe extern "C" fn(*mut c_void, *mut spdk_lvol, i32),
        done_cb_arg: *mut ::std::os::raw::c_void,
        receiver: oneshot::Receiver<(i32, *mut spdk_lvol)>,
    ) -> Result<Lvol, Error> {
        unsafe {
            self.create_clone_inner(&clone_param, done_cb, done_cb_arg)?;
        }
        // Wait till operation succeeds, if requested.
        let (error, lvol_ptr) = receiver
            .await
            .expect("Snapshot Clone done callback disappeared");
        match error {
            0 => Ok(Lvol::from_inner_ptr(lvol_ptr)),
            _ => Err(Error::SnapshotCloneCreate {
                source: Errno::from_i32(error),
                msg: clone_param.clone_name().unwrap_or_default(),
            }),
        }
    }
    /// Common API to set SnapshotDescriptor for ListReplicaSnapshot.
    fn snapshot_descriptor(
        &self,
        parent: Option<&Lvol>,
    ) -> Option<VolumeSnapshotDescriptor> {
        let mut valid_snapshot = true;
        let mut snapshot_param: SnapshotParams = Default::default();
        for attr in SnapshotXattrs::iter() {
            let curr_attr_val = match Self::get_blob_xattr(self, attr.name()) {
                Some(val) => val,
                None => {
                    valid_snapshot = false;
                    continue;
                }
            };
            match attr {
                SnapshotXattrs::ParentId => {
                    if let Some(parent_lvol) = parent {
                        // Skip snapshots if it's parent is not matched.
                        if curr_attr_val != parent_lvol.uuid() {
                            return None;
                        }
                    }
                    snapshot_param.set_parent_id(curr_attr_val);
                }
                SnapshotXattrs::EntityId => {
                    snapshot_param.set_entity_id(curr_attr_val);
                }
                SnapshotXattrs::TxId => {
                    snapshot_param.set_txn_id(curr_attr_val);
                }
                SnapshotXattrs::SnapshotUuid => {
                    snapshot_param.set_snapshot_uuid(curr_attr_val);
                }
                SnapshotXattrs::SnapshotCreateTime => {
                    snapshot_param.set_create_time(curr_attr_val);
                }
                SnapshotXattrs::DiscardedSnapshot => {
                    snapshot_param.set_discarded_snapshot(
                        curr_attr_val.parse().unwrap_or_default(),
                    );
                }
            }
        }
        // set remaining snapshot parameters for snapshot list
        snapshot_param.set_name(self.name());
        // set parent replica uuid and size of the snapshot
        let parent_uuid = if let Some(parent_lvol) = parent {
            parent_lvol.uuid()
        } else {
            match Bdev::lookup_by_uuid_str(
                snapshot_param.parent_id().unwrap_or_default().as_str(),
            )
            .and_then(|b| Lvol::try_from(b).ok())
            {
                Some(parent) => parent.uuid(),
                None => String::default(),
            }
        };
        let snapshot_descriptor = VolumeSnapshotDescriptor::new(
            self.to_owned(),
            parent_uuid,
            self.usage().allocated_bytes,
            snapshot_param,
            self.list_clones_by_snapshot_uuid().len() as u64,
            valid_snapshot,
        );
        Some(snapshot_descriptor)
    }

    /// Create Snapshot Common API for Local Device.
    async fn create_snapshot(
        &self,
        snap_param: SnapshotParams,
    ) -> Result<Lvol, Error> {
        extern "C" fn snapshot_create_done_cb(
            arg: *mut c_void,
            lvol_ptr: *mut spdk_lvol,
            errno: i32,
        ) {
            let s = unsafe {
                Box::from_raw(
                    arg as *mut oneshot::Sender<(i32, *mut spdk_lvol)>,
                )
            };
            if errno != 0 {
                error!("vbdev_lvol_create_snapshot failed errno {}", errno);
            }
            s.send((errno, lvol_ptr)).ok();
        }

        let (s, r) = oneshot::channel::<(i32, *mut spdk_lvol)>();

        let create_snapshot = self
            .do_create_snapshot(
                snap_param,
                snapshot_create_done_cb,
                cb_arg(s),
                r,
            )
            .await;
        create_snapshot
    }
    /// Create a snapshot in Remote.
    async fn create_snapshot_remote(
        &self,
        nvmf_req: &NvmfReq,
        snapshot_params: SnapshotParams,
    ) {
        extern "C" fn snapshot_done_cb(
            nvmf_req_ptr: *mut c_void,
            _lvol_ptr: *mut spdk_lvol,
            errno: i32,
        ) {
            let nvmf_req = NvmfReq::from(nvmf_req_ptr);

            match errno {
                0 => nvmf_req.complete(),
                _ => {
                    error!("vbdev_lvol_create_snapshot_ext errno {}", errno);
                    nvmf_req.complete_error(errno);
                }
            };
        }

        info!(
            volume = self.name(),
            ?snapshot_params,
            "Creating a remote snapshot"
        );

        if let Err(error) = self
            .do_create_snapshot_remote(
                snapshot_params,
                snapshot_done_cb,
                nvmf_req.0.as_ptr().cast(),
            )
            .await
        {
            error!(
                ?error,
                volume = self.name(),
                "Failed to create remote snapshot"
            );
        }
    }
    /// Get a Snapshot Iterator.
    async fn snapshot_iter(self) -> LvolSnapshotIter {
        LvolSnapshotIter::new(self)
    }
    /// Destroy snapshot.
    async fn destroy_snapshot(mut self) -> Result<(), Self::Error> {
        if self.list_clones_by_snapshot_uuid().is_empty() {
            self.reset_snapshot_parent_successor_usage_cache();
            self.destroy().await?;
        } else {
            self.set_blob_attr(
                SnapshotXattrs::DiscardedSnapshot.name(),
                true.to_string(),
                true,
            )
            .await?;
        }
        Ok(())
    }
    /// List Snapshot details based on source UUID from which snapshot is
    /// created.
    fn list_snapshot_by_source_uuid(&self) -> Vec<VolumeSnapshotDescriptor> {
        let mut snapshot_list: Vec<VolumeSnapshotDescriptor> = Vec::new();
        if let Some(bdev) = UntypedBdev::bdev_first() {
            let lvol_devices = bdev
                .into_iter()
                .filter(|b| b.driver() == "lvol")
                .map(|b| Lvol::try_from(b).unwrap())
                .collect::<Vec<Lvol>>();
            for snapshot_lvol in lvol_devices {
                // skip lvol if it is not snapshot.
                if !snapshot_lvol.is_snapshot() {
                    continue;
                }

                match snapshot_lvol.snapshot_descriptor(Some(self)) {
                    Some(snapshot_descriptor) => {
                        snapshot_list.push(snapshot_descriptor)
                    }
                    None => continue,
                }
            }
        }
        snapshot_list
    }
    /// List Single snapshot details based on snapshot UUID.
    fn list_snapshot_by_snapshot_uuid(&self) -> Vec<VolumeSnapshotDescriptor> {
        let mut snapshot_list: Vec<VolumeSnapshotDescriptor> = Vec::new();
        if let Some(bdev) = UntypedBdev::bdev_first() {
            if let Some(lvol) = bdev
                .into_iter()
                .find(|b| {
                    b.driver() == "lvol" && b.uuid_as_string() == self.uuid()
                })
                .map(|b| Lvol::try_from(b).unwrap())
            {
                if let Some(snapshot_descriptor) =
                    lvol.snapshot_descriptor(None)
                {
                    snapshot_list.push(snapshot_descriptor);
                }
            }
        }
        snapshot_list
    }

    /// List All Snapshot.
    fn list_all_snapshots() -> Vec<VolumeSnapshotDescriptor> {
        let mut snapshot_list: Vec<VolumeSnapshotDescriptor> = Vec::new();

        let bdev = match UntypedBdev::bdev_first() {
            Some(b) => b,
            None => return Vec::new(), /* No devices available, provide no
                                       snapshots */
        };

        let lvol_devices = bdev
            .into_iter()
            .filter(|b| b.driver() == "lvol")
            .map(|b| Lvol::try_from(b).unwrap())
            .collect::<Vec<Lvol>>();

        for snapshot_lvol in lvol_devices {
            // skip lvol if it is not snapshot.
            if !snapshot_lvol.is_snapshot() {
                continue;
            }
            match snapshot_lvol.snapshot_descriptor(None) {
                Some(snapshot_descriptor) => {
                    snapshot_list.push(snapshot_descriptor)
                }
                None => continue,
            }
        }
        snapshot_list
    }

    /// Create snapshot clone.
    async fn create_clone(
        &self,
        clone_param: CloneParams,
    ) -> Result<Self::Lvol, Self::Error> {
        extern "C" fn clone_done_cb(
            arg: *mut c_void,
            lvol_ptr: *mut spdk_lvol,
            errno: i32,
        ) {
            let s = unsafe {
                Box::from_raw(
                    arg as *mut oneshot::Sender<(i32, *mut spdk_lvol)>,
                )
            };
            if errno != 0 {
                error!("Snapshot Clone failed errno {}", errno);
            }
            s.send((errno, lvol_ptr)).ok();
        }

        let (s, r) = oneshot::channel::<(i32, *mut spdk_lvol)>();

        let create_clone = self
            .do_create_clone(clone_param, clone_done_cb, cb_arg(s), r)
            .await;
        create_clone
    }

    /// List clones based on snapshot_uuid.
    fn list_clones_by_snapshot_uuid(&self) -> Vec<Lvol> {
        let bdev = match UntypedBdev::bdev_first() {
            Some(b) => b,
            None => return Vec::new(), /* No devices available, no clones */
        };
        bdev.into_iter()
            .filter(|b| b.driver() == "lvol")
            .map(|b| Lvol::try_from(b).unwrap())
            .filter_map(|b| {
                let snap_lvol = b.is_snapshot_clone();
                if snap_lvol.is_some()
                    && snap_lvol.unwrap().uuid() == self.uuid()
                {
                    Some(b)
                } else {
                    None
                }
            })
            .collect::<Vec<Lvol>>()
    }

    /// List All Clones.
    fn list_all_clones() -> Vec<Lvol> {
        let bdev = match UntypedBdev::bdev_first() {
            Some(b) => b,
            None => return Vec::new(), /* No devices available, no clones */
        };
        bdev.into_iter()
            .filter(|b| b.driver() == "lvol")
            .map(|b| Lvol::try_from(b).unwrap())
            .filter(|b| b.is_snapshot_clone().is_some())
            .collect::<Vec<Lvol>>()
    }
    /// Check if the snapshot has been discarded.
    fn is_discarded_snapshot(&self) -> bool {
        Lvol::get_blob_xattr(self, SnapshotXattrs::DiscardedSnapshot.name())
            .unwrap_or_default()
            .parse()
            .unwrap_or_default()
    }

    /// During destroying the last linked cloned, if there is any fault
    /// happened, it is possible that, last clone can be deleted, but linked
    /// snapshot marked as discarded still present in the system. As part of
    /// pool import, do the garbage collection to clean the discarded snapshots
    /// leftout in the system.
    async fn destroy_pending_discarded_snapshot() {
        let Some(bdev) = UntypedBdev::bdev_first() else {
            return; /* No devices available */
        };
        let snap_list = bdev
            .into_iter()
            .filter(|b| b.driver() == "lvol")
            .map(|b| Lvol::try_from(b).unwrap())
            .filter(|b| {
                b.is_snapshot()
                    && b.is_discarded_snapshot()
                    && b.list_clones_by_snapshot_uuid().is_empty()
            })
            .collect::<Vec<Lvol>>();
        snap_list
            .iter()
            .for_each(|s| s.reset_snapshot_parent_successor_usage_cache());
        let futures = snap_list.into_iter().map(|s| s.destroy());
        let result = join_all(futures).await;
        for r in result {
            match r {
                Ok(r) => {
                    debug!("Pending discarded snapshot {r:?} destroy success")
                }
                _ => warn!("Pending discarded snapshot destroy failed"),
            }
        }
    }
    // if self is clone or a snapshot whose parent is clone, then do ancestor
    // calculation for all snapshot linked to clone.
    fn calculate_clone_source_snap_usage(
        &self,
        total_ancestor_snap_size: u64,
    ) -> Option<u64> {
        // if self is snapshot created from clone.
        if self.is_snapshot() {
            match UntypedBdev::lookup_by_uuid_str(
                &Lvol::get_blob_xattr(self, SnapshotXattrs::ParentId.name())
                    .unwrap_or_default(),
            ) {
                Some(bdev) => match Lvol::try_from(bdev) {
                    Ok(l) => match l.is_snapshot_clone() {
                        Some(parent_snap_lvol) => {
                            let usage = parent_snap_lvol.usage();
                            Some(
                                total_ancestor_snap_size
                                    - (usage.allocated_bytes_snapshots
                                        + usage.allocated_bytes),
                            )
                        }
                        None => None,
                    },
                    _ => None,
                },
                _ => None,
            }
        // if self is clone.
        } else if self.is_snapshot_clone().is_some() {
            Some(
                self.list_snapshot_by_source_uuid()
                    .iter()
                    .map(|v| v.snapshot_lvol().usage().allocated_bytes)
                    .sum(),
            )
        } else {
            None
        }
    }
    /// When snapshot is destroyed, reset the parent lvol usage cache and its
    /// successor snapshot and clone usage cache.
    fn reset_snapshot_parent_successor_usage_cache(&self) {
        if let Some(snapshot_parent_uuid) =
            Lvol::get_blob_xattr(self, SnapshotXattrs::ParentId.name())
        {
            if let Some(bdev) =
                UntypedBdev::lookup_by_uuid_str(snapshot_parent_uuid.as_str())
            {
                if let Ok(parent_lvol) = Lvol::try_from(bdev) {
                    unsafe {
                        spdk_blob_reset_used_clusters_cache(
                            parent_lvol.blob_checked(),
                        );
                    }
                }
            }
            self.reset_successor_lvol_usage_cache(snapshot_parent_uuid);
        }
    }

    /// When snapshot is destroyed, reset cache of successor snapshots and
    /// clones based on snapshot parent uuid.
    fn reset_successor_lvol_usage_cache(&self, snapshot_parent_uuid: String) {
        let mut successor_snapshots = Lvol::list_all_snapshots()
            .iter()
            .map(|v| v.snapshot_lvol())
            .filter_map(|l| {
                let uuid =
                    Lvol::get_blob_xattr(self, SnapshotXattrs::ParentId.name());
                match uuid {
                    Some(uuid) if uuid == snapshot_parent_uuid => {
                        Some(l.clone())
                    }
                    _ => None,
                }
            })
            .collect::<Vec<Lvol>>();
        let mut successor_clones: Vec<Lvol> = vec![];

        while !successor_snapshots.is_empty() || !successor_clones.is_empty() {
            if let Some(snapshot) = successor_snapshots.pop() {
                unsafe {
                    spdk_blob_reset_used_clusters_cache(
                        snapshot.blob_checked(),
                    );
                }
                let new_clone_list = snapshot.list_clones_by_snapshot_uuid();
                successor_clones.extend(new_clone_list);
            }

            if let Some(clone) = successor_clones.pop() {
                unsafe {
                    spdk_blob_reset_used_clusters_cache(clone.blob_checked());
                }
                let new_snap_list = clone
                    .list_snapshot_by_source_uuid()
                    .iter()
                    .map(|v| v.snapshot_lvol().clone())
                    .collect::<Vec<Lvol>>();
                successor_snapshots.extend(new_snap_list);
            }
        }
    }
}
