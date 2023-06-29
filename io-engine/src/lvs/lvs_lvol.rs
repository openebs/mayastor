use async_trait::async_trait;
use byte_unit::Byte;
use chrono::Utc;
use futures::channel::oneshot;
use nix::errno::Errno;
use pin_utils::core_reexport::fmt::Formatter;

use std::{
    convert::TryFrom,
    ffi::{c_ushort, c_void, CStr, CString},
    fmt::{Debug, Display},
    os::raw::c_char,
    pin::Pin,
    ptr::NonNull,
};

use spdk_rs::libspdk::{
    spdk_blob,
    spdk_blob_calc_used_clusters,
    spdk_blob_get_num_clusters,
    spdk_blob_get_num_clusters_ancestors,
    spdk_blob_get_xattr_value,
    spdk_blob_is_clone,
    spdk_blob_is_read_only,
    spdk_blob_is_snapshot,
    spdk_blob_is_thin_provisioned,
    spdk_blob_set_xattr,
    spdk_blob_sync_md,
    spdk_bs_get_cluster_size,
    spdk_bs_iter_next,
    spdk_lvol,
    spdk_xattr_descriptor,
    vbdev_lvol_create_clone_ext,
    vbdev_lvol_create_snapshot_ext,
    vbdev_lvol_destroy,
    vbdev_lvol_get_from_bdev,
    LVS_CLEAR_WITH_UNMAP,
};

use super::{Error, Lvs};

use crate::{
    bdev::PtplFileOps,
    core::{
        logical_volume::LogicalVolume,
        snapshot::{CloneParams, SnapshotDescriptor, VolumeSnapshotDescriptor},
        Bdev,
        CloneXattrs,
        Protocol,
        PtplProps,
        Share,
        ShareProps,
        SnapshotOps,
        SnapshotParams,
        SnapshotXattrs,
        UntypedBdev,
        UpdateProps,
    },
    ffihelper::{
        cb_arg,
        errno_result_from_i32,
        pair,
        ErrnoResult,
        FfiResult,
        IntoCString,
    },
    lvs::LvolSnapshotIter,
    subsys::NvmfReq,
};
use strum::{EnumCount, IntoEnumIterator};

// Wipe `WIPE_SUPER_LEN` bytes if unmap is not supported.
pub(crate) const WIPE_SUPER_LEN: u64 = (1 << 20) * 8;

/// properties we allow for being set on the lvol, this information is stored on
/// disk
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum PropValue {
    Shared(bool),
    AllowedHosts(Vec<String>),
}

#[derive(Debug)]
#[non_exhaustive]
pub enum PropName {
    Shared,
    AllowedHosts,
}

impl From<&PropValue> for PropName {
    fn from(v: &PropValue) -> Self {
        match v {
            PropValue::Shared(_) => Self::Shared,
            PropValue::AllowedHosts(_) => Self::AllowedHosts,
        }
    }
}
impl From<PropValue> for PropName {
    fn from(v: PropValue) -> Self {
        Self::from(&v)
    }
}

impl Display for PropName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            PropName::Shared => "shared",
            PropName::AllowedHosts => "allowed-hosts",
        };
        write!(f, "{name}")
    }
}

/// Lvol space usage.
#[derive(Default, Copy, Clone, Debug)]
pub struct LvolSpaceUsage {
    /// Lvol size in bytes.
    pub capacity_bytes: u64,
    /// Amount of actually allocated disk space for this replica in bytes.
    pub allocated_bytes: u64,
    /// Cluster size in bytes.
    pub cluster_size: u64,
    /// Total number of clusters.
    pub num_clusters: u64,
    /// Number of actually allocated clusters.
    pub num_allocated_clusters: u64,
    /// Amount of disk space allocated by snapshots of this volume.
    pub allocated_bytes_snapshots: u64,
    /// Number of clusters allocated by snapshots of this volume.
    pub num_allocated_clusters_snapshots: u64,
}
#[derive(Clone)]
/// struct representing an lvol
pub struct Lvol {
    inner: NonNull<spdk_lvol>,
}

impl TryFrom<UntypedBdev> for Lvol {
    type Error = Error;

    fn try_from(mut b: UntypedBdev) -> Result<Self, Self::Error> {
        if b.driver() == "lvol" {
            unsafe {
                Ok(Lvol {
                    inner: NonNull::new_unchecked(vbdev_lvol_get_from_bdev(
                        b.unsafe_inner_mut_ptr(),
                    )),
                })
            }
        } else {
            Err(Error::NotALvol {
                source: Errno::EINVAL,
                name: b.name().to_string(),
            })
        }
    }
}

impl Debug for Lvol {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Lvol '{}/{}/{}' [{}{}]",
            self.pool_name(),
            self.pool_uuid(),
            self.name(),
            if self.is_thin() { "thin " } else { "" },
            Byte::from(self.size()).get_appropriate_unit(true)
        )
    }
}

#[async_trait(? Send)]
impl Share for Lvol {
    type Error = Error;
    type Output = String;

    /// share the lvol as a nvmf target
    async fn share_nvmf(
        mut self: Pin<&mut Self>,
        props: Option<ShareProps>,
    ) -> Result<Self::Output, Self::Error> {
        let allowed_hosts = props
            .as_ref()
            .map(|s| s.allowed_hosts().clone())
            .unwrap_or_default();
        let share = Pin::new(&mut self.as_bdev())
            .share_nvmf(props)
            .await
            .map_err(|e| Error::LvolShare {
                source: e,
                name: self.name(),
            })?;

        self.as_mut().set_no_sync(PropValue::Shared(true)).await?;
        self.as_mut()
            .set(PropValue::AllowedHosts(allowed_hosts))
            .await?;
        info!("{:?}: shared as NVMF", self);
        Ok(share)
    }

    async fn update_properties<P: Into<Option<UpdateProps>>>(
        self: Pin<&mut Self>,
        props: P,
    ) -> Result<(), Self::Error> {
        Pin::new(&mut self.as_bdev())
            .update_properties(props)
            .await
            .map_err(|e| Error::UpdateShareProperties {
                source: e,
                name: self.name(),
            })?;
        Ok(())
    }

    /// unshare the nvmf target
    async fn unshare(mut self: Pin<&mut Self>) -> Result<(), Self::Error> {
        Pin::new(&mut self.as_bdev()).unshare().await.map_err(|e| {
            Error::LvolUnShare {
                source: e,
                name: self.name(),
            }
        })?;

        self.as_mut().set(PropValue::Shared(false)).await?;

        info!("{:?}: unshared ", self);
        Ok(())
    }

    /// return the protocol this bdev is shared under
    fn shared(&self) -> Option<Protocol> {
        self.as_bdev().shared()
    }

    /// returns the share URI this lvol is shared as
    /// this URI includes a UUID as a query parameter which can be used to
    /// uniquely identify a replica as the replica UUID is currently set to its
    /// name, which is *NOT* unique and in MOAC's use case, is the volume UUID
    fn share_uri(&self) -> Option<String> {
        let uri_no_uuid = self.as_bdev().share_uri();
        uri_no_uuid.map(|uri| format!("{}?uuid={}", uri, self.uuid()))
    }

    fn allowed_hosts(&self) -> Vec<String> {
        self.as_bdev().allowed_hosts()
    }

    /// returns the URI that is used to construct the bdev. This is always None
    /// as lvols can not be created by URIs directly, but only through the
    /// ['Lvs'] interface.
    fn bdev_uri(&self) -> Option<String> {
        None
    }

    fn bdev_uri_original(&self) -> Option<String> {
        None
    }
}

impl Lvol {
    /// TODO
    pub(super) fn from_inner_ptr(p: *mut spdk_lvol) -> Self {
        Self {
            inner: NonNull::new(p).unwrap(),
        }
    }

    /// TODO
    #[inline(always)]
    unsafe fn as_inner_ptr(&self) -> *mut spdk_lvol {
        self.inner.as_ptr()
    }

    /// TODO
    #[inline(always)]
    fn as_inner_ref(&self) -> &spdk_lvol {
        unsafe { self.inner.as_ref() }
    }

    /// TODO
    #[inline(always)]
    fn blob_checked(&self) -> *mut spdk_blob {
        let blob = self.as_inner_ref().blob;
        assert!(!blob.is_null());
        blob
    }

    // wipe the first 8MB if unmap is not supported on failure the operation
    // needs to be repeated
    pub async fn wipe_super(&self) -> Result<(), Error> {
        if self.as_inner_ref().clear_method != LVS_CLEAR_WITH_UNMAP {
            let hdl = Bdev::open(&self.as_bdev(), true)
                .and_then(|desc| desc.into_handle())
                .map_err(|e| {
                    error!(?self, ?e, "failed to wipe lvol");
                    Error::RepDestroy {
                        source: Errno::ENXIO,
                        name: self.name(),
                        msg: "failed to wipe lvol".into(),
                    }
                })?;

            // write zero to the first 8MB which wipes the metadata and the
            // first 4MB of the data partition
            let wipe_size =
                std::cmp::min(self.as_bdev().size_in_bytes(), WIPE_SUPER_LEN);
            hdl.write_zeroes_at(0, wipe_size).await.map_err(|e| {
                error!(?self, ?e);
                Error::RepDestroy {
                    source: Errno::EIO,
                    name: self.name(),
                    msg: "failed to write to lvol".into(),
                }
            })?;
        }
        Ok(())
    }

    /// generic callback for lvol operations
    pub(crate) extern "C" fn lvol_cb(
        sender_ptr: *mut c_void,
        lvol_ptr: *mut spdk_lvol,
        errno: i32,
    ) {
        let sender = unsafe {
            Box::from_raw(
                sender_ptr as *mut oneshot::Sender<ErrnoResult<*mut spdk_lvol>>,
            )
        };
        sender
            .send(errno_result_from_i32(lvol_ptr, errno))
            .expect("Receiver is gone");
    }
    /// Format snapshot name
    /// base_name is the nexus or replica UUID
    pub fn format_snapshot_name(base_name: &str, snapshot_time: u64) -> String {
        format!("{base_name}-snap-{snapshot_time}")
    }
    /// Get a `PtplFileOps` from `&self`.
    pub(crate) fn ptpl(&self) -> impl PtplFileOps {
        LvolPtpl::from(self)
    }

    /// TODO:
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
    fn create_snapshot_inner(
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
        self.create_snapshot_inner(&snap_param, done_cb, done_cb_arg)?;

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
        self.create_snapshot_inner(&snap_param, done_cb, done_cb_arg)?;
        Ok(())
    }
    /// Common API to get the xattr from blob.
    pub fn get_blob_xattr(lvol: &Lvol, attr: &str) -> Option<String> {
        let mut val: *const libc::c_char = std::ptr::null::<libc::c_char>();
        let mut size: u64 = 0;
        let attribute = attr.into_cstring();

        unsafe {
            let blob = lvol.bs_iter_first();
            let r = spdk_blob_get_xattr_value(
                blob,
                attribute.as_ptr(),
                &mut val as *mut *const c_char as *mut *const c_void,
                &mut size as *mut u64,
            );

            if r != 0 || size == 0 {
                return None;
            }

            // Parse attribute into a string, transparently removing
            // null-terminating character for SPDK system attributes like UUID,
            // which are stored in C-string format.
            let mut last_char = val.offset((size as isize) - 1);

            // Attribute contains null-terminated string, so remove all zero
            // characters from it. Always assume there can potentially be more
            // then one null terminating character (by mistake), so
            // skip them all.
            if *last_char == 0 {
                size -= 1; // account the first null terminator.

                while size > 0 {
                    last_char = val.offset((size as isize) - 1);

                    if *last_char != 0 {
                        break;
                    }

                    size -= 1;
                }

                // Make sure attribute still contains non-null terminating
                // characters (assume malformed attribute value
                // contains only zeroes).
                if size == 0 {
                    warn!(?lvol, ?attribute, "attribute contains no value",);
                    return None;
                }
            }

            let sl =
                std::slice::from_raw_parts(val as *const u8, size as usize);
            std::str::from_utf8(sl).map_or_else(
                |error| {
                    warn!(
                        ?lvol,
                        attribute = attr,
                        ?error,
                        "Failed to parse attribute, default to empty string"
                    );
                    None
                },
                |v| Some(v.to_string()),
            )
        }
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
    fn create_clone_inner(
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
        self.create_clone_inner(&clone_param, done_cb, done_cb_arg)?;

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
    pub fn snapshot_descriptor(
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
                            warn!("presisted parent ?curr_attr_val not matched to input parent ?parent_lvol.uuid()");
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
    /// List All Snapshot.
    pub fn list_all_snapshots() -> Vec<VolumeSnapshotDescriptor> {
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
    /// List All Clones.
    pub fn list_all_clones() -> Vec<Lvol> {
        let bdev = match UntypedBdev::bdev_first() {
            Some(b) => b,
            None => return Vec::new(), /* No devices available, no clones */
        };
        bdev.into_iter()
            .filter(|b| b.driver() == "lvol")
            .map(|b| Lvol::try_from(b).unwrap())
            .filter(|b| b.is_clone())
            .filter(|b| {
                Lvol::get_blob_xattr(b, CloneXattrs::SourceUuid.name())
                    .is_some()
            })
            .collect::<Vec<Lvol>>()
    }
}

struct LvolPtpl {
    lvs: super::lvs_store::LvsPtpl,
    uuid: String,
}
impl LvolPtpl {
    fn lvs(&self) -> &super::lvs_store::LvsPtpl {
        &self.lvs
    }
    fn uuid(&self) -> &str {
        &self.uuid
    }
}
impl From<&Lvol> for LvolPtpl {
    fn from(lvol: &Lvol) -> Self {
        Self {
            lvs: (&lvol.lvs()).into(),
            uuid: lvol.uuid(),
        }
    }
}

impl PtplFileOps for LvolPtpl {
    fn create(&self) -> Result<Option<PtplProps>, std::io::Error> {
        if let Some(path) = self.path() {
            self.lvs().create()?;
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            return Ok(Some(PtplProps::new(path)));
        }
        Ok(None)
    }

    fn destroy(&self) -> Result<(), std::io::Error> {
        if let Some(path) = self.path() {
            std::fs::remove_file(path)?;
        }
        Ok(())
    }

    fn subpath(&self) -> std::path::PathBuf {
        self.lvs()
            .subpath()
            .join("replica/")
            .join(self.uuid())
            .with_extension("json")
    }
}

///  LvsLvol Trait Provide the interface for all the Volume Specific Interface
#[async_trait(?Send)]
pub trait LvsLvol: LogicalVolume + Share {
    /// Return lvs for the Logical Volume.
    fn lvs(&self) -> Lvs;
    /// Returns the underlying bdev of the lvol.
    fn as_bdev(&self) -> UntypedBdev;

    /// Returns a boolean indicating if the lvol is a snapshot.
    fn is_snapshot(&self) -> bool;

    /// Returns a boolean indicating if the lvol is a clone.
    fn is_clone(&self) -> bool;

    /// Get/Read a property from this lvol from disk.
    async fn get(&self, prop: PropName) -> Result<PropValue, Error>;

    /// Destroy the lvol.
    async fn destroy(mut self) -> Result<String, Error>;

    /// Write the property prop on to the lvol but do not sync the metadata yet.
    async fn set_no_sync(
        self: Pin<&mut Self>,
        prop: PropValue,
    ) -> Result<(), Error>;

    /// Write the property prop on to the lvol which is stored on disk.
    async fn set(
        mut self: Pin<&mut Self>,
        prop: PropValue,
    ) -> Result<(), Error>;

    /// Callback executed after synchronizing the lvols metadata.
    extern "C" fn blob_sync_cb(sender_ptr: *mut c_void, errno: i32);

    /// Write the property prop on to the lvol which is stored on disk.
    async fn sync_metadata(self: Pin<&mut Self>) -> Result<(), Error>;

    /// Create a snapshot in Remote.
    async fn create_snapshot_remote(
        &self,
        nvmf_req: &NvmfReq,
        snapshot_params: SnapshotParams,
    );
    /// Callback is executed when blobstore fetching is done using spdk api.
    extern "C" fn blob_op_complete_cb(
        arg: *mut c_void,
        _blob: *mut spdk_blob,
        errno: i32,
    );

    /// Get the first spdk_blob from the Lvol Blobstor.
    fn bs_iter_first(&self) -> *mut spdk_blob;

    /// Get the next spdk_blob from the current blob.
    async fn bs_iter_next(
        &self,
        curr_blob: *mut spdk_blob,
    ) -> Option<*mut spdk_blob>;

    /// Build Snapshot Parameters from Blob.
    fn build_snapshot_param(&self, blob: *mut spdk_blob) -> SnapshotParams;
}

///  LogicalVolume implement Generic interface for Lvol.
impl LogicalVolume for Lvol {
    /// Returns the name of the Snapshot.
    fn name(&self) -> String {
        self.as_bdev().name().to_string()
    }

    /// Returns the UUID of the Snapshot.
    fn uuid(&self) -> String {
        self.as_bdev().uuid_as_string()
    }

    /// Returns the pool name of the Snapshot.
    fn pool_name(&self) -> String {
        self.lvs().name().to_string()
    }

    /// Returns the pool uuid of the Snapshot.
    fn pool_uuid(&self) -> String {
        self.lvs().uuid()
    }

    /// Returns a boolean indicating if the Logical Volume is thin provisioned.
    fn is_thin(&self) -> bool {
        unsafe { spdk_blob_is_thin_provisioned(self.blob_checked()) }
    }

    /// Returns a boolean indicating if the Logical Volume is read-only.
    fn is_read_only(&self) -> bool {
        unsafe { spdk_blob_is_read_only(self.blob_checked()) }
    }

    /// Return the size of the Snapshot in bytes.
    fn size(&self) -> u64 {
        self.as_bdev().size_in_bytes()
    }

    /// Returns Lvol disk space usage.
    fn usage(&self) -> LvolSpaceUsage {
        let bs = self.lvs().blob_store();
        let blob = self.blob_checked();
        unsafe {
            let cluster_size = spdk_bs_get_cluster_size(bs);
            let num_clusters = spdk_blob_get_num_clusters(blob);
            let num_allocated_clusters = spdk_blob_calc_used_clusters(blob);

            let num_allocated_clusters_snapshots = {
                let mut c: u64 = 0;

                match spdk_blob_get_num_clusters_ancestors(bs, blob, &mut c) {
                    0 => c,
                    errno => {
                        error!(
                            ?self,
                            errno, "Failed to get snapshot space usage"
                        );
                        0
                    }
                }
            };

            LvolSpaceUsage {
                capacity_bytes: self.size(),
                allocated_bytes: cluster_size * num_allocated_clusters,
                cluster_size,
                num_clusters,
                num_allocated_clusters,
                num_allocated_clusters_snapshots,
                allocated_bytes_snapshots: cluster_size
                    * num_allocated_clusters_snapshots,
            }
        }
    }
}

/// LvsLvol Trait Implementation for Lvol for Volume Specific Interface.
#[async_trait(? Send)]
impl LvsLvol for Lvol {
    /// Return lvs for the Logical Volume.
    fn lvs(&self) -> Lvs {
        Lvs::from_inner_ptr(self.as_inner_ref().lvol_store)
    }

    /// Returns the underlying bdev of the lvol.
    fn as_bdev(&self) -> UntypedBdev {
        Bdev::checked_from_ptr(self.as_inner_ref().bdev).unwrap()
    }

    /// Returns a boolean indicating if the lvol is a snapshot.
    fn is_snapshot(&self) -> bool {
        unsafe { spdk_blob_is_snapshot(self.blob_checked()) }
    }
    /// Returns a boolean indicating if the lvol is a clone.
    fn is_clone(&self) -> bool {
        unsafe { spdk_blob_is_clone(self.blob_checked()) }
    }
    /// Get/Read a property from this lvol from disk.
    async fn get(&self, prop: PropName) -> Result<PropValue, Error> {
        let blob = self.blob_checked();

        match prop {
            PropName::Shared => {
                let name = prop.to_string().into_cstring();
                let mut value: *const libc::c_char =
                    std::ptr::null::<libc::c_char>();
                let mut value_len: u64 = 0;
                unsafe {
                    spdk_blob_get_xattr_value(
                        blob,
                        name.as_ptr(),
                        &mut value as *mut *const c_char as *mut *const c_void,
                        &mut value_len,
                    )
                }
                .to_result(|e| Error::GetProperty {
                    source: Errno::from_i32(e),
                    prop,
                    name: self.name(),
                })?;
                match unsafe { CStr::from_ptr(value).to_str() } {
                    Ok("true") => Ok(PropValue::Shared(true)),
                    Ok("false") => Ok(PropValue::Shared(false)),
                    _ => Err(Error::Property {
                        source: Errno::EINVAL,
                        name: self.name(),
                    }),
                }
            }
            PropName::AllowedHosts => {
                let name = prop.to_string().into_cstring();
                let mut value: *const libc::c_char =
                    std::ptr::null::<libc::c_char>();
                let mut value_len: u64 = 0;
                unsafe {
                    spdk_blob_get_xattr_value(
                        blob,
                        name.as_ptr(),
                        &mut value as *mut *const c_char as *mut *const c_void,
                        &mut value_len,
                    )
                }
                .to_result(|e| Error::GetProperty {
                    source: Errno::from_i32(e),
                    prop,
                    name: self.name(),
                })?;
                match unsafe { CStr::from_ptr(value).to_str() } {
                    Ok(list) if list.is_empty() => {
                        Ok(PropValue::AllowedHosts(vec![]))
                    }
                    Ok(list) => Ok(PropValue::AllowedHosts(
                        list.split(',')
                            .map(|s| s.to_string())
                            .collect::<Vec<_>>(),
                    )),
                    _ => Err(Error::Property {
                        source: Errno::EINVAL,
                        name: self.name(),
                    }),
                }
            }
        }
    }

    /// Callback executed after synchronizing the lvols metadata.
    extern "C" fn blob_sync_cb(sender_ptr: *mut c_void, errno: i32) {
        let sender =
            unsafe { Box::from_raw(sender_ptr as *mut oneshot::Sender<i32>) };
        sender.send(errno).expect("blob cb receiver is gone");
    }
    /// Destroy the lvol.
    async fn destroy(mut self) -> Result<String, Error> {
        extern "C" fn destroy_cb(sender: *mut c_void, errno: i32) {
            let sender =
                unsafe { Box::from_raw(sender as *mut oneshot::Sender<i32>) };
            sender.send(errno).unwrap();
        }

        // We must always unshare before destroying bdev.
        let _ = Pin::new(&mut self).unshare().await;

        let name = self.name();
        let ptpl = self.ptpl();

        let (s, r) = pair::<i32>();
        unsafe {
            vbdev_lvol_destroy(self.as_inner_ptr(), Some(destroy_cb), cb_arg(s))
        };

        r.await
            .expect("lvol destroy callback is gone")
            .to_result(|e| {
                warn!("error while destroying lvol {}", name);
                Error::RepDestroy {
                    source: Errno::from_i32(e),
                    name: name.clone(),
                    msg: "error while destroying lvol".into(),
                }
            })?;
        if let Err(error) = ptpl.destroy() {
            tracing::error!(
                "{}: Failed to clean up persistence through power loss for replica: {}",
                name,
                error
            );
        }

        info!("destroyed lvol {}", name);
        Ok(name)
    }

    /// Write the property prop on to the lvol but do not sync the metadata yet.
    async fn set_no_sync(
        self: Pin<&mut Self>,
        prop: PropValue,
    ) -> Result<(), Error> {
        let blob = self.blob_checked();

        if self.is_snapshot() {
            warn!("ignoring set property on snapshot {}", self.name());
            return Ok(());
        }
        if self.is_read_only() {
            warn!("{} is read-only", self.name());
        }
        match prop.clone() {
            PropValue::Shared(val) => {
                let name = PropName::from(&prop).to_string().into_cstring();
                let value = if val { "true" } else { "false" }.into_cstring();
                unsafe {
                    spdk_blob_set_xattr(
                        blob,
                        name.as_ptr(),
                        value.as_bytes_with_nul().as_ptr() as *const _,
                        value.as_bytes_with_nul().len() as u16,
                    )
                }
                .to_result(|e| Error::SetProperty {
                    source: Errno::from_i32(e),
                    prop: prop.into(),
                    name: self.name(),
                })?;
            }
            PropValue::AllowedHosts(hosts) => {
                let name = PropName::from(&prop).to_string().into_cstring();
                let value = hosts.join(",").into_cstring();
                unsafe {
                    spdk_blob_set_xattr(
                        blob,
                        name.as_ptr(),
                        value.as_bytes_with_nul().as_ptr() as *const _,
                        value.as_bytes_with_nul().len() as u16,
                    )
                }
                .to_result(|e| Error::SetProperty {
                    source: Errno::from_i32(e),
                    prop: prop.into(),
                    name: self.name(),
                })?;
            }
        }
        Ok(())
    }

    /// Write the property prop on to the lvol which is stored on disk.
    async fn set(
        mut self: Pin<&mut Self>,
        prop: PropValue,
    ) -> Result<(), Error> {
        self.as_mut().set_no_sync(prop).await?;
        self.sync_metadata().await
    }

    /// Write the property prop on to the lvol which is stored on disk.
    async fn sync_metadata(self: Pin<&mut Self>) -> Result<(), Error> {
        let blob = self.blob_checked();

        if self.is_snapshot() {
            return Ok(());
        }
        if self.is_read_only() {
            warn!("{} is read-only", self.name());
        }

        let (s, r) = pair::<i32>();
        unsafe {
            spdk_blob_sync_md(blob, Some(Self::blob_sync_cb), cb_arg(s));
        };

        r.await.expect("sync callback is gone").to_result(|e| {
            Error::SyncProperty {
                source: Errno::from_i32(e),
                name: self.name(),
            }
        })?;

        Ok(())
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

    /// Blobstore Common Callback function.
    extern "C" fn blob_op_complete_cb(
        arg: *mut c_void,
        blob: *mut spdk_blob,
        errno: i32,
    ) {
        let s = unsafe {
            Box::from_raw(arg as *mut oneshot::Sender<(*mut spdk_blob, i32)>)
        };
        if errno != 0 {
            error!("Blobstore Operation failed, errno {}", errno);
        }
        s.send((blob, errno)).ok();
    }

    /// Get the first spdk_blob from the Lvol Blobstor.
    fn bs_iter_first(&self) -> *mut spdk_blob {
        self.as_inner_ref().blob
    }

    /// Get the next spdk_blob from the current blob.
    async fn bs_iter_next(
        &self,
        curr_blob: *mut spdk_blob,
    ) -> Option<*mut spdk_blob> {
        let (s, r) = oneshot::channel::<(*mut spdk_blob, i32)>();
        unsafe {
            spdk_bs_iter_next(
                self.lvs().blob_store(),
                curr_blob,
                Some(Self::blob_op_complete_cb),
                cb_arg(s),
            )
        };
        match r.await {
            Ok((blob, _err)) => Some(blob),
            Err(_) => None,
        }
    }

    /// Build Snapshot Parameters from Blob.
    fn build_snapshot_param(&self, _blob: *mut spdk_blob) -> SnapshotParams {
        // TODO: need to Integrate with Snapshot Property Enumeration
        // Currently it is stub.
        SnapshotParams::new(
            Some(self.name()),
            Some(self.name()),
            Some(self.name()),
            Some(self.name()),
            Some(self.name()),
            Some(Utc::now().to_string()),
        )
    }
}

#[async_trait(?Send)]
impl SnapshotOps for Lvol {
    type Error = Error;
    type SnapshotIter = LvolSnapshotIter;
    type Lvol = Lvol;
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
    /// Get a Snapshot Iterator.
    async fn snapshot_iter(self) -> LvolSnapshotIter {
        LvolSnapshotIter::new(self)
    }
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
        ))
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

    /// List clones based on snapshot_uuid.
    fn list_clones_by_snapshot_uuid(&self) -> Vec<Lvol> {
        let bdev = match UntypedBdev::bdev_first() {
            Some(b) => b,
            None => return Vec::new(), /* No devices available, no clones */
        };
        bdev.into_iter()
            .filter(|b| b.driver() == "lvol")
            .map(|b| Lvol::try_from(b).unwrap())
            .filter(|b| b.is_clone())
            .filter(|b| {
                let source_uuid =
                    Lvol::get_blob_xattr(b, CloneXattrs::SourceUuid.name())
                        .unwrap_or_default();
                // If clone source uuid is match with snapshot uuid
                source_uuid == self.uuid()
            })
            .collect::<Vec<Lvol>>()
    }
}
