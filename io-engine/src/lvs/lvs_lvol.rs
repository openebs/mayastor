use async_trait::async_trait;
use byte_unit::Byte;
use events_api::event::EventAction;
use futures::channel::oneshot;
use nix::errno::Errno;
use pin_utils::core_reexport::fmt::Formatter;

use std::{
    convert::TryFrom,
    ffi::{c_ushort, c_void, CStr},
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
    spdk_blob_is_read_only,
    spdk_blob_is_thin_provisioned,
    spdk_blob_set_xattr,
    spdk_blob_sync_md,
    spdk_bs_get_cluster_size,
    spdk_bs_get_parent_blob,
    spdk_bs_iter_next,
    spdk_lvol,
    vbdev_lvol_destroy,
    vbdev_lvol_get_from_bdev,
    vbdev_lvol_resize,
    LVS_CLEAR_WITH_UNMAP,
};

use super::{BsError, Lvs, LvsError};

use crate::{
    bdev::PtplFileOps,
    core::{
        logical_volume::{LogicalVolume, LvolSpaceUsage},
        Bdev,
        CloneXattrs,
        LvolSnapshotOps,
        NvmfShareProps,
        Protocol,
        PtplProps,
        Share,
        SnapshotXattrs,
        UntypedBdev,
        UpdateProps,
    },
    eventing::Event,
    ffihelper::{
        cb_arg,
        done_cb,
        errno_result_from_i32,
        pair,
        ErrnoResult,
        FfiResult,
        IntoCString,
    },
    pool_backend::PoolBackend,
};

// Wipe `WIPE_SUPER_LEN` bytes if unmap is not supported.
pub(crate) const WIPE_SUPER_LEN: u64 = (1 << 20) * 8;

/// properties we allow for being set on the lvol, this information is stored on
/// disk
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum PropValue {
    Shared(bool),
    AllowedHosts(Vec<String>),
    EntityId(String),
}

#[derive(Debug, Copy, Clone)]
#[non_exhaustive]
pub enum PropName {
    Shared,
    AllowedHosts,
    EntityId,
}

impl From<&PropValue> for PropName {
    fn from(v: &PropValue) -> Self {
        match v {
            PropValue::Shared(_) => Self::Shared,
            PropValue::AllowedHosts(_) => Self::AllowedHosts,
            PropValue::EntityId(_) => Self::EntityId,
        }
    }
}
impl From<PropValue> for PropName {
    fn from(v: PropValue) -> Self {
        Self::from(&v)
    }
}

impl Display for PropValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl Display for PropName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            PropName::Shared => "shared",
            PropName::AllowedHosts => "allowed-hosts",
            PropName::EntityId => "entity_id",
        };
        write!(f, "{name}")
    }
}

/// Resize context to be passed as callback args pointer to spdk.
struct ResizeCbCtx {
    /// The lvol to be resized.
    lvol: *mut spdk_lvol,
    /// Oneshot sender for sending resize operation result.
    sender: *mut c_void,
    /// The new requested size of lvol.
    req_size: u64,
}

#[derive(Clone)]
/// struct representing an lvol
pub struct Lvol {
    inner: NonNull<spdk_lvol>,
}

impl TryFrom<UntypedBdev> for Lvol {
    type Error = LvsError;

    fn try_from(mut b: UntypedBdev) -> Result<Self, Self::Error> {
        if Lvol::is_lvol(&b) {
            unsafe {
                Ok(Lvol {
                    inner: NonNull::new_unchecked(vbdev_lvol_get_from_bdev(
                        b.unsafe_inner_mut_ptr(),
                    )),
                })
            }
        } else {
            Err(LvsError::NotALvol {
                source: BsError::InvalidArgument {},
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
    type Error = LvsError;
    type Output = String;

    /// share the lvol as a nvmf target
    async fn share_nvmf(
        mut self: Pin<&mut Self>,
        props: Option<NvmfShareProps>,
    ) -> Result<Self::Output, Self::Error> {
        let allowed_hosts = props
            .as_ref()
            .map(|s| s.allowed_hosts().clone())
            .unwrap_or_default();
        let share = Pin::new(&mut self.as_bdev())
            .share_nvmf(props)
            .await
            .map_err(|e| LvsError::LvolShare {
                source: e,
                name: self.name(),
            })?;

        self.as_mut()
            .set_props(vec![
                PropValue::Shared(true),
                PropValue::AllowedHosts(allowed_hosts),
            ])
            .await?;
        info!("{:?}: shared as NVMF", self);
        Ok(share)
    }

    fn create_ptpl(&self) -> Result<Option<PtplProps>, Self::Error> {
        self.ptpl().create().map_err(|source| LvsError::LvolShare {
            source: crate::core::CoreError::Ptpl {
                reason: source.to_string(),
            },
            name: self.name(),
        })
    }

    async fn update_properties<P: Into<Option<UpdateProps>>>(
        mut self: Pin<&mut Self>,
        props: P,
    ) -> Result<(), Self::Error> {
        let props = UpdateProps::from(props.into());
        let allowed_hosts = props.allowed_hosts().clone();
        // Set but don't sync in lvol metadata. Sync will happen in case the
        // node reboots e.g during upgrade.
        self.as_mut()
            .set_no_sync(PropValue::AllowedHosts(allowed_hosts))
            .await?;

        Pin::new(&mut self.as_bdev())
            .update_properties(props)
            .await
            .map_err(|e| LvsError::UpdateShareProperties {
                source: e,
                name: self.name(),
            })?;
        Ok(())
    }

    /// unshare the nvmf target
    async fn unshare(mut self: Pin<&mut Self>) -> Result<(), Self::Error> {
        Pin::new(&mut self.as_bdev()).unshare().await.map_err(|e| {
            LvsError::LvolUnShare {
                source: e,
                name: self.name(),
            }
        })?;

        self.as_mut().set(PropValue::Shared(false)).await?;

        info!("{:?}: unshared", self);
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
    fn bdev_uri(&self) -> Option<url::Url> {
        None
    }

    fn bdev_uri_original(&self) -> Option<url::Url> {
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
    pub fn as_inner_ref(&self) -> &spdk_lvol {
        unsafe { self.inner.as_ref() }
    }

    pub fn ok_from(mut bdev: UntypedBdev) -> Option<Self> {
        if !Self::is_lvol(&bdev) {
            return None;
        }
        Some(unsafe {
            Lvol {
                inner: NonNull::new_unchecked(vbdev_lvol_get_from_bdev(
                    bdev.unsafe_inner_mut_ptr(),
                )),
            }
        })
    }

    pub fn is_lvol(bdev: &UntypedBdev) -> bool {
        bdev.driver() == "lvol"
    }

    /// Wipe the first 8MB if unmap is not supported on failure the operation
    /// needs to be repeated.
    pub async fn wipe_super(&self) -> Result<(), LvsError> {
        if self.as_inner_ref().clear_method != LVS_CLEAR_WITH_UNMAP {
            let hdl = Bdev::open(&self.as_bdev(), true)
                .and_then(|desc| desc.into_handle())
                .map_err(|e| {
                    error!(?self, ?e, "failed to wipe lvol");
                    LvsError::RepDestroy {
                        source: BsError::from_errno(Errno::ENXIO),
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
                LvsError::RepDestroy {
                    source: BsError::from_errno(Errno::EIO),
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
    /// Get a `PtplFileOps` from `&self`.
    pub(crate) fn ptpl(&self) -> impl PtplFileOps {
        LvolPtpl::from(self)
    }

    /// Common API to get the xattr from blob.
    pub fn get_blob_xattr(blob: *mut spdk_blob, attr: &str) -> Option<String> {
        if blob.is_null() {
            return None;
        }
        let blob_inner = blob;
        let mut val: *const libc::c_char = std::ptr::null::<libc::c_char>();
        let mut size: u64 = 0;
        let attribute = attr.into_cstring();

        unsafe {
            let r = spdk_blob_get_xattr_value(
                blob_inner,
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
                    warn!(?attribute, "attribute contains no value",);
                    return None;
                }
            }

            let sl =
                std::slice::from_raw_parts(val as *const u8, size as usize);
            std::str::from_utf8(sl).map_or_else(
                |error| {
                    warn!(
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

    /// Low-level function to set blob attributes.
    pub async fn set_blob_attr<A: AsRef<str>>(
        &self,
        attr: A,
        value: String,
        sync_metadata: bool,
    ) -> Result<(), LvsError> {
        extern "C" fn blob_attr_set_cb(cb_arg: *mut c_void, errno: i32) {
            done_cb(cb_arg, errno);
        }

        let attr_name = attr.as_ref().into_cstring();
        let attr_val = value.clone().into_cstring();

        let r = unsafe {
            spdk_blob_set_xattr(
                self.blob_checked(),
                attr_name.as_ptr() as *const c_char,
                attr_val.as_ptr() as *const c_void,
                attr_val.to_bytes().len() as c_ushort,
            )
        };

        if r != 0 {
            error!(
                lvol = self.name(),
                attr = attr.as_ref(),
                value,
                errno = r,
                "Failed to set blob attribute"
            );
            return Err(LvsError::SetProperty {
                source: BsError::from_i32(r),
                prop: attr.as_ref().to_owned(),
                name: self.name(),
            });
        }

        if !sync_metadata {
            return Ok(());
        }

        // Sync metadata if requested.
        let (snd, rcv) = oneshot::channel::<i32>();

        unsafe {
            spdk_blob_sync_md(
                self.blob_checked(),
                Some(blob_attr_set_cb),
                cb_arg(snd),
            )
        };

        match rcv.await.expect("sync attribute callback disappeared") {
            0 => Ok(()),
            errno => {
                error!(lvol=self.name(), errno,"Failed to sync blob metadata, properties might be out of sync");
                Err(LvsError::SyncProperty {
                    source: BsError::from_i32(errno),
                    name: self.name(),
                })
            }
        }
    }
}

pub struct LvolPtpl {
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

    /// Lvol is considered as clone if its sourceuuid attribute is a valid
    /// snapshot. if it is clone, return the snapshot lvol.
    fn is_snapshot_clone(&self) -> Option<Lvol>;

    /// Get/Read a property of this lvol from the in-memory metadata copy.
    async fn get(&self, prop: PropName) -> Result<PropValue, LvsError>;

    /// Destroy the lvol.
    async fn destroy(mut self) -> Result<String, LvsError>;

    /// Write the property prop on to the lvol but do not sync the metadata yet.
    /// Returns whether the property was modified or not.
    async fn set_no_sync(
        self: Pin<&mut Self>,
        prop: PropValue,
    ) -> Result<bool, LvsError>;

    /// Write the property prop on to the lvol which is stored on disk.
    /// If the property has been modified the metadata is synced.
    async fn set(
        mut self: Pin<&mut Self>,
        prop: PropValue,
    ) -> Result<(), LvsError>;

    /// Write the properties on to the lvol which is stored on disk.
    /// If any of the properties are modified the metadata is synced.
    async fn set_props(
        mut self: Pin<&mut Self>,
        props: Vec<PropValue>,
    ) -> Result<(), LvsError> {
        let mut sync = false;
        for property in props {
            if self.as_mut().set_no_sync(property).await? {
                sync = true;
            }
        }
        if sync {
            self.sync_metadata().await?;
        }
        Ok(())
    }

    /// Callback executed after synchronizing the lvols metadata.
    extern "C" fn blob_sync_cb(sender_ptr: *mut c_void, errno: i32);

    /// Write the property prop on to the lvol which is stored on disk.
    async fn sync_metadata(self: Pin<&mut Self>) -> Result<(), LvsError>;

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

    /// Get the next spdk_blob from the parent blob.
    unsafe fn bs_iter_parent(
        &self,
        curr_blob: *mut spdk_blob,
    ) -> Option<*mut spdk_blob>;

    /// Get lvol inner ptr.
    fn as_inner_ptr(&self) -> *mut spdk_lvol;

    /// Get BlobPtr from spdk_lvol.
    fn blob_checked(&self) -> *mut spdk_blob;

    /// Wrapper function to destroy replica and its associated snapshot if
    /// replica is identified as last clone.
    async fn destroy_replica(mut self) -> Result<String, LvsError>;

    /// Resize a replica. The resize can be expand or shrink, depending
    /// upon if required size is more or less than current size of
    /// the replica.
    async fn resize_replica(&mut self, resize_to: u64) -> Result<(), LvsError>;
}

/// LogicalVolume implement Generic interface for Lvol.
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

    /// Returns entity id of the Logical Volume.
    fn entity_id(&self) -> Option<String> {
        Lvol::get_blob_xattr(self.blob_checked(), "entity_id")
    }

    /// Returns a boolean indicating if the Logical Volume is thin provisioned.
    fn is_thin(&self) -> bool {
        unsafe { spdk_blob_is_thin_provisioned(self.blob_checked()) }
    }

    /// Returns a boolean indicating if the Logical Volume is read-only.
    fn is_read_only(&self) -> bool {
        unsafe { spdk_blob_is_read_only(self.blob_checked()) }
    }

    /// Return the size of the Logical Volume in bytes.
    fn size(&self) -> u64 {
        self.as_bdev().size_in_bytes()
    }
    /// Return the committed size of the Logical Volume in bytes.
    fn committed(&self) -> u64 {
        match self.is_snapshot() {
            true => self.allocated(),
            false => self.size(),
        }
    }
    /// Return the allocated size of the Logical Volume in bytes.
    fn allocated(&self) -> u64 {
        let bs = self.lvs().blob_store();
        let blob = self.blob_checked();
        let cluster_size = unsafe { spdk_bs_get_cluster_size(bs) };
        let num_allocated_clusters =
            unsafe { spdk_blob_calc_used_clusters(blob) };
        cluster_size * num_allocated_clusters
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
            let allocated_bytes_snapshots =
                cluster_size * num_allocated_clusters_snapshots;
            LvolSpaceUsage {
                capacity_bytes: self.size(),
                allocated_bytes: cluster_size * num_allocated_clusters,
                cluster_size,
                num_clusters,
                num_allocated_clusters,
                num_allocated_clusters_snapshots,
                allocated_bytes_snapshots,
                // If there are multiple snapshots created from replica and
                // then a clone is created from snapshot, following multiple
                // snapshots created from clones in following sequence,
                // R1 => S1 -> S2 => S3 => C1 => S4 => S5
                // For S5, allocated_bytes_snapshots will be S1+S2+S3+S4+S5
                // Where as allocated_bytes_snapshot_from_clone will S4 + S5
                // for the clone C1. For S5 allocated_bytes_snapshot_from_clone
                // will consider ancestor value from C1.
                allocated_bytes_snapshot_from_clone: self
                    .calculate_clone_source_snap_usage(
                        allocated_bytes_snapshots,
                    ),
            }
        }
    }

    /// Returns a boolean indicating if the lvol is a snapshot.
    /// Currently in place of SPDK native API to judge lvol as snapshot, xattr
    /// is checked here. When there is only single Lvol(snapshot) present in
    /// the system and there is restart of io-engine and pool import
    /// happens, SPDK native API consider lvol(snapshot) as normal lvol.
    /// Looks like a bug in SPDK, but all snapshot attribute are intact in
    /// SPDK after io-engine restarts.
    fn is_snapshot(&self) -> bool {
        Lvol::get_blob_xattr(
            self.blob_checked(),
            SnapshotXattrs::SnapshotCreateTime.name(),
        )
        .is_some()
    }

    fn is_clone(&self) -> bool {
        self.is_snapshot_clone().is_some()
    }

    fn backend(&self) -> PoolBackend {
        PoolBackend::Lvs
    }

    fn snapshot_uuid(&self) -> Option<String> {
        Lvol::get_blob_xattr(
            self.blob_checked(),
            CloneXattrs::SourceUuid.name(),
        )
    }

    fn share_protocol(&self) -> Protocol {
        self.shared().unwrap_or_default()
    }

    fn bdev_share_uri(&self) -> Option<String> {
        self.share_uri()
    }

    fn nvmf_allowed_hosts(&self) -> Vec<String> {
        self.allowed_hosts()
    }
}

/// LvsLvol Trait Implementation for Lvol for Volume Specific Interface.
#[async_trait(?Send)]
impl LvsLvol for Lvol {
    /// Return lvs for the Logical Volume.
    fn lvs(&self) -> Lvs {
        Lvs::from_inner_ptr(self.as_inner_ref().lvol_store)
    }

    /// Returns the underlying bdev of the lvol.
    fn as_bdev(&self) -> UntypedBdev {
        Bdev::checked_from_ptr(self.as_inner_ref().bdev).unwrap()
    }

    /// Lvol is considered as clone if its sourceuuid attribute is a valid
    /// snapshot. if it is clone, return the snapshot lvol.
    fn is_snapshot_clone(&self) -> Option<Lvol> {
        if let Some(source_uuid) = Lvol::get_blob_xattr(
            self.blob_checked(),
            CloneXattrs::SourceUuid.name(),
        ) {
            let snap_lvol =
                match UntypedBdev::lookup_by_uuid_str(source_uuid.as_str()) {
                    Some(bdev) => match Lvol::try_from(bdev) {
                        Ok(l) => l,
                        _ => return None,
                    },
                    None => return None,
                };
            return Some(snap_lvol);
        }
        None
    }

    /// Get/Read a property of this lvol from the in-memory metadata copy.
    async fn get(&self, prop: PropName) -> Result<PropValue, LvsError> {
        let blob = self.blob_checked();

        let name = prop.to_string().into_cstring();
        let mut value: *const libc::c_char = std::ptr::null::<libc::c_char>();
        let mut value_len: u64 = 0;
        unsafe {
            spdk_blob_get_xattr_value(
                blob,
                name.as_ptr(),
                &mut value as *mut *const c_char as *mut *const c_void,
                &mut value_len,
            )
        }
        .to_result(|e| LvsError::GetProperty {
            source: BsError::from_i32(e),
            prop,
            name: self.name(),
        })?;
        let einval = || {
            Err(LvsError::Property {
                source: BsError::InvalidArgument {},
                name: self.name(),
            })
        };

        match prop {
            PropName::Shared => {
                match unsafe { CStr::from_ptr(value).to_str() } {
                    Ok("true") => Ok(PropValue::Shared(true)),
                    Ok("false") => Ok(PropValue::Shared(false)),
                    _ => einval(),
                }
            }
            PropName::AllowedHosts => {
                match unsafe { CStr::from_ptr(value).to_str() } {
                    Ok(list) if list.is_empty() => {
                        Ok(PropValue::AllowedHosts(vec![]))
                    }
                    Ok(list) => Ok(PropValue::AllowedHosts(
                        list.split(',')
                            .map(|s| s.to_string())
                            .collect::<Vec<_>>(),
                    )),
                    _ => einval(),
                }
            }
            PropName::EntityId => {
                match unsafe { CStr::from_ptr(value).to_str() } {
                    Ok(id) => Ok(PropValue::EntityId(id.to_string())),
                    _ => einval(),
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
    async fn destroy(mut self) -> Result<String, LvsError> {
        let event = self.event(EventAction::Delete);
        extern "C" fn destroy_cb(sender: *mut c_void, errno: i32) {
            let sender =
                unsafe { Box::from_raw(sender as *mut oneshot::Sender<i32>) };
            sender.send(errno).unwrap();
        }
        self.reset_snapshot_tree_usage_cache(!self.is_snapshot());
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
                warn!("error while destroying lvol {name}");
                LvsError::RepDestroy {
                    source: BsError::from_i32(e),
                    name: name.clone(),
                    msg: "error while destroying lvol".into(),
                }
            })?;
        if let Err(error) = ptpl.destroy() {
            tracing::error!(
                "{name}: Failed to clean up persistence through power loss for replica: {error}",
            );
        }

        info!("destroyed lvol {name}");
        event.generate();
        Ok(name)
    }

    async fn set_no_sync(
        self: Pin<&mut Self>,
        prop: PropValue,
    ) -> Result<bool, LvsError> {
        let blob = self.blob_checked();

        if self.is_snapshot() {
            warn!("ignoring set property on snapshot {}", self.name());
            return Ok(false);
        }
        if self.is_read_only() {
            warn!("{} is read-only", self.name());
        }
        let set_value = match prop.clone() {
            PropValue::Shared(val) => {
                if matches!(self.get(PropName::Shared).await, Ok(PropValue::Shared(s)) if s == val)
                {
                    return Ok(false);
                }
                if val { "true" } else { "false" }.into_cstring()
            }
            PropValue::AllowedHosts(mut hosts) => {
                if let Ok(PropValue::AllowedHosts(mut hlist)) =
                    self.get(PropName::AllowedHosts).await
                {
                    if hlist.len() == hosts.len() {
                        hosts.sort();
                        hlist.sort();
                        if hosts == hlist {
                            return Ok(false);
                        }
                    }
                }
                hosts.join(",").into_cstring()
            }
            PropValue::EntityId(id) => {
                if matches!(self.get(PropName::EntityId).await, Ok(PropValue::EntityId(e)) if e == id)
                {
                    return Ok(false);
                }
                id.into_cstring()
            }
        };
        let name = PropName::from(&prop).to_string().into_cstring();
        unsafe {
            spdk_blob_set_xattr(
                blob,
                name.as_ptr(),
                set_value.as_bytes_with_nul().as_ptr() as *const _,
                set_value.as_bytes_with_nul().len() as u16,
            )
        }
        .to_result(|e| LvsError::SetProperty {
            source: BsError::from_i32(e),
            prop: prop.to_string(),
            name: self.name(),
        })?;
        Ok(true)
    }

    async fn set(
        mut self: Pin<&mut Self>,
        prop: PropValue,
    ) -> Result<(), LvsError> {
        if self.as_mut().set_no_sync(prop).await? {
            self.sync_metadata().await?;
        }
        Ok(())
    }

    async fn sync_metadata(self: Pin<&mut Self>) -> Result<(), LvsError> {
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
            LvsError::SyncProperty {
                source: BsError::from_i32(e),
                name: self.name(),
            }
        })?;

        Ok(())
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
            error!("Blobstore Operation failed, errno {errno}");
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

    /// Get the parent spdk_blob from the current blob.
    unsafe fn bs_iter_parent(
        &self,
        curr_blob: *mut spdk_blob,
    ) -> Option<*mut spdk_blob> {
        let parent_blob = spdk_bs_get_parent_blob(curr_blob);
        if parent_blob.is_null() {
            None
        } else {
            Some(parent_blob)
        }
    }

    /// Get lvol inner ptr.
    fn as_inner_ptr(&self) -> *mut spdk_lvol {
        self.inner.as_ptr()
    }
    /// Get BlobPtr from spdk_lvol.
    fn blob_checked(&self) -> *mut spdk_blob {
        let blob = self.as_inner_ref().blob;
        assert!(!blob.is_null());
        blob
    }

    /// Wrapper function to destroy replica and its associated snapshot if
    /// replica is identified as last clone.
    async fn destroy_replica(mut self) -> Result<String, LvsError> {
        let snapshot_lvol = self.is_snapshot_clone();
        let name = self.name();
        self.destroy().await?;

        // If destroy replica is a snapshot clone and it is the last
        // clone from the snapshot, destroy the snapshot
        // if it is already marked as discarded snapshot.
        if let Some(snapshot_lvol) = snapshot_lvol {
            if snapshot_lvol.list_clones_by_snapshot_uuid().is_empty()
                && snapshot_lvol.is_discarded_snapshot()
            {
                snapshot_lvol.destroy().await?;
            }
        }
        Ok(name)
    }

    /// Resize a replica. The resize can be expand or shrink, depending
    /// upon if required size is more or less than current size of
    /// the replica.
    async fn resize_replica(&mut self, resize_to: u64) -> Result<(), LvsError> {
        let (s, r) = pair::<ErrnoResult<*mut spdk_lvol>>();
        let mut ctx = ResizeCbCtx {
            lvol: self.as_inner_ptr(),
            sender: cb_arg(s),
            req_size: resize_to,
        };

        unsafe {
            vbdev_lvol_resize(
                self.as_inner_ptr(),
                resize_to,
                Some(lvol_resize_cb),
                &mut ctx as *mut _ as *mut c_void,
            );
        }

        let cb_ret = r.await.expect("lvol resize callback dropped");

        match cb_ret {
            Ok(_) => {
                info!("Resized {:?} successfully", self);
                Ok(())
            }
            Err(errno) => {
                error!("Resize {:?} failed, errno {errno}", self);
                Err(LvsError::RepResize {
                    source: BsError::from_errno(errno),
                    name: self.name(),
                })
            }
        }
    }
}

extern "C" fn lvol_resize_cb(cb_arg: *mut c_void, errno: i32) {
    let mut retcode = errno;
    let ctx = cb_arg as *mut ResizeCbCtx;
    let (lvol, req_size) =
        unsafe { (Lvol::from_inner_ptr((*ctx).lvol), (*ctx).req_size) };
    let sender = unsafe {
        Box::from_raw(
            (*ctx).sender as *mut oneshot::Sender<ErrnoResult<*mut spdk_lvol>>,
        )
    };

    if retcode == 0 && (lvol.size() < req_size) {
        // Make sure resize worked, and account for metadata while comparing
        // i.e. the actual size will be a little more than requested.
        debug_assert!(false, "errno 0 - replica resize must have succeeded !");
        retcode = -libc::EAGAIN;
    }

    sender
        .send(errno_result_from_i32(lvol.as_inner_ptr(), retcode))
        .expect("Receiver is gone");
}
