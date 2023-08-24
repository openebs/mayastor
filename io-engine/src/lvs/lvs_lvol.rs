use async_trait::async_trait;
use byte_unit::Byte;
use chrono::Utc;
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
    spdk_bs_iter_next,
    spdk_lvol,
    vbdev_lvol_destroy,
    vbdev_lvol_get_from_bdev,
    LVS_CLEAR_WITH_UNMAP,
};

use super::{Error, Lvs};

use crate::{
    bdev::PtplFileOps,
    core::{
        logical_volume::LogicalVolume,
        wiper::{WipeMethod, Wiper},
        Bdev,
        CloneXattrs,
        Protocol,
        PtplProps,
        Share,
        ShareProps,
        SnapshotOps,
        SnapshotParams,
        SnapshotXattrs,
        ToErrno,
        UntypedBdev,
        UpdateProps,
    },
    ffihelper::{
        cb_arg,
        done_cb,
        errno_result_from_i32,
        pair,
        ErrnoResult,
        FfiResult,
        IntoCString,
    },
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
    /// Actual Amount of disk space allocated by snapshot which is created from
    /// clone.
    pub allocated_bytes_snapshot_from_clone: Option<u64>,
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
    fn as_inner_ref(&self) -> &spdk_lvol {
        unsafe { self.inner.as_ref() }
    }

    /// Wipe the first 8MB if unmap is not supported on failure the operation
    /// needs to be repeated.
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

    /// Get a wiper for this replica.
    pub(crate) fn wiper(
        &self,
        wipe_method: WipeMethod,
    ) -> Result<Wiper, Error> {
        let hdl = Bdev::open(&self.as_bdev(), true)
            .and_then(|desc| desc.into_handle())
            .map_err(|e| Error::Invalid {
                msg: e.to_string(),
                source: e.to_errno(),
            })?;

        let wiper = Wiper::new(hdl, wipe_method)?;
        Ok(wiper)
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

    /// Low-level function to set blob attributes.
    pub async fn set_blob_attr<A: AsRef<str>>(
        &self,
        attr: A,
        value: String,
        sync_metadata: bool,
    ) -> Result<(), Error> {
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
            return Err(Error::SetProperty {
                source: Errno::from_i32(r),
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
                Err(Error::SyncProperty {
                    source: Errno::from_i32(errno),
                    name: self.name(),
                })
            }
        }
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

    /// Lvol is considered as clone if its sourceuuid attribute is a valid
    /// snapshot. if it is clone, return the snapshot lvol.
    fn is_snapshot_clone(&self) -> Option<Lvol>;

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

    /// Wrapper function to destroy replica and its associated snapshot if
    /// replica is identified as last clone.
    async fn destroy_replica(mut self) -> Result<String, Error>;
}

///  LogicalVolume implement Generic interface for Lvol.
impl LogicalVolume for Lvol {
    type InnerPtr = *mut spdk_lvol;
    type BlobPtr = *mut spdk_blob;

    /// Get lvol inner ptr.
    fn as_inner_ptr(&self) -> Self::InnerPtr {
        self.inner.as_ptr()
    }
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

    /// Return the size of the Logical Volume in bytes.
    fn size(&self) -> u64 {
        self.as_bdev().size_in_bytes()
    }

    /// Get BlobPtr from spdk_lvol.
    fn blob_checked(&self) -> Self::BlobPtr {
        let blob = self.as_inner_ref().blob;
        assert!(!blob.is_null());
        blob
    }

    /// Return the committed size of the Logical Volume in bytes.
    fn committed(&self) -> u64 {
        match self.is_snapshot() {
            true => self.usage().allocated_bytes,
            false => self.size(),
        }
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
    /// Currently in place of SPDK native API to judge lvol as snapshot, xattr
    /// is checked here. When there is only single Lvol(snapshot) present in
    /// the system and there is restart of io-engine and pool import
    /// happens, SPDK native API consider lvol(snapshot) as normal lvol.
    /// Looks like a bug in SPDK, but all snapshot attribute are intact in
    /// SPDK after io-engine restarts.
    fn is_snapshot(&self) -> bool {
        Lvol::get_blob_xattr(self, SnapshotXattrs::SnapshotCreateTime.name())
            .is_some()
    }

    /// Lvol is considered as clone if its sourceuuid attribute is a valid
    /// snapshot. if it is clone, return the snapshot lvol.
    fn is_snapshot_clone(&self) -> Option<Lvol> {
        if let Some(source_uuid) =
            Lvol::get_blob_xattr(self, CloneXattrs::SourceUuid.name())
        {
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
                    prop: prop.to_string(),
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
                    prop: prop.to_string(),
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
            false,
        )
    }
    /// Wrapper function to destroy replica and its associated snapshot if
    /// replica is identified as last clone.
    async fn destroy_replica(mut self) -> Result<String, Error> {
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
                snapshot_lvol.reset_snapshot_parent_successor_usage_cache();
                snapshot_lvol.destroy().await?;
            }
        }
        Ok(name)
    }
}
