use std::{
    convert::TryFrom,
    ffi::{c_void, CStr},
    fmt::{Debug, Display},
    os::raw::c_char,
    pin::Pin,
    ptr::NonNull,
};

use async_trait::async_trait;
use byte_unit::Byte;
use futures::channel::oneshot;
use nix::errno::Errno;
use pin_utils::core_reexport::fmt::Formatter;

use spdk_rs::libspdk::{
    spdk_blob,
    spdk_blob_calc_used_clusters,
    spdk_blob_get_num_clusters,
    spdk_blob_get_xattr_value,
    spdk_blob_is_read_only,
    spdk_blob_is_snapshot,
    spdk_blob_set_xattr,
    spdk_blob_sync_md,
    spdk_bs_get_cluster_size,
    spdk_lvol,
    vbdev_lvol_create_snapshot,
    vbdev_lvol_destroy,
    vbdev_lvol_get_from_bdev,
    LVS_CLEAR_WITH_UNMAP,
    SPDK_BDEV_LARGE_BUF_MAX_SIZE,
};

use super::{Error, Lvs};

use crate::{
    bdev::PtplFileOps,
    core::{
        logical_volume::LogicalVolume,
        snapshot::SnapshotDescriptor,
        Bdev,
        Protocol,
        PtplProps,
        Share,
        ShareProps,
        SnapshotOps,
        SnapshotParams,
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
    subsys::NvmfReq,
};
use spdk_rs::libspdk::spdk_nvmf_request_complete;

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
}

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

            // Set the buffer size to the maximum allowed by SPDK.
            let buf_size = SPDK_BDEV_LARGE_BUF_MAX_SIZE as u64;
            let buf = hdl.dma_malloc(buf_size).map_err(|e| {
                error!(
                    ?self,
                    ?e,
                    "no memory available to allocate zero buffer"
                );
                Error::RepDestroy {
                    source: Errno::ENOMEM,
                    name: self.name(),
                    msg: "no memory available to allocate zero buffer".into(),
                }
            })?;
            // write zero to the first 8MB which wipes the metadata and the
            // first 4MB of the data partition
            let range =
                std::cmp::min(self.as_bdev().size_in_bytes(), WIPE_SUPER_LEN);
            for offset in 0 .. (range / buf_size) {
                hdl.write_at(offset * buf.len(), &buf).await.map_err(|e| {
                    error!(?self, ?e);
                    Error::RepDestroy {
                        source: Errno::EIO,
                        name: self.name(),
                        msg: "failed to write to lvol".into(),
                    }
                })?;
            }
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
    /// Return lvs for the Logical Volume
    fn lvs(&self) -> Lvs;
    /// Returns the underlying bdev of the lvol
    fn as_bdev(&self) -> UntypedBdev;

    /// Returns a boolean indicating if the lvol is a snapshot
    fn is_snapshot(&self) -> bool;

    /// Get/Read a property from this lvol from disk
    async fn get(&self, prop: PropName) -> Result<PropValue, Error>;

    /// Destroy the lvol
    async fn destroy(mut self) -> Result<String, Error>;

    /// Write the property prop on to the lvol but do not sync the metadata yet.
    async fn set_no_sync(
        self: Pin<&mut Self>,
        prop: PropValue,
    ) -> Result<(), Error>;

    /// Write the property prop on to the lvol which is stored on disk
    async fn set(
        mut self: Pin<&mut Self>,
        prop: PropValue,
    ) -> Result<(), Error>;

    /// Callback executed after synchronizing the lvols metadata
    extern "C" fn blob_sync_cb(sender_ptr: *mut c_void, errno: i32);

    /// Write the property prop on to the lvol which is stored on disk
    async fn sync_metadata(self: Pin<&mut Self>) -> Result<(), Error>;

    /// Create a snapshot in Remote
    async fn create_snapshot_remote(
        &self,
        nvmf_req: &NvmfReq,
        snapshot_name: &str,
    );
}

///  LogicalVolume implement Generic interface for Lvol
impl LogicalVolume for Lvol {
    /// Returns the name of the Snapshot
    fn name(&self) -> String {
        self.as_bdev().name().to_string()
    }

    /// Returns the UUID of the Snapshot
    fn uuid(&self) -> String {
        self.as_bdev().uuid_as_string()
    }

    /// Returns the pool name of the Snapshot
    fn pool_name(&self) -> String {
        self.lvs().name().to_string()
    }

    /// Returns the pool uuid of the Snapshot
    fn pool_uuid(&self) -> String {
        self.lvs().uuid()
    }

    /// Returns a boolean indicating if the Logical Volume is thin provisioned
    fn is_thin(&self) -> bool {
        self.as_inner_ref().thin_provision
    }

    /// Returns a boolean indicating if the Logical Volume is read-only
    fn is_read_only(&self) -> bool {
        unsafe { spdk_blob_is_read_only(self.blob_checked()) }
    }

    /// Return the size of the Snapshot in bytes
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

            LvolSpaceUsage {
                capacity_bytes: self.size(),
                allocated_bytes: cluster_size * num_allocated_clusters,
                cluster_size,
                num_clusters,
                num_allocated_clusters,
            }
        }
    }
}

/// LvsLvol Trait Implementation for Lvol for Volume Specific Interface.
#[async_trait(? Send)]
impl LvsLvol for Lvol {
    /// Return lvs for the Logical Volume
    fn lvs(&self) -> Lvs {
        Lvs::from_inner_ptr(self.as_inner_ref().lvol_store)
    }

    /// Returns the underlying bdev of the lvol
    fn as_bdev(&self) -> UntypedBdev {
        Bdev::checked_from_ptr(self.as_inner_ref().bdev).unwrap()
    }

    /// Returns a boolean indicating if the lvol is a snapshot
    fn is_snapshot(&self) -> bool {
        unsafe { spdk_blob_is_snapshot(self.blob_checked()) }
    }

    /// Get/Read a property from this lvol from disk
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

    /// Callback executed after synchronizing the lvols metadata
    extern "C" fn blob_sync_cb(sender_ptr: *mut c_void, errno: i32) {
        let sender =
            unsafe { Box::from_raw(sender_ptr as *mut oneshot::Sender<i32>) };
        sender.send(errno).expect("blob cb receiver is gone");
    }
    /// Destroy the lvol
    async fn destroy(mut self) -> Result<String, Error> {
        extern "C" fn destroy_cb(sender: *mut c_void, errno: i32) {
            let sender =
                unsafe { Box::from_raw(sender as *mut oneshot::Sender<i32>) };
            sender.send(errno).unwrap();
        }

        // we must always unshare before destroying bdev
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

    /// Write the property prop on to the lvol which is stored on disk
    async fn set(
        mut self: Pin<&mut Self>,
        prop: PropValue,
    ) -> Result<(), Error> {
        self.as_mut().set_no_sync(prop).await?;
        self.sync_metadata().await
    }

    /// Write the property prop on to the lvol which is stored on disk
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
    /// Create a snapshot in Remote
    async fn create_snapshot_remote(
        &self,
        nvmf_req: &NvmfReq,
        snapshot_name: &str,
    ) {
        extern "C" fn snapshot_done_cb(
            nvmf_req_ptr: *mut c_void,
            _lvol_ptr: *mut spdk_lvol,
            errno: i32,
        ) {
            let nvmf_req = NvmfReq::from(nvmf_req_ptr);
            let mut rsp = nvmf_req.response();
            let nvme_status = rsp.status();

            nvme_status.set_sct(0); // SPDK_NVME_SCT_GENERIC
            nvme_status.set_sc(match errno {
                0 => 0,
                _ => {
                    error!("vbdev_lvol_create_snapshot errno {}", errno);
                    0x06 // SPDK_NVME_SC_INTERNAL_DEVICE_ERROR
                }
            });

            // From nvmf_bdev_ctrlr_complete_cmd
            unsafe {
                spdk_nvmf_request_complete(nvmf_req.0.as_ptr());
            }
        }

        let c_snapshot_name = snapshot_name.into_cstring();
        unsafe {
            vbdev_lvol_create_snapshot(
                self.as_inner_ptr(),
                c_snapshot_name.as_ptr(),
                Some(snapshot_done_cb),
                nvmf_req.0.as_ptr().cast(),
            )
        };

        info!("{:?}: creating snapshot '{}'", self, snapshot_name);
    }
}

#[async_trait(?Send)]
impl SnapshotOps for Lvol {
    type Error = Error;
    /// Create Snapshot Common API for Remote Device.
    async fn create_snapshot(
        &self,
        snap_param: SnapshotParams,
    ) -> Result<(), Error> {
        extern "C" fn snapshot_create_done_cb(
            arg: *mut c_void,
            _lvol_ptr: *mut spdk_lvol,
            errno: i32,
        ) {
            let s = unsafe { Box::from_raw(arg as *mut oneshot::Sender<i32>) };
            if errno != 0 {
                error!("vbdev_lvol_create_snapshot failed errno {}", errno);
            }
            s.send(errno).ok();
        }

        let c_snapshot_name = snap_param.name().unwrap().into_cstring();
        let (s, r) = oneshot::channel::<i32>();
        unsafe {
            vbdev_lvol_create_snapshot(
                self.as_inner_ptr(),
                c_snapshot_name.as_ptr(),
                Some(snapshot_create_done_cb),
                cb_arg(s),
            )
        };
        r.await
            .expect("snapshot_create_done_cb")
            .to_result(|error| Error::SnapshotCreate {
                source: Errno::from_i32(error),
                msg: c_snapshot_name.into_string().unwrap(),
            })
    }
}
