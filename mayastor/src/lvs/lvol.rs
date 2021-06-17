use std::{
    convert::TryFrom,
    ffi::{c_void, CStr},
    fmt::Display,
    os::raw::c_char,
    ptr::NonNull,
};

use async_trait::async_trait;
use futures::channel::oneshot;
use nix::errno::Errno;
use pin_utils::core_reexport::fmt::Formatter;
use tracing::instrument;

use spdk_sys::{
    spdk_blob_get_xattr_value,
    spdk_blob_is_read_only,
    spdk_blob_is_snapshot,
    spdk_blob_set_xattr,
    spdk_blob_sync_md,
    spdk_lvol,
    vbdev_lvol_create_snapshot,
    vbdev_lvol_destroy,
    vbdev_lvol_get_from_bdev,
};

use crate::{
    bdev::nexus::nexus_bdev::Nexus,
    core::{Bdev, CoreError, Mthread, Protocol, Share},
    ffihelper::{
        cb_arg,
        errno_result_from_i32,
        pair,
        ErrnoResult,
        FfiResult,
        IntoCString,
    },
    lvs::{error::Error, lvs_pool::Lvs},
    subsys::NvmfReq,
};

/// properties we allow for being set on the lvol, this information is stored on
/// disk
#[derive(Debug, Copy, Clone, PartialEq)]
#[non_exhaustive]
pub enum PropValue {
    Shared(bool),
}

#[derive(Debug)]
#[non_exhaustive]
pub enum PropName {
    Shared,
}

impl From<PropValue> for PropName {
    fn from(v: PropValue) -> Self {
        match v {
            PropValue::Shared(_) => Self::Shared,
        }
    }
}

impl Display for PropName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            PropName::Shared => "shared",
        };
        write!(f, "{}", name)
    }
}

#[derive(Debug)]
/// struct representing an lvol
pub struct Lvol(pub(crate) NonNull<spdk_lvol>);

impl TryFrom<Bdev> for Lvol {
    type Error = Error;

    fn try_from(b: Bdev) -> Result<Self, Self::Error> {
        if b.driver() == "lvol" {
            unsafe {
                Ok(Lvol(NonNull::new_unchecked(vbdev_lvol_get_from_bdev(
                    b.as_ptr(),
                ))))
            }
        } else {
            Err(Error::NotALvol {
                source: Errno::EINVAL,
                name: b.name(),
            })
        }
    }
}

impl From<Lvol> for Bdev {
    fn from(l: Lvol) -> Self {
        Bdev::from(unsafe { l.0.as_ref().bdev })
    }
}

impl Display for Lvol {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.pool(), self.name())
    }
}

#[async_trait(? Send)]
impl Share for Lvol {
    type Error = Error;
    type Output = String;

    /// we dont (want to) support replica's over iSCSI
    async fn share_iscsi(&self) -> Result<Self::Output, Self::Error> {
        Err(Error::LvolShare {
            source: CoreError::NotSupported {
                source: Errno::EINVAL,
            },
            name: self.name(),
        })
    }

    /// share the lvol as a nvmf target
    #[instrument(level = "debug", err)]
    async fn share_nvmf(
        &self,
        cntlid_range: Option<(u16, u16)>,
    ) -> Result<Self::Output, Self::Error> {
        let share =
            self.as_bdev().share_nvmf(cntlid_range).await.map_err(|e| {
                Error::LvolShare {
                    source: e,
                    name: self.name(),
                }
            })?;

        self.set(PropValue::Shared(true)).await?;
        info!("shared {}", self);
        Ok(share)
    }

    /// unshare the nvmf target
    #[instrument(level = "debug", err)]
    async fn unshare(&self) -> Result<Self::Output, Self::Error> {
        let share =
            self.as_bdev()
                .unshare()
                .await
                .map_err(|e| Error::LvolUnShare {
                    source: e,
                    name: self.name(),
                })?;

        self.set(PropValue::Shared(false)).await?;
        info!("unshared {}", self);
        Ok(share)
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

    /// returns the URI that is used to construct the bdev. This is always None
    /// as lvols can not be created by URIs directly, but only through the
    /// ['Lvs'] interface.
    fn bdev_uri(&self) -> Option<String> {
        None
    }
}

impl Lvol {
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

    /// returns the underlying bdev of the lvol
    pub(crate) fn as_bdev(&self) -> Bdev {
        Bdev::from(unsafe { self.0.as_ref().bdev })
    }
    /// return the size of the lvol in bytes
    pub fn size(&self) -> u64 {
        self.as_bdev().size_in_bytes()
    }

    /// returns the name of the bdev
    pub fn name(&self) -> String {
        self.as_bdev().name()
    }

    /// returns the UUID of the lvol
    pub fn uuid(&self) -> String {
        self.as_bdev().uuid_as_string()
    }

    /// returns the pool of the lvol
    pub fn pool(&self) -> String {
        unsafe {
            Lvs(NonNull::new_unchecked(self.0.as_ref().lvol_store))
                .name()
                .to_string()
        }
    }

    /// returns a boolean indicating if the lvol is thin provisioned
    pub fn is_thin(&self) -> bool {
        unsafe { self.0.as_ref().thin_provision }
    }

    /// returns a boolean indicating if the lvol is read-only
    pub fn is_read_only(&self) -> bool {
        unsafe { spdk_blob_is_read_only(self.0.as_ref().blob) }
    }

    /// returns a boolean indicating if the lvol is a snapshot
    pub fn is_snapshot(&self) -> bool {
        unsafe { spdk_blob_is_snapshot(self.0.as_ref().blob) }
    }

    /// destroy the lvol
    #[instrument(level = "debug", err)]
    pub async fn destroy(self) -> Result<String, Error> {
        extern "C" fn destroy_cb(sender: *mut c_void, errno: i32) {
            let sender =
                unsafe { Box::from_raw(sender as *mut oneshot::Sender<i32>) };
            sender.send(errno).unwrap();
        }

        let name = self.name();

        // we must always unshare before destroying bdev
        let _ = self.unshare().await;

        let (s, r) = pair::<i32>();
        unsafe {
            vbdev_lvol_destroy(self.0.as_ptr(), Some(destroy_cb), cb_arg(s))
        };

        r.await
            .expect("lvol destroy callback is gone")
            .to_result(|e| Error::RepDestroy {
                source: Errno::from_i32(e),
                name: self.name(),
            })?;

        info!("Destroyed {}", name);
        Ok(name)
    }

    /// callback executed after synchronizing the lvols metadata
    extern "C" fn blob_sync_cb(sender_ptr: *mut c_void, errno: i32) {
        let sender =
            unsafe { Box::from_raw(sender_ptr as *mut oneshot::Sender<i32>) };
        sender.send(errno).expect("blob cb receiver is gone");
    }

    /// write the property prop on to the lvol which is stored on disk
    #[allow(clippy::unit_arg)] // here to silence the Ok(()) variant
    #[instrument(level = "debug", err)]
    pub async fn set(&self, prop: PropValue) -> Result<(), Error> {
        let blob = unsafe { self.0.as_ref().blob };
        assert!(!blob.is_null());

        if self.is_snapshot() {
            warn!("ignoring set property on snapshot {}", self.name());
            return Ok(());
        }
        if self.is_read_only() {
            warn!("{} is read-only", self.name());
        }
        match prop {
            PropValue::Shared(val) => {
                let name = PropName::from(prop).to_string().into_cstring();
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
        };

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

    /// get/read a property from this lvol from disk
    #[instrument(level = "debug", err)]
    pub async fn get(&self, prop: PropName) -> Result<PropValue, Error> {
        let blob = unsafe { self.0.as_ref().blob };
        assert!(!blob.is_null());

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
        }
    }

    /// Format snapshot name
    /// base_name is the nexus or replica UUID
    pub fn format_snapshot_name(base_name: &str, snapshot_time: u64) -> String {
        format!("{}-snap-{}", base_name, snapshot_time)
    }

    /// Create a snapshot
    pub async fn create_snapshot(
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
                spdk_sys::spdk_nvmf_request_complete(nvmf_req.0.as_ptr());
            }
        }

        let c_snapshot_name = snapshot_name.into_cstring();
        unsafe {
            vbdev_lvol_create_snapshot(
                self.0.as_ptr(),
                c_snapshot_name.as_ptr(),
                Some(snapshot_done_cb),
                nvmf_req.0.as_ptr().cast(),
            )
        };

        info!("Creating snapshot {} on {}", snapshot_name, &self);
    }

    /// Create snapshot for local replica
    pub async fn create_snapshot_local(
        &self,
        io: *mut spdk_sys::spdk_bdev_io,
        snapshot_name: &str,
    ) {
        extern "C" fn snapshot_done_cb(
            bio_ptr: *mut c_void,
            _lvol_ptr: *mut spdk_lvol,
            errno: i32,
        ) {
            if errno != 0 {
                error!("vbdev_lvol_create_snapshot errno {}", errno);
            }
            // Must complete IO on thread IO was submitted from
            Mthread::from(unsafe {
                spdk_sys::spdk_bdev_io_get_thread(bio_ptr.cast())
            })
            .with(|| Nexus::io_completion_local(errno == 0, bio_ptr));
        }

        let c_snapshot_name = snapshot_name.into_cstring();
        unsafe {
            vbdev_lvol_create_snapshot(
                self.0.as_ptr(),
                c_snapshot_name.as_ptr(),
                Some(snapshot_done_cb),
                io.cast(),
            )
        };

        info!("Creating snapshot {} on {}", snapshot_name, &self);
    }
}
