use std::{
    convert::TryFrom,
    ffi::CStr,
    os::raw::{c_char, c_int, c_ulong, c_void},
    ptr::copy_nonoverlapping,
};

use async_trait::async_trait;
use futures::channel::oneshot;
use snafu::ResultExt;
use url::Url;

use spdk_rs::libspdk::{
    bdev_nvme_create,
    bdev_nvme_delete,
    spdk_nvme_transport_id,
};

use crate::{
    bdev::{CreateDestroy, GetName},
    core::Bdev,
    ffihelper::{cb_arg, errno_result_from_i32, ErrnoResult, IntoCString},
    nexus_uri::{self, NexusBdevError},
};

#[derive(Debug)]
#[allow(clippy::upper_case_acronyms)]
pub(super) struct NVMe {
    /// name of the bdev that should be created
    name: String,
    url: Url,
}

/// Convert a URI to NVMe object
impl TryFrom<&Url> for NVMe {
    type Error = NexusBdevError;

    fn try_from(url: &Url) -> Result<Self, Self::Error> {
        Ok(Self {
            name: url.path()[1 ..].into(),
            url: url.clone(),
        })
    }
}

impl GetName for NVMe {
    fn get_name(&self) -> String {
        format!("{}n1", self.name)
    }
}

#[async_trait(? Send)]
impl CreateDestroy for NVMe {
    type Error = NexusBdevError;

    async fn create(&self) -> Result<String, Self::Error> {
        extern "C" fn nvme_create_cb(
            arg: *mut c_void,
            _bdev_count: c_ulong,
            errno: c_int,
        ) {
            let sender = unsafe {
                Box::from_raw(arg as *mut oneshot::Sender<ErrnoResult<()>>)
            };

            sender
                .send(errno_result_from_i32((), errno))
                .expect("done callback receiver side disappeared");
        }

        if Bdev::lookup_by_name(&self.name).is_some() {
            return Err(NexusBdevError::BdevExists {
                name: self.name.clone(),
            });
        }

        let cname = self.name.clone().into_cstring();
        let mut context = NvmeCreateContext::new(self);

        let (sender, receiver) = oneshot::channel::<ErrnoResult<()>>();

        let errno = unsafe {
            bdev_nvme_create(
                &mut context.trid,
                cname.as_ptr(),
                &mut context.names[0],
                context.count,
                context.prchk_flags,
                Some(nvme_create_cb),
                cb_arg(sender),
                std::ptr::null_mut(),
                false,
                0,
                0,
                0,
            )
        };

        errno_result_from_i32((), errno).context(
            nexus_uri::CreateBdevInvalidParams {
                name: self.name.clone(),
            },
        )?;

        receiver
            .await
            .context(nexus_uri::CancelBdev {
                name: self.name.clone(),
            })?
            .context(nexus_uri::CreateBdev {
                name: self.name.clone(),
            })?;

        let success = Bdev::lookup_by_name(&self.get_name())
            .map(|mut b| b.as_mut().add_alias(&self.url.to_string()))
            .expect("bdev created but not found!");

        if !success {
            error!("failed to added alias too created bdev")
        }

        Ok(unsafe { CStr::from_ptr(context.names[0]) }
            .to_str()
            .unwrap()
            .to_string())
    }

    async fn destroy(self: Box<Self>) -> Result<(), Self::Error> {
        if let Some(mut bdev) = Bdev::lookup_by_name(&self.get_name()) {
            bdev.as_mut().remove_alias(&self.url.to_string());
            let errno = unsafe {
                bdev_nvme_delete(
                    self.name.clone().into_cstring().as_ptr(),
                    std::ptr::null(),
                )
            };
            errno_result_from_i32((), errno).context(nexus_uri::DestroyBdev {
                name: self.name.clone(),
            })
        } else {
            Err(NexusBdevError::BdevNotFound {
                name: self.get_name(),
            })
        }
    }
}

const MAX_NAMESPACES: usize = 1;

struct NvmeCreateContext {
    trid: spdk_nvme_transport_id,
    names: [*const c_char; MAX_NAMESPACES],
    prchk_flags: u32,
    count: u32,
}

unsafe impl Send for NvmeCreateContext {}

impl NvmeCreateContext {
    pub fn new(nvme: &NVMe) -> NvmeCreateContext {
        let mut trid = spdk_nvme_transport_id::default();
        unsafe {
            copy_nonoverlapping(
                nvme.name.as_ptr() as *const c_void,
                &mut trid.traddr[0] as *const _ as *mut c_void,
                nvme.name.len(),
            );
        }

        trid.trtype = spdk_rs::libspdk::SPDK_NVME_TRANSPORT_PCIE;

        NvmeCreateContext {
            trid,
            names: [std::ptr::null_mut() as *mut c_char; MAX_NAMESPACES],
            prchk_flags: 0,
            count: MAX_NAMESPACES as u32,
        }
    }
}
