use std::{
    convert::TryFrom,
    ffi::CStr,
    os::raw::{c_char, c_int, c_ulong, c_void},
};

use async_trait::async_trait;
use futures::channel::oneshot;
use snafu::ResultExt;
use url::Url;

use spdk_rs::{
    ffihelper::copy_str_with_null,
    libspdk::{
        bdev_nvme_create,
        bdev_nvme_delete,
        spdk_nvme_transport_id,
        nvme_path_id,
    },
};

use crate::{
    bdev::{util::uri, CreateDestroy, GetName},
    bdev_api::{self, BdevError},
    core::UntypedBdev,
    ffihelper::{cb_arg, errno_result_from_i32, ErrnoResult, IntoCString},
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
    type Error = BdevError;

    fn try_from(url: &Url) -> Result<Self, Self::Error> {
        if uri::segments(url).is_empty() {
            return Err(BdevError::InvalidUri {
                uri: url.to_string(),
                message: String::from("no path segments"),
            });
        }

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
    type Error = BdevError;

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

        if UntypedBdev::lookup_by_name(&self.name).is_some() {
            return Err(BdevError::BdevExists {
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
                Some(nvme_create_cb),
                cb_arg(sender),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                false,
            )
        };

        errno_result_from_i32((), errno).context(
            bdev_api::CreateBdevInvalidParams {
                name: self.name.clone(),
            },
        )?;

        receiver
            .await
            .context(bdev_api::BdevCommandCanceled {
                name: self.name.clone(),
            })?
            .context(bdev_api::CreateBdevFailed {
                name: self.name.clone(),
            })?;

        let success = UntypedBdev::lookup_by_name(&self.get_name())
            .map(|mut b| b.add_alias(self.url.as_ref()))
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
        if let Some(mut bdev) = UntypedBdev::lookup_by_name(&self.get_name()) {
            let mut path_id = nvme_path_id::default();
            bdev.remove_alias(self.url.as_ref());
            let errno = unsafe {
                bdev_nvme_delete(
                    self.name.clone().into_cstring().as_ptr(),
                    &mut path_id as *mut nvme_path_id,
                )
            };
            errno_result_from_i32((), errno).context(
                bdev_api::DestroyBdevFailed {
                    name: self.name.clone(),
                },
            )
        } else {
            Err(BdevError::BdevNotFound {
                name: self.get_name(),
            })
        }
    }
}

const MAX_NAMESPACES: usize = 1;

struct NvmeCreateContext {
    trid: spdk_nvme_transport_id,
    names: [*const c_char; MAX_NAMESPACES],
    count: u32,
}

unsafe impl Send for NvmeCreateContext {}

impl NvmeCreateContext {
    pub fn new(nvme: &NVMe) -> NvmeCreateContext {
        let mut trid = spdk_nvme_transport_id::default();
        copy_str_with_null(&nvme.name, &mut trid.traddr);
        trid.trtype = spdk_rs::libspdk::SPDK_NVME_TRANSPORT_PCIE;

        NvmeCreateContext {
            trid,
            names: [std::ptr::null_mut() as *mut c_char; MAX_NAMESPACES],
            count: MAX_NAMESPACES as u32,
        }
    }
}
