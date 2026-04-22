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
    bdevs::bdev_nvme_delete_async,
    ffihelper::copy_str_with_null,
    libspdk::{self, spdk_bdev_nvme_create, spdk_nvme_transport_id},
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

impl NVMe {
    /// Returns the driver name bound to a PCI device (e.g., "vfio-pci", "nvme")
    fn pci_driver(&self) -> Result<Option<String>, std::io::Error> {
        let driver_path = format!("/sys/bus/pci/devices/{}/driver", self.name);
        let path = std::path::Path::new(&driver_path);

        if !path.exists() {
            return Ok(None);
        }

        // Resolve the symlink: /sys/.../driver -> ../../../bus/pci/drivers/<driver>
        let driver_link = path.read_link()?;

        // Extract the last component (driver name)
        let driver = driver_link.file_name().and_then(|os| os.to_str());
        Ok(driver.map(ToString::to_string))
    }
    fn is_pci_nvme(&self) -> bool {
        let pci = format!("/sys/module/nvme/drivers/pci:nvme/{}", self.name);
        std::fs::exists(&pci).unwrap_or_default()
    }
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
            name: url.path()[1..].into(),
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
        extern "C" fn nvme_create_cb(arg: *mut c_void, _bdev_count: c_ulong, errno: c_int) {
            let sender = unsafe { Box::from_raw(arg as *mut oneshot::Sender<ErrnoResult<()>>) };

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

        let mut drv_opts: libspdk::spdk_nvme_ctrlr_opts = unsafe { std::mem::zeroed() };
        unsafe {
            libspdk::spdk_nvme_ctrlr_get_default_ctrlr_opts(
                &mut drv_opts,
                size_of::<libspdk::spdk_nvme_ctrlr_opts>() as u64,
            );
        }

        let errno = unsafe {
            spdk_bdev_nvme_create(
                &mut context.trid,
                cname.as_ptr(),
                &mut context.names[0],
                context.count,
                Some(nvme_create_cb),
                cb_arg(sender),
                &mut drv_opts,
                std::ptr::null_mut(),
            )
        };

        errno_result_from_i32((), errno).context(bdev_api::CreateBdevInvalidParams {
            name: self.name.clone(),
        })?;

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
        if UntypedBdev::lookup_by_name(&self.get_name()).is_some() {
            let res = bdev_nvme_delete_async(&self.name, None).await;

            // Restore the alias in the case the deletion failed.
            if !res.is_ok_and(|e| e.is_ok()) {
                UntypedBdev::lookup_by_name(&self.get_name())
                    .map(|mut b| b.add_alias(self.url.as_ref()));
            }

            res.context(bdev_api::BdevCommandCanceled {
                name: self.name.clone(),
            })?
            .context(bdev_api::DestroyBdevFailed {
                name: self.name.clone(),
            })
        } else {
            Err(BdevError::BdevNotFound {
                name: self.get_name(),
            })
        }
    }
}

impl super::Probe for NVMe {
    fn probe(&self, _opts: &super::ProbeOpts) -> Result<(), super::ProbeError> {
        const PCIE_DRVS: [&str; 2] = ["vfio-pci", "uio_pci_generic"];

        let driver = match self.pci_driver() {
            Ok(Some(driver)) if !driver.is_empty() => Ok(driver),
            Ok(_) => Err(super::ProbeError {
                code: super::ProbeErrorCode::DiskNotFound as i32,
                msg: None,
            }),
            Err(error) => Err(super::ProbeError {
                code: super::ProbeErrorCode::ProbeUnknown as i32,
                msg: Some(error.to_string()),
            }),
        }?;

        if PCIE_DRVS.contains(&driver.as_str()) {
            return Ok(());
        }

        if driver == "nvme" {
            return Err(super::ProbeError {
                code: super::ProbeErrorCode::PciKernelBound as i32,
                msg: None,
            });
        }

        let code = if self.is_pci_nvme() {
            super::ProbeErrorCode::PciDriverUnsupported
        } else {
            super::ProbeErrorCode::PciNotNvme
        } as i32;

        Err(super::ProbeError { code, msg: None })
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
