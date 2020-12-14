use async_trait::async_trait;
use futures::channel::oneshot;
use std::{os::raw::c_void, ptr::NonNull};

use crate::{
    bdev::{dev::nvmx::NvmeBlockDevice, nexus::nexus_io::nvme_admin_opc},
    core::{BlockDevice, BlockDeviceHandle, CoreError, DmaBuf},
    ffihelper::{cb_arg, done_cb},
    nexus_uri::NexusBdevError,
};

use spdk_sys::{
    self,
    spdk_get_io_channel,
    spdk_io_channel,
    spdk_nvme_cpl,
    spdk_nvme_ctrlr,
    spdk_nvme_ctrlr_cmd_admin_raw,
    spdk_nvme_ns,
};
/*
 * I/O handle for NVMe block device.
 */
#[derive(Debug)]
pub struct NvmeDeviceHandle {
    io_channel: NonNull<spdk_io_channel>,
    ctrlr: NonNull<spdk_nvme_ctrlr>,
    name: String,
    ns: NonNull<spdk_nvme_ns>,
}

impl NvmeDeviceHandle {
    pub fn create(
        name: &str,
        id: u64,
        ctrlr: NonNull<spdk_nvme_ctrlr>,
        ns: NonNull<spdk_nvme_ns>,
    ) -> Result<NvmeDeviceHandle, NexusBdevError> {
        // Obtain SPDK I/O channel for NVMe controller.
        let io_channel: *mut spdk_io_channel =
            unsafe { spdk_get_io_channel(id as *mut c_void) };

        if io_channel.is_null() {
            Err(NexusBdevError::BdevNotFound {
                name: name.to_string(),
            })
        } else {
            Ok(NvmeDeviceHandle {
                name: name.to_string(),
                io_channel: NonNull::new(io_channel).unwrap(),
                ctrlr,
                ns,
            })
        }
    }

    pub async fn send_ctrlr_admin_cmd(
        &self,
        cmd: &mut spdk_sys::spdk_nvme_cmd,
        buffer: Option<&mut DmaBuf>,
    ) -> Result<(), CoreError> {
        let (ptr, size) = match buffer {
            Some(buf) => (**buf, buf.len()),
            None => (std::ptr::null_mut(), 0),
        };

        let (s, r) = oneshot::channel::<bool>();

        let _rc = unsafe {
            spdk_nvme_ctrlr_cmd_admin_raw(
                self.ctrlr.as_ptr(),
                cmd,
                ptr,
                size as u32,
                Some(nvme_admin_passthru_done),
                cb_arg(s),
            )
        };

        if r.await.expect("Failed awaiting NVMe Admin command I/O") {
            Ok(())
        } else {
            Err(CoreError::NvmeAdminFailed {
                opcode: (*cmd).opc(),
            })
        }
    }
}

extern "C" fn nvme_admin_passthru_done(
    ctx: *mut c_void,
    _cpl: *const spdk_nvme_cpl,
) {
    println!("Admin passthrough completed !");
    done_cb(ctx, true);
}

#[async_trait(?Send)]
impl BlockDeviceHandle for NvmeDeviceHandle {
    fn get_device(&self) -> Box<dyn BlockDevice> {
        Box::new(NvmeBlockDevice::from_ns(&self.name, self.ns.as_ptr()))
    }

    async fn nvme_identify_ctrlr(&self) -> Result<DmaBuf, CoreError> {
        let mut buf = DmaBuf::new(4096, 8).map_err(|_e| {
            CoreError::DmaAllocationError {
                size: 4096,
            }
        })?;

        let mut cmd = spdk_sys::spdk_nvme_cmd::default();
        cmd.set_opc(nvme_admin_opc::IDENTIFY.into());
        cmd.nsid = 0xffffffff;
        // Controller Identifier
        unsafe { *spdk_sys::nvme_cmd_cdw10_get(&mut cmd) = 1 };
        self.send_ctrlr_admin_cmd(&mut cmd, Some(&mut buf)).await?;
        Ok(buf)
    }
}
