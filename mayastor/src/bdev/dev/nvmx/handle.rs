use async_trait::async_trait;
use futures::channel::oneshot;
use std::{os::raw::c_void, ptr::NonNull, sync::Arc};

use crate::{
    bdev::{
        dev::nvmx::{NvmeBlockDevice, NvmeIoChannel, NvmeNamespace},
        nexus::nexus_io::nvme_admin_opc,
    },
    core::{
        BlockDevice,
        BlockDeviceHandle,
        CoreError,
        DmaBuf,
        DmaError,
        IoCompletionCallback,
    },
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
    spdk_nvme_ns_cmd_read,
    spdk_nvme_ns_cmd_write,
};
/*
 * I/O handle for NVMe block device.
 */
pub struct NvmeDeviceHandle {
    io_channel: NonNull<spdk_io_channel>,
    ctrlr: NonNull<spdk_nvme_ctrlr>,
    name: String,
    ns: Arc<NvmeNamespace>,
    prchk_flags: u32,

    // Static values cached for performance.
    _num_blocks: u64,
    block_len: u64,
    _size_in_bytes: u64,
}

impl NvmeDeviceHandle {
    pub fn create(
        name: &str,
        id: u64,
        ctrlr: NonNull<spdk_nvme_ctrlr>,
        ns: Arc<NvmeNamespace>,
        prchk_flags: u32,
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
                _num_blocks: ns.num_blocks(),
                block_len: ns.block_len(),
                _size_in_bytes: ns.size_in_bytes(),
                prchk_flags,
                ns,
            })
        }
    }

    #[inline]
    fn bytes_to_blocks(
        &self,
        offset_bytes: u64,
        num_bytes: u64,
    ) -> (bool, u64, u64) {
        let offset_blocks = offset_bytes / self.block_len;
        let num_blocks = num_bytes / self.block_len;
        let alignment =
            (offset_bytes % self.block_len) | (num_bytes % self.block_len);

        (alignment == 0, offset_blocks, num_blocks)
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

extern "C" fn nvme_io_completion(
    _ctx: *mut c_void,
    _cpl: *const spdk_nvme_cpl,
) {
    println!("NVMe I/O completed !");
}

extern "C" fn nvme_async_io_completion(
    ctx: *mut c_void,
    _cpl: *const spdk_nvme_cpl,
) {
    println!("Async NVMe I/O completed !");
    done_cb(ctx, true);
}

#[async_trait(? Send)]
impl BlockDeviceHandle for NvmeDeviceHandle {
    fn get_device(&self) -> Box<dyn BlockDevice> {
        Box::new(NvmeBlockDevice::from_ns(&self.name, Arc::clone(&self.ns)))
    }

    fn dma_malloc(&self, size: u64) -> Result<DmaBuf, DmaError> {
        DmaBuf::new(size, self.ns.alignment())
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

    async fn read_at(
        &self,
        offset: u64,
        buffer: &DmaBuf,
    ) -> Result<u64, CoreError> {
        let (valid, offset_blocks, num_blocks) =
            self.bytes_to_blocks(offset, buffer.len());

        debug!(
            "{} read(offset={}, size={})",
            self.name,
            offset,
            buffer.len()
        );
        // Make sure offset/size matches device block size.
        if !valid {
            error!(
                "{} invalid offset/buffer size: (offset={}, size={})",
                self.name,
                offset,
                buffer.len()
            );
            return Err(CoreError::InvalidOffset {
                offset,
            });
        }

        let inner = NvmeIoChannel::inner_from_channel(self.io_channel.as_ptr());
        let (s, r) = oneshot::channel::<bool>();

        let rc = unsafe {
            spdk_nvme_ns_cmd_read(
                self.ns.as_ptr(),
                inner.qpair.as_ptr(),
                **buffer,
                offset_blocks,
                num_blocks as u32,
                Some(nvme_async_io_completion),
                cb_arg(s),
                self.prchk_flags,
            )
        };

        if rc != 0 && rc != libc::ENOMEM {
            error!("{} read failed: rc = {}", self.name, rc);
            return Err(CoreError::ReadFailed {
                offset,
                len: buffer.len(),
            });
        }

        if r.await.expect("Failed awaiting at read_at()") {
            Ok(buffer.len())
        } else {
            Err(CoreError::ReadFailed {
                offset,
                len: buffer.len(),
            })
        }
    }

    async fn write_at(
        &self,
        offset: u64,
        buffer: &DmaBuf,
    ) -> Result<u64, CoreError> {
        let (valid, offset_blocks, num_blocks) =
            self.bytes_to_blocks(offset, buffer.len());

        debug!(
            "{} write(offset={}, size={})",
            self.name,
            offset,
            buffer.len()
        );
        // Make sure offset/size matches device block size.
        if !valid {
            error!(
                "{} invalid offset/buffer size: (offset={}, size={})",
                self.name,
                offset,
                buffer.len()
            );
            return Err(CoreError::InvalidOffset {
                offset,
            });
        }

        let inner = NvmeIoChannel::inner_from_channel(self.io_channel.as_ptr());
        let (s, r) = oneshot::channel::<bool>();

        let rc = unsafe {
            spdk_nvme_ns_cmd_write(
                self.ns.as_ptr(),
                inner.qpair.as_ptr(),
                **buffer,
                offset_blocks,
                num_blocks as u32,
                Some(nvme_async_io_completion),
                cb_arg(s),
                self.prchk_flags,
            )
        };

        if rc != 0 && rc != libc::ENOMEM {
            error!("{} write failed: rc = {}", self.name, rc);
            return Err(CoreError::WriteFailed {
                offset,
                len: buffer.len(),
            });
        }

        if r.await.expect("Failed awaiting at write_at()") {
            Ok(buffer.len())
        } else {
            Err(CoreError::WriteFailed {
                offset,
                len: buffer.len(),
            })
        }
    }

    fn read(
        &self,
        offset: u64,
        buffer: &DmaBuf,
        _cb: IoCompletionCallback,
        ctx: *mut c_void,
    ) -> i32 {
        let (valid, offset_blocks, num_blocks) =
            self.bytes_to_blocks(offset, buffer.len());

        debug!(
            "{} read(offset={}, size={})",
            self.name,
            offset,
            buffer.len()
        );
        // Make sure offset/size matches device block size.
        if !valid {
            return -libc::EINVAL;
        }
        // nbdev->disk.dif_check_flags
        //self.prchk_flags

        let inner = NvmeIoChannel::inner_from_channel(self.io_channel.as_ptr());

        let rc = unsafe {
            spdk_nvme_ns_cmd_read(
                self.ns.as_ptr(),
                inner.qpair.as_ptr(),
                **buffer,
                offset_blocks,
                num_blocks as u32,
                Some(nvme_io_completion),
                ctx,
                self.prchk_flags,
            )
        };

        if rc != 0 && rc != libc::ENOMEM {
            error!("{} read failed: rc = {}", self.name, rc);
        }

        rc
    }
}
