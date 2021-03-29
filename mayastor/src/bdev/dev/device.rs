use std::{
    collections::HashMap,
    convert::TryFrom,
    os::raw::c_void,
    sync::{Arc, RwLock},
};

use crate::core::{
    mempool::MemoryPool,
    nvme_admin_opc,
    Bdev,
    BdevHandle,
    Bio,
    BlockDevice,
    BlockDeviceDescriptor,
    BlockDeviceHandle,
    BlockDeviceIoStats,
    CoreError,
    Descriptor,
    DeviceEventListener,
    DeviceEventType,
    DeviceIoController,
    DmaBuf,
    DmaError,
    IoCompletionCallback,
    IoCompletionCallbackArg,
    IoCompletionStatus,
    IoType,
    NvmeCommandStatus,
};

use async_trait::async_trait;
use nix::errno::Errno;
use once_cell::sync::Lazy;
use spdk_sys::{
    iovec,
    spdk_bdev_free_io,
    spdk_bdev_io,
    spdk_bdev_readv_blocks,
    spdk_bdev_reset,
    spdk_bdev_unmap_blocks,
    spdk_bdev_write_zeroes_blocks,
    spdk_bdev_writev_blocks,
};

static BDEV_LISTENERS: Lazy<RwLock<HashMap<String, Vec<DeviceEventListener>>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

// Size of the memory pool for NVMe I/O structures.
const IOCTX_POOL_SIZE: u64 = 64 * 1024 - 1;
static IOCTX_POOL: Lazy<MemoryPool<IoCtx>> = Lazy::new(|| {
    MemoryPool::<IoCtx>::create("bdev_io_ctx", IOCTX_POOL_SIZE)
        .expect("Failed to create memory pool for bdev I/O context")
});
/// Wrapper around native SPDK block devices, which mimics target SPDK block
/// device as an abstract BlockDevice instance.
pub(crate) struct SpdkBlockDevice {
    bdev: Bdev,
}
/// Wrapper around native SPDK block device descriptor, which mimics target SPDK
/// descriptor as an abstract BlockDeviceDescriptor instance.
struct SpdkBlockDeviceDescriptor(Arc<Descriptor>);
/// Wrapper around native SPDK block device I/O, which mimics target SPDK I/O
/// handle as an abstract BlockDeviceDescriptor instance.
struct SpdkBlockDeviceHandle {
    device: Box<dyn BlockDevice>,
    handle: BdevHandle,
}

struct IoCtx<'a> {
    handle: &'a SpdkBlockDeviceHandle,
    cb: IoCompletionCallback,
    cb_arg: IoCompletionCallbackArg,
}

impl From<Descriptor> for SpdkBlockDeviceDescriptor {
    fn from(descr: Descriptor) -> Self {
        Self(Arc::new(descr))
    }
}

impl BlockDeviceDescriptor for SpdkBlockDeviceDescriptor {
    fn get_device(&self) -> Box<dyn BlockDevice> {
        Box::new(SpdkBlockDevice::from(self.0.get_bdev()))
    }

    fn into_handle(
        self: Box<Self>,
    ) -> Result<Box<dyn BlockDeviceHandle>, CoreError> {
        let handle = SpdkBlockDeviceHandle::try_from(self.0)?;
        Ok(Box::new(handle))
    }

    fn get_io_handle(&self) -> Result<Box<dyn BlockDeviceHandle>, CoreError> {
        let handle = SpdkBlockDeviceHandle::try_from(Arc::clone(&self.0))?;
        Ok(Box::new(handle))
    }

    fn unclaim(&self) {
        self.0.unclaim()
    }
}

impl From<Bdev> for SpdkBlockDevice {
    fn from(bdev: Bdev) -> Self {
        Self {
            bdev,
        }
    }
}

impl SpdkBlockDevice {
    fn new(bdev: Bdev) -> Self {
        Self {
            bdev,
        }
    }

    /// Lookup existing SPDK bdev by its name.
    pub fn lookup_by_name(name: &str) -> Option<Box<dyn BlockDevice>> {
        let bdev = Bdev::lookup_by_name(name)?;
        Some(Box::new(SpdkBlockDevice::new(bdev)))
    }

    /// Open SPDK bdev by its name and get a block device desriptor.
    pub fn open_by_name(
        name: &str,
        read_write: bool,
    ) -> Result<Box<dyn BlockDeviceDescriptor>, CoreError> {
        let descr = Bdev::open_by_name(name, read_write)?;
        Ok(Box::new(SpdkBlockDeviceDescriptor::from(descr)))
    }

    pub fn process_device_event(event: DeviceEventType, device: &str) {
        // Keep a separate copy of all registered listeners in order to not
        // invoke them with the lock held.
        let listeners = {
            let map = BDEV_LISTENERS.read().expect("lock poisoned");
            match map.get(device) {
                Some(listeners) => listeners.clone(),
                None => return,
            }
        };

        // Notify all listeners of this SPDK bdev.
        for l in listeners {
            (l)(event, device);
        }
    }
}

#[async_trait(?Send)]
impl BlockDevice for SpdkBlockDevice {
    fn size_in_bytes(&self) -> u64 {
        self.bdev.size_in_bytes()
    }

    fn block_len(&self) -> u64 {
        self.bdev.block_len() as u64
    }

    fn num_blocks(&self) -> u64 {
        self.bdev.num_blocks()
    }

    fn uuid(&self) -> String {
        self.bdev.uuid_as_string()
    }

    fn product_name(&self) -> String {
        self.bdev.product_name()
    }

    fn driver_name(&self) -> String {
        self.bdev.driver()
    }

    fn device_name(&self) -> String {
        self.bdev.name()
    }

    fn alignment(&self) -> u64 {
        self.bdev.alignment()
    }

    fn io_type_supported(&self, io_type: IoType) -> bool {
        self.bdev.io_type_supported(io_type)
    }

    async fn io_stats(&self) -> Result<BlockDeviceIoStats, CoreError> {
        self.bdev.stats().await
    }

    fn claimed_by(&self) -> Option<String> {
        self.bdev.claimed_by()
    }

    fn open(
        &self,
        read_write: bool,
    ) -> Result<Box<dyn BlockDeviceDescriptor>, CoreError> {
        let descr = self.bdev.open(read_write)?;
        Ok(Box::new(SpdkBlockDeviceDescriptor::from(descr)))
    }

    fn get_io_controller(&self) -> Option<Box<dyn DeviceIoController>> {
        None
    }

    fn add_event_listener(
        &self,
        listener: DeviceEventListener,
    ) -> Result<(), CoreError> {
        let mut map = BDEV_LISTENERS.write().expect("lock poisoned");
        let listeners = map.entry(self.bdev.name()).or_default();
        listeners.push(listener);
        Ok(())
    }
}

impl TryFrom<Arc<Descriptor>> for SpdkBlockDeviceHandle {
    type Error = CoreError;

    fn try_from(desc: Arc<Descriptor>) -> Result<Self, Self::Error> {
        let handle = BdevHandle::try_from(desc)?;
        Ok(SpdkBlockDeviceHandle::from(handle))
    }
}

impl From<BdevHandle> for SpdkBlockDeviceHandle {
    fn from(handle: BdevHandle) -> Self {
        Self {
            device: Box::new(SpdkBlockDevice::from(handle.get_bdev())),
            handle,
        }
    }
}

fn io_type_to_err(
    op: IoType,
    source: Errno,
    offset: u64,
    len: u64,
) -> CoreError {
    match op {
        IoType::Read => CoreError::ReadDispatch {
            source,
            offset,
            len,
        },
        IoType::Write => CoreError::WriteDispatch {
            source,
            offset,
            len,
        },
        IoType::Unmap | IoType::WriteZeros => CoreError::UnmapDispatch {
            source,
            offset,
            len,
        },
        IoType::Reset => CoreError::ResetDispatch {
            source,
        },
        _ => {
            warn!("Unsupported I/O operation: {:?}", op);
            CoreError::NotSupported {
                source,
            }
        }
    }
}

fn alloc_io_ctx(
    op: IoType,
    ctx: IoCtx,
    offset_blocks: u64,
    num_blocks: u64,
) -> Result<*mut IoCtx, CoreError> {
    IOCTX_POOL.get(ctx).ok_or_else(|| {
        io_type_to_err(op, Errno::ENOMEM, offset_blocks, num_blocks)
    })
}

extern "C" fn bdev_io_completion(
    child_bio: *mut spdk_bdev_io,
    success: bool,
    ctx: *mut c_void,
) {
    let bio = unsafe { &mut *(ctx as *mut IoCtx) };

    // Get extended NVMe error status from original bio in case of error.
    let status = if success {
        IoCompletionStatus::Success
    } else {
        let nvme_status = Bio::from(child_bio).nvme_status();
        let nvme_cmd_status = NvmeCommandStatus::from_command_status(
            nvme_status.status_type(),
            nvme_status.status_code(),
        );
        IoCompletionStatus::NvmeError(nvme_cmd_status)
    };

    (bio.cb)(&bio.handle.device, status, bio.cb_arg);

    // Free replica's bio.
    unsafe {
        spdk_bdev_free_io(child_bio);
    }
}

#[async_trait(?Send)]
impl BlockDeviceHandle for SpdkBlockDeviceHandle {
    fn get_device(&self) -> &Box<dyn BlockDevice> {
        &self.device
    }

    fn dma_malloc(&self, size: u64) -> Result<DmaBuf, DmaError> {
        DmaBuf::new(size, self.device.alignment())
    }

    async fn read_at(
        &self,
        offset: u64,
        buffer: &mut DmaBuf,
    ) -> Result<u64, CoreError> {
        self.handle.read_at(offset, buffer).await
    }

    async fn write_at(
        &self,
        offset: u64,
        buffer: &DmaBuf,
    ) -> Result<u64, CoreError> {
        self.handle.write_at(offset, buffer).await
    }

    fn readv_blocks(
        &self,
        iov: *mut iovec,
        iovcnt: i32,
        offset_blocks: u64,
        num_blocks: u64,
        cb: IoCompletionCallback,
        cb_arg: IoCompletionCallbackArg,
    ) -> Result<(), CoreError> {
        let ctx = alloc_io_ctx(
            IoType::Read,
            IoCtx {
                handle: self,
                cb,
                cb_arg,
            },
            offset_blocks,
            num_blocks,
        )?;

        let (desc, chan) = self.handle.io_tuple();
        let rc = unsafe {
            spdk_bdev_readv_blocks(
                desc,
                chan,
                iov,
                iovcnt,
                offset_blocks,
                num_blocks,
                Some(bdev_io_completion),
                ctx as *mut c_void,
            )
        };

        if rc < 0 {
            Err(CoreError::ReadDispatch {
                source: Errno::from_i32(-rc),
                offset: offset_blocks,
                len: num_blocks,
            })
        } else {
            Ok(())
        }
    }

    fn writev_blocks(
        &self,
        iov: *mut iovec,
        iovcnt: i32,
        offset_blocks: u64,
        num_blocks: u64,
        cb: IoCompletionCallback,
        cb_arg: IoCompletionCallbackArg,
    ) -> Result<(), CoreError> {
        let ctx = alloc_io_ctx(
            IoType::Write,
            IoCtx {
                handle: self,
                cb,
                cb_arg,
            },
            offset_blocks,
            num_blocks,
        )?;

        let (desc, chan) = self.handle.io_tuple();
        let rc = unsafe {
            spdk_bdev_writev_blocks(
                desc,
                chan,
                iov,
                iovcnt,
                offset_blocks,
                num_blocks,
                Some(bdev_io_completion),
                ctx as *mut c_void,
            )
        };

        if rc < 0 {
            Err(CoreError::WriteDispatch {
                source: Errno::from_i32(-rc),
                offset: offset_blocks,
                len: num_blocks,
            })
        } else {
            Ok(())
        }
    }

    fn reset(
        &self,
        cb: IoCompletionCallback,
        cb_arg: IoCompletionCallbackArg,
    ) -> Result<(), CoreError> {
        let ctx = alloc_io_ctx(
            IoType::Reset,
            IoCtx {
                handle: self,
                cb,
                cb_arg,
            },
            0,
            0,
        )?;

        let (desc, chan) = self.handle.io_tuple();
        let rc = unsafe {
            spdk_bdev_reset(
                desc,
                chan,
                Some(bdev_io_completion),
                ctx as *mut c_void,
            )
        };

        if rc < 0 {
            Err(CoreError::ResetDispatch {
                source: Errno::ENOMEM,
            })
        } else {
            Ok(())
        }
    }

    fn unmap_blocks(
        &self,
        offset_blocks: u64,
        num_blocks: u64,
        cb: IoCompletionCallback,
        cb_arg: IoCompletionCallbackArg,
    ) -> Result<(), CoreError> {
        let ctx = alloc_io_ctx(
            IoType::Unmap,
            IoCtx {
                handle: self,
                cb,
                cb_arg,
            },
            offset_blocks,
            num_blocks,
        )?;

        let (desc, chan) = self.handle.io_tuple();
        let rc = unsafe {
            spdk_bdev_unmap_blocks(
                desc,
                chan,
                offset_blocks,
                num_blocks,
                Some(bdev_io_completion),
                ctx as *mut c_void,
            )
        };

        if rc < 0 {
            Err(CoreError::UnmapDispatch {
                source: Errno::ENOMEM,
                offset: offset_blocks,
                len: num_blocks,
            })
        } else {
            Ok(())
        }
    }

    fn write_zeroes(
        &self,
        offset_blocks: u64,
        num_blocks: u64,
        cb: IoCompletionCallback,
        cb_arg: IoCompletionCallbackArg,
    ) -> Result<(), CoreError> {
        let ctx = alloc_io_ctx(
            IoType::WriteZeros,
            IoCtx {
                handle: self,
                cb,
                cb_arg,
            },
            offset_blocks,
            num_blocks,
        )?;

        let (desc, chan) = self.handle.io_tuple();
        let rc = unsafe {
            spdk_bdev_write_zeroes_blocks(
                desc,
                chan,
                offset_blocks,
                num_blocks,
                Some(bdev_io_completion),
                ctx as *mut c_void,
            )
        };

        if rc < 0 {
            Err(CoreError::UnmapDispatch {
                source: Errno::ENOMEM,
                offset: offset_blocks,
                len: num_blocks,
            })
        } else {
            Ok(())
        }
    }

    // NVMe commands are not applicable for non-NVMe devices.
    async fn nvme_admin_custom(&self, opcode: u8) -> Result<(), CoreError> {
        Err(CoreError::NvmeAdminDispatch {
            source: Errno::ENXIO,
            opcode: opcode.into(),
        })
    }

    // NVMe commands are not applicable for non-NVMe devices.
    async fn nvme_admin(
        &self,
        nvme_cmd: &spdk_sys::spdk_nvme_cmd,
        _buffer: Option<&mut DmaBuf>,
    ) -> Result<(), CoreError> {
        Err(CoreError::NvmeAdminDispatch {
            source: Errno::ENXIO,
            opcode: nvme_cmd.opc(),
        })
    }

    // NVMe commands are not applicable for non-NVMe devices.
    async fn nvme_identify_ctrlr(&self) -> Result<DmaBuf, CoreError> {
        Err(CoreError::NvmeAdminDispatch {
            source: Errno::ENXIO,
            opcode: nvme_admin_opc::IDENTIFY.into(),
        })
    }
}
