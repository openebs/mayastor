//!
//! Trait implementation for native bdev

use std::{
    collections::HashMap,
    convert::TryFrom,
    os::raw::c_void,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use nix::errno::Errno;
use once_cell::sync::{Lazy, OnceCell};

use spdk_rs::{
    libspdk::{
        iovec,
        spdk_bdev_free_io,
        spdk_bdev_io,
        spdk_bdev_readv_blocks,
        spdk_bdev_reset,
        spdk_bdev_unmap_blocks,
        spdk_bdev_write_zeroes_blocks,
        spdk_bdev_writev_blocks,
    },
    nvme_admin_opc,
    DmaBuf,
    DmaError,
    IoType,
};

use crate::core::{
    mempool::MemoryPool,
    Bdev,
    BdevHandle,
    BlockDevice,
    BlockDeviceDescriptor,
    BlockDeviceHandle,
    BlockDeviceIoStats,
    CoreError,
    Descriptor,
    DeviceEventDispatcher,
    DeviceEventSink,
    DeviceEventType,
    DeviceIoController,
    IoCompletionCallback,
    IoCompletionCallbackArg,
    IoCompletionStatus,
    NvmeCommandStatus,
    NvmeStatus,
    UntypedBdev,
};

/// TODO
type EventDispatcherMap = HashMap<String, DeviceEventDispatcher>;

/// TODO
static BDEV_EVENT_DISPATCHER: Lazy<Mutex<EventDispatcherMap>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

// Memory pool for bdev I/O context.
static BDEV_IOCTX_POOL: OnceCell<MemoryPool<IoCtx>> = OnceCell::new();

/// Wrapper around native SPDK block devices, which mimics target SPDK block
/// device as an abstract BlockDevice instance.
pub struct SpdkBlockDevice {
    bdev: UntypedBdev,
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
        let handle = SpdkBlockDeviceHandle::try_from(self.0.clone())?;
        Ok(Box::new(handle))
    }

    fn unclaim(&self) {
        self.0.unclaim()
    }
}

impl From<UntypedBdev> for SpdkBlockDevice {
    fn from(bdev: UntypedBdev) -> Self {
        Self {
            bdev,
        }
    }
}

impl SpdkBlockDevice {
    fn new(bdev: UntypedBdev) -> Self {
        Self {
            bdev,
        }
    }

    /// Lookup existing SPDK bdev by its name.
    pub fn lookup_by_name(name: &str) -> Option<Box<dyn BlockDevice>> {
        let bdev = UntypedBdev::lookup_by_name(name)?;
        Some(Box::new(SpdkBlockDevice::new(bdev)))
    }

    /// Open SPDK bdev by its name and get a block device descriptor.
    pub fn open_by_name(
        name: &str,
        read_write: bool,
    ) -> Result<Box<dyn BlockDeviceDescriptor>, CoreError> {
        let descr = UntypedBdev::open_by_name(name, read_write)?;
        Ok(Box::new(SpdkBlockDeviceDescriptor::from(descr)))
    }

    /// Called by spdk when there is an asynchronous bdev event i.e. removal.
    pub(crate) fn bdev_event_callback(
        event: spdk_rs::BdevEvent,
        bdev: spdk_rs::UntypedBdev,
    ) {
        let dev = SpdkBlockDevice::new(Bdev::new(bdev));

        // Translate SPDK events into common device events.
        let event = match event {
            spdk_rs::BdevEvent::Remove => {
                info!("Received remove event for Bdev '{}'", dev.device_name());
                DeviceEventType::DeviceRemoved
            }
            spdk_rs::BdevEvent::Resize => {
                warn!("Received resize event for Bdev '{}'", dev.device_name());
                DeviceEventType::DeviceResized
            }
            spdk_rs::BdevEvent::MediaManagement => {
                warn!(
                    "Received media management event for Bdev '{}'",
                    dev.device_name()
                );
                DeviceEventType::MediaManagement
            }
        };

        // Forward event to the high-level handler.
        dev.notify_listeners(event);
    }

    /// Notifies all listeners of this SPDK Bdev.
    fn notify_listeners(self, event: DeviceEventType) {
        let mut map = BDEV_EVENT_DISPATCHER.lock().expect("lock poisoned");
        let name = self.device_name();
        if let Some(disp) = map.get_mut(&name) {
            disp.dispatch_event(event, &name);
        }
    }
}

#[async_trait(?Send)]
impl BlockDevice for SpdkBlockDevice {
    /// return the size in bytes
    fn size_in_bytes(&self) -> u64 {
        self.bdev.size_in_bytes()
    }
    /// returns the length of the block size in bytes
    fn block_len(&self) -> u64 {
        self.bdev.block_len() as u64
    }
    /// number of blocks the device has
    fn num_blocks(&self) -> u64 {
        self.bdev.num_blocks()
    }
    /// the UUID of the device
    fn uuid(&self) -> uuid::Uuid {
        self.bdev.uuid()
    }
    //// returns the product name
    fn product_name(&self) -> String {
        self.bdev.product_name().to_string()
    }
    //// returns the driver name of the block device
    fn driver_name(&self) -> String {
        self.bdev.driver().to_string()
    }
    /// returns the name of the device
    fn device_name(&self) -> String {
        self.bdev.name().to_string()
    }
    //// returns the alignment of the device
    fn alignment(&self) -> u64 {
        self.bdev.alignment()
    }
    /// returns true if the IO type is supported
    fn io_type_supported(&self, io_type: IoType) -> bool {
        self.bdev.io_type_supported(io_type)
    }
    /// returns the IO statistics
    async fn io_stats(&self) -> Result<BlockDeviceIoStats, CoreError> {
        self.bdev.stats_async().await
    }
    /// returns which module has returned driver
    fn claimed_by(&self) -> Option<String> {
        self.bdev.claimed_by()
    }
    /// open the device returning descriptor to the device
    fn open(
        &self,
        read_write: bool,
    ) -> Result<Box<dyn BlockDeviceDescriptor>, CoreError> {
        let descr = self.bdev.open(read_write)?;
        Ok(Box::new(SpdkBlockDeviceDescriptor::from(descr)))
    }

    /// returns the IO controller
    fn get_io_controller(&self) -> Option<Box<dyn DeviceIoController>> {
        None
    }
    /// add a callback to be called when a particular event is received
    fn add_event_listener(
        &self,
        listener: DeviceEventSink,
    ) -> Result<(), CoreError> {
        let mut map = BDEV_EVENT_DISPATCHER.lock().expect("lock poisoned");
        let disp = map.entry(self.bdev.name().to_string()).or_default();
        disp.add_listener(listener);
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

#[inline]
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

/// Initialize memory pool for allocating bdev I/O contexts.
/// This must be called before the first I/O operations take place.
pub fn bdev_io_ctx_pool_init(size: u64) {
    BDEV_IOCTX_POOL.get_or_init(|| {
        MemoryPool::<IoCtx>::create("bdev_io_ctx", size).expect(
            "Failed to create memory pool [bdev_io_ctx] for bdev I/O contexts",
        )
    });
}

/// Allocate a bdev I/O context from the pool.
fn alloc_bdev_io_ctx(
    op: IoType,
    ctx: IoCtx,
    offset_blocks: u64,
    num_blocks: u64,
) -> Result<*mut IoCtx, CoreError> {
    let pool = BDEV_IOCTX_POOL.get().unwrap();
    pool.get(ctx).ok_or_else(|| {
        io_type_to_err(op, Errno::ENOMEM, offset_blocks, num_blocks)
    })
}

/// Release the memory used by the bdev I/O context back to the pool.
fn free_bdev_io_ctx(ctx: *mut IoCtx) {
    let pool = BDEV_IOCTX_POOL.get().unwrap();
    pool.put(ctx);
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
        let nvme_status = NvmeStatus::from(child_bio);
        let nvme_cmd_status = NvmeCommandStatus::from_command_status(
            nvme_status.status_type(),
            nvme_status.status_code(),
        );
        IoCompletionStatus::NvmeError(nvme_cmd_status)
    };

    (bio.cb)(&*bio.handle.device, status, bio.cb_arg);

    // Free ctx.
    free_bdev_io_ctx(&mut *bio);

    // Free replica's bio.
    unsafe {
        spdk_bdev_free_io(child_bio);
    }
}

#[async_trait(?Send)]
impl BlockDeviceHandle for SpdkBlockDeviceHandle {
    fn get_device(&self) -> &dyn BlockDevice {
        &*self.device
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
        let ctx = alloc_bdev_io_ctx(
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
        let ctx = alloc_bdev_io_ctx(
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
        let ctx = alloc_bdev_io_ctx(
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
        let ctx = alloc_bdev_io_ctx(
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
        let ctx = alloc_bdev_io_ctx(
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

    /// NVMe commands are not applicable for non-NVMe devices.
    async fn nvme_admin_custom(&self, opcode: u8) -> Result<(), CoreError> {
        Err(CoreError::NvmeAdminDispatch {
            source: Errno::ENXIO,
            opcode: opcode.into(),
        })
    }

    /// NVMe commands are not applicable for non-NVMe devices.
    async fn nvme_admin(
        &self,
        nvme_cmd: &spdk_rs::libspdk::spdk_nvme_cmd,
        _buffer: Option<&mut DmaBuf>,
    ) -> Result<(), CoreError> {
        Err(CoreError::NvmeAdminDispatch {
            source: Errno::ENXIO,
            opcode: nvme_cmd.opc(),
        })
    }

    /// NVMe commands are not applicable for non-NVMe devices.
    async fn nvme_identify_ctrlr(&self) -> Result<DmaBuf, CoreError> {
        Err(CoreError::NvmeAdminDispatch {
            source: Errno::ENXIO,
            opcode: nvme_admin_opc::IDENTIFY.into(),
        })
    }

    // NVMe commands are not applicable for non-NVMe devices.
    async fn create_snapshot(&self) -> Result<u64, CoreError> {
        Err(CoreError::NotSupported {
            source: Errno::ENXIO,
        })
    }
}
