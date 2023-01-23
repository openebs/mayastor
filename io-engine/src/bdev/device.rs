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
        spdk_bdev_comparev_blocks,
        spdk_bdev_flush,
        spdk_bdev_free_io,
        spdk_bdev_io,
        spdk_bdev_readv_blocks_with_flags,
        spdk_bdev_reset,
        spdk_bdev_unmap_blocks,
        spdk_bdev_write_zeroes_blocks,
        spdk_bdev_writev_blocks,
        SPDK_NVME_IO_FLAGS_UNWRITTEN_READ_FAIL,
        SPDK_NVME_IO_FLAG_CURRENT_UNWRITTEN_READ_FAIL,
    },
    nvme_admin_opc,
    AsIoVecPtr,
    BdevOps,
    DmaBuf,
    DmaError,
    IoType,
    IoVec,
};

use crate::{
    core::{
        mempool::MemoryPool,
        snapshot::SnapshotOps,
        Bdev,
        BdevHandle,
        BlockDevice,
        BlockDeviceDescriptor,
        BlockDeviceHandle,
        BlockDeviceIoStats,
        CoreError,
        DeviceEventDispatcher,
        DeviceEventSink,
        DeviceEventType,
        DeviceIoController,
        IoCompletionCallback,
        IoCompletionCallbackArg,
        IoCompletionStatus,
        NvmeStatus,
        ReadOptions,
        SnapshotParams,
        ToErrno,
        UntypedBdev,
        UntypedBdevHandle,
        UntypedDescriptorGuard,
        ZonedBlockDevice,
    },
    lvs::Lvol,
};

#[cfg(feature = "fault-injection")]
use crate::core::fault_injection::{
    inject_completion_error,
    inject_submission_error,
    FaultDomain,
    InjectIoCtx,
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
#[derive(Copy, Clone)]
pub struct SpdkBlockDevice(UntypedBdev);

impl SpdkBlockDevice {
    fn new(bdev: UntypedBdev) -> Self {
        Self(bdev)
    }

    /// Lookup existing SPDK bdev by its name.
    pub fn lookup_by_name(name: &str) -> Option<Box<dyn BlockDevice>> {
        debug!("Searching SPDK devices for '{}'...", name);
        let bdev = UntypedBdev::lookup_by_name(name)?;
        debug!("SPDK {} device found: '{}'", bdev.driver(), bdev.name());
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
}

#[async_trait(?Send)]
impl BlockDevice for SpdkBlockDevice {
    /// return the size in bytes
    fn size_in_bytes(&self) -> u64 {
        self.0.size_in_bytes()
    }
    /// returns the length of the block size in bytes
    fn block_len(&self) -> u64 {
        self.0.block_len() as u64
    }
    /// number of blocks the device has
    fn num_blocks(&self) -> u64 {
        self.0.num_blocks()
    }
    /// the UUID of the device
    fn uuid(&self) -> uuid::Uuid {
        self.0.uuid()
    }
    //// returns the product name
    fn product_name(&self) -> String {
        self.0.product_name().to_string()
    }
    //// returns the driver name of the block device
    fn driver_name(&self) -> String {
        self.0.driver().to_string()
    }
    /// returns the name of the device
    fn device_name(&self) -> String {
        self.0.name().to_string()
    }
    //// returns the alignment of the device
    fn alignment(&self) -> u64 {
        self.0.alignment()
    }
    /// returns true if the IO type is supported
    fn io_type_supported(&self, io_type: IoType) -> bool {
        match io_type {
            //IoType::NvmeIo => true,
            _ => self.io_type_supported_by_device(io_type),
        }
    }

    fn io_type_supported_by_device(&self, io_type: IoType) -> bool {
        self.0.io_type_supported(io_type)
    }

    /// returns the IO statistics
    async fn io_stats(&self) -> Result<BlockDeviceIoStats, CoreError> {
        self.0.stats_async().await
    }
    /// open the device returning descriptor to the device
    fn open(
        &self,
        read_write: bool,
    ) -> Result<Box<dyn BlockDeviceDescriptor>, CoreError> {
        let descr = self.0.open(read_write)?;
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
        let disp = map.entry(self.device_name()).or_default();
        disp.add_listener(listener);
        Ok(())
    }
}

#[async_trait(?Send)]
impl ZonedBlockDevice for SpdkBlockDevice {
    fn is_zoned(&self) -> bool {
        self.0.is_zoned()
    }

    fn zone_size(&self) -> u64 {
        self.0.zone_size()
    }

    fn num_zones(&self) -> u64 {
        self.0.num_zones()
    }

    fn max_zone_append_size(&self) -> u32 {
        self.0.max_zone_append_size()
    }

    fn max_open_zones(&self) -> u32 {
        self.0.max_open_zones()
    }

    fn max_active_zones(&self) -> u32 {
        self.0.max_active_zones()
    }

    fn optimal_open_zones(&self) -> u32 {
        self.0.optimal_open_zones()
    }
}

/// Wrapper around native SPDK block device descriptor, which mimics target SPDK
/// descriptor as an abstract BlockDeviceDescriptor instance.
struct SpdkBlockDeviceDescriptor(Arc<UntypedDescriptorGuard>);

impl From<UntypedDescriptorGuard> for SpdkBlockDeviceDescriptor {
    fn from(descr: UntypedDescriptorGuard) -> Self {
        Self(Arc::new(descr))
    }
}

#[async_trait(?Send)]
impl BlockDeviceDescriptor for SpdkBlockDeviceDescriptor {
    fn get_device(&self) -> Box<dyn BlockDevice> {
        Box::new(SpdkBlockDevice::new(self.0.bdev()))
    }

    fn device_name(&self) -> String {
        self.0.bdev().name().to_string()
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

    async fn get_io_handle_nonblock(
        &self,
    ) -> Result<Box<dyn BlockDeviceHandle>, CoreError> {
        let handle = SpdkBlockDeviceHandle::try_from(self.0.clone())?;
        Ok(Box::new(handle))
    }

    fn unclaim(&self) {
        self.0.unclaim()
    }
}

/// Wrapper around native SPDK block device I/O, which mimics target SPDK I/O
/// handle as an abstract BlockDeviceDescriptor instance.
struct SpdkBlockDeviceHandle {
    device: SpdkBlockDevice,
    handle: UntypedBdevHandle,
}

impl TryFrom<Arc<UntypedDescriptorGuard>> for SpdkBlockDeviceHandle {
    type Error = CoreError;

    fn try_from(
        desc: Arc<UntypedDescriptorGuard>,
    ) -> Result<Self, Self::Error> {
        let handle = BdevHandle::try_from(desc)?;
        Ok(SpdkBlockDeviceHandle::from(handle))
    }
}

impl From<UntypedBdevHandle> for SpdkBlockDeviceHandle {
    fn from(handle: UntypedBdevHandle) -> Self {
        Self {
            device: SpdkBlockDevice::new(handle.get_bdev()),
            handle,
        }
    }
}

#[async_trait(?Send)]
impl BlockDeviceHandle for SpdkBlockDeviceHandle {
    fn get_device(&self) -> &dyn BlockDevice {
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
        iovs: &mut [IoVec],
        offset_blocks: u64,
        num_blocks: u64,
        opts: ReadOptions,
        cb: IoCompletionCallback,
        cb_arg: IoCompletionCallbackArg,
    ) -> Result<(), CoreError> {
        let flags: u32 = opts.into();

        let ctx = alloc_bdev_io_ctx(
            IoType::Read,
            IoCtx {
                device: self.device,
                cb,
                cb_arg,
                #[cfg(feature = "fault-injection")]
                inj_op: InjectIoCtx::with_iovs(
                    FaultDomain::BlockDevice,
                    self.get_device(),
                    IoType::Read,
                    offset_blocks,
                    num_blocks,
                    iovs,
                ),
            },
            offset_blocks,
            num_blocks,
        )?;

        #[cfg(feature = "fault-injection")]
        inject_submission_error(unsafe { &(*ctx).inj_op })?;

        let (desc, chan) = self.handle.io_tuple();
        let rc = unsafe {
            spdk_bdev_readv_blocks_with_flags(
                desc,
                chan,
                iovs.as_io_vec_mut_ptr(),
                iovs.len() as i32,
                offset_blocks,
                num_blocks,
                Some(bdev_io_completion),
                ctx as *mut c_void,
                flags,
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
        iovs: &[IoVec],
        offset_blocks: u64,
        num_blocks: u64,
        cb: IoCompletionCallback,
        cb_arg: IoCompletionCallbackArg,
    ) -> Result<(), CoreError> {
        let ctx = alloc_bdev_io_ctx(
            IoType::Write,
            IoCtx {
                device: self.device,
                cb,
                cb_arg,
                #[cfg(feature = "fault-injection")]
                inj_op: InjectIoCtx::with_iovs(
                    FaultDomain::BlockDevice,
                    self.get_device(),
                    IoType::Write,
                    offset_blocks,
                    num_blocks,
                    iovs,
                ),
            },
            offset_blocks,
            num_blocks,
        )?;

        #[cfg(feature = "fault-injection")]
        inject_submission_error(unsafe { &(*ctx).inj_op })?;

        let (desc, chan) = self.handle.io_tuple();
        let rc = unsafe {
            spdk_bdev_writev_blocks(
                desc,
                chan,
                iovs.as_ptr() as *mut _,
                iovs.len() as i32,
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

    fn comparev_blocks(
        &self,
        iovs: &[IoVec],
        offset_blocks: u64,
        num_blocks: u64,
        cb: IoCompletionCallback,
        cb_arg: IoCompletionCallbackArg,
    ) -> Result<(), CoreError> {
        let ctx = alloc_bdev_io_ctx(
            IoType::Compare,
            IoCtx {
                device: self.device,
                cb,
                cb_arg,
                #[cfg(feature = "fault-injection")]
                inj_op: InjectIoCtx::new(FaultDomain::BlockDevice),
            },
            offset_blocks,
            num_blocks,
        )?;

        let (desc, chan) = self.handle.io_tuple();
        let rc = unsafe {
            spdk_bdev_comparev_blocks(
                desc,
                chan,
                iovs.as_ptr() as *mut _,
                iovs.len() as i32,
                offset_blocks,
                num_blocks,
                Some(bdev_io_completion),
                ctx as *mut c_void,
            )
        };

        if rc < 0 {
            Err(CoreError::CompareDispatch {
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
                device: self.device,
                cb,
                cb_arg,
                #[cfg(feature = "fault-injection")]
                inj_op: InjectIoCtx::new(FaultDomain::BlockDevice),
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
                device: self.device,
                cb,
                cb_arg,
                #[cfg(feature = "fault-injection")]
                inj_op: InjectIoCtx::new(FaultDomain::BlockDevice),
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
                device: self.device,
                cb,
                cb_arg,
                #[cfg(feature = "fault-injection")]
                inj_op: InjectIoCtx::new(FaultDomain::BlockDevice),
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
    async fn create_snapshot(
        &self,
        snapshot: SnapshotParams,
    ) -> Result<u64, CoreError> {
        let bdev = self.handle.get_bdev();

        // Snapshots are supported only for LVOLs.
        if bdev.driver() != "lvol" {
            return Err(CoreError::NotSupported {
                source: Errno::ENXIO,
            });
        }

        let lvol =
            Lvol::try_from(bdev).map_err(|_e| CoreError::BdevNotFound {
                name: bdev.name().to_string(),
            })?;

        lvol.create_snapshot(snapshot).await.map_err(|e| {
            CoreError::SnapshotCreate {
                reason: e.to_string(),
                source: e.to_errno(),
            }
        })?;

        Ok(0)
    }
    // Flush the io in buffer to disk, for the Local Block Device.
    fn flush_io(
        &self,
        cb: IoCompletionCallback,
        cb_arg: IoCompletionCallbackArg,
    ) -> Result<(), CoreError> {
        let ctx = alloc_bdev_io_ctx(
            IoType::Flush,
            IoCtx {
                device: self.device,
                cb,
                cb_arg,
                #[cfg(feature = "fault-injection")]
                inj_op: InjectIoCtx::new(FaultDomain::BlockDevice),
            },
            0,
            0,
        )?;

        let (desc, chan) = self.handle.io_tuple();
        let bdev_size = self.device.size_in_bytes();
        let rc = unsafe {
            spdk_bdev_flush(
                desc,
                chan,
                0,
                bdev_size,
                Some(bdev_io_completion),
                ctx as *mut c_void,
            )
        };

        if rc < 0 {
            Err(CoreError::FlushDispatch {
                source: Errno::ENOMEM,
            })
        } else {
            Ok(())
        }
    }
}

/// TODO
struct IoCtx {
    device: SpdkBlockDevice,
    cb: IoCompletionCallback,
    cb_arg: IoCompletionCallbackArg,
    #[cfg(feature = "fault-injection")]
    inj_op: InjectIoCtx,
}

/// TODO
#[inline]
pub fn io_type_to_err(
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
        IoType::Compare => CoreError::CompareDispatch {
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
        IoCompletionStatus::from(NvmeStatus::from(child_bio))
    };

    #[cfg(feature = "fault-injection")]
    let status = inject_completion_error(&bio.inj_op, status);

    (bio.cb)(&bio.device, status, bio.cb_arg);

    // Free ctx.
    free_bdev_io_ctx(&mut *bio);

    // Free replica's bio.
    unsafe {
        spdk_bdev_free_io(child_bio);
    }
}

/// Forwards event to the high-level handler.
fn dispatch_bdev_event(event: DeviceEventType, name: &str) {
    let mut map = BDEV_EVENT_DISPATCHER.lock().expect("lock poisoned");
    if let Some(disp) = map.get_mut(name) {
        disp.dispatch_event(event, name);
    }
}

/// Called by spdk when there is an asynchronous bdev event i.e. removal.
pub fn bdev_event_callback<T: BdevOps>(
    event: spdk_rs::BdevEvent,
    bdev: spdk_rs::Bdev<T>,
) {
    let dev = Bdev::<T>::new(bdev);

    // Translate SPDK events into common device events.
    let event = match event {
        spdk_rs::BdevEvent::Remove => {
            info!("Received SPDK remove event for bdev '{}'", dev.name());
            DeviceEventType::DeviceRemoved
        }
        spdk_rs::BdevEvent::Resize => {
            warn!("Received SPDK resize event for bdev '{}'", dev.name());
            DeviceEventType::DeviceResized
        }
        spdk_rs::BdevEvent::MediaManagement => {
            warn!(
                "Received SPDK media management event for Bdev '{}'",
                dev.name()
            );
            DeviceEventType::MediaManagement
        }
    };

    dispatch_bdev_event(event, dev.name());
}

/// Dispatches a special event for loopback device removal.
pub fn dispatch_loopback_removed(name: &str) {
    dispatch_bdev_event(DeviceEventType::LoopbackRemoved, name);
}

impl From<ReadOptions> for u32 {
    fn from(opts: ReadOptions) -> Self {
        match opts {
            ReadOptions::None => 0,
            ReadOptions::UnwrittenFail => {
                SPDK_NVME_IO_FLAGS_UNWRITTEN_READ_FAIL
            }
            ReadOptions::CurrentUnwrittenFail => {
                SPDK_NVME_IO_FLAG_CURRENT_UNWRITTEN_READ_FAIL
            }
        }
    }
}
