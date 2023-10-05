use std::{alloc::Layout, mem::ManuallyDrop, os::raw::c_void, sync::Arc};

use async_trait::async_trait;
use futures::channel::oneshot;
use nix::errno::Errno;
use once_cell::sync::OnceCell;

use spdk_rs::{
    libspdk::{
        iovec,
        nvme_cmd_cdw10_get,
        spdk_get_io_channel,
        spdk_io_channel,
        spdk_nvme_cmd,
        spdk_nvme_cpl,
        spdk_nvme_ctrlr_cmd_admin_raw,
        spdk_nvme_ctrlr_cmd_io_raw,
        spdk_nvme_dsm_range,
        spdk_nvme_ns_cmd_compare,
        spdk_nvme_ns_cmd_comparev,
        spdk_nvme_ns_cmd_dataset_management,
        spdk_nvme_ns_cmd_flush,
        spdk_nvme_ns_cmd_read,
        spdk_nvme_ns_cmd_readv,
        spdk_nvme_ns_cmd_write,
        spdk_nvme_ns_cmd_write_zeroes,
        spdk_nvme_ns_cmd_writev,
        SPDK_NVME_IO_FLAGS_UNWRITTEN_READ_FAIL,
        SPDK_NVME_SC_INTERNAL_DEVICE_ERROR,
    },
    nvme_admin_opc,
    nvme_nvm_opcode,
    nvme_reservation_register_action,
    AsIoVecPtr,
    DmaBuf,
    DmaError,
    IoVec,
    NvmeStatus,
};

use crate::{
    bdev::nvmx::{
        channel::NvmeControllerIoChannel,
        controller_inner::SpdkNvmeController,
        utils,
        utils::{nvme_cpl_is_pi_error, nvme_cpl_succeeded},
        NvmeBlockDevice,
        NvmeIoChannel,
        NvmeNamespace,
        NvmeSnapshotMessage,
        NvmeSnapshotMessageV1,
        NVME_CONTROLLERS,
    },
    core::{
        mempool::MemoryPool,
        BlockDevice,
        BlockDeviceHandle,
        CoreError,
        IoCompletionCallback,
        IoCompletionCallbackArg,
        IoCompletionStatus,
        IoType,
        Reactors,
        ReadOptions,
        SnapshotParams,
    },
    ffihelper::{cb_arg, done_cb, FfiResult},
    subsys,
};

#[cfg(feature = "fault-injection")]
use crate::core::fault_injection::{
    inject_completion_error,
    inject_submission_error,
    FaultDomain,
    InjectIoCtx,
};

use super::NvmeIoChannelInner;

/*
 * I/O context for NVMe controller I/O operation. Used as a placeholder for
 * storing user context and also private state of I/O operations, specific to
 * the controller.
 */
struct NvmeIoCtx {
    cb: IoCompletionCallback,
    cb_arg: IoCompletionCallbackArg,
    iov: *mut iovec,
    iovcnt: u64,
    iovpos: u64,
    iov_offset: u64,
    op: IoType,
    num_blocks: u64,
    channel: *mut spdk_io_channel,
    #[cfg(feature = "fault-injection")]
    inj_op: InjectIoCtx,
}

unsafe impl Send for NvmeIoCtx {}
unsafe impl Sync for NvmeIoCtx {}

// Memory pool for NVMe controller specific I/O context,
// which is used in every user BIO-based I/O operation.
static NVME_IOCTX_POOL: OnceCell<MemoryPool<NvmeIoCtx>> = OnceCell::new();

// Maximum number of range sets that may be specified in the dataset management
// command.
const SPDK_NVME_DATASET_MANAGEMENT_MAX_RANGES: u64 = 256;

// Maximum number of blocks that may be specified in a single dataset management
// range.
const SPDK_NVME_DATASET_MANAGEMENT_RANGE_MAX_BLOCKS: u64 = 0xFFFFFFFF;

/// I/O handle for NVMe block device.
pub struct NvmeDeviceHandle {
    /// io channel for the current thread
    io_channel: ManuallyDrop<NvmeControllerIoChannel>,
    /// NVMe controller
    ctrlr: SpdkNvmeController,
    /// name of the controller
    name: String,
    /// namespaces associated with this controller
    ns: Arc<NvmeNamespace>,
    /// TODO
    prchk_flags: u32,
    /// Private instance of the block device backed by the NVMe namespace.
    block_device: Box<dyn BlockDevice>,
    /// TODO
    block_len: u64,
}
/// Context for reset operation.
struct ResetCtx {
    cb: IoCompletionCallback,
    cb_arg: IoCompletionCallbackArg,
    device: Box<dyn BlockDevice>,
}

impl NvmeDeviceHandle {
    fn create_handle(
        name: &str,
        id: u64,
        ctrlr: SpdkNvmeController,
        ns: Arc<NvmeNamespace>,
        prchk_flags: u32,
    ) -> Result<NvmeDeviceHandle, CoreError> {
        // Obtain SPDK I/O channel for NVMe controller.
        let io_channel = NvmeControllerIoChannel::from_null_checked(unsafe {
            spdk_get_io_channel(id as *mut c_void)
        })
        .ok_or(CoreError::GetIoChannel {
            name: name.to_string(),
        })?;

        Ok(NvmeDeviceHandle {
            name: name.to_string(),
            io_channel: ManuallyDrop::new(io_channel),
            ctrlr,
            block_device: Self::get_nvme_device(name, &ns),
            block_len: ns.block_len(),
            prchk_flags,
            ns,
        })
    }

    // Create and perform a synchronous connect.
    pub fn create(
        name: &str,
        id: u64,
        ctrlr: SpdkNvmeController,
        ns: Arc<NvmeNamespace>,
        prchk_flags: u32,
    ) -> Result<NvmeDeviceHandle, CoreError> {
        let mut handle = Self::create_handle(name, id, ctrlr, ns, prchk_flags)?;
        handle.connect_sync();
        Ok(handle)
    }

    // Create and perform an asynchronous connect.
    pub async fn create_async(
        name: &str,
        id: u64,
        ctrlr: SpdkNvmeController,
        ns: Arc<NvmeNamespace>,
        prchk_flags: u32,
    ) -> Result<NvmeDeviceHandle, CoreError> {
        let mut handle = Self::create_handle(name, id, ctrlr, ns, prchk_flags)?;

        #[cfg(feature = "spdk-async-qpair-connect")]
        handle.connect_async().await?;

        #[cfg(not(feature = "spdk-async-qpair-connect"))]
        handle.connect_sync();

        Ok(handle)
    }

    /// TODO:
    fn connect_sync(&mut self) {
        let inner = NvmeIoChannel::inner_from_channel(self.io_channel.as_ptr());

        match inner.qpair_mut() {
            Some(q) => {
                q.connect();
            }
            None => warn!("No I/O qpair in NvmeDeviceHandle, can't connect()"),
        };
    }

    #[cfg(feature = "spdk-async-qpair-connect")]
    /// TODO:
    pub(crate) async fn connect_async(&mut self) -> Result<(), CoreError> {
        let inner = NvmeIoChannel::inner_from_channel(self.io_channel.as_ptr());

        match inner.qpair_mut() {
            Some(q) => q.connect_async().await,
            None => {
                error!("No I/O qpair in NvmeDeviceHandle, can't connect()");
                Err(CoreError::InvalidNvmeDeviceHandle {
                    msg: "No active I/O qpair".to_string(),
                })
            }
        }
    }

    fn get_nvme_device(
        name: &str,
        ns: &Arc<NvmeNamespace>,
    ) -> Box<dyn BlockDevice> {
        Box::new(NvmeBlockDevice::from_ns(name, ns.clone()))
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

        // TODO: Optimize for ^2.
        (alignment == 0, offset_blocks, num_blocks)
    }
}

extern "C" fn nvme_admin_passthru_done(
    ctx: *mut c_void,
    cpl: *const spdk_nvme_cpl,
) {
    // In case of admin command failures error code is transferred via cdw0.
    let status = unsafe { (*cpl).cdw0 };

    trace!(
        succeeded = nvme_cpl_succeeded(cpl),
        status,
        "Admin passthrough completed"
    );

    let res: u32 = if nvme_cpl_succeeded(cpl) {
        0
    } else {
        // Handle situations when no error code is passed explicitly and assume
        // EIO.
        if status == 0 {
            libc::EIO as u32
        } else {
            status
        }
    };

    Reactors::master().send_future(async move {
        done_cb(ctx, res);
    });
}

extern "C" fn nvme_queued_reset_sgl(ctx: *mut c_void, sgl_offset: u32) {
    let nvme_io_ctx = unsafe { &mut *(ctx as *mut NvmeIoCtx) };

    nvme_io_ctx.iov_offset = sgl_offset as u64;
    nvme_io_ctx.iovpos = 0;

    while nvme_io_ctx.iovpos < nvme_io_ctx.iovcnt {
        unsafe {
            let iov = nvme_io_ctx.iov.add(nvme_io_ctx.iovpos as usize);
            if nvme_io_ctx.iov_offset < (*iov).iov_len {
                break;
            }

            nvme_io_ctx.iov_offset -= (*iov).iov_len;
        }

        nvme_io_ctx.iovpos += 1;
    }
}

extern "C" fn nvme_queued_next_sge(
    ctx: *mut c_void,
    address: *mut *mut c_void,
    length: *mut u32,
) -> i32 {
    let nvme_io_ctx = unsafe { &mut *(ctx as *mut NvmeIoCtx) };

    assert!(nvme_io_ctx.iovpos < nvme_io_ctx.iovcnt);

    unsafe {
        let iov = nvme_io_ctx.iov.add(nvme_io_ctx.iovpos as usize);

        let mut a = (*iov).iov_base as u64;
        *length = (*iov).iov_len as u32;

        if nvme_io_ctx.iov_offset > 0 {
            assert!(nvme_io_ctx.iov_offset <= (*iov).iov_len);
            a += nvme_io_ctx.iov_offset;
            *length -= nvme_io_ctx.iov_offset as u32;
        }

        nvme_io_ctx.iov_offset += *length as u64;
        if nvme_io_ctx.iov_offset == (*iov).iov_len {
            nvme_io_ctx.iovpos += 1;
            nvme_io_ctx.iov_offset = 0;
        }

        *(address as *mut u64) = a;
    }

    0
}

/// Notify the caller and deallocate Nvme IO context.
#[inline]
fn complete_nvme_command(ctx: *mut NvmeIoCtx, cpl: *const spdk_nvme_cpl) {
    let io_ctx = unsafe { &mut *ctx };
    let op_succeeded = nvme_cpl_succeeded(cpl);
    let inner = NvmeIoChannel::inner_from_channel(io_ctx.channel);

    // Update I/O statistics in case the operation succeeded.
    if op_succeeded {
        let stats_controller = inner.get_io_stats_controller();
        stats_controller.account_block_io(io_ctx.op, 1, io_ctx.num_blocks);
    }

    // Adjust the number of active I/O operations in case operation is
    // accountable.
    match io_ctx.op {
        IoType::Flush => {}
        _ => inner.discard_io(),
    }

    // Invoke caller's callback and free I/O context.
    let status = if op_succeeded {
        IoCompletionStatus::Success
    } else {
        IoCompletionStatus::from(NvmeStatus::from(cpl))
    };

    #[cfg(feature = "fault-injection")]
    let status = inject_completion_error(&io_ctx.inj_op, status);

    (io_ctx.cb)(&*inner.device, status, io_ctx.cb_arg);

    free_nvme_io_ctx(ctx);
}

/// Completion handler for vectored write requests.
extern "C" fn nvme_writev_done(ctx: *mut c_void, cpl: *const spdk_nvme_cpl) {
    let nvme_io_ctx = ctx as *mut NvmeIoCtx;

    trace!("NVMe writev I/O completed !");

    // Check if operation successfully completed.
    if nvme_cpl_is_pi_error(cpl) {
        error!("writev completed with PI error");
    }

    complete_nvme_command(nvme_io_ctx, cpl);
}

/// I/O completion handler for all read requests (vectored/non-vectored)
/// and non-vectored write requests.
extern "C" fn nvme_io_done(ctx: *mut c_void, cpl: *const spdk_nvme_cpl) {
    let nvme_io_ctx = ctx as *mut NvmeIoCtx;

    // Check if operation successfully completed.
    if nvme_cpl_is_pi_error(cpl) {
        error!("I/O completed with PI error");
    }

    complete_nvme_command(nvme_io_ctx, cpl);
}

extern "C" fn nvme_async_io_completion(
    ctx: *mut c_void,
    cpl: *const spdk_nvme_cpl,
) {
    done_cb(ctx, NvmeStatus::from(cpl));
}

extern "C" fn nvme_unmap_completion(
    ctx: *mut c_void,
    cpl: *const spdk_nvme_cpl,
) {
    let nvme_io_ctx = ctx as *mut NvmeIoCtx;
    trace!("Async unmap completed");
    complete_nvme_command(nvme_io_ctx, cpl);
}

extern "C" fn nvme_flush_completion(
    ctx: *mut c_void,
    cpl: *const spdk_nvme_cpl,
) {
    let nvme_io_ctx = ctx as *mut NvmeIoCtx;
    trace!("Async flush completed");
    complete_nvme_command(nvme_io_ctx, cpl);
}

fn check_io_args(
    op: IoType,
    iovs: &[IoVec],
    offset_blocks: u64,
    num_blocks: u64,
) -> Result<(), CoreError> {
    // Make sure I/O structures look sane.
    // As of now, we assume that I/O vector is fully prepared by the caller.
    if iovs.is_empty() {
        error!("empty I/O vector");
        return Err(io_type_to_err(
            op,
            libc::EINVAL,
            offset_blocks,
            num_blocks,
        ));
    }

    if !iovs[0].is_initialized() {
        error!("I/O vector is not initialized");
        return Err(io_type_to_err(
            op,
            libc::EINVAL,
            offset_blocks,
            num_blocks,
        ));
    }

    Ok(())
}

fn io_type_to_err(
    op: IoType,
    errno: i32,
    offset_blocks: u64,
    num_blocks: u64,
) -> CoreError {
    assert!(errno > 0, "Errno code must be provided");
    let source = Errno::from_i32(errno);

    match op {
        IoType::Read => CoreError::ReadDispatch {
            source,
            offset: offset_blocks,
            len: num_blocks,
        },
        IoType::Write => CoreError::WriteDispatch {
            source,
            offset: offset_blocks,
            len: num_blocks,
        },
        IoType::Compare => CoreError::CompareDispatch {
            source,
            offset: offset_blocks,
            len: num_blocks,
        },
        IoType::Unmap => CoreError::UnmapDispatch {
            source,
            offset: offset_blocks,
            len: num_blocks,
        },
        IoType::NvmeIo => CoreError::NvmeIoPassthruDispatch {
            source,
            opcode: 0xff,
        },
        _ => {
            warn!("Unsupported I/O operation: {:?}", op);
            CoreError::NotSupported {
                source,
            }
        }
    }
}

/// Initialize memory pool for allocating NVMe controller I/O contexts.
/// This must be called before the first I/O operations take place.
pub fn nvme_io_ctx_pool_init(size: u64) {
    NVME_IOCTX_POOL.get_or_init(|| {
        MemoryPool::<NvmeIoCtx>::create("nvme_ctrl_io_ctx", size)
            .expect("Failed to create memory pool [nvme_ctrl_io_ctx] for NVMe controller I/O contexts")
    });
}

/// Allocate an NVMe controller I/O context from the pool.
fn alloc_nvme_io_ctx(
    op: IoType,
    ctx: NvmeIoCtx,
    offset_blocks: u64,
    num_blocks: u64,
) -> Result<*mut NvmeIoCtx, CoreError> {
    let pool = NVME_IOCTX_POOL.get().unwrap();
    pool.get(ctx).ok_or_else(|| {
        io_type_to_err(op, libc::ENOMEM, offset_blocks, num_blocks)
    })
}

/// Release the memory used by the NVMe controller I/O context back to the pool.
fn free_nvme_io_ctx(ctx: *mut NvmeIoCtx) {
    let pool = NVME_IOCTX_POOL.get().unwrap();
    pool.put(ctx);
}

/// Check whether channel is suitable for serving I/O.
fn check_channel_for_io(
    op: IoType,
    inner: &NvmeIoChannelInner,
    offset_blocks: u64,
    num_blocks: u64,
) -> Result<(), CoreError> {
    let mut errno = 0;

    // Check against concurrent controller reset, which results in valid
    // I/O channel but deactivated I/O pair.
    if inner.qpair().is_none() {
        errno = libc::ENODEV;
    }

    if errno == 0 {
        Ok(())
    } else {
        Err(io_type_to_err(op, errno, offset_blocks, num_blocks))
    }
}

/// Handler for controller reset operation.
/// Serves as a proxy layer between NVMe controller and block device layer
/// (represented by device I/O handle): we need to pass block device
/// reference to user callback for handle-based operation.
fn reset_callback(success: bool, arg: *mut c_void) {
    let ctx = unsafe { Box::from_raw(arg as *mut ResetCtx) };

    // Translate success/failure into NVMe errors.
    let status = if success {
        IoCompletionStatus::Success
    } else {
        IoCompletionStatus::NvmeError(NvmeStatus::Generic(
            SPDK_NVME_SC_INTERNAL_DEVICE_ERROR,
        ))
    };

    (ctx.cb)(&*ctx.device, status, ctx.cb_arg);
}

#[async_trait(?Send)]
impl BlockDeviceHandle for NvmeDeviceHandle {
    fn get_device(&self) -> &dyn BlockDevice {
        &*self.block_device
    }

    fn dma_malloc(&self, size: u64) -> Result<DmaBuf, DmaError> {
        DmaBuf::new(size, self.ns.alignment())
    }

    async fn read_at(
        &self,
        offset: u64,
        buffer: &mut DmaBuf,
    ) -> Result<u64, CoreError> {
        let (valid, offset_blocks, num_blocks) =
            self.bytes_to_blocks(offset, buffer.len());

        trace!(
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

        // Make sure channel allows I/O.
        check_channel_for_io(IoType::Read, inner, offset_blocks, num_blocks)?;

        let (s, r) = oneshot::channel::<NvmeStatus>();

        let rc = unsafe {
            spdk_nvme_ns_cmd_read(
                self.ns.as_ptr(),
                inner.qpair_ptr(),
                buffer.as_mut_ptr(),
                offset_blocks,
                num_blocks as u32,
                Some(nvme_async_io_completion),
                cb_arg(s),
                self.prchk_flags,
            )
        };

        if rc != 0 && rc != -libc::ENOMEM {
            error!("{} read failed: rc = {}", self.name, rc);
            return Err(CoreError::ReadDispatch {
                source: Errno::from_i32(-rc),
                offset,
                len: buffer.len(),
            });
        }

        inner.account_io();
        let ret = match r.await.expect("Failed awaiting at read_at()") {
            NvmeStatus::SUCCESS => {
                inner.get_io_stats_controller().account_block_io(
                    IoType::Read,
                    1,
                    num_blocks,
                );
                Ok(buffer.len())
            }
            NvmeStatus::UNWRITTEN_BLOCK => {
                Err(CoreError::ReadingUnallocatedBlock {
                    offset,
                    len: buffer.len(),
                })
            }
            status => Err(CoreError::ReadFailed {
                status: IoCompletionStatus::NvmeError(status),
                offset,
                len: buffer.len(),
            }),
        };
        inner.discard_io();
        ret
    }

    async fn write_at(
        &self,
        offset: u64,
        buffer: &DmaBuf,
    ) -> Result<u64, CoreError> {
        let (valid, offset_blocks, num_blocks) =
            self.bytes_to_blocks(offset, buffer.len());

        trace!(
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

        // Make sure channel allows I/O.
        check_channel_for_io(IoType::Write, inner, offset_blocks, num_blocks)?;

        let (s, r) = oneshot::channel::<NvmeStatus>();

        let rc = unsafe {
            spdk_nvme_ns_cmd_write(
                self.ns.as_ptr(),
                inner.qpair_ptr(),
                buffer.as_ptr() as *mut _,
                offset_blocks,
                num_blocks as u32,
                Some(nvme_async_io_completion),
                cb_arg(s),
                self.prchk_flags,
            )
        };

        if rc != 0 && rc != -libc::ENOMEM {
            error!("{} write failed: rc = {}", self.name, rc);
            return Err(CoreError::WriteDispatch {
                source: Errno::from_i32(-rc),
                offset,
                len: buffer.len(),
            });
        }

        inner.account_io();
        let ret = match r.await.expect("Failed awaiting at write_at()") {
            NvmeStatus::SUCCESS => {
                inner.get_io_stats_controller().account_block_io(
                    IoType::Write,
                    1,
                    num_blocks,
                );
                Ok(buffer.len())
            }
            status => Err(CoreError::WriteFailed {
                status: IoCompletionStatus::NvmeError(status),
                offset,
                len: buffer.len(),
            }),
        };
        inner.discard_io();
        ret
    }

    // bdev_nvme_get_buf_cb
    fn readv_blocks(
        &self,
        iovs: &mut [IoVec],
        offset_blocks: u64,
        num_blocks: u64,
        opts: ReadOptions,
        cb: IoCompletionCallback,
        cb_arg: IoCompletionCallbackArg,
    ) -> Result<(), CoreError> {
        check_io_args(IoType::Read, iovs, offset_blocks, num_blocks)?;

        // Get read flags.
        let flags = match opts {
            ReadOptions::None => self.prchk_flags,
            ReadOptions::UnwrittenFail => {
                self.prchk_flags | SPDK_NVME_IO_FLAGS_UNWRITTEN_READ_FAIL
            }
        };

        let channel = self.io_channel.as_ptr();
        let inner = NvmeIoChannel::inner_from_channel(channel);

        // Make sure channel allows I/O.
        check_channel_for_io(IoType::Read, inner, offset_blocks, num_blocks)?;

        let bio = alloc_nvme_io_ctx(
            IoType::Read,
            NvmeIoCtx {
                cb,
                cb_arg,
                iov: iovs.as_io_vec_mut_ptr(),
                iovcnt: iovs.len() as u64,
                iovpos: 0,
                iov_offset: 0,
                channel,
                op: IoType::Read,
                num_blocks,
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
        inject_submission_error(unsafe { &(*bio).inj_op })?;

        let rc = if iovs.len() == 1 {
            unsafe {
                spdk_nvme_ns_cmd_read(
                    self.ns.as_ptr(),
                    inner.qpair_ptr(),
                    iovs[0].as_mut_ptr(),
                    offset_blocks,
                    num_blocks as u32,
                    Some(nvme_io_done),
                    bio as *mut c_void,
                    flags,
                )
            }
        } else {
            unsafe {
                spdk_nvme_ns_cmd_readv(
                    self.ns.as_ptr(),
                    inner.qpair_ptr(),
                    offset_blocks,
                    num_blocks as u32,
                    Some(nvme_io_done),
                    bio as *mut c_void,
                    flags,
                    Some(nvme_queued_reset_sgl),
                    Some(nvme_queued_next_sge),
                )
            }
        };

        if rc < 0 {
            Err(CoreError::ReadDispatch {
                source: Errno::from_i32(-rc),
                offset: offset_blocks,
                len: num_blocks,
            })
        } else {
            inner.account_io();
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
        check_io_args(IoType::Write, iovs, offset_blocks, num_blocks)?;

        let channel = self.io_channel.as_ptr();
        let inner = NvmeIoChannel::inner_from_channel(channel);

        // Make sure channel allows I/O.
        check_channel_for_io(IoType::Write, inner, offset_blocks, num_blocks)?;

        let bio = alloc_nvme_io_ctx(
            IoType::Write,
            NvmeIoCtx {
                cb,
                cb_arg,
                iov: iovs.as_ptr() as *mut _,
                iovcnt: iovs.len() as u64,
                iovpos: 0,
                iov_offset: 0,
                channel,
                op: IoType::Write,
                num_blocks,
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
        inject_submission_error(unsafe { &(*bio).inj_op })?;

        let rc = if iovs.len() == 1 {
            unsafe {
                spdk_nvme_ns_cmd_write(
                    self.ns.as_ptr(),
                    inner.qpair_ptr(),
                    iovs[0].as_ptr() as *mut _,
                    offset_blocks,
                    num_blocks as u32,
                    Some(nvme_io_done),
                    bio as *mut c_void,
                    self.prchk_flags,
                )
            }
        } else {
            unsafe {
                spdk_nvme_ns_cmd_writev(
                    self.ns.as_ptr(),
                    inner.qpair_ptr(),
                    offset_blocks,
                    num_blocks as u32,
                    Some(nvme_writev_done),
                    bio as *mut c_void,
                    self.prchk_flags,
                    Some(nvme_queued_reset_sgl),
                    Some(nvme_queued_next_sge),
                )
            }
        };

        if rc < 0 {
            Err(CoreError::WriteDispatch {
                source: Errno::from_i32(-rc),
                offset: offset_blocks,
                len: num_blocks,
            })
        } else {
            inner.account_io();
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
        check_io_args(IoType::Compare, iovs, offset_blocks, num_blocks)?;

        let channel = self.io_channel.as_ptr();
        let inner = NvmeIoChannel::inner_from_channel(channel);

        // Make sure channel allows I/O.
        check_channel_for_io(
            IoType::Compare,
            inner,
            offset_blocks,
            num_blocks,
        )?;

        let bio = alloc_nvme_io_ctx(
            IoType::Compare,
            NvmeIoCtx {
                cb,
                cb_arg,
                iov: iovs.as_ptr() as *mut _,
                iovcnt: iovs.len() as u64,
                iovpos: 0,
                iov_offset: 0,
                channel,
                op: IoType::Compare,
                num_blocks,
                #[cfg(feature = "fault-injection")]
                inj_op: InjectIoCtx::new(FaultDomain::BlockDevice),
            },
            offset_blocks,
            num_blocks,
        )?;

        let rc = if iovs.len() == 1 {
            unsafe {
                spdk_nvme_ns_cmd_compare(
                    self.ns.as_ptr(),
                    inner.qpair_ptr(),
                    iovs[0].as_ptr() as *mut _,
                    offset_blocks,
                    num_blocks as u32,
                    Some(nvme_io_done),
                    bio as *mut c_void,
                    self.prchk_flags,
                )
            }
        } else {
            unsafe {
                spdk_nvme_ns_cmd_comparev(
                    self.ns.as_ptr(),
                    inner.qpair_ptr(),
                    offset_blocks,
                    num_blocks as u32,
                    Some(nvme_io_done),
                    bio as *mut c_void,
                    self.prchk_flags,
                    Some(nvme_queued_reset_sgl),
                    Some(nvme_queued_next_sge),
                )
            }
        };

        if rc < 0 {
            Err(CoreError::CompareDispatch {
                source: Errno::from_i32(-rc),
                offset: offset_blocks,
                len: num_blocks,
            })
        } else {
            inner.account_io();
            Ok(())
        }
    }

    fn reset(
        &self,
        cb: IoCompletionCallback,
        cb_arg: IoCompletionCallbackArg,
    ) -> Result<(), CoreError> {
        let controller = NVME_CONTROLLERS.lookup_by_name(&self.name).ok_or(
            CoreError::BdevNotFound {
                name: self.name.to_string(),
            },
        )?;
        let mut controller = controller.lock();

        let ctx = Box::new(ResetCtx {
            cb,
            cb_arg,
            device: Self::get_nvme_device(&self.name, &self.ns),
        });

        // Schedule asynchronous controller reset.
        controller.reset(
            reset_callback,
            Box::into_raw(ctx) as *mut c_void,
            false,
        )
    }

    fn flush_io(
        &self,
        cb: IoCompletionCallback,
        cb_arg: IoCompletionCallbackArg,
    ) -> Result<(), CoreError> {
        let channel = self.io_channel.as_ptr();
        let inner = NvmeIoChannel::inner_from_channel(channel);
        let num_blocks = self.block_device.num_blocks();

        // Make sure channel allows I/O.
        check_channel_for_io(IoType::Flush, inner, 0, num_blocks)?;

        let bio = alloc_nvme_io_ctx(
            IoType::Flush,
            NvmeIoCtx {
                cb,
                cb_arg,
                iov: std::ptr::null_mut(), // No I/O vec involved.
                iovcnt: 0,
                iovpos: 0,
                iov_offset: 0,
                channel,
                op: IoType::Flush,
                num_blocks,
                #[cfg(feature = "fault-injection")]
                inj_op: InjectIoCtx::new(FaultDomain::BlockDevice),
            },
            0,
            num_blocks, // Flush all device blocks.
        )?;

        // Setup range that describes the remaining blocks and schedule unmap.
        let rc = unsafe {
            spdk_nvme_ns_cmd_flush(
                self.ns.as_ptr(),
                inner.qpair_ptr(),
                Some(nvme_flush_completion),
                bio as *mut c_void,
            )
        };

        if rc < 0 {
            Err(CoreError::FlushDispatch {
                source: Errno::from_i32(-rc),
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
        let num_ranges =
            (num_blocks + SPDK_NVME_DATASET_MANAGEMENT_RANGE_MAX_BLOCKS - 1)
                / SPDK_NVME_DATASET_MANAGEMENT_RANGE_MAX_BLOCKS;

        if num_ranges > SPDK_NVME_DATASET_MANAGEMENT_MAX_RANGES {
            return Err(CoreError::UnmapDispatch {
                source: Errno::EINVAL,
                offset: offset_blocks,
                len: num_blocks,
            });
        }

        let channel = self.io_channel.as_ptr();
        let inner = NvmeIoChannel::inner_from_channel(channel);

        // Make sure channel allows I/O.
        check_channel_for_io(IoType::Unmap, inner, offset_blocks, num_blocks)?;

        let bio = alloc_nvme_io_ctx(
            IoType::Unmap,
            NvmeIoCtx {
                cb,
                cb_arg,
                iov: std::ptr::null_mut(), // No I/O vec involved.
                iovcnt: 0,
                iovpos: 0,
                iov_offset: 0,
                channel,
                op: IoType::Unmap,
                num_blocks,
                #[cfg(feature = "fault-injection")]
                inj_op: InjectIoCtx::new(FaultDomain::BlockDevice),
            },
            offset_blocks,
            num_blocks,
        )?;

        let l = Layout::array::<spdk_nvme_dsm_range>(
            SPDK_NVME_DATASET_MANAGEMENT_MAX_RANGES as usize,
        )
        .unwrap();
        let dsm_ranges =
            unsafe { std::alloc::alloc(l) as *mut spdk_nvme_dsm_range };

        let mut remaining = num_blocks;
        let mut offset = offset_blocks;
        let mut range_id: usize = 0;

        // Fill max-size ranges until the remaining blocks fit into one range.
        while remaining > SPDK_NVME_DATASET_MANAGEMENT_RANGE_MAX_BLOCKS {
            unsafe {
                let mut range = spdk_nvme_dsm_range::default();

                range.attributes.raw = 0;
                range.length =
                    SPDK_NVME_DATASET_MANAGEMENT_RANGE_MAX_BLOCKS as u32;
                range.starting_lba = offset;

                *dsm_ranges.add(range_id) = range;
            }

            offset += SPDK_NVME_DATASET_MANAGEMENT_RANGE_MAX_BLOCKS;
            remaining -= SPDK_NVME_DATASET_MANAGEMENT_RANGE_MAX_BLOCKS;
            range_id += 1;
        }

        // Setup range that describes the remaining blocks and schedule unmap.
        let rc = unsafe {
            let mut range = spdk_nvme_dsm_range::default();

            range.attributes.raw = 0;
            range.length = remaining as u32;
            range.starting_lba = offset;

            *dsm_ranges.add(range_id) = range;

            spdk_nvme_ns_cmd_dataset_management(
                self.ns.as_ptr(),
                inner.qpair_ptr(),
                utils::NvmeDsmAttribute::Deallocate as u32,
                dsm_ranges,
                num_ranges as u16,
                Some(nvme_unmap_completion),
                bio as *mut c_void,
            )
        };

        if rc < 0 {
            Err(CoreError::UnmapDispatch {
                source: Errno::from_i32(-rc),
                offset: offset_blocks,
                len: num_blocks,
            })
        } else {
            inner.account_io();
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
        let channel = self.io_channel.as_ptr();
        let inner = NvmeIoChannel::inner_from_channel(channel);

        // Make sure channel allows I/O
        check_channel_for_io(
            IoType::WriteZeros,
            inner,
            offset_blocks,
            num_blocks,
        )?;

        let bio = alloc_nvme_io_ctx(
            IoType::WriteZeros,
            NvmeIoCtx {
                cb,
                cb_arg,
                iov: std::ptr::null_mut(), // No I/O vec involved.
                iovcnt: 0,
                iovpos: 0,
                iov_offset: 0,
                channel,
                op: IoType::WriteZeros,
                num_blocks,
                #[cfg(feature = "fault-injection")]
                inj_op: InjectIoCtx::new(FaultDomain::BlockDevice),
            },
            offset_blocks,
            num_blocks,
        )?;

        let rc = unsafe {
            spdk_nvme_ns_cmd_write_zeroes(
                self.ns.as_ptr(),
                inner.qpair_ptr(),
                offset_blocks,
                num_blocks as u32,
                Some(nvme_io_done),
                bio as *mut c_void,
                self.prchk_flags,
            )
        };

        if rc < 0 {
            Err(CoreError::WriteZeroesDispatch {
                source: Errno::from_i32(-rc),
                offset: offset_blocks,
                len: num_blocks,
            })
        } else {
            inner.account_io();
            Ok(())
        }
    }

    async fn create_snapshot(
        &self,
        snapshot: SnapshotParams,
    ) -> Result<u64, CoreError> {
        let mut cmd = spdk_nvme_cmd::default();
        cmd.set_opc(nvme_admin_opc::CREATE_SNAPSHOT.into());
        let now = subsys::set_snapshot_time(&mut cmd);

        let msg = NvmeSnapshotMessage::V1(NvmeSnapshotMessageV1::new(snapshot));
        let encoded_msg = bincode::serialize(&msg)
            .expect("Failed to serialize snapshot message");

        let mut payload =
            self.dma_malloc(encoded_msg.len() as u64).map_err(|_| {
                CoreError::DmaAllocationFailed {
                    size: encoded_msg.len() as u64,
                }
            })?;

        payload
            .as_mut_slice()
            .clone_from_slice(encoded_msg.as_slice());
        self.nvme_admin(&cmd, Some(&mut payload)).await?;

        Ok(now)
    }

    async fn nvme_admin_custom(&self, opcode: u8) -> Result<(), CoreError> {
        let mut cmd = spdk_nvme_cmd::default();
        cmd.set_opc(opcode.into());
        self.nvme_admin(&cmd, None).await
    }

    async fn nvme_admin(
        &self,
        cmd: &spdk_nvme_cmd,
        buffer: Option<&mut DmaBuf>,
    ) -> Result<(), CoreError> {
        let mut pcmd = *cmd; // Make a private mutable copy of the command.

        let inner = NvmeIoChannel::inner_from_channel(self.io_channel.as_ptr());

        // Make sure channel allows I/O.
        if inner.qpair().is_none() {
            return Err(CoreError::NvmeAdminDispatch {
                source: Errno::ENODEV,
                opcode: cmd.opc(),
            });
        }

        let (ptr, size) = match buffer {
            Some(buf) => (buf.as_mut_ptr(), buf.len()),
            None => (std::ptr::null_mut(), 0),
        };

        let (s, r) = oneshot::channel::<u32>();

        unsafe {
            spdk_nvme_ctrlr_cmd_admin_raw(
                self.ctrlr.as_ptr(),
                &mut pcmd,
                ptr,
                size as u32,
                Some(nvme_admin_passthru_done),
                cb_arg(s),
            )
        }
        .to_result(|e| CoreError::NvmeAdminDispatch {
            source: Errno::from_i32(e),
            opcode: cmd.opc(),
        })?;

        inner.account_io();
        let status = r.await.expect("Failed awaiting NVMe Admin command I/O");
        inner.discard_io();

        match status {
            0 => {
                debug!("nvme_admin() done");
                Ok(())
            }
            _ => {
                error!("nvme_admin() failed, errno={status}");
                Err(CoreError::NvmeAdminFailed {
                    opcode: (*cmd).opc(),
                    source: Errno::from_i32(status as i32),
                })
            }
        }
    }

    async fn nvme_identify_ctrlr(&self) -> Result<DmaBuf, CoreError> {
        let mut buf = DmaBuf::new(4096, 8).map_err(|_e| {
            CoreError::DmaAllocationFailed {
                size: 4096,
            }
        })?;

        let mut cmd = spdk_nvme_cmd::default();
        cmd.set_opc(nvme_admin_opc::IDENTIFY.into());
        cmd.nsid = 0xffffffff;
        // Controller Identifier
        unsafe { *nvme_cmd_cdw10_get(&mut cmd) = 1 };
        self.nvme_admin(&cmd, Some(&mut buf)).await?;
        Ok(buf)
    }

    /// NVMe Reservation Register
    /// cptpl: Change Persist Through Power Loss state
    async fn nvme_resv_register(
        &self,
        current_key: u64,
        new_key: u64,
        register_action: u8,
        cptpl: u8,
    ) -> Result<(), CoreError> {
        let mut cmd = spdk_nvme_cmd::default();
        cmd.set_opc(nvme_nvm_opcode::RESERVATION_REGISTER.into());
        cmd.nsid = 0x1;
        unsafe {
            cmd.__bindgen_anon_1
                .cdw10_bits
                .resv_register
                .set_rrega(register_action.into());
            cmd.__bindgen_anon_1
                .cdw10_bits
                .resv_register
                .set_cptpl(cptpl.into());
            if register_action == nvme_reservation_register_action::REPLACE_KEY
            {
                cmd.__bindgen_anon_1.cdw10_bits.resv_register.set_iekey(1);
            }
        }
        let mut buffer = self.dma_malloc(16).unwrap();
        let (ck, nk) = buffer.as_mut_slice().split_at_mut(8);
        ck.copy_from_slice(&current_key.to_le_bytes());
        nk.copy_from_slice(&new_key.to_le_bytes());
        self.io_passthru(&cmd, Some(&mut buffer)).await
    }

    /// NVMe Reservation Acquire
    async fn nvme_resv_acquire(
        &self,
        current_key: u64,
        preempt_key: u64,
        acquire_action: u8,
        resv_type: u8,
    ) -> Result<(), CoreError> {
        let mut cmd = spdk_nvme_cmd::default();
        cmd.set_opc(nvme_nvm_opcode::RESERVATION_ACQUIRE.into());
        cmd.nsid = 0x1;
        unsafe {
            cmd.__bindgen_anon_1
                .cdw10_bits
                .resv_acquire
                .set_racqa(acquire_action.into());
            cmd.__bindgen_anon_1
                .cdw10_bits
                .resv_acquire
                .set_rtype(resv_type.into());
        }
        let mut buffer = self.dma_malloc(16).unwrap();
        let (ck, pk) = buffer.as_mut_slice().split_at_mut(8);
        ck.copy_from_slice(&current_key.to_le_bytes());
        pk.copy_from_slice(&preempt_key.to_le_bytes());
        self.io_passthru(&cmd, Some(&mut buffer)).await
    }

    /// NVMe Reservation Release
    async fn nvme_resv_release(
        &self,
        current_key: u64,
        resv_type: u8,
        release_action: u8,
    ) -> Result<(), CoreError> {
        let mut cmd = spdk_nvme_cmd::default();
        cmd.set_opc(nvme_nvm_opcode::RESERVATION_RELEASE.into());
        cmd.nsid = 0x1;
        unsafe {
            cmd.__bindgen_anon_1
                .cdw10_bits
                .resv_acquire
                .set_racqa(release_action.into());
            cmd.__bindgen_anon_1
                .cdw10_bits
                .resv_acquire
                .set_rtype(resv_type.into());
        }
        let mut buffer = self.dma_malloc(16).unwrap();
        let (ck, _pk) = buffer.as_mut_slice().split_at_mut(8);
        ck.copy_from_slice(&current_key.to_le_bytes());
        self.io_passthru(&cmd, Some(&mut buffer)).await
    }

    /// NVMe Reservation Report
    /// cdw11: bit 0- Extended Data Structure
    async fn nvme_resv_report(
        &self,
        cdw11: u32,
        buffer: &mut DmaBuf,
    ) -> Result<(), CoreError> {
        let mut cmd = spdk_nvme_cmd::default();
        cmd.set_opc(nvme_nvm_opcode::RESERVATION_REPORT.into());
        cmd.nsid = 0x1;
        // Number of dwords to transfer
        cmd.__bindgen_anon_1.cdw10 = ((buffer.len() >> 2) - 1) as u32;
        cmd.__bindgen_anon_2.cdw11 = cdw11;
        self.io_passthru(&cmd, Some(buffer)).await
    }

    /// sends the specified NVMe IO Passthru command
    async fn io_passthru(
        &self,
        nvme_cmd: &spdk_nvme_cmd,
        buffer: Option<&mut DmaBuf>,
    ) -> Result<(), CoreError> {
        extern "C" fn nvme_io_passthru_done(
            ctx: *mut c_void,
            cpl: *const spdk_nvme_cpl,
        ) {
            debug!(
                "IO passthrough completed, succeeded={}",
                nvme_cpl_succeeded(cpl)
            );
            done_cb(ctx, nvme_cpl_succeeded(cpl));
        }

        let mut pcmd = *nvme_cmd; // Make a private mutable copy of the command.

        let inner = NvmeIoChannel::inner_from_channel(self.io_channel.as_ptr());

        // Make sure channel allows I/O.
        if inner.qpair().is_none() {
            return Err(CoreError::NvmeIoPassthruDispatch {
                source: Errno::ENODEV,
                opcode: nvme_cmd.opc(),
            });
        }

        let (ptr, size) = match buffer {
            Some(buf) => (buf.as_mut_ptr(), buf.len()),
            None => (std::ptr::null_mut(), 0),
        };

        let (s, r) = oneshot::channel::<bool>();

        unsafe {
            spdk_nvme_ctrlr_cmd_io_raw(
                self.ctrlr.as_ptr(),
                inner.qpair_ptr(),
                &mut pcmd,
                ptr,
                size as u32,
                Some(nvme_io_passthru_done),
                cb_arg(s),
            )
        }
        .to_result(|e| CoreError::NvmeIoPassthruDispatch {
            source: Errno::from_i32(e),
            opcode: nvme_cmd.opc(),
        })?;

        inner.account_io();
        let ret = if r.await.expect("Failed awaiting NVMe IO passthru command")
        {
            debug!("io_passthru() done");
            Ok(())
        } else {
            Err(CoreError::NvmeIoPassthruFailed {
                opcode: nvme_cmd.opc(),
            })
        };
        inner.discard_io();
        ret
    }

    /// Returns NVMe extended host identifier
    async fn host_id(&self) -> Result<[u8; 16], CoreError> {
        let controller = NVME_CONTROLLERS.lookup_by_name(&self.name).ok_or(
            CoreError::BdevNotFound {
                name: self.name.to_string(),
            },
        )?;
        let controller = controller.lock();
        let inner = controller.controller().ok_or(CoreError::BdevNotFound {
            name: self.name.to_string(),
        })?;
        let id = inner.ext_host_id();
        Ok(*id)
    }
}

impl Drop for NvmeDeviceHandle {
    fn drop(&mut self) {
        unsafe { ManuallyDrop::drop(&mut self.io_channel) }
    }
}
