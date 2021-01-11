use async_trait::async_trait;
use futures::channel::oneshot;
use nix::errno::Errno;
use once_cell::sync::OnceCell;
use std::{mem::ManuallyDrop, os::raw::c_void, ptr::NonNull, sync::Arc};

use crate::core::mempool::MemoryPool;

use crate::{
    bdev::{
        dev::nvmx::{
            channel::NvmeControllerIoChannel,
            utils::{nvme_cpl_is_pi_error, nvme_cpl_succeeded},
            NvmeBlockDevice,
            NvmeIoChannel,
            NvmeNamespace,
            NVME_CONTROLLERS,
        },
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
};

use spdk_sys::{
    self,
    iovec,
    spdk_get_io_channel,
    spdk_nvme_cpl,
    spdk_nvme_ctrlr,
    spdk_nvme_ctrlr_cmd_admin_raw,
    spdk_nvme_ns_cmd_read,
    spdk_nvme_ns_cmd_readv,
    spdk_nvme_ns_cmd_write,
    spdk_nvme_ns_cmd_writev,
};

use super::NvmeIoChannelInner;

/*
 * I/O context for NVMe controller I/O operation. Used as a placeholder for
 * storing user context and also private state of I/O operations, specific to
 * the controller.
 */
struct NvmeIoCtx {
    cb: IoCompletionCallback,
    cb_arg: *const c_void,
    iov: *mut iovec,
    iovcnt: u64,
    iovpos: u64,
    iov_offset: u64,
}

unsafe impl Send for NvmeIoCtx {}
unsafe impl Sync for NvmeIoCtx {}

// Size of the memory pool for NVMe I/O structures.
const IOCTX_POOL_SIZE: u64 = 64 * 1024 - 1;

// Memory pool for NVMe controller - specific I/O context, which is used
// in every user BIO-based I/O operation.
static IOCTX_POOL: OnceCell<MemoryPool<NvmeIoCtx>> = OnceCell::new();

/*
 * I/O handle for NVMe block device.
 */
pub struct NvmeDeviceHandle {
    io_channel: ManuallyDrop<NvmeControllerIoChannel>,
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
    ) -> Result<NvmeDeviceHandle, CoreError> {
        // Initialize memory pool for holding I/O context now, during the slow
        // path, to make sure it's available before the first I/O
        // oepration takes place.
        IOCTX_POOL.get_or_init(|| MemoryPool::<NvmeIoCtx>::create(
            "nvme_ctrl_io_ctx",
            IOCTX_POOL_SIZE
        ).expect("Failed to create memory pool for NVMe controller I/O contexts"));

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
            _num_blocks: ns.num_blocks(),
            block_len: ns.block_len(),
            _size_in_bytes: ns.size_in_bytes(),
            prchk_flags,
            ns,
        })
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
    debug!("Admin passthrough completed !");
    done_cb(ctx, nvme_cpl_succeeded(cpl));
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
fn complete_nvme_command(
    nvme_io_ctx: *mut NvmeIoCtx,
    cpl: *const spdk_nvme_cpl,
) {
    // Invoke caller's callback.
    unsafe {
        ((*nvme_io_ctx).cb)(nvme_cpl_succeeded(cpl), (*nvme_io_ctx).cb_arg);
    }

    let pool = IOCTX_POOL.get().unwrap();
    pool.put(nvme_io_ctx);
}

/// Completion handler for vectored write requests.
extern "C" fn nvme_writev_done(ctx: *mut c_void, cpl: *const spdk_nvme_cpl) {
    let nvme_io_ctx = ctx as *mut NvmeIoCtx;

    debug!("NVMe writev I/O completed !");

    // Check if operation successfully completed.
    if nvme_cpl_is_pi_error(cpl) {
        error!("readv completed with PI error");
    }

    complete_nvme_command(nvme_io_ctx, cpl);
}

/// I/O completion handler for all read requests (vectored/non-vectored)
/// and non-vectored write requests.
extern "C" fn nvme_io_done(ctx: *mut c_void, cpl: *const spdk_nvme_cpl) {
    let nvme_io_ctx = ctx as *mut NvmeIoCtx;

    debug!("NVMe I/O completed !");

    // Check if operation successfully completed.
    if nvme_cpl_is_pi_error(cpl) {
        error!("readv completed with PI error");
    }

    complete_nvme_command(nvme_io_ctx, cpl);
}

extern "C" fn nvme_async_io_completion(
    ctx: *mut c_void,
    cpl: *const spdk_nvme_cpl,
) {
    debug!("Async NVMe I/O completed !");
    done_cb(ctx, nvme_cpl_succeeded(cpl));
}

fn check_io_args(
    iov: *mut iovec,
    iovcnt: i32,
    offset_blocks: u64,
    num_blocks: u64,
) -> Result<(), CoreError> {
    // Make sure I/O structures look sane.
    // As of now, we assume that I/O vector is fully prepared by the caller.
    if iovcnt <= 0 {
        error!("insufficient number of elements in I/O vector: {}", iovcnt);
        return Err(CoreError::ReadDispatch {
            source: Errno::EINVAL,
            offset: offset_blocks,
            len: num_blocks,
        });
    }
    unsafe {
        if (*iov).iov_base.is_null() {
            error!("I/O vector is not initialized");

            return Err(CoreError::ReadDispatch {
                source: Errno::EINVAL,
                offset: offset_blocks,
                len: num_blocks,
            });
        }
    }
    Ok(())
}

fn alloc_nvme_io_ctx(
    ctx: NvmeIoCtx,
    offset_blocks: u64,
    num_blocks: u64,
) -> Result<*mut NvmeIoCtx, CoreError> {
    let pool = IOCTX_POOL.get().unwrap();

    if let Some(c) = pool.get(ctx) {
        Ok(c)
    } else {
        Err(CoreError::ReadDispatch {
            source: Errno::ENOMEM,
            offset: offset_blocks,
            len: num_blocks,
        })
    }
}

enum IoType {
    READ,
    WRITE,
}

/// Check whether channel is suitable for serving I/O.
fn check_channel_for_rw_io(
    op: IoType,
    inner: &NvmeIoChannelInner,
    offset_blocks: u64,
    num_blocks: u64,
) -> Result<(), CoreError> {
    let mut errno = 0;

    // Check against concurrent controller reset, which results in valid
    // I/O channel but deactivated I/O pair.
    if inner.qpair.is_null() {
        errno = libc::EBUSY
    }

    if errno == 0 {
        Ok(())
    } else {
        match op {
            IoType::READ => Err(CoreError::ReadDispatch {
                source: Errno::from_i32(errno),
                offset: offset_blocks,
                len: num_blocks,
            }),
            IoType::WRITE => Err(CoreError::WriteDispatch {
                source: Errno::from_i32(errno),
                offset: offset_blocks,
                len: num_blocks,
            }),
        }
    }
}

#[async_trait(?Send)]
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
        self.nvme_admin(&cmd, Some(&mut buf)).await?;
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

        // Make sure channel allows I/O.
        check_channel_for_rw_io(
            IoType::READ,
            inner,
            offset_blocks,
            num_blocks,
        )?;

        let (s, r) = oneshot::channel::<bool>();

        let rc = unsafe {
            spdk_nvme_ns_cmd_read(
                self.ns.as_ptr(),
                inner.qpair,
                **buffer,
                offset_blocks,
                num_blocks as u32,
                Some(nvme_async_io_completion),
                cb_arg(s),
                self.prchk_flags,
            )
        };

        if rc != 0 && rc != -libc::ENOMEM {
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

        // Make sure channel allows I/O.
        check_channel_for_rw_io(
            IoType::WRITE,
            inner,
            offset_blocks,
            num_blocks,
        )?;

        let (s, r) = oneshot::channel::<bool>();

        let rc = unsafe {
            spdk_nvme_ns_cmd_write(
                self.ns.as_ptr(),
                inner.qpair,
                **buffer,
                offset_blocks,
                num_blocks as u32,
                Some(nvme_async_io_completion),
                cb_arg(s),
                self.prchk_flags,
            )
        };

        if rc != 0 && rc != -libc::ENOMEM {
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

    // bdev_nvme_get_buf_cb
    fn readv_blocks(
        &self,
        iov: *mut iovec,
        iovcnt: i32,
        offset_blocks: u64,
        num_blocks: u64,
        cb: IoCompletionCallback,
        cb_arg: *const c_void,
    ) -> Result<(), CoreError> {
        check_io_args(iov, iovcnt, offset_blocks, num_blocks)?;

        let inner = NvmeIoChannel::inner_from_channel(self.io_channel.as_ptr());

        // Make sure channel allows I/O.
        check_channel_for_rw_io(
            IoType::READ,
            inner,
            offset_blocks,
            num_blocks,
        )?;

        let bio = alloc_nvme_io_ctx(
            NvmeIoCtx {
                cb,
                cb_arg,
                iov,
                iovcnt: iovcnt as u64,
                iovpos: 0,
                iov_offset: 0,
            },
            offset_blocks,
            num_blocks,
        )?;

        let rc;

        if iovcnt == 1 {
            rc = unsafe {
                spdk_nvme_ns_cmd_read(
                    self.ns.as_ptr(),
                    inner.qpair,
                    (*iov).iov_base,
                    offset_blocks,
                    num_blocks as u32,
                    Some(nvme_io_done),
                    bio as *mut c_void,
                    self.prchk_flags,
                )
            };
        } else {
            rc = unsafe {
                spdk_nvme_ns_cmd_readv(
                    self.ns.as_ptr(),
                    inner.qpair,
                    offset_blocks,
                    num_blocks as u32,
                    Some(nvme_io_done),
                    bio as *mut c_void,
                    self.prchk_flags,
                    Some(nvme_queued_reset_sgl),
                    Some(nvme_queued_next_sge),
                )
            }
        }

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
        cb_arg: *const c_void,
    ) -> Result<(), CoreError> {
        check_io_args(iov, iovcnt, offset_blocks, num_blocks)?;

        let inner = NvmeIoChannel::inner_from_channel(self.io_channel.as_ptr());

        // Make sure channel allows I/O.
        check_channel_for_rw_io(
            IoType::WRITE,
            inner,
            offset_blocks,
            num_blocks,
        )?;

        let bio = alloc_nvme_io_ctx(
            NvmeIoCtx {
                cb,
                cb_arg,
                iov,
                iovcnt: iovcnt as u64,
                iovpos: 0,
                iov_offset: 0,
            },
            offset_blocks,
            num_blocks,
        )?;

        let rc;

        if iovcnt == 1 {
            rc = unsafe {
                spdk_nvme_ns_cmd_write(
                    self.ns.as_ptr(),
                    inner.qpair,
                    (*iov).iov_base,
                    offset_blocks,
                    num_blocks as u32,
                    Some(nvme_io_done),
                    bio as *mut c_void,
                    self.prchk_flags,
                )
            };
        } else {
            rc = unsafe {
                spdk_nvme_ns_cmd_writev(
                    self.ns.as_ptr(),
                    inner.qpair,
                    offset_blocks,
                    num_blocks as u32,
                    Some(nvme_writev_done),
                    bio as *mut c_void,
                    self.prchk_flags,
                    Some(nvme_queued_reset_sgl),
                    Some(nvme_queued_next_sge),
                )
            }
        }

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

    async fn nvme_admin_custom(&self, opcode: u8) -> Result<(), CoreError> {
        let mut cmd = spdk_sys::spdk_nvme_cmd::default();
        cmd.set_opc(opcode.into());
        self.nvme_admin(&cmd, None).await
    }

    async fn nvme_admin(
        &self,
        cmd: &spdk_sys::spdk_nvme_cmd,
        buffer: Option<&mut DmaBuf>,
    ) -> Result<(), CoreError> {
        let mut pcmd = *cmd; // Make a private mutable copy of the command.

        let (ptr, size) = match buffer {
            Some(buf) => (**buf, buf.len()),
            None => (std::ptr::null_mut(), 0),
        };

        let (s, r) = oneshot::channel::<bool>();

        let _rc = unsafe {
            spdk_nvme_ctrlr_cmd_admin_raw(
                self.ctrlr.as_ptr(),
                &mut pcmd,
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

    fn reset(
        &self,
        cb: IoCompletionCallback,
        cb_arg: *const c_void,
    ) -> Result<(), CoreError> {
        let controller = NVME_CONTROLLERS.lookup_by_name(&self.name).ok_or(
            CoreError::BdevNotFound {
                name: self.name.to_string(),
            },
        )?;
        let mut controller = controller.lock().expect("lock poisoned");

        // Schedule asynchronous controller reset.
        controller.reset(cb, cb_arg, false)
    }
}

impl Drop for NvmeDeviceHandle {
    fn drop(&mut self) {
        unsafe { ManuallyDrop::drop(&mut self.io_channel) }
    }
}
