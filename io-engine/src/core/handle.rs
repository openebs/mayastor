use std::{
    convert::TryFrom,
    fmt::{Debug, Error, Formatter},
    sync::Arc,
};

use futures::channel::oneshot;
use libc::c_void;
use nix::errno::Errno;

use spdk_rs::{
    libspdk::{
        spdk_bdev_desc,
        spdk_bdev_free_io,
        spdk_bdev_io,
        spdk_bdev_nvme_admin_passthru_ro,
        spdk_bdev_read_with_flags,
        spdk_bdev_reset,
        spdk_bdev_write,
        spdk_bdev_write_zeroes,
        spdk_io_channel,
        spdk_nvme_cmd,
        SPDK_NVME_IO_FLAGS_UNWRITTEN_READ_FAIL,
    },
    nvme_admin_opc,
    BdevOps,
    DmaBuf,
    DmaError,
    GenericStatusCode,
    IoChannelGuard,
    MediaErrorStatusCode,
    NvmeStatus,
};

use crate::{
    core::{Bdev, CoreError, DescriptorGuard, ReadMode, SnapshotParams},
    ffihelper::cb_arg,
    subsys,
};

/// A handle to a bdev, is an interface to submit IO. The ['Descriptor'] may be
/// shared between cores freely. The ['IoChannel'] however, must be allocated on
/// the core where the IO is submitted from.
pub struct BdevHandle<T: BdevOps> {
    /// Rust guarantees proper ordering of dropping. The channel MUST be
    /// dropped before we close the descriptor
    channel: IoChannelGuard<T::ChannelData>,
    /// TODO
    desc: Arc<DescriptorGuard<T>>,
}

pub type UntypedBdevHandle = BdevHandle<()>;

impl<T: BdevOps> BdevHandle<T> {
    /// Opens a new bdev handle allocating a new descriptor as well as a new
    /// I/O channel.
    pub fn open(
        name: &str,
        read_write: bool,
        claim: bool,
    ) -> Result<BdevHandle<T>, CoreError> {
        if let Ok(desc) = Bdev::<T>::open_by_name(name, read_write) {
            if claim && !desc.claim() {
                return Err(CoreError::BdevNotFound {
                    name: name.into(),
                });
            }
            return BdevHandle::try_from(Arc::new(desc));
        }

        Err(CoreError::BdevNotFound {
            name: name.into(),
        })
    }

    /// Opens a new bdev handle given a bdev.
    pub fn open_with_bdev(
        bdev: &Bdev<T>,
        read_write: bool,
    ) -> Result<BdevHandle<T>, CoreError> {
        let desc = bdev.open(read_write)?;
        BdevHandle::try_from(Arc::new(desc))
    }

    /// Closes the BdevHandle.
    pub fn close(self) {
        drop(self);
    }

    /// get the bdev associated with this handle
    pub fn get_bdev(&self) -> Bdev<T> {
        self.desc.bdev()
    }

    /// return a tuple to be used directly for read/write operations
    pub fn io_tuple(&self) -> (*mut spdk_bdev_desc, *mut spdk_io_channel) {
        (self.desc.legacy_as_ptr(), self.channel.legacy_as_ptr())
    }

    /// Allocate memory from the memory pool (the mem is zeroed out)
    /// with given size and proper alignment for the bdev.
    pub fn dma_malloc(&self, size: u64) -> Result<DmaBuf, DmaError> {
        DmaBuf::new(size, self.desc.bdev().alignment())
    }

    /// private io completion callback that sends back the success status of the
    /// IO. When the IO is freed, it is returned to the memory pool. The
    /// buffer is not freed.
    extern "C" fn io_completion_cb(
        io: *mut spdk_bdev_io,
        success: bool,
        arg: *mut c_void,
    ) {
        let sender = unsafe {
            Box::from_raw(arg as *const _ as *mut oneshot::Sender<NvmeStatus>)
        };

        unsafe {
            spdk_bdev_free_io(io);
        }

        let status = if success {
            NvmeStatus::Generic(GenericStatusCode::Success)
        } else {
            NvmeStatus::from(io)
        };

        sender.send(status).expect("io completion error");
    }

    /// write the ['DmaBuf'] to the given offset. This function is implemented
    /// using a ['Future'] and is not intended for non-internal IO.
    pub async fn write_at(
        &self,
        offset: u64,
        buffer: &DmaBuf,
    ) -> Result<u64, CoreError> {
        let (s, r) = oneshot::channel::<NvmeStatus>();
        let errno = unsafe {
            spdk_bdev_write(
                self.desc.legacy_as_ptr(),
                self.channel.legacy_as_ptr(),
                **buffer,
                offset,
                buffer.len(),
                Some(Self::io_completion_cb),
                cb_arg(s),
            )
        };

        if errno != 0 {
            return Err(CoreError::WriteDispatch {
                source: Errno::from_i32(errno.abs()),
                offset,
                len: buffer.len(),
            });
        }

        match r.await.expect("Failed awaiting write IO") {
            NvmeStatus::Generic(GenericStatusCode::Success) => Ok(buffer.len()),
            status => Err(CoreError::WriteFailed {
                status,
                offset,
                len: buffer.len(),
            }),
        }
    }
    /// read at given offset into the ['DmaBuf']
    pub async fn read_at(
        &self,
        offset: u64,
        buffer: &mut DmaBuf,
    ) -> Result<u64, CoreError> {
        self.read_at_ex(offset, buffer, None).await
    }

    /// read at given offset into the ['DmaBuf']
    pub(crate) async fn read_at_ex(
        &self,
        offset: u64,
        buffer: &mut DmaBuf,
        mode: Option<ReadMode>,
    ) -> Result<u64, CoreError> {
        let flags = mode.map_or(0, |m| match m {
            ReadMode::Normal => 0,
            ReadMode::UnwrittenFail => SPDK_NVME_IO_FLAGS_UNWRITTEN_READ_FAIL,
        });

        let (s, r) = oneshot::channel::<NvmeStatus>();
        let errno = unsafe {
            spdk_bdev_read_with_flags(
                self.desc.legacy_as_ptr(),
                self.channel.legacy_as_ptr(),
                **buffer,
                offset,
                buffer.len(),
                Some(Self::io_completion_cb),
                cb_arg(s),
                flags,
            )
        };

        if errno != 0 {
            return Err(CoreError::ReadDispatch {
                source: Errno::from_i32(errno.abs()),
                offset,
                len: buffer.len(),
            });
        }

        match r.await.expect("Failed awaiting read IO") {
            NvmeStatus::Generic(GenericStatusCode::Success) => Ok(buffer.len()),
            NvmeStatus::MediaError(
                MediaErrorStatusCode::DeallocatedOrUnwrittenBlock,
            ) => Err(CoreError::ReadingUnallocatedBlock {
                offset,
                len: buffer.len(),
            }),
            status => Err(CoreError::ReadFailed {
                status,
                offset,
                len: buffer.len(),
            }),
        }
    }

    pub async fn reset(&self) -> Result<(), CoreError> {
        let (s, r) = oneshot::channel::<NvmeStatus>();
        let errno = unsafe {
            spdk_bdev_reset(
                self.desc.legacy_as_ptr(),
                self.channel.legacy_as_ptr(),
                Some(Self::io_completion_cb),
                cb_arg(s),
            )
        };

        if errno != 0 {
            return Err(CoreError::ResetDispatch {
                source: Errno::from_i32(errno.abs()),
            });
        }

        if r.await.expect("Failed awaiting reset IO")
            == NvmeStatus::Generic(GenericStatusCode::Success)
        {
            Ok(())
        } else {
            Err(CoreError::ResetFailed {})
        }
    }

    pub async fn write_zeroes_at(
        &self,
        offset: u64,
        len: u64,
    ) -> Result<(), CoreError> {
        let (s, r) = oneshot::channel::<NvmeStatus>();
        let errno = unsafe {
            spdk_bdev_write_zeroes(
                self.desc.legacy_as_ptr(),
                self.channel.legacy_as_ptr(),
                offset,
                len,
                Some(Self::io_completion_cb),
                cb_arg(s),
            )
        };

        if errno != 0 {
            return Err(CoreError::WriteZeroesDispatch {
                source: Errno::from_i32(errno.abs()),
                offset,
                len,
            });
        }

        if r.await.expect("Failed awaiting write zeroes IO")
            == NvmeStatus::Generic(GenericStatusCode::Success)
        {
            Ok(())
        } else {
            Err(CoreError::WriteZeroesFailed {
                offset,
                len,
            })
        }
    }

    /// create a snapshot, only works for nvme bdev
    /// returns snapshot time as u64 seconds since Unix epoch
    pub async fn create_snapshot(
        &self,
        _snapshot: SnapshotParams,
    ) -> Result<u64, CoreError> {
        let mut cmd = spdk_nvme_cmd::default();
        cmd.set_opc(nvme_admin_opc::CREATE_SNAPSHOT.into());
        let now = subsys::set_snapshot_time(&mut cmd);
        debug!("Creating snapshot at {}", now);
        self.nvme_admin(&cmd, None).await?;
        Ok(now)
    }

    /// identify controller
    /// buffer must be at least 4096B
    pub async fn nvme_identify_ctrlr(
        &self,
        buffer: &mut DmaBuf,
    ) -> Result<(), CoreError> {
        let mut cmd = spdk_nvme_cmd::default();
        cmd.set_opc(nvme_admin_opc::IDENTIFY.into());
        cmd.nsid = 0xffffffff;
        // Controller Identifier
        unsafe { *spdk_rs::libspdk::nvme_cmd_cdw10_get(&mut cmd) = 1 };
        self.nvme_admin(&cmd, Some(buffer)).await
    }

    /// sends an NVMe Admin command, only for read commands without buffer
    pub async fn nvme_admin_custom(&self, opcode: u8) -> Result<(), CoreError> {
        let mut cmd = spdk_nvme_cmd::default();
        cmd.set_opc(opcode.into());
        self.nvme_admin(&cmd, None).await
    }

    /// sends the specified NVMe Admin command, only read commands
    pub async fn nvme_admin(
        &self,
        nvme_cmd: &spdk_nvme_cmd,
        buffer: Option<&mut DmaBuf>,
    ) -> Result<(), CoreError> {
        trace!("Sending nvme_admin {}", nvme_cmd.opc());
        let (s, r) = oneshot::channel::<NvmeStatus>();
        // Use the spdk-rs variant spdk_bdev_nvme_admin_passthru that
        // assumes read commands
        let errno = unsafe {
            spdk_bdev_nvme_admin_passthru_ro(
                self.desc.legacy_as_ptr(),
                self.channel.legacy_as_ptr(),
                nvme_cmd,
                match buffer {
                    Some(ref b) => ***b,
                    None => std::ptr::null_mut(),
                },
                match buffer {
                    Some(b) => b.len(),
                    None => 0,
                },
                Some(Self::io_completion_cb),
                cb_arg(s),
            )
        };

        if errno != 0 {
            return Err(CoreError::NvmeAdminDispatch {
                source: Errno::from_i32(errno.abs()),
                opcode: (*nvme_cmd).opc(),
            });
        }

        if r.await.expect("Failed awaiting NVMe Admin IO")
            == NvmeStatus::Generic(GenericStatusCode::Success)
        {
            Ok(())
        } else {
            Err(CoreError::NvmeAdminFailed {
                opcode: (*nvme_cmd).opc(),
                source: Errno::EIO,
            })
        }
    }
}

impl<T: BdevOps> Debug for BdevHandle<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "{:?}", self.desc)?;
        write!(f, "{:?}", self.channel)
    }
}

impl<T: BdevOps> TryFrom<DescriptorGuard<T>> for BdevHandle<T> {
    type Error = CoreError;

    fn try_from(desc: DescriptorGuard<T>) -> Result<Self, Self::Error> {
        Self::try_from(Arc::new(desc))
    }
}

impl<T: BdevOps> TryFrom<Arc<DescriptorGuard<T>>> for BdevHandle<T> {
    type Error = CoreError;

    fn try_from(desc: Arc<DescriptorGuard<T>>) -> Result<Self, Self::Error> {
        if let Ok(channel) = desc.io_channel() {
            return Ok(Self {
                channel,
                desc,
            });
        }

        Err(CoreError::GetIoChannel {
            name: desc.bdev().name().to_string(),
        })
    }
}
