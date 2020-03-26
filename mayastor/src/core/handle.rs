use std::{
    convert::TryFrom, fmt::Debug, mem::ManuallyDrop, os::raw::c_void, sync::Arc,
};

use futures::channel::oneshot;
use nix::errno::Errno;
use serde::export::{fmt::Error, Formatter};

use spdk_sys::{
    spdk_bdev_desc, spdk_bdev_free_io, spdk_bdev_io, spdk_bdev_read,
    spdk_bdev_write, spdk_io_channel,
};

use crate::{
    core::{Bdev, CoreError, Descriptor, DmaBuf, DmaError, IoChannel},
    ffihelper::cb_arg,
};

/// A handle to a bdev, is an interface to submit IO. The ['Descriptor'] may be
/// shared between cores freely. The ['IoChannel'] however, must be allocated on
/// the core where the IO is submitted from.
pub struct BdevHandle {
    pub desc: ManuallyDrop<Arc<Descriptor>>,
    pub channel: ManuallyDrop<IoChannel>,
}

impl BdevHandle {
    /// open a new bdev handle allocating a new ['Descriptor'] as well as a new
    /// ['IoChannel']
    pub fn open(
        name: &str,
        read_write: bool,
        claim: bool,
    ) -> Result<BdevHandle, CoreError> {
        if let Ok(desc) = Bdev::open_by_name(name, read_write) {
            if claim && !desc.claim() {
                return Err(CoreError::BdevNotFound { name: name.into() });
            }
            return BdevHandle::try_from(Arc::new(desc));
        }

        Err(CoreError::BdevNotFound { name: name.into() })
    }

    /// close the BdevHandle causing
    pub fn close(self) {
        drop(self);
    }

    /// get the bdev associated with this handle
    pub fn get_bdev(&self) -> Bdev {
        self.desc.get_bdev()
    }

    /// return a tuple to be used directly for read/write operations
    pub fn io_tuple(&self) -> (*mut spdk_bdev_desc, *mut spdk_io_channel) {
        (self.desc.as_ptr(), self.channel.as_ptr())
    }

    /// Allocate memory from the memory pool (the mem is zeroed out)
    /// with given size and proper alignment for the bdev.
    pub fn dma_malloc(&self, size: usize) -> Result<DmaBuf, DmaError> {
        DmaBuf::new(size, self.desc.get_bdev().alignment())
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
            Box::from_raw(arg as *const _ as *mut oneshot::Sender<bool>)
        };

        unsafe {
            spdk_bdev_free_io(io);
        }

        sender.send(success).expect("io completion error");
    }

    /// write the ['DmaBuf'] to the given offset. This function is implemented
    /// using a ['Future'] and is not intended for non-internal IO.
    pub async fn write_at(
        &self,
        offset: u64,
        buffer: &DmaBuf,
    ) -> Result<usize, CoreError> {
        let (s, r) = oneshot::channel::<bool>();
        let errno = unsafe {
            spdk_bdev_write(
                self.desc.as_ptr(),
                self.channel.as_ptr(),
                **buffer,
                offset,
                buffer.len() as u64,
                Some(Self::io_completion_cb),
                cb_arg(s),
            )
        };

        if errno != 0 {
            return Err(CoreError::WriteDispatch {
                source: Errno::from_i32(errno),
                offset,
                len: buffer.len(),
            });
        }

        if r.await.expect("Failed awaiting write IO") {
            Ok(buffer.len() as usize)
        } else {
            Err(CoreError::WriteFailed {
                offset,
                len: buffer.len(),
            })
        }
    }

    /// read at given offset into the ['DmaBuf']
    pub async fn read_at(
        &self,
        offset: u64,
        buffer: &mut DmaBuf,
    ) -> Result<usize, CoreError> {
        let (s, r) = oneshot::channel::<bool>();
        let errno = unsafe {
            spdk_bdev_read(
                self.desc.as_ptr(),
                self.channel.as_ptr(),
                **buffer,
                offset,
                buffer.len() as u64,
                Some(Self::io_completion_cb),
                cb_arg(s),
            )
        };

        if errno != 0 {
            return Err(CoreError::ReadDispatch {
                source: Errno::from_i32(errno),
                offset,
                len: buffer.len(),
            });
        }

        if r.await.expect("Failed awaiting read IO") {
            Ok(buffer.len())
        } else {
            Err(CoreError::ReadFailed {
                offset,
                len: buffer.len(),
            })
        }
    }
}

impl Drop for BdevHandle {
    fn drop(&mut self) {
        unsafe {
            trace!("{:?}", self);
            // the order of dropping has to be deterministic
            ManuallyDrop::drop(&mut self.channel);
            ManuallyDrop::drop(&mut self.desc);
        }
    }
}

impl Debug for BdevHandle {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "{:?}", self.desc)?;
        write!(f, "{:?}", self.channel)
    }
}

impl TryFrom<Descriptor> for BdevHandle {
    type Error = CoreError;

    fn try_from(desc: Descriptor) -> Result<Self, Self::Error> {
        if let Some(channel) = desc.get_channel() {
            return Ok(Self {
                desc: ManuallyDrop::new(Arc::new(desc)),
                channel: ManuallyDrop::new(channel),
            });
        }

        Err(CoreError::GetIoChannel {
            name: desc.get_bdev().name(),
        })
    }
}

impl TryFrom<Arc<Descriptor>> for BdevHandle {
    type Error = CoreError;

    fn try_from(desc: Arc<Descriptor>) -> Result<Self, Self::Error> {
        if let Some(channel) = desc.get_channel() {
            return Ok(Self {
                desc: ManuallyDrop::new(desc),
                channel: ManuallyDrop::new(channel),
            });
        }

        Err(CoreError::GetIoChannel {
            name: desc.get_bdev().name(),
        })
    }
}
