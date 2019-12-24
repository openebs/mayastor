//! Analogous to a file descriptor, IO from a read-write perspective is driven
//! by a bdev descriptor.
//!
//! A descriptor is obtained by opening a bdev. Once opened, a reference to a
//! channel is created, and the two allow for submitting IO to the bdevs.
//!
//! The buffers written to the bdev must be allocated by the provided allocation
//! methods. These buffers are allocated from mem pools and huge pages and allow
//! for DMA transfers in the case of, for example, NVMe devices.
//!
//! # Example:
//! ```ignore
//! use mayastor::descriptor::Descriptor;
//! // open a descriptor to the bdev, in readonly or read/write
//! let bdev = Descriptor::open("my_bdev", true).unwrap();
//! let mut buf = bdev.dma_malloc(4096).unwrap();
//! buf.fill(0xff);
//! bdev.write_at(0, &buf).await.unwrap();
//!
//! // fill the buffer with zeros and read the written data back into the buffer
//! buf.fill(0x00);
//! bdev.read_at(0, &mut buf).await.unwrap();
//!
//! let slice = buf.as_slice();
//! assert_eq!(slice[0], 0xff);
//! ```

use std::{
    ffi::c_void,
    ops::{Deref, DerefMut},
};

use futures::channel::oneshot;
use nix::errno::Errno;
use snafu::{ResultExt, Snafu};
use spdk_sys::{
    spdk_bdev_close,
    spdk_bdev_desc,
    spdk_bdev_desc_get_bdev,
    spdk_bdev_free_io,
    spdk_bdev_get_io_channel,
    spdk_bdev_io,
    spdk_bdev_open,
    spdk_bdev_read,
    spdk_bdev_write,
    spdk_get_io_channel,
    spdk_io_channel,
    spdk_put_io_channel,
};

use crate::{
    bdev::Bdev,
    dma::{DmaBuf, DmaError},
    executor::{cb_arg, errno_result_from_i32},
};

#[derive(Debug, Snafu)]
pub enum DescError {
    #[snafu(display("Failed to open bdev"))]
    OpenBdev { source: Errno },
    #[snafu(display("Failed to obtain IO channel for bdev"))]
    GetIoChannel {},
    #[snafu(display("Invalid IO offset {}", offset))]
    InvalidOffset { offset: u64 },
    #[snafu(display(
        "Failed to dispatch write at offset {} length {}",
        offset,
        len
    ))]
    WriteError {
        source: Errno,
        offset: u64,
        len: usize,
    },
    #[snafu(display(
        "Failed to dispatch read at offset {} length {}",
        offset,
        len
    ))]
    ReadError {
        source: Errno,
        offset: u64,
        len: usize,
    },
    #[snafu(display("Write failed at offset {} length {}", offset, len))]
    WriteFailed { offset: u64, len: usize },
    #[snafu(display("Read failed at offset {} length {}", offset, len))]
    ReadFailed { offset: u64, len: usize },
}

/// Block Device Descriptor
#[derive(Debug)]
pub struct Descriptor {
    /// the allocated descriptor
    desc: *mut spdk_bdev_desc,
    /// the io channel
    ch: *mut spdk_io_channel,
}

impl Descriptor {
    /// Open a descriptor to the bdev with given `name` in read only or
    /// read/write mode.
    ///
    /// NOTE: The descriptor is closed when the descriptor on returned
    /// handle is closed.
    pub fn open(bdev: &Bdev, write_enable: bool) -> Result<Self, DescError> {
        let mut desc: *mut spdk_bdev_desc = std::ptr::null_mut();
        let errno = unsafe {
            spdk_bdev_open(
                bdev.as_ptr(),
                write_enable,
                None,
                std::ptr::null_mut(),
                &mut desc,
            )
        };
        errno_result_from_i32((), errno).context(OpenBdev {})?;

        let ch = unsafe { spdk_bdev_get_io_channel(desc) };
        if ch.is_null() {
            unsafe { spdk_bdev_close(desc) };
            return Err(DescError::GetIoChannel {});
        }

        Ok(Self {
            desc,
            ch,
        })
    }

    /// Allocate memory from the memory pool (the mem is zeroed out)
    /// with given size and proper alignment for the bdev.
    pub fn dma_malloc(&self, size: usize) -> Result<DmaBuf, DmaError> {
        DmaBuf::new(size, self.get_bdev().alignment())
    }

    /// io completion callback that sends back the success status of the IO.
    /// When the IO is freed, it is returned to the memory pool. The buffer is
    /// not freed this is not very optimal right now, as we use oneshot
    /// channels from futures 0.3 which (AFAIK) does not have unsync support
    /// yet.
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

    /// write the `buffer` to the given `offset`
    pub async fn write_at(
        &self,
        offset: u64,
        buffer: &DmaBuf,
    ) -> Result<usize, DescError> {
        if offset % u64::from(self.get_bdev().block_len()) != 0 {
            return Err(DescError::InvalidOffset {
                offset,
            });
        }

        let (s, r) = oneshot::channel::<bool>();
        let errno = unsafe {
            spdk_bdev_write(
                self.desc,
                self.ch,
                **buffer,
                offset,
                buffer.len() as u64,
                Some(Self::io_completion_cb),
                cb_arg(s),
            )
        };

        errno_result_from_i32((), errno).context(WriteError {
            offset,
            len: buffer.len(),
        })?;

        if r.await.expect("Failed awaiting write IO") {
            Ok(buffer.len() as usize)
        } else {
            Err(DescError::WriteFailed {
                offset,
                len: buffer.len(),
            })
        }
    }

    /// read from the given `offset` into the `buffer` note that the buffer
    /// is allocated internally and should be copied. Also, its unknown to me
    /// what will happen if you for example, where to turn this into a vec
    /// but for sure -- not what you want.
    pub async fn read_at(
        &self,
        offset: u64,
        buffer: &mut DmaBuf,
    ) -> Result<usize, DescError> {
        if offset % u64::from(self.get_bdev().block_len()) != 0 {
            return Err(DescError::InvalidOffset {
                offset,
            });
        }
        let (s, r) = oneshot::channel::<bool>();
        let errno = unsafe {
            spdk_bdev_read(
                self.desc,
                self.ch,
                **buffer,
                offset,
                buffer.len() as u64,
                Some(Self::io_completion_cb),
                cb_arg(s),
            )
        };

        errno_result_from_i32((), errno).context(ReadError {
            offset,
            len: buffer.len(),
        })?;

        if r.await.expect("Failed awaiting read IO") {
            Ok(buffer.len())
        } else {
            Err(DescError::ReadFailed {
                offset,
                len: buffer.len(),
            })
        }
    }

    /// return the bdev associated with this descriptor
    pub fn get_bdev(&self) -> Bdev {
        unsafe { Bdev::from(spdk_bdev_desc_get_bdev(self.desc)) }
    }

    /// Return raw pointer to SPDK representation of the bdev descriptor.
    ///
    /// NOTE: Use only in exceptional cases when using raw calls to SPDK to
    /// do the IO. For anything else write_at(), read_at() methods should be
    /// used.
    pub fn as_ptr(&self) -> *mut spdk_bdev_desc {
        self.desc
    }

    /// Get raw pointer to SPDK io channel used in the descriptor.
    ///
    /// NOTE: Use only in exceptional cases when using raw calls to SPDK to
    /// do the IO. For anything else write_at(), read_at() methods should be
    /// used.
    pub fn channel(&self) -> *mut spdk_io_channel {
        self.ch
    }
}

impl Drop for Descriptor {
    /// close the descriptor, any allocated buffers remain allocated and must
    /// be dropped/freed separately consume self.
    fn drop(&mut self) {
        if cfg!(debug_assertions) {
            trace!("Dropping descriptor {:?}", self);
        }
        unsafe {
            spdk_put_io_channel(self.ch);
            spdk_bdev_close(self.desc);
        }
    }
}

/// Automatic IO channel handle
#[derive(Debug)]
pub struct IoChannel {
    /// the channel handle
    handle: *mut spdk_io_channel,
}

impl IoChannel {
    /// Acquire an io channel for the given nexus.
    /// This channel has guard semantics, and will be released when dropped.
    ///
    /// # Safety
    ///
    /// The pointer specified must be io_device pointer which has been
    /// previously registered using spdk_io_device_register()
    pub unsafe fn new(nexus: *mut c_void) -> Self {
        IoChannel {
            handle: spdk_get_io_channel(nexus),
        }
    }
}

impl Drop for IoChannel {
    fn drop(&mut self) {
        unsafe {
            spdk_put_io_channel(self.handle);
        };
    }
}

impl Deref for IoChannel {
    type Target = *mut spdk_io_channel;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

impl DerefMut for IoChannel {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.handle
    }
}
