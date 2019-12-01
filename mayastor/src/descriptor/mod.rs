//! Analogous to a file descriptor, IO from a read-write perspective is driven
//! by a bdev descriptor.
//!
//! A descriptor is obtained by opening a bdev. Once opened, a reference to a
//! channel is created and the two allow for submitting IO to the bdevs.
//!
//! The buffers written to the bdev must be allocated by the provided allocation
//! methods. These buffers are allocated from mem pools and huge pages and allow
//! for DMA transfers in the case of, for example, NVMe devices.
//!
//! The callbacks are implemented by the regular oneshot channels. As the unsync
//! features of futures 0.2 are not part of futures 0.3 yet (if ever?) it is
//! not optimized for performance yet. Its not our goal to directly have a user
//! space API to be consumable for this purpose either but they might be useful
//! for other scenarios in the future.
//!
//! Also, it would be nice to support, future, the AsyncRead/Write traits such
//! that any rust program, directly, can consume user space IO.
//!
//! # Example:
//! ```ignore
//! use mayastor::descriptor::Descriptor;
//! // open a descriptor to the bdev, in readonly or read/write
//! let bdev = Descriptor::open("my_bdev", true).unwrap();
//! let mut buf = bdev.dma_zmalloc(4096).unwrap();
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

use crate::{
    bdev::{bdev_lookup_by_name, nexus::Error, Bdev},
    executor::cb_arg,
};
use futures::channel::oneshot;
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
    spdk_dma_free,
    spdk_dma_zmalloc,
    spdk_get_io_channel,
    spdk_io_channel,
    spdk_put_io_channel,
};
use std::{
    ffi::c_void,
    ops::{Deref, DerefMut},
    slice::{from_raw_parts, from_raw_parts_mut},
};

/// DmaBuf that is allocated from the memory pool
#[derive(Debug)]
pub struct DmaBuf {
    /// a raw pointer to the buffer
    pub buf: *mut c_void,
    /// the length of the allocated buffer
    len: usize,
}

impl DmaBuf {
    /// convert the buffer to a slice
    pub fn as_slice(&self) -> &[u8] {
        if cfg!(debug_assertions) && self.buf.is_null() {
            panic!("self.buf is null");
        }

        unsafe { from_raw_parts(self.buf as *mut u8, self.len as usize) }
    }

    /// convert the buffer to a mutable slice
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { from_raw_parts_mut(self.buf as *mut u8, self.len as usize) }
    }

    /// fill the buffer with the given value
    pub fn fill(&mut self, val: u8) {
        if cfg!(debug_assertions) && self.buf.is_null() {
            panic!("self buf is null");
        }

        unsafe {
            std::ptr::write_bytes(
                self.as_mut_slice().as_ptr() as *mut u8,
                val,
                self.len,
            )
        }
    }

    pub fn new(size: usize, alignment: u8) -> Result<Self, Error> {
        let buf;
        unsafe {
            buf = spdk_dma_zmalloc(
                size,
                1 << alignment as usize,
                std::ptr::null_mut(),
            )
        };

        if buf.is_null() {
            trace!("zmalloc for size {} failed", size);
            Err(Error::OutOfMemory)
        } else {
            Ok(DmaBuf {
                buf,
                len: size,
            })
        }
    }
}

impl Deref for DmaBuf {
    type Target = *mut c_void;

    fn deref(&self) -> &Self::Target {
        &self.buf
    }
}

impl DerefMut for DmaBuf {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.buf
    }
}

impl Drop for DmaBuf {
    fn drop(&mut self) {
        if cfg!(debug_assertions) {
            trace!("dropping Dmabuf {:?}", self);
        }
        unsafe { spdk_dma_free(self.buf as *mut c_void) }
        self.buf = std::ptr::null_mut();
    }
}

/// The `Reply` that is send back over the one shot channel
type Reply = bool;

/// Block Device Descriptor
#[derive(Debug)]
pub struct Descriptor {
    /// the allocated descriptor
    pub desc: *mut spdk_bdev_desc,
    /// the io channel
    pub ch: *mut spdk_io_channel,
}

impl Descriptor {
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
            Box::from_raw(arg as *const _ as *mut oneshot::Sender<Reply>)
        };

        unsafe {
            spdk_bdev_free_io(io);
        }

        sender.send(success).expect("io completion error");
    }

    /// allocate zeroed memory from the memory pool with given size and proper
    /// alignment
    pub fn dma_zmalloc(&self, size: usize) -> Result<DmaBuf, Error> {
        let buf;
        unsafe {
            buf = spdk_dma_zmalloc(
                size,
                1 << self.get_bdev().alignment() as usize,
                std::ptr::null_mut(),
            )
        };

        if buf.is_null() {
            trace!("Zmalloc for size {} failed", size);
            Err(Error::OutOfMemory)
        } else {
            Ok(DmaBuf {
                buf,
                len: size,
            })
        }
    }

    /// allocate memory from the memory pool that is not zeroed out
    pub fn dma_malloc(&self, size: usize) -> Result<DmaBuf, Error> {
        let buf;
        unsafe {
            buf = spdk_dma_zmalloc(
                size,
                1 << self.get_bdev().alignment(),
                std::ptr::null_mut(),
            )
        };

        if buf.is_null() {
            trace!("Malloc for size {} failed", size);
            Err(Error::OutOfMemory)
        } else {
            Ok(DmaBuf {
                buf,
                len: size,
            })
        }
    }

    /// write the `buffer` to the given `offset`
    pub async fn write_at(
        &self,
        offset: u64,
        buffer: &DmaBuf,
    ) -> Result<usize, i32> {
        if offset % u64::from(self.get_bdev().block_len()) != 0 {
            return Err(-1);
        }

        let (s, r) = oneshot::channel::<Reply>();
        let rc = unsafe {
            spdk_bdev_write(
                self.desc,
                self.ch,
                buffer.buf as *mut c_void,
                offset,
                buffer.len as u64,
                Some(Self::io_completion_cb),
                cb_arg(s),
            )
        };

        if rc != 0 {
            return Err(rc);
        }

        if r.await.expect("Failed awaiting write IO") {
            Ok(buffer.len as usize)
        } else {
            Err(-1)
        }
    }

    /// read from the given `offset` into the `buffer` note that the buffer
    /// is allocated internally and should be copied. Also its unknown to me
    /// what will happen if you for example, where to turn this into a vec
    /// but for sure -- not what you want.
    pub async fn read_at(
        &self,
        offset: u64,
        buffer: &mut DmaBuf,
    ) -> Result<usize, i32> {
        if offset % u64::from(self.get_bdev().block_len()) != 0 {
            return Err(-1);
        }
        let (s, r) = oneshot::channel::<Reply>();
        let rc = unsafe {
            spdk_bdev_read(
                self.desc,
                self.ch,
                buffer.buf as *mut c_void,
                offset,
                buffer.len as u64,
                Some(Self::io_completion_cb),
                cb_arg(s),
            )
        };

        if rc != 0 {
            return Err(rc);
        }

        if r.await.expect("Failed awaiting read IO") {
            Ok(buffer.len)
        } else {
            Err(-1)
        }
    }

    /// open a descriptor to the bdev with given `name` in read only or
    /// read/write
    pub fn open(name: &str, write_enable: bool) -> Option<Self> {
        let bdev = bdev_lookup_by_name(name)?;
        let mut desc: *mut spdk_bdev_desc = std::ptr::null_mut();

        let rc = unsafe {
            spdk_bdev_open(
                bdev.as_ptr(),
                write_enable,
                None,
                std::ptr::null_mut(),
                &mut desc,
            )
        };

        if rc != 0 {
            return None;
        }

        let ch = unsafe { spdk_bdev_get_io_channel(desc) };

        if ch.is_null() {
            unsafe { spdk_bdev_close(desc) };
            return None;
        }

        Some(Descriptor {
            desc,
            ch,
        })
    }

    /// close the descriptor, any allocated buffers remain allocated and must
    /// be dropped/freed separately consume self.
    pub fn close(mut self) {
        unsafe {
            spdk_put_io_channel(self.ch);
            spdk_bdev_close(self.desc)
        };
        self.ch = std::ptr::null_mut();
        self.desc = std::ptr::null_mut();
    }

    /// return the bdev associated with this descriptor
    pub fn get_bdev(&self) -> Bdev {
        unsafe { Bdev::from(spdk_bdev_desc_get_bdev(self.desc)) }
    }
}

impl Drop for Descriptor {
    fn drop(&mut self) {
        if cfg!(debug_assertions) {
            trace!("Dropping descriptor {:?}", self);
        }
        unsafe {
            if !self.ch.is_null() {
                spdk_put_io_channel(self.ch);
            }
            if !self.desc.is_null() {
                spdk_bdev_close(self.desc)
            }
        };
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
