//! The buffers written to the bdev must be allocated by the provided allocation
//! methods. These buffers are allocated from mem pools and huge pages and allow
//! for DMA transfers in the case of, for example, NVMe devices.

use std::{
    ffi::c_void,
    ops::{Deref, DerefMut},
    slice::{from_raw_parts, from_raw_parts_mut},
};

use snafu::Snafu;

use spdk_sys::{spdk_dma_free, spdk_dma_zmalloc};

#[derive(Debug, Snafu)]
pub enum DmaError {
    #[snafu(display("Failed to allocate DMA buffer"))]
    Alloc {},
}

/// DmaBuf that is allocated from the memory pool
#[derive(Debug)]
pub struct DmaBuf {
    /// a raw pointer to the buffer
    buf: *mut c_void,
    /// the length of the allocated buffer
    length: usize,
}

impl DmaBuf {
    /// convert the buffer to a slice
    pub fn as_slice(&self) -> &[u8] {
        unsafe { from_raw_parts(self.buf as *mut u8, self.length as usize) }
    }

    /// convert the buffer to a mutable slice
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { from_raw_parts_mut(self.buf as *mut u8, self.length as usize) }
    }

    /// fill the buffer with the given value
    pub fn fill(&mut self, val: u8) {
        unsafe {
            std::ptr::write_bytes(
                self.as_mut_slice().as_ptr() as *mut u8,
                val,
                self.length,
            )
        }
    }

    /// Allocate a buffer suitable for IO (wired and backed by huge page memory)
    pub fn new(size: usize, alignment: u8) -> Result<Self, DmaError> {
        let buf;
        unsafe {
            buf = spdk_dma_zmalloc(
                size,
                1 << alignment as usize,
                std::ptr::null_mut(),
            )
        };

        if buf.is_null() {
            Err(DmaError::Alloc {})
        } else {
            Ok(DmaBuf {
                buf,
                length: size,
            })
        }
    }

    /// Return length of the allocated buffer.
    pub fn len(&self) -> usize {
        self.length
    }

    /// Returns if the length of the allocated buffer is empty.
    /// Pretty useless but the best friends len and is_empty cannot be parted.
    pub fn is_empty(&self) -> bool {
        self.length == 0
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
    }
}
