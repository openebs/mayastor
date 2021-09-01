use spdk_sys::{spdk_io_channel, spdk_io_channel_get_ctx_hpl};
use std::{marker::PhantomData, ptr::NonNull};

/// Wrapper for SPDK `spdk_io_channel` structure.
pub struct IoChannel<ChannelData> {
    inner: NonNull<spdk_io_channel>,
    _cd: PhantomData<ChannelData>,
}

impl<ChannelData> IoChannel<ChannelData> {
    /// Returns a channel data instance that this I/O channel owns.
    pub fn channel_data<'a>(&self) -> &'a ChannelData {
        unsafe {
            &*(spdk_io_channel_get_ctx_hpl(self.inner.as_ptr())
                as *mut ChannelData)
        }
    }

    /// Makes a new `IoChannel` wrapper from a raw SPDK structure pointer.
    pub(crate) fn from_ptr(ptr: *mut spdk_io_channel) -> Self {
        Self {
            inner: NonNull::new(ptr).unwrap(),
            _cd: Default::default(),
        }
    }
}
