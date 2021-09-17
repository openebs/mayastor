use std::{marker::PhantomData, ptr::NonNull};

use spdk_sys::{
    spdk_io_channel,
    spdk_io_channel_get_ctx_hpl,
    spdk_io_channel_iter,
    spdk_io_channel_iter_get_channel,
};

/// Wrapper for SPDK `spdk_io_channel` structure.
#[derive(Debug)]
pub struct IoChannel<ChannelData> {
    inner: NonNull<spdk_io_channel>,
    _cd: PhantomData<ChannelData>,
}

impl<ChannelData> IoChannel<ChannelData> {
    /// Returns a reference to the channel data instance that this I/O channel
    /// owns.
    pub fn channel_data(&self) -> &ChannelData {
        unsafe {
            &*(spdk_io_channel_get_ctx_hpl(self.inner.as_ptr())
                as *mut ChannelData)
        }
    }

    /// Returns a mutable reference to the channel data instance that this I/O
    /// channel owns.
    pub fn channel_data_mut(&mut self) -> &mut ChannelData {
        unsafe {
            &mut *(spdk_io_channel_get_ctx_hpl(self.inner.as_ptr())
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

    /// Makes a new `IoChannel` wrapper from a raw `spdk_io_channel_iter`
    /// pointer.
    pub fn from_iter(ptr: *mut spdk_io_channel_iter) -> Self {
        let io_chan = unsafe { spdk_io_channel_iter_get_channel(ptr) };
        Self::from_ptr(io_chan)
    }
}

impl<ChannelData> Clone for IoChannel<ChannelData> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner,
            _cd: Default::default(),
        }
    }
}
