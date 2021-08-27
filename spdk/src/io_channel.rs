use spdk_sys::{spdk_io_channel, spdk_io_channel_get_ctx_hpl};
use std::{marker::PhantomData, ptr::NonNull};

/// Wrapper for SPDK `spdk_io_channel` structure.
pub struct IoChannel<ChannelData: Sized> {
    inner: NonNull<spdk_io_channel>,
    _cd: PhantomData<ChannelData>,
}

impl<ChannelData: Sized> IoChannel<ChannelData> {
    /// Makes a new `IoChannel` instance from a raw SPDK structure pointer.
    pub(crate) fn new(raw_chan: *mut spdk_io_channel) -> Self {
        Self {
            inner: NonNull::new(raw_chan).unwrap(),
            _cd: Default::default(),
        }
    }

    /// Returns a channel data instance that this I/O channel owns.
    pub fn channel_data<'a>(&self) -> &'a ChannelData {
        unsafe {
            &*(spdk_io_channel_get_ctx_hpl(self.inner.as_ptr())
                as *mut ChannelData)
        }
    }

    /// TODO
    pub fn dbg(&self) -> String {
        format!("IoChan[dev '{:p}']", unsafe { self.inner.as_ref().dev })
    }
}
