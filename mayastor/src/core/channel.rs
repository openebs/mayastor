use serde::export::{fmt::Error, Formatter};
use spdk_sys::{spdk_io_channel, spdk_put_io_channel};
use std::fmt::Debug;

pub struct IoChannel(*mut spdk_io_channel);

impl IoChannel {
    pub fn from_null_checked(ch: *mut spdk_io_channel) -> Option<IoChannel> {
        if ch.is_null() {
            None
        } else {
            Some(IoChannel(ch))
        }
    }

    /// return the ptr
    pub fn as_ptr(&self) -> *mut spdk_io_channel {
        self.0
    }

    /// return the name of the io channel which is used to register the device,
    /// this can either be a string containing the pointer address (?) an
    /// actual name
    fn name(&self) -> &str {
        unsafe {
            // struct is opaque
            std::ffi::CStr::from_ptr(
                (*self.0)
                    .dev
                    .add(std::mem::size_of::<*mut spdk_io_channel>())
                    as *mut i8,
            )
            .to_str()
            .unwrap()
        }
    }
}

impl Drop for IoChannel {
    fn drop(&mut self) {
        trace!("[D] {:?}", self);
        unsafe { spdk_put_io_channel(self.0) }
    }
}

impl Debug for IoChannel {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "io channel {:p} to bdev {}", self.0, self.name())
    }
}
