use std::{
    fmt::{Debug, Error, Formatter},
    ptr::NonNull,
};

use spdk_rs::libspdk::{
    spdk_io_channel,
    spdk_io_channel_get_io_device_name,
    spdk_put_io_channel,
    spdk_thread_get_name,
};

/// TODO
pub struct IoChannel(NonNull<spdk_io_channel>);

impl From<*mut spdk_io_channel> for IoChannel {
    fn from(channel: *mut spdk_io_channel) -> Self {
        IoChannel(NonNull::new(channel).expect("channel ptr is null"))
    }
}

impl IoChannel {
    /// return the ptr
    pub fn as_ptr(&self) -> *mut spdk_io_channel {
        self.0.as_ptr()
    }

    /// return the name of the io channel which is used to register the device,
    /// this can either be a string containing the pointer address, or an
    /// actual name
    fn name(&self) -> &str {
        unsafe {
            std::ffi::CStr::from_ptr(spdk_io_channel_get_io_device_name(
                self.0.as_ptr(),
            ))
            .to_str()
            .unwrap()
        }
    }

    fn thread_name(&self) -> &str {
        unsafe {
            std::ffi::CStr::from_ptr(spdk_thread_get_name(
                self.0.as_ref().thread,
            ))
            .to_str()
            .unwrap()
        }
    }
}

impl Drop for IoChannel {
    fn drop(&mut self) {
        unsafe { spdk_put_io_channel(self.0.as_ptr()) }
    }
}

impl Debug for IoChannel {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(
            f,
            "io channel {:p} on thread {} to bdev {}",
            self.0.as_ptr(),
            self.thread_name(),
            self.name()
        )
    }
}
