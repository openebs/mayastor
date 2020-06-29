use std::fmt::Debug;

use serde::export::{fmt::Error, Formatter};

use spdk_sys::{spdk_io_channel, spdk_put_io_channel};

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

    fn thread_name(&self) -> &str {
        unsafe {
            std::ffi::CStr::from_ptr(&(*(*self.0).thread).name[0])
                .to_str()
                .unwrap()
        }
    }
}

impl Drop for IoChannel {
    fn drop(&mut self) {
        // temporarily comment out the trace message as it floods the test logs
        // (1 per rebuild IO)
        // trace!("[D] {:?}", self);
        unsafe { spdk_put_io_channel(self.0) }
    }
}

impl Debug for IoChannel {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(
            f,
            "io channel {:p} on thread {} to bdev {}",
            self.0,
            self.thread_name(),
            self.name()
        )
    }
}
