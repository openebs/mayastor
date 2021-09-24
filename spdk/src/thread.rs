use std::{ffi::CStr, ptr::NonNull};

use spdk_sys::{spdk_get_thread, spdk_thread, spdk_thread_get_name};

/// Wrapper for `spdk_thread`.
pub struct Thread {
    inner: NonNull<spdk_thread>,
}

impl Thread {
    /// Gets a handle to the current thread.
    /// Returns an SPDK thread wrapper instance if this is an SPDK thread,
    /// or `None` otherwise.
    pub fn current() -> Option<Self> {
        let thread = unsafe { spdk_get_thread() };
        if thread.is_null() {
            None
        } else {
            Some(Self::from_ptr(thread))
        }
    }

    /// Returns thread name.
    pub fn name(&self) -> &str {
        unsafe {
            CStr::from_ptr(spdk_thread_get_name(self.as_ptr()))
                .to_str()
                .unwrap()
        }
    }

    /// TODO
    pub(crate) fn from_ptr(ptr: *mut spdk_thread) -> Self {
        Self {
            inner: NonNull::new(ptr).unwrap(),
        }
    }

    /// Returns a pointer to the underlying `spdk_thread` structure.
    pub(crate) fn as_ptr(&self) -> *mut spdk_thread {
        self.inner.as_ptr()
    }
}
