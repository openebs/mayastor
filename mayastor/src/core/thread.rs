use std::ffi::CString;

use snafu::Snafu;

use spdk_sys::{
    spdk_set_thread,
    spdk_thread,
    spdk_thread_create,
    spdk_thread_destroy,
    spdk_thread_exit,
    spdk_thread_is_exited,
    spdk_thread_poll,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Event spawned from a non-spdk thread"))]
    InvalidThread {},
}

#[derive(Debug, Clone, Copy)]
/// struct that wraps an SPDK thread. The name thread is chosen poorly and
/// should not be confused with an actual thread. Consider it more to be
/// analogous to a container to which you can submit work and poll it to drive
/// the submitted work to completion.
pub struct Sthread(pub(crate) *mut spdk_thread);

unsafe impl Send for Sthread {}
unsafe impl Sync for Sthread {}

impl Sthread {
    ///
    /// With the given thread as context, execute the closure on that thread.
    ///
    /// Any function can be executed here however, this should typically be used
    /// to execute functions that reference any FFI to SPDK.

    pub fn new(name: String) -> Option<Self> {
        let name = CString::new(name).unwrap();
        let t =
            unsafe { spdk_thread_create(name.as_ptr(), std::ptr::null_mut()) };
        Self::from_null_checked(t)
    }
    ///
    /// # Note
    ///
    /// Avoid any blocking calls as it will block the reactor, and avoid
    /// long-running functions in general follow the nodejs event loop
    /// model, and you should be good.
    pub fn with<F: FnOnce()>(self, f: F) -> Self {
        assert_eq!(
            unsafe { spdk_sys::spdk_get_thread() },
            std::ptr::null_mut()
        );
        f();
        self.poll();
        self
    }

    pub fn poll(self) -> Self {
        let mut done = false;
        while !done {
            let rc = unsafe { spdk_thread_poll(self.0, 0, 0) };
            if rc < 1 {
                done = true
            }
        }
        self
    }

    #[inline]
    pub fn enter(self) -> Self {
        unsafe { spdk_set_thread(self.0) };
        self
    }

    #[inline]
    pub fn exit(self) {
        unsafe { spdk_set_thread(std::ptr::null_mut()) };
    }

    /// destroy the given thread waiting for it to become ready to destroy
    pub fn destroy(self) {
        debug!("destroying thread...{:p}", self.0);
        unsafe {
            spdk_set_thread(self.0);
            // set that we *want* to exit, but we have not exited yet
            spdk_thread_exit(self.0);

            // no wait until the thread is actually excited the internal
            // state is updated by spdk_thread_poll()
            while !spdk_thread_is_exited(self.0) {
                spdk_thread_poll(self.0, 0, 0);
            }
            spdk_thread_destroy(self.0);
        }

        debug!("thread {:p} destroyed", self.0);
    }

    pub fn inner(self) -> *const spdk_thread {
        self.0
    }

    pub fn inner_mut(self) -> *mut spdk_thread {
        self.0
    }

    pub fn from_null_checked(t: *mut spdk_thread) -> Option<Self> {
        if t.is_null() {
            None
        } else {
            Some(Sthread(t))
        }
    }
}
