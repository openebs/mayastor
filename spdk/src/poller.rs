use std::{
    ffi::{c_void, CString},
    ptr::NonNull,
    time::Duration,
};

use crate::{cpu_cores::Cores, ffihelper::IntoCString};
use spdk_sys::{
    spdk_poller,
    spdk_poller_fn,
    spdk_poller_pause,
    spdk_poller_register,
    spdk_poller_register_named,
    spdk_poller_resume,
    spdk_poller_unregister,
};

/// TODO
struct PollerData<'a, PollerContext: 'a> {
    name: Option<CString>,
    context: Box<PollerContext>,
    poll_fn: Box<dyn Fn(&PollerContext) -> i32 + 'a>,
}

/// Poller structure that allows us to pause, stop, resume periodic tasks
pub struct Poller<'a, PollerContext: 'a> {
    inner: NonNull<spdk_poller>,
    data: Box<PollerData<'a, PollerContext>>,
}

impl<'a, PollerContext: 'a> Poller<'a, PollerContext> {
    /// Consumers the poller instance and stops it.
    pub fn stop(self) {
        std::mem::drop(self);
    }

    /// Pauses the poller.
    pub fn pause(&self) {
        unsafe {
            spdk_poller_pause(self.inner.as_ptr());
        }
    }

    /// Resumes the poller.
    pub fn resume(&self) {
        unsafe {
            spdk_poller_resume(self.inner.as_ptr());
        }
    }

    /// Returns poller's context.
    pub fn context(&self) -> &PollerContext {
        self.data.context.as_ref()
    }
}

impl<'a, PollerContext: 'a> Drop for Poller<'a, PollerContext> {
    fn drop(&mut self) {
        dbgln!(Poller, ""; "dropped");
        unsafe {
            let mut ptr: *mut spdk_poller = self.inner.as_ptr();
            spdk_poller_unregister(&mut ptr);
        }
    }
}

/// TODO
unsafe extern "C" fn inner_poller_cb<'a, PollerContext: 'a>(
    ctx: *mut c_void,
) -> i32 {
    let data = &*(ctx as *mut PollerData<PollerContext>);
    (data.poll_fn)(data.context.as_ref());
    0
}

/// Builder type to create a new poller.
pub struct PollerBuilder<'a, PollerContext> {
    name: Option<CString>,
    context: Option<Box<PollerContext>>,
    poll_fn: Option<Box<dyn Fn(&PollerContext) -> i32 + 'a>>,
    interval: std::time::Duration,
}

impl<'a, PollerContext> Default for PollerBuilder<'a, PollerContext> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a, PollerContext> PollerBuilder<'a, PollerContext> {
    /// Creates a new nameless poller that runs every time the thread the poller
    /// is created on is polled.
    pub fn new() -> Self {
        Self {
            name: None,
            context: None,
            poll_fn: None,
            interval: Duration::from_micros(0),
        }
    }

    /// Sets poller name.
    pub fn with_name(mut self, name: &str) -> Self {
        self.name = Some(String::from(name).into_cstring());
        self
    }

    /// Sets the poller context instance.
    pub fn with_context(mut self, ctx: Box<PollerContext>) -> Self {
        self.context = Some(ctx);
        self
    }

    /// Sets the pool function for this poller.
    pub fn with_poll_fn(
        mut self,
        poll_fn: impl Fn(&PollerContext) -> i32 + 'a,
    ) -> Self {
        self.poll_fn = Some(Box::new(poll_fn));
        self
    }

    /// Sets the polling interval for this poller.
    pub fn with_interval(mut self, usec: u64) -> Self {
        self.interval = Duration::from_micros(usec);
        self
    }

    /// Consumes a `PollerBuild` instance and registers a new poller within
    /// SPDK.
    pub fn build(self) -> Poller<'a, PollerContext> {
        // Create a new `PollerData` instance.
        let mut data = Box::new(PollerData {
            name: self.name,
            context: self.context.expect("Poller context must be set"),
            poll_fn: self.poll_fn.expect("Poller function must be set"),
        });

        let pf: spdk_poller_fn = Some(inner_poller_cb::<PollerContext>);
        let pd = data.as_mut() as *mut PollerData<_> as *mut c_void;
        let t = self.interval.as_micros() as u64;

        // Register the poller.
        let inner = unsafe {
            match &data.name {
                None => spdk_poller_register(pf, pd, t),
                Some(s) => spdk_poller_register_named(pf, pd, t, s.as_ptr()),
            }
        };

        Poller {
            inner: NonNull::new(inner).unwrap(),
            data,
        }
    }
}
