use std::{
    ffi::{c_void, CString},
    ptr::NonNull,
    time::Duration,
};

use crate::ffihelper::IntoCString;

use spdk_sys::{
    spdk_poller,
    spdk_poller_fn,
    spdk_poller_pause,
    spdk_poller_register,
    spdk_poller_register_named,
    spdk_poller_resume,
    spdk_poller_unregister,
};

/// A structure for poller context.
struct Context<'a, PollerData: 'a> {
    name: Option<CString>,
    data: PollerData,
    poll_fn: Box<dyn Fn(&PollerData) -> i32 + 'a>,
}

/// Poller structure that allows us to pause, stop, resume periodic tasks
pub struct Poller<'a, PollerData: 'a> {
    inner: NonNull<spdk_poller>,
    ctx: Box<Context<'a, PollerData>>,
}

impl<'a, PollerData: 'a> Poller<'a, PollerData> {
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

    /// Returns a reference to poller's data object.
    pub fn data(&self) -> &PollerData {
        &self.ctx.data
    }
}

impl<'a, PollerData: 'a> Drop for Poller<'a, PollerData> {
    fn drop(&mut self) {
        unsafe {
            let mut ptr: *mut spdk_poller = self.inner.as_ptr();
            spdk_poller_unregister(&mut ptr);
        }
    }
}

/// TODO
unsafe extern "C" fn inner_poller_cb<'a, PollerData: 'a>(
    ctx: *mut c_void,
) -> i32 {
    let ctx = &*(ctx as *mut Context<PollerData>);
    (ctx.poll_fn)(&ctx.data);
    0
}

/// Builder type to create a new poller.
pub struct PollerBuilder<'a, PollerData> {
    name: Option<CString>,
    data: Option<PollerData>,
    poll_fn: Option<Box<dyn Fn(&PollerData) -> i32 + 'a>>,
    interval: std::time::Duration,
}

impl<'a, PollerData> Default for PollerBuilder<'a, PollerData> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a, PollerData> PollerBuilder<'a, PollerData> {
    /// Creates a new nameless poller that runs every time the thread the poller
    /// is created on is polled.
    pub fn new() -> Self {
        Self {
            name: None,
            data: None,
            poll_fn: None,
            interval: Duration::from_micros(0),
        }
    }

    /// Sets optional poller name.
    pub fn with_name(mut self, name: &str) -> Self {
        self.name = Some(String::from(name).into_cstring());
        self
    }

    /// Sets the poller data instance.
    /// This Poller parameter is manadory.
    pub fn with_data(mut self, data: PollerData) -> Self {
        self.data = Some(data);
        self
    }

    /// Sets the poll function for this poller.
    /// This Poller parameter is manadory.
    pub fn with_poll_fn(
        mut self,
        poll_fn: impl Fn(&PollerData) -> i32 + 'a,
    ) -> Self {
        self.poll_fn = Some(Box::new(poll_fn));
        self
    }

    /// Sets the polling interval for this poller.
    pub fn with_interval(mut self, usec: u64) -> Self {
        self.interval = Duration::from_micros(usec);
        self
    }

    /// Consumes a `PollerBuilder` instance, and registers a new poller within
    /// SPDK.
    pub fn build(self) -> Poller<'a, PollerData> {
        // Create a new `PollerData` instance.
        let mut data = Box::new(Context {
            name: self.name,
            data: self.data.expect("Poller data must be set"),
            poll_fn: self.poll_fn.expect("Poller function must be set"),
        });

        let pf: spdk_poller_fn = Some(inner_poller_cb::<PollerData>);
        let pd = data.as_mut() as *mut Context<_> as *mut c_void;
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
            ctx: data,
        }
    }
}
