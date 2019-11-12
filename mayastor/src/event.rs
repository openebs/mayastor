use crate::bdev::nexus::Error;
use spdk_sys::{spdk_event, spdk_event_allocate, spdk_event_call};
use std::os::raw::c_void;

pub struct Event {
    /// pointer to the allocated event
    inner: *mut spdk_event,
}

pub type EventFn = extern "C" fn(*mut c_void, *mut c_void);

impl Event {
    /// create a new event that can be called later by the reactor. T will be
    /// forgotten and passed over to FFI. If this function returns an error,
    /// T is implicitly dropped as it consumes T when called.
    pub(crate) fn new<T>(
        core: u32,
        start_fn: EventFn,
        argx: Box<T>,
    ) -> Result<Self, Error> {
        let ptr = Box::into_raw(argx);
        let inner = unsafe {
            spdk_event_allocate(
                core,
                Some(start_fn),
                ptr as *mut _,
                std::ptr::null_mut(),
            )
        };

        if inner.is_null() {
            // take a hold of the data again to ensure it is dropped
            let _ = unsafe { Box::from_raw(ptr) };
            Err(Error::Internal("failed to allocate event".into()))
        } else {
            Ok(Self {
                inner,
            })
        }
    }

    /// call the event (or more accurately add it to the reactor) when called
    /// the event is put back into the pool
    pub fn call(self) {
        unsafe { spdk_event_call(self.inner) }
    }
}
