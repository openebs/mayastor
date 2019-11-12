use spdk_sys::{spdk_event, spdk_event_allocate, spdk_event_call};
use std::os::raw::c_void;

pub struct Event {
    /// pointer to the allocated event
    inner: *mut spdk_event,
}

pub type EventFn = extern "C" fn(*mut c_void, *mut c_void);

impl Event {
    /// create a new event that can be called later by the reactor
    pub(crate) fn new<X>(
        core: u32,
        start_fn: EventFn,
        argx: Box<X>,
    ) -> Option<Self> {
        let inner = unsafe {
            spdk_event_allocate(
                core,
                Some(start_fn),
                Box::into_raw(argx) as *mut _,
                std::ptr::null_mut(),
            )
        };

        if inner.is_null() {
            None
        } else {
            Some(Self {
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
