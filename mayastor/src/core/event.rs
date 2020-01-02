use snafu::Snafu;
use spdk_sys::{
    spdk_event_allocate,
    spdk_event_call,
    spdk_get_thread,
    spdk_set_thread,
    spdk_thread,
    spdk_thread_create,
    spdk_thread_poll,
};
use std::os::raw::c_void;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Event spawned from a non-spdk thread"))]
    InvalidThread {},
}

/// trait that ensures we can get the context passed to FFI threads
pub trait MayaCtx {
    type Item;
    fn into_ctx<'a>(arg: *mut c_void) -> &'a mut Self::Item;
}

pub type EventFn = extern "C" fn(*mut c_void, *mut c_void);

#[derive(Debug, Copy, Clone)]
/// struct that wraps an SPDK thread. The name thread is chosen poorly and
/// should not be confused with an actual thread. Consider it more to be
/// analogous to a container to which you can submit work and poll it to drive
/// the submitted work to completion.
pub struct Mthread(*mut spdk_thread);

unsafe impl Send for Mthread {}
unsafe impl Sync for Mthread {}

impl Mthread {
    ///
    /// With the given thread as context, execute the closure on that thread.
    ///
    /// Any function can be executed here however, this should typically be used
    /// to execute functions that reference any FFI functions from SPDK.
    ///
    /// # Note
    ///
    /// Avoid any blocking calls as it will block the reactor, and avoid
    /// long-running functions in general follow the nodejs event loop
    /// model, and you should be good.
    pub fn with<F: FnOnce()>(self, f: F) -> Self {
        unsafe { spdk_set_thread(self.0) };

        f();
        let mut done = false;

        while !done {
            let rc = unsafe { spdk_thread_poll(self.0, 0, 0) };
            if rc < 1 {
                done = true
            }
        }
        unsafe { spdk_set_thread(std::ptr::null_mut()) };
        self
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
            Some(Mthread(t))
        }
    }
}

/// spawn closure `F` on the reactor running on core `core`. This function must
/// be called within the context of the reactor. This is verified at runtime, to
/// avoid accidental mistakes. Consumes `arg`
pub fn spawn_on_core<T, F>(core: u32, arg: T, f: F) -> Result<(), Error>
where
    T: MayaCtx,
    F: FnOnce(&mut T::Item),
{
    extern "C" fn unwrap<F, T>(f: *mut c_void, t: *mut c_void)
    where
        F: FnOnce(&mut T::Item),
        T: MayaCtx,
    {
        unsafe {
            let f: Box<F> = Box::from_raw(f as *mut F);
            let arg = T::into_ctx(t);
            f(arg)
        }
    }

    let thread = { unsafe { spdk_get_thread() } };

    if thread.is_null() {
        return Err(Error::InvalidThread {});
    }

    let ptr = Box::into_raw(Box::new(f)) as *mut c_void;
    let arg_ptr = Box::into_raw(Box::new(arg)) as *mut c_void;

    let event = unsafe {
        spdk_event_allocate(core, Some(unwrap::<F, T>), ptr, arg_ptr)
    };

    if event.is_null() {
        panic!("failed to allocate event");
    }

    unsafe { spdk_event_call(event) };
    Ok(())
}

/// Create a new thread, the core that will execute the thread will be chosen in
/// a RR fashion. Once created, the closure `F` is executed within the context
/// of that thread. Once all events in the context of that thread have been
/// processed, the execution context will return.
pub fn spawn_thread<F>(f: F) -> Result<Mthread, Error>
where
    F: FnOnce(),
{
    let thread = Mthread(unsafe {
        spdk_thread_create(std::ptr::null_mut(), std::ptr::null_mut())
    });

    if thread.0.is_null() {
        return Err(Error::InvalidThread {});
    }

    unsafe { spdk_set_thread(thread.0) };

    f();
    let mut done = false;

    while !done {
        let rc = unsafe { spdk_thread_poll(thread.0, 0, 0) };
        if rc < 1 {
            done = true
        }
    }

    Ok(thread)
}

/// allocate an event on the core `core` and execute `f` on it.
pub fn on_core<F: FnOnce()>(core: u32, f: F) {
    extern "C" fn unwrap<F>(args: *mut c_void, _arg2: *mut c_void)
    where
        F: FnOnce(),
    {
        unsafe {
            let f: Box<F> = Box::from_raw(args as *mut F);
            f()
        }
    }

    let ptr = Box::into_raw(Box::new(f)) as *mut c_void;
    let event = unsafe {
        spdk_event_allocate(core, Some(unwrap::<F>), ptr, std::ptr::null_mut())
    };

    if event.is_null() {
        panic!("failed to allocate event");
    }
    unsafe { spdk_event_call(event) }
}
