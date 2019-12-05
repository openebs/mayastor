use crate::bdev::nexus::Error;
use spdk_sys::{
    spdk_event_allocate,
    spdk_event_call,
    spdk_get_thread,
    spdk_set_thread,
    spdk_thread,
    spdk_thread_create,
    spdk_thread_destroy,
    spdk_thread_exit,
    spdk_thread_poll,
};

/// trait that ensures we can get the context passed to FFI threads
pub trait MayaCtx {
    type Item;
    fn into_ctx<'a>(arg: *mut c_void) -> &'a mut Self::Item;
}

use std::os::raw::c_void;

pub type EventFn = extern "C" fn(*mut c_void, *mut c_void);

struct Mthread(*mut spdk_thread);

impl Drop for Mthread {
    fn drop(&mut self) {
        unsafe {
            if !self.0.is_null() {
                spdk_thread_exit(self.0);
                spdk_thread_destroy(self.0);
            }
        }
    }
}

///
/// spawn closure `F` on the reactor running on core `core`. This function must
/// be called within the context of the reactor. This is verified at runtime, to
/// accidental mistakes.
///
/// Async closures are not supported (yet) as there is only a single executor on
/// core 0
pub fn spawn_on_core<T, F>(
    core: u32,
    arg: Box<T>,
    f: F,
) -> Result<Box<T>, Error>
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
        return Err(Error::InvalidThread);
    }

    let ptr = Box::into_raw(Box::new(f)) as *mut c_void;
    let arg_ptr = &*arg as *const _ as *mut c_void;
    let event = unsafe {
        spdk_event_allocate(core, Some(unwrap::<F, T>), ptr, arg_ptr)
    };

    if event.is_null() {
        panic!("failed to allocate event");
    }
    unsafe { spdk_event_call(event) };
    Ok(arg)
}

/// create a new thread, on which core is unpredictable right now. Once the
/// thread is created run the closure within that context of that thread until
/// the reactor is empty. Once all events are processed the thread is
/// destroyed.
pub fn spawn_thread<F>(f: F) -> Result<(), Error>
where
    F: FnOnce(),
{
    let thread = Mthread(unsafe {
        spdk_thread_create(std::ptr::null_mut(), std::ptr::null_mut())
    });

    if thread.0.is_null() {
        return Err(Error::InvalidThread);
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

    Ok(())
}

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
