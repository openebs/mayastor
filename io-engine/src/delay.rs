use std::{cell::RefCell, os::raw::c_void, time::Duration};

use spdk_rs::libspdk::{
    spdk_poller,
    spdk_poller_register,
    spdk_poller_unregister,
};

thread_local! {
    /// Delay poller pointer for unregistering the poller at the end
    static DELAY_POLLER: RefCell<Option<*mut spdk_poller>> = RefCell::new(None);
}

/// Delay function called from the spdk poller to prevent draining of cpu
/// in cases when performance is not a priority (i.e. unit tests).
extern "C" fn sleep(_ctx: *mut c_void) -> i32 {
    std::thread::sleep(Duration::from_millis(1));
    0
}

/// Start delaying reactor every 1ms by 1ms. It blocks the thread for a
/// short moment so it is not able to perform any useful work when sleeping.
pub fn register() {
    warn!("*** Delaying reactor every 1ms by 1ms ***");
    let delay_poller = unsafe {
        spdk_poller_register(Some(sleep), std::ptr::null_mut(), 1000)
    };
    DELAY_POLLER.with(move |poller_cell| {
        let mut poller_maybe = poller_cell.try_borrow_mut().unwrap();
        if poller_maybe.is_some() {
            panic!("Delay poller registered twice");
        }
        *poller_maybe = Some(delay_poller);
    });
}

// By unregistering the delay poller we avoid a warning about unregistered
// poller at the end.
pub fn unregister() {
    DELAY_POLLER.with(move |poller_cell| {
        let poller_maybe = poller_cell.try_borrow_mut().unwrap().take();
        if let Some(mut poller) = poller_maybe {
            unsafe { spdk_poller_unregister(&mut poller) };
        }
    });
}
