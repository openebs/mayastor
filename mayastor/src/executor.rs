//! Future executor and a couple of related utility functions.
//! The executor is started on current thread and saved to TLS.
//! It is a local single-threaded executor.
//! Any code wishing to spawn a future and runnig on the same thread
//! can obtain the spawner from TLS and spawn it.

use futures::{
    channel::oneshot,
    executor::{LocalPool, LocalSpawner},
};
use libc::c_void;
use spdk_sys::{spdk_poller, spdk_poller_register, spdk_poller_unregister};
use std::{cell::RefCell, fmt, thread};

thread_local! {
    static LOCAL_SPAWNER: RefCell<Option<LocalSpawner>> = RefCell::new(None);
    static EXECUTOR_POLLER: RefCell<Option<*mut spdk_poller>> = RefCell::new(None);
}

/// Run whatever work might be queued for the executor without blocking.
extern "C" fn tick(ctx: *mut c_void) -> i32 {
    let mut pool: Box<futures::executor::LocalPool> =
        unsafe { Box::from_raw(ctx as *mut futures::executor::LocalPool) };
    // Tasks which are generated while the executor runs are left in the queue
    // until the tick() is called again.
    let _work = pool.try_run_one();
    std::mem::forget(pool);
    0
}

/// Start future executor and register its poll method with spdk so that the
/// tasks can make steady progress.
pub fn start_executor() {
    let pool = Box::new(LocalPool::new());
    let spawner = pool.spawner();
    let pool_ptr = Box::into_raw(pool) as *mut c_void;
    let executor_poller = unsafe {
        spdk_poller_register(Some(tick), pool_ptr as *mut c_void, 1000)
    };
    EXECUTOR_POLLER.with(|poller| {
        if (*poller.borrow()).is_some() {
            panic!("Executor was already started on this thread");
        }
        *poller.borrow_mut() = Some(executor_poller);
    });
    LOCAL_SPAWNER.with(|local_spawner| {
        if (*local_spawner.borrow()).is_some() {
            panic!("Executor was already started on this thread");
        }
        *local_spawner.borrow_mut() = Some(spawner);
    });
    debug!(
        "Started future executor on thread {:?}",
        thread::current().id()
    );
}

/// Return task spawner for executor started on current thread.
pub fn get_spawner() -> LocalSpawner {
    LOCAL_SPAWNER.with(|local_spawner| match &*local_spawner.borrow() {
        Some(spawner) => spawner.clone(),
        None => panic!("Executor was not started on this thread"),
    })
}

/// Deallocate executor.
pub fn stop_executor() {
    debug!(
        "Stopping future executor on thread {:?}",
        thread::current().id()
    );

    crate::nvmf_target::NVMF_TGT.with(move |nvmf_tgt| {
        let _ = nvmf_tgt.borrow_mut().take();
    });

    EXECUTOR_POLLER.with(|poller| {
        let poller = poller.borrow_mut().take();
        match poller {
            Some(mut poller) => unsafe {
                // unregister will write NULL pointer to poller but that should
                // be ok as we won't be using poller pointer anymore
                spdk_poller_unregister(&mut poller)
            },
            None => panic!("Executor was not started on this thread"),
        }
    });
    LOCAL_SPAWNER.with(|local_spawner| {
        // free the spawner
        let _ = local_spawner.borrow_mut().take();
    });
}

/// Construct callback argument for spdk async function.
/// The argument is a oneshot sender channel for result of the operation.
pub fn cb_arg<T>(sender: oneshot::Sender<T>) -> *mut c_void {
    Box::into_raw(Box::new(sender)) as *const _ as *mut c_void
}

/// Generic callback for spdk async functions expecting to be called with
/// single argument which is a sender channel to notify the other end about
/// the result.
pub extern "C" fn complete_callback_1<T>(sender_ptr: *mut c_void, val: T)
where
    T: fmt::Debug,
{
    let sender =
        unsafe { Box::from_raw(sender_ptr as *mut oneshot::Sender<T>) };
    sender.send(val).expect("Receiver is gone");
}
