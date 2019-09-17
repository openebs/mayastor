//! Future executor and a couple of related utility functions.
//! The executor is started on current thread and saved to TLS.
//! It is a local single-threaded executor.
//! Any code wishing to spawn a future and runnig on the same thread
//! can obtain the spawner for executor in TLS and spawn it.

use futures::{
    channel::oneshot,
    executor::{LocalPool, LocalSpawner},
    future::Future,
    task::LocalSpawnExt,
};
use libc::c_void;
use spdk_sys::{spdk_poller, spdk_poller_register, spdk_poller_unregister};
use std::{cell::RefCell, fmt, ptr, thread};

/// Everything we need to store to thread local storage (kinda global state) to
/// manage and dispatch tasks to executor.
struct ExecutorCtx {
    pool: LocalPool,
    poller: *mut spdk_poller,
}

thread_local! {
    /// Executor context.
    static EXECUTOR_CTX: RefCell<Option<ExecutorCtx>> = RefCell::new(None);
    /// Shutdown callback. If set, the executor poller is unregistered and
    /// the executor destroyed. The callback is called afterwards.
    ///
    /// NOTE: The reason why shutdown cb cannot be part of executor context
    /// above is that we need to be able to set it from a future executing on
    /// the executor. If it was placed in executor ctx it would result in
    /// double mutable reference.
    static SHUTDOWN_CB: RefCell<Option<Box<dyn FnOnce()>>> = RefCell::new(None);
}

/// Run whatever work might be queued for the executor without blocking.
/// Or shutdown executor if "global" shutdown callback is set.
extern "C" fn tick(_ctx: *mut c_void) -> i32 {
    let shutdown_cb_maybe = SHUTDOWN_CB.with(|cb| cb.borrow_mut().take());

    EXECUTOR_CTX.with(move |ctx| {
        if let Some(shutdown_cb) = shutdown_cb_maybe {
            debug!(
                "Stopping future executor on thread {:?}",
                thread::current().id()
            );
            match ctx.borrow_mut().take() {
                Some(mut ctx) => unsafe {
                    // unregister will write NULL pointer to poller but that
                    // should be ok as we won't be using
                    // poller pointer anymore
                    spdk_poller_unregister(&mut ctx.poller)
                },
                None => panic!("Executor was not started on this thread"),
            }
            shutdown_cb();
        } else {
            match &mut *ctx.borrow_mut() {
                Some(ctx) => {
                    // Tasks which are generated while the executor runs are
                    // left in the queue until the tick() is
                    // called again.
                    let _work = ctx.pool.try_run_one();
                }
                None => panic!(
                    "tick was called while the executor has been shut down"
                ),
            }
        }
    });
    0
}

/// Start future executor and register its poll method with spdk so that the
/// tasks can make steady progress.
pub fn start_executor() {
    EXECUTOR_CTX.with(|ctx| {
        if (*ctx.borrow()).is_some() {
            panic!("Executor was already started on this thread");
        }

        let pool = LocalPool::new();
        let poller =
            unsafe { spdk_poller_register(Some(tick), ptr::null_mut(), 1000) };

        *ctx.borrow_mut() = Some(ExecutorCtx {
            pool,
            poller,
        });
    });

    debug!(
        "Started future executor on thread {:?}",
        thread::current().id()
    );
}

/// Return task spawner for executor started on current thread.
pub fn get_spawner() -> LocalSpawner {
    EXECUTOR_CTX.with(|ctx| match &*ctx.borrow() {
        Some(ctx) => ctx.pool.spawner(),
        None => panic!("Executor was not started on this thread"),
    })
}

/// Stop and deallocate executor but only after the provided future has
/// completed. When done, call the provided callback function.
pub fn stop_executor<T, F>(fut: T, cb: Box<F>)
where
    T: Future<Output = ()> + 'static,
    F: FnOnce() + 'static,
{
    // Chain provided future with a code indicating that the future has
    // completed.
    let fut = async {
        fut.await;
        SHUTDOWN_CB.with(|shutdown_cb| *shutdown_cb.borrow_mut() = Some(cb));
    };
    get_spawner().spawn_local(fut).unwrap();
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
