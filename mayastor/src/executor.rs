//! Future executor and a couple of related utility functions.
//! The executor is started on current thread and saved to TLS.
//! It is a local single-threaded executor. It is fine to use it for
//! mgmt tasks but it should not be used for IO which should scale with
//! number of cpu cores.
//!
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
use std::{
    cell::{Cell, RefCell},
    fmt, ptr, thread,
};

/// Everything we need to store to thread local storage (kinda global state) to
/// manage and dispatch tasks to executor.
///
/// More on how concurrent access to the context is guaranteed to be safe:
/// The context is stored in TLS and it is not sent between threads so the
/// concurrent access is not possible. Still we must prevent situations like:
///
/// fn A has mut ref of member 1, it calls fn B which obtains mut ref of
/// member 1 from TLS. Thus fn A has no way of knowing that the data it has
/// referenced might have been changed after fn B finished executing. Simply
/// put: mut-mut, mut-const combinations should be forbidden as always in Rust.
///
/// This can happen easily. If tick() which references executor, executes a
/// future and the future calls start, stop or spawn function. To prevent
/// getting into these unsafe situations, we have following basic rules:
///
///   1) no restrictions on start() as it is creating the context and
///      there can't be prior reference to the context.
///   2) after the executor is created the only place where mutable ref to the
///      executor may be obtained is the tick().
///   3) spawn() may only use pre-allocated spawner from the context structure.
///   4) stop() may only set shutdown_cb, but the actual release of
///      executor must be done inside the tick().
struct ExecutorCtx {
    /// The local pool executor.
    pool: RefCell<LocalPool>,
    /// The handle for spawning new futures on the executor.
    spawner: RefCell<LocalSpawner>,
    /// Spdk poller routine - the work horse of futures.
    poller: *mut spdk_poller,
    /// Shutdown callback. If set, the spdk poller for executor is unregistered
    /// and the executor destroyed in tick. The shutdown callback is called
    /// afterwards.
    shutdown_cb: Cell<Option<Box<dyn FnOnce()>>>,
}

thread_local! {
    /// Thread local executor context.
    static EXECUTOR_CTX: RefCell<Option<ExecutorCtx>> = RefCell::new(None);
}

/// Start future executor and register its poll method with spdk so that the
/// tasks can make steady progress.
pub fn start() {
    EXECUTOR_CTX.with(|ctx_cell| {
        let mut ctx_maybe = ctx_cell.try_borrow_mut().expect(
            "start executor must be called before any other executor method",
        );

        if ctx_maybe.is_some() {
            panic!(
                "Executor was already started on thread {:?}",
                thread::current().id()
            );
        }

        let pool = LocalPool::new();
        let spawner = pool.spawner();
        let poller =
            unsafe { spdk_poller_register(Some(tick), ptr::null_mut(), 1000) };

        *ctx_maybe = Some(ExecutorCtx {
            pool: RefCell::new(pool),
            poller,
            spawner: RefCell::new(spawner),
            shutdown_cb: Cell::new(None),
        });
    });

    debug!(
        "Started future executor on thread {:?}",
        thread::current().id()
    );
}

/// Spawn a future on the executor running on the same thread.
pub fn spawn<F>(f: F)
where
    F: Future<Output = ()> + 'static,
{
    EXECUTOR_CTX.with(|ctx_cell| {
        let ctx_maybe = ctx_cell.borrow();

        match ctx_maybe.as_ref() {
            // The only place we grab ref to spawner is here and since only
            // a single thread can access it, it is safe.
            Some(ctx) => ctx.spawner.borrow_mut().spawn_local(f).unwrap(),
            None => panic!(
                "Executor was not started on thread {:?}",
                thread::current().id()
            ),
        }
    })
}

/// Stop and deallocate executor but only after the provided future has
/// completed. When done, call the provided callback function.
pub fn stop<F, T>(f: F, cb: Box<T>)
where
    F: Future<Output = ()> + 'static,
    T: FnOnce() + 'static,
{
    // Chain a code which sets shutdown flag at the end of the future
    let wrapped_f = async {
        f.await;
        EXECUTOR_CTX.with(|ctx_cell| {
            let ctx_maybe = ctx_cell.borrow();
            match ctx_maybe.as_ref() {
                Some(ctx) => {
                    if ctx.shutdown_cb.replace(Some(cb)).is_some() {
                        panic!("stop executor called twice");
                    }
                }
                None => panic!(
                    "Executor was not started on thread {:?}",
                    thread::current().id()
                ),
            }
        })
    };
    debug!(
        "Initiating shutdown of future executor on thread {:?}",
        thread::current().id()
    );
    spawn(wrapped_f);
}

/// Run whatever work might be queued for the executor without blocking
/// or shutdown executor if shutdown flag is set.
extern "C" fn tick(_ptr: *mut c_void) -> i32 {
    EXECUTOR_CTX.with(|ctx_cell| {
        let ctx_maybe = ctx_cell.borrow();

        let shutdown_cb = match ctx_maybe.as_ref() {
            Some(ctx) => ctx.shutdown_cb.take(),
            None => panic!(
                "tick was called on thread {:?} while executor has been shut down",
                thread::current().id()
            )
        };
        // drop the ref so that we can grab a mutable ref later
        drop(ctx_maybe);

        match shutdown_cb {
            Some(cb) => {
                debug!(
                    "Stopping future executor on thread {:?}",
                    thread::current().id()
                );
                // we won't be running any futures so it is safe to grab mut ref
                let mut ctx = ctx_cell.borrow_mut().take().unwrap();
                unsafe {
                    // unregister will write NULL pointer to poller but that
                    // should be ok as we won't be using poller pointer anymore
                    spdk_poller_unregister(&mut ctx.poller);
                }
                cb();
            }
            None => {
                let ctx_maybe = ctx_cell.borrow();
                // we know that ctx is "some" from previous step
                let ctx = ctx_maybe.as_ref().unwrap();
                // Tasks which are generated while the executor runs are
                // left in the queue until the tick() is called again.
                let _work = ctx.pool.borrow_mut().try_run_one();
            }
        }
    });
    0
}

/// Construct callback argument for spdk async function.
/// The argument is a oneshot sender channel for result of the operation.
pub fn cb_arg<T>(sender: oneshot::Sender<T>) -> *mut c_void {
    Box::into_raw(Box::new(sender)) as *const _ as *mut c_void
}

/// Generic callback for spdk async functions expecting to be called with
/// single argument which is a sender channel to notify the other end about
/// the result.
pub extern "C" fn done_cb<T>(sender_ptr: *mut c_void, val: T)
where
    T: fmt::Debug,
{
    let sender =
        unsafe { Box::from_raw(sender_ptr as *mut oneshot::Sender<T>) };

    // the receiver side might be gone, if this happens it either means that the
    // function has gone out of scope or that the future was cancelled. We can
    // not cancel futures as they are driven by reactor. We currently fail
    // hard if the receiver is gone but in reality the determination of it
    // being fatal depends largely on what the future was supposed to do.
    sender
        .send(val)
        .expect("done callback receiver side disappeared");
}
