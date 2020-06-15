//! The first thing that needs to happen is to initialize DPDK, this among
//! others provides us with lockless queues which we use to send messages
//! between the cores.  Typically these messages contain simple function
//! pointers and argument pointers.
//!
//! Per core data structure, there are so-called threads. These threads
//! represent, not to be confused with OS threads are created dynamically
//! depending on the subsystem(s). For example, an NVMF subsystem might create a
//! new thread instance on a given core. This separates different types of work
//! and allows us to divide work between cores evenly.
//!
//! To summarize, a reactor instance to CPU core is a one-to-one relation. A
//! reactor, in turn, may have one or more thread objects. The thread objects
//! MAY hold messages for a specific subsystem. During init, per reactor, we
//! create one thread which is always thread 0.
//!
//! During the poll loop, we traverse all threads and poll each queue of that
//! thread. The functions executed are all executed within the context of that
//! thread. This in effect means that we set a TLS pointer to the thread that we
//! are polling. This context is verified during run time, such that we can
//! ensure that, for example,  a bdev (say) that is open, is closed within the
//! same context as it was opened with.
//!
//! The queue of each thread is unique, but the messages in the queue are all
//! preallocated from a global pool. This prevents allocations at runtime.
//!
//! Alongside that, each reactor (currently) has two additional queues. One
//! queue is for receiving and sending messages between cores. The other queue
//! is used for holding on to the messages while it is being processed. Once
//! processed (or completed) it is dropped from the queue. Unlike the native
//! SPDK messages, these futures -- are allocated before they execute.
use spdk_sys::spdk_get_thread;
use std::{cell::Cell, os::raw::c_void, pin::Pin, slice::Iter, time::Duration};

use crossbeam::channel::{unbounded, Receiver, Sender};
use futures::{
    task::{Context, Poll},
    Future,
};
use log::info;
use once_cell::sync::OnceCell;

use spdk_sys::{
    spdk_env_thread_launch_pinned,
    spdk_env_thread_wait_all,
    spdk_thread_lib_init,
};

use crate::core::{Cores, Mthread};

pub(crate) const INIT: usize = 1;
pub(crate) const RUNNING: usize = 1 << 1;
pub(crate) const SHUTDOWN: usize = 1 << 2;
pub(crate) const SUSPEND: usize = 1 << 3;
pub(crate) const DEVELOPER_DELAY: usize = 1 << 4;

#[derive(Debug)]
pub struct Reactors(Vec<Reactor>);

unsafe impl Sync for Reactors {}
unsafe impl Send for Reactors {}

unsafe impl Sync for Reactor {}
unsafe impl Send for Reactor {}
pub static REACTOR_LIST: OnceCell<Reactors> = OnceCell::new();
#[repr(C, align(64))]
#[derive(Debug)]
pub struct Reactor {
    /// vector of threads allocated by the various subsystems
    threads: Vec<Mthread>,
    /// the logical core this reactor is created on
    lcore: u32,
    /// represents the state of the reactor
    flags: Cell<usize>,
    /// sender and Receiver for sending futures across cores without going
    /// through FFI
    sx: Sender<Pin<Box<dyn Future<Output = ()> + 'static>>>,
    rx: Receiver<Pin<Box<dyn Future<Output = ()> + 'static>>>,
}

thread_local! {
    /// This queue holds any in coming futures from other cores
    static QUEUE: (Sender<async_task::Task<()>>, Receiver<async_task::Task<()>>) = unbounded();
}

impl Reactors {
    /// initialize the reactor subsystem for each core assigned to us
    pub fn init() {
        REACTOR_LIST.get_or_init(|| {
            let rc = unsafe { spdk_thread_lib_init(None, 0) };
            assert_eq!(rc, 0);

            Reactors(
                Cores::count()
                    .into_iter()
                    .map(|c| {
                        debug!("init core: {}", c);
                        Reactor::new(c)
                    })
                    .collect::<Vec<_>>(),
            )
        });
    }

    /// launch the poll loop on the master core, this is implemented somewhat
    /// different from the remote cores.
    pub fn launch_master() {
        assert_eq!(Cores::current(), Cores::first());
        Reactor::poll(Cores::current() as *const u32 as *mut c_void);
        // wait for all other cores to exit before we unblock.
        unsafe { spdk_env_thread_wait_all() };
    }

    /// start polling the reactors on the given core, when multiple cores are
    /// involved they must be running during init as they must process in coming
    /// messages that are send as part of the init process.
    pub fn launch_remote(core: u32) -> Result<(), ()> {
        // the master core -- who is the only core that can call this function
        // should not be launched this way. For that use ['launch_master`].
        // Nothing prevents anyone from call this function twice now.
        if core == Cores::current() {
            return Ok(());
        }

        if Cores::count().into_iter().any(|c| c == core) {
            let rc = unsafe {
                spdk_env_thread_launch_pinned(
                    core,
                    Some(Reactor::poll),
                    core as *const u32 as *mut c_void,
                )
            };
            if rc == 0 {
                return Ok(());
            }
        }

        error!("failed to launch core {}", core);
        Err(())
    }

    /// get a reference to a ['Reactor'] associated with the given core.
    pub fn get_by_core(core: u32) -> Option<&'static Reactor> {
        Reactors::iter().find(|c| c.lcore == core)
    }

    /// get a reference to a reactor on the current core
    pub fn current() -> &'static Reactor {
        Self::get_by_core(Cores::current()).expect("no reactor allocated")
    }

    pub fn master() -> &'static Reactor {
        Self::get_by_core(Cores::first()).expect("no reactor allocated")
    }

    /// returns an iterator over all reactors
    pub fn iter() -> Iter<'static, Reactor> {
        REACTOR_LIST.get().unwrap().into_iter()
    }
}

impl<'a> IntoIterator for &'a Reactors {
    type Item = &'a Reactor;
    type IntoIter = ::std::slice::Iter<'a, Reactor>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl Reactor {
    /// create a new ['Reactor'] instance
    fn new(core: u32) -> Self {
        // allocate a new thread which provides the SPDK context
        let t = Mthread::new(format!("core_{}", core));
        // create a channel to receive futures on
        let (sx, rx) =
            unbounded::<Pin<Box<dyn Future<Output = ()> + 'static>>>();

        Self {
            threads: vec![t],
            lcore: core,
            flags: Cell::new(INIT),
            sx,
            rx,
        }
    }

    /// this function gets called by DPDK
    extern "C" fn poll(core: *mut c_void) -> i32 {
        debug!("Start polling of reactor {}", core as u32);
        let reactor = Reactors::get_by_core(core as u32).unwrap();
        reactor.thread_enter();
        if reactor.flags.get() != INIT {
            warn!("calling poll on a reactor who is not in the INIT state");
        }

        if cfg!(debug_assertions) {
            reactor.developer_delayed();
        } else {
            reactor.running();
        }
        // loops
        reactor.poll_reactor();
        0
    }

    /// run the futures received on the channel
    fn run_futures(&self) {
        QUEUE.with(|(_, r)| r.try_iter().for_each(|f| f.run()));
    }

    /// receive futures if any
    fn receive_futures(&self) {
        self.rx.try_iter().for_each(|m| {
            self.spawn_local(m);
        });
    }

    /// send messages to the core/thread -- similar as spdk_thread_send_msg()
    pub fn send_future<F>(&self, future: F)
    where
        F: Future<Output = ()> + 'static,
    {
        self.sx.send(Box::pin(future)).unwrap();
    }

    /// spawn a future locally on this core
    pub fn spawn_local<F, R>(&self, future: F) -> async_task::JoinHandle<R, ()>
    where
        F: Future<Output = R> + 'static,
        R: 'static,
    {
        // our scheduling right now is basically non-existent but -- in the
        // future we want to schedule work to cores that are not very
        // busy etc.
        let schedule = |t| QUEUE.with(|(s, _)| s.send(t).unwrap());

        let (task, handle) = async_task::spawn_local(future, schedule, ());
        task.schedule();
        // the handler typically has no meaning to us unless we want to wait for
        // the spawned future to complete before we continue which is
        // done, in example with ['block_on']
        handle
    }

    /// spawn a future locally on the current core block until the future is
    /// completed. The first thread of the current core is implicitly used.
    pub fn block_on<F, R>(future: F) -> Option<R>
    where
        F: Future<Output = R> + 'static,
        R: 'static,
    {
        // get the current reactor and ensure that we are running within the
        // context of the first thread.
        let reactor = Reactors::current();
        reactor.thread_enter();
        let schedule = |t| QUEUE.with(|(s, _)| s.send(t).unwrap());
        let (task, handle) = async_task::spawn_local(future, schedule, ());

        let waker = handle.waker();
        let cx = &mut Context::from_waker(&waker);

        pin_utils::pin_mut!(handle);
        task.schedule();

        // here we only poll the first thread, this is fine because the future
        // is also scheduled within that first threads context.

        loop {
            match handle.as_mut().poll(cx) {
                Poll::Ready(output) => {
                    return output;
                }
                Poll::Pending => {
                    assert_eq!(reactor.threads[0].0, unsafe {
                        spdk_get_thread()
                    });
                    reactor.receive_futures();
                    reactor.run_futures();
                    reactor.threads[0].poll();
                }
            }
        }
    }

    /// set the state of this reactor
    fn set_state(&self, state: usize) {
        match state {
            SUSPEND | RUNNING | SHUTDOWN | DEVELOPER_DELAY => {
                self.flags.set(state)
            }
            _ => panic!("Invalid state"),
        }
    }

    /// suspend (sleep in a loop) the reactor until the state has been set to
    /// running again
    pub fn suspend(&self) {
        self.set_state(SUSPEND)
    }

    /// set the state of the reactor to running. In this state the reactor will
    /// poll for work on the thread message pools as well as its own queue
    /// to launch futures.
    pub fn running(&self) {
        self.set_state(RUNNING)
    }

    /// set the reactor to sleep each iteration
    pub fn developer_delayed(&self) {
        info!("core {} set to developer delayed poll mode", self.lcore);
        self.set_state(DEVELOPER_DELAY)
    }

    /// initiate shutdown of the reactor and stop polling
    pub fn shutdown(&self) {
        debug!("shutdown requested for core {}", self.lcore);
        self.set_state(SHUTDOWN);
    }

    /// returns the current state of the reactor
    pub fn get_sate(&self) -> usize {
        self.flags.get()
    }

    /// returns core number of this reactor
    pub fn core(&self) -> u32 {
        self.lcore
    }

    /// poll this reactor to complete any work that is pending
    pub fn poll_reactor(&self) {
        loop {
            match self.flags.get() {
                SUSPEND => {
                    std::thread::sleep(Duration::from_millis(100));
                }
                // running is the default mode for all cores. All cores, except
                // the master core spin within this specific
                // loop
                RUNNING => {
                    self.poll_once();
                }
                SHUTDOWN => {
                    info!("reactor {} shutdown requested", self.lcore);
                    break;
                }
                DEVELOPER_DELAY => {
                    std::thread::sleep(Duration::from_millis(1));
                    self.poll_once();
                }
                _ => panic!("invalid reactor state {}", self.flags.get()),
            }
        }

        debug!("initiating shutdown for core {}", Cores::current());
        // clean up the threads, threads can only be destroyed during shutdown
        // in the rest of SPDK
        self.threads.iter().for_each(|t| t.destroy());

        if self.lcore == Cores::first() {
            debug!("master core stopped polling");
        }
    }

    /// Enters the first reactor thread.  By default, we are not running within
    /// the context of any thread. We always need to set a context before we
    /// can process, or submit any messages.
    #[inline]
    pub fn thread_enter(&self) {
        self.threads[0].enter();
    }

    /// polls the reactor only once for any work regardless of its state. For
    /// now, the threads are all entered and exited explicitly.
    #[inline]
    pub fn poll_once(&self) {
        self.receive_futures();
        self.run_futures();

        // poll any thread for events
        self.threads.iter().for_each(|t| {
            t.poll();
        });

        // during polling, we switch context ensure we leave the poll routine
        // with setting a context again.
        self.thread_enter();
    }
}

/// This implements the poll() method of the for the reactor future. Only the
/// master core is polled by the Future abstraction. There are two reasons for
/// this
///
///  1. The master core is the management core, it is the only core that handles
/// gRPC calls  2. The master core handles the setup and tear down of the slave
/// cores
impl Future for &'static Reactor {
    type Output = Result<(), ()>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        match self.flags.get() {
            RUNNING => {
                self.poll_once();
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            SHUTDOWN => {
                info!("future reactor {} shutdown requested", self.lcore);

                self.threads.iter().for_each(|t| t.destroy());
                unsafe { spdk_env_thread_wait_all() };
                Poll::Ready(Err(()))
            }
            DEVELOPER_DELAY => {
                std::thread::sleep(Duration::from_millis(1));
                self.poll_once();
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            INIT => {
                self.poll_once();
                if cfg!(debug_assertions) {
                    self.developer_delayed();
                } else {
                    self.running();
                }
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            _ => panic!(),
        }
    }
}
