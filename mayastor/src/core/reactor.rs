//! The first thing that needs to happen is to initialize DPDK, this among
//! others provides us with lockless queues which we use to send messages
//! between the cores.  Typically, these messages contain simple function
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
use std::{
    cell::RefCell,
    collections::VecDeque,
    fmt,
    fmt::Display,
    os::raw::c_void,
    pin::Pin,
    slice::Iter,
    time::Duration,
};

use crossbeam::{
    atomic::AtomicCell,
    channel::{unbounded, Receiver, Sender},
};
use futures::{
    task::{Context, Poll},
    Future,
};
use log::info;
use once_cell::sync::OnceCell;
use serde::export::Formatter;
use spdk_sys::{
    spdk_cpuset_get_cpu,
    spdk_env_thread_launch_pinned,
    spdk_env_thread_wait_all,
    spdk_thread,
    spdk_thread_get_cpumask,
    spdk_thread_lib_init_ext,
};
use tracing::instrument;

use crate::core::{Cores, Mthread};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ReactorState {
    Init,
    Running,
    Shutdown,
    Delayed,
}

impl Display for ReactorState {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let s = match self {
            ReactorState::Init => "Init",
            ReactorState::Running => "Running",
            ReactorState::Shutdown => "Shutdown",
            ReactorState::Delayed => "Delayed",
        };
        write!(f, "{}", s)
    }
}

#[derive(Debug)]
pub struct Reactors(Vec<Reactor>);

unsafe impl Sync for Reactors {}
unsafe impl Send for Reactors {}

unsafe impl Sync for Reactor {}
unsafe impl Send for Reactor {}
pub static REACTOR_LIST: OnceCell<Reactors> = OnceCell::new();

#[derive(Debug)]
pub struct Reactor {
    /// vector of threads allocated by the various subsystems
    threads: RefCell<VecDeque<Mthread>>,
    /// incomming threads that have been scheduled to this core but are not
    /// polled yet
    incomming: crossbeam::queue::SegQueue<Mthread>,
    /// the logical core this reactor is created on
    lcore: u32,
    /// represents the state of the reactor
    flags: AtomicCell<ReactorState>,
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
            let rc = unsafe {
                spdk_thread_lib_init_ext(
                    Some(Self::do_op),
                    Some(Self::can_op),
                    0,
                )
            };
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

        // construct one main init thread, this thread is used to bootstrap
        // and should be used to teardown as well.
        if let Some(t) = Mthread::new("init_thread".into(), Cores::first()) {
            info!("Init thread ID {}", t.id());
        }
    }

    /// advertise what scheduling options we support
    extern "C" fn can_op(op: spdk_sys::spdk_thread_op) -> bool {
        match op {
            spdk_sys::SPDK_THREAD_OP_NEW => true,
            _ => false,
        }
    }

    /// do the advertised scheduling option
    extern "C" fn do_op(
        thread: *mut spdk_thread,
        op: spdk_sys::spdk_thread_op,
    ) -> i32 {
        match op {
            spdk_sys::SPDK_THREAD_OP_NEW => Self::schedule(thread),
            _ => -1,
        }
    }

    /// schedule a thread in here, we should make smart choices based
    /// on load etc, right now we schedule to the current core.
    fn schedule(thread: *mut spdk_sys::spdk_thread) -> i32 {
        let mask = unsafe { spdk_thread_get_cpumask(thread) };
        let scheduled = Reactors::iter().any(|r| {
            if unsafe { spdk_cpuset_get_cpu(mask, r.lcore) } {
                let mt = Mthread(thread);
                info!(
                    "scheduled {} {:p} on core:{}",
                    mt.name(),
                    thread,
                    r.lcore
                );
                r.incomming.push(Mthread(thread));
                return true;
            }
            false
        });

        if !scheduled {
            error!("failed to find core for thread {:p}!", thread);
            1
        } else {
            0
        }
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
        // create a channel to receive futures on
        let (sx, rx) =
            unbounded::<Pin<Box<dyn Future<Output = ()> + 'static>>>();

        Self {
            threads: RefCell::new(VecDeque::new()),
            incomming: crossbeam::queue::SegQueue::new(),
            lcore: core,
            flags: AtomicCell::new(ReactorState::Init),
            sx,
            rx,
        }
    }

    /// this function gets called by DPDK
    extern "C" fn poll(core: *mut c_void) -> i32 {
        debug!("Start polling of reactor {}", core as u32);
        let reactor = Reactors::get_by_core(core as u32).unwrap();
        if reactor.get_state() != ReactorState::Init {
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
        QUEUE.with(|(_, r)| {
            r.try_iter().for_each(|f| {
                f.run();
            })
        });
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
    /// completed. The master core is used.
    pub fn block_on<F, R>(future: F) -> Option<R>
    where
        F: Future<Output = R> + 'static,
        R: 'static,
    {
        let _thread = Mthread::current();
        Mthread::get_init().enter();
        let schedule = |t| QUEUE.with(|(s, _)| s.send(t).unwrap());
        let (task, handle) = async_task::spawn_local(future, schedule, ());

        let waker = handle.waker();
        let cx = &mut Context::from_waker(&waker);

        pin_utils::pin_mut!(handle);
        task.schedule();
        let reactor = Reactors::master();

        loop {
            match handle.as_mut().poll(cx) {
                Poll::Ready(output) => {
                    Mthread::get_init().exit();
                    _thread.map(|t| {
                        debug!(
                            "restoring thread from {:?} to {:?}",
                            Mthread::current(),
                            _thread
                        );
                        t.enter()
                    });
                    return output;
                }
                Poll::Pending => {
                    reactor.poll_once();
                }
            };

            match reactor.get_state() {
                ReactorState::Shutdown => {
                    info!("block_on aborted by shutdown");
                    return None;
                }
                _ => continue,
            };
        }
    }

    /// set the state of this reactor
    fn set_state(&self, state: ReactorState) {
        match state {
            ReactorState::Init
            | ReactorState::Delayed
            | ReactorState::Shutdown
            | ReactorState::Running => {
                self.flags.store(state);
            }
        }
    }

    /// set the state of the reactor to running. In this state the reactor will
    /// poll for work on the thread message pools as well as its own queue
    /// to launch futures.
    pub fn running(&self) {
        self.set_state(ReactorState::Running)
    }

    /// set the reactor to sleep each iteration
    pub fn developer_delayed(&self) {
        info!("core {} set to developer delayed poll mode", self.lcore);
        self.set_state(ReactorState::Delayed);
    }

    /// initiate shutdown of the reactor and stop polling
    #[instrument]
    pub fn shutdown(&self) {
        info!("shutdown requested for core {}", self.lcore);
        self.set_state(ReactorState::Shutdown);
    }

    /// returns the current state of the reactor
    pub fn get_state(&self) -> ReactorState {
        self.flags.load()
    }

    /// returns core number of this reactor
    pub fn core(&self) -> u32 {
        self.lcore
    }

    /// poll this reactor to complete any work that is pending
    pub fn poll_reactor(&self) {
        loop {
            match self.get_state() {
                // running is the default mode for all cores. All cores, except
                // the master core spin within this specific loop
                ReactorState::Running => {
                    self.poll_once();
                }
                ReactorState::Shutdown => {
                    info!("reactor {} shutdown requested", self.lcore);
                    break;
                }
                ReactorState::Delayed => {
                    std::thread::sleep(Duration::from_millis(1));
                    self.poll_once();
                }
                _ => panic!("invalid reactor state {:?}", self.get_state()),
            }
        }

        debug!("initiating shutdown for core {}", Cores::current());

        if self.lcore == Cores::first() {
            debug!("master core stopped polling");
        }
    }

    /// polls the reactor only once for any work regardless of its state. For
    /// now, the threads are all entered and exited explicitly.
    #[inline]
    pub fn poll_once(&self) {
        self.receive_futures();
        self.run_futures();
        self.threads.borrow().iter().for_each(|t| {
            t.poll();
        });

        while let Ok(i) = self.incomming.pop() {
            self.threads.borrow_mut().push_back(i);
        }
    }

    pub fn poll_times(&self, times: u32) {
        for _ in 0 .. times {
            self.poll_once();
        }
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
        match self.get_state() {
            ReactorState::Running => {
                self.poll_once();
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            ReactorState::Shutdown => {
                info!("future reactor {} shutdown requested", self.lcore);
                while let Some(t) = self.threads.borrow_mut().pop_front() {
                    t.destroy();
                }

                unsafe { spdk_env_thread_wait_all() };
                Poll::Ready(Err(()))
            }
            ReactorState::Delayed => {
                self.poll_once();
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            ReactorState::Init => {
                if cfg!(debug_assertions) {
                    self.developer_delayed();
                } else {
                    self.running();
                }
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}
