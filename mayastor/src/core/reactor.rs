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
    fmt::{self, Display, Formatter},
    os::raw::c_void,
    pin::Pin,
    slice::Iter,
    time::Duration,
};

use crossbeam::channel::{unbounded, Receiver, Sender};
use futures::{
    ready,
    stream::FuturesUnordered,
    task::{Context, Poll},
    Future,
    FutureExt,
    StreamExt,
};
use once_cell::sync::OnceCell;

use spdk_sys::{
    spdk_cpuset_get_cpu,
    spdk_env_thread_launch_pinned,
    spdk_env_thread_wait_all,
    spdk_thread,
    spdk_thread_get_cpumask,
    spdk_thread_lib_init_ext,
};
use tokio::{
    runtime::{Builder, Runtime},
    sync::oneshot,
};

use crate::core::{CoreError, Cores, Mthread};
use nix::errno::Errno;
use std::cell::Cell;

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

// TODO: we only have one "type" of core however, only the master core deals
// with futures we can TODO: should consider creating two variants of the
// Reactor: master and remote
#[derive(Debug)]
pub struct Reactor {
    /// Vector of threads allocated by the various subsystems. The threads are
    /// protected by a RefCell to avoid, at runtime, mutating the vector.
    /// This, ideally, we don't want to do but considering the unsafety we
    /// keep it for now
    threads: RefCell<VecDeque<Mthread>>,
    /// incoming threads that have been scheduled to this core but are not
    /// polled yet
    incoming: crossbeam::queue::SegQueue<Mthread>,
    /// the logical core this reactor is created on
    lcore: u32,
    /// represents the state of the reactor
    flags: Cell<ReactorState>,
    /// sender and Receiver for sending futures across cores without going
    /// through FFI
    sx: Sender<Pin<Box<dyn Future<Output = ()> + 'static>>>,
    rx: Receiver<Pin<Box<dyn Future<Output = ()> + 'static>>>,

    tokio_rt: Runtime,
}

type FutureSender = Sender<Pin<Box<dyn Future<Output = ()>>>>;
type FutureReceiver = Receiver<Pin<Box<dyn Future<Output = ()>>>>;

thread_local! {
    /// This queue holds any in coming futures from other cores
    static QUEUE: (FutureSender, FutureReceiver) = unbounded();
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
        matches!(op, spdk_sys::SPDK_THREAD_OP_NEW)
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
                let mt = Mthread::from(thread);
                info!(
                    "scheduled {} {:p} on core:{}",
                    mt.name(),
                    thread,
                    r.lcore
                );
                r.incoming.push(mt);
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
    #[allow(clippy::needless_return)]
    pub fn launch_remote(core: u32) -> Result<(), CoreError> {
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
            return if rc == 0 {
                Ok(())
            } else {
                error!("failed to launch core {}", core);
                Err(CoreError::ReactorError {
                    source: Errno::from_i32(rc),
                })
            };
        } else {
            Err(CoreError::ReactorError {
                source: Errno::ENOSYS,
            })
        }
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

        // SAFETY: If this fails, we're in some deep deep trouble already so a
        // panic isn't very wrong.
        let tokio_rt = Builder::new_current_thread()
            .build()
            .expect("Failed to create tokio runtime");
        Self {
            threads: RefCell::new(VecDeque::new()),
            incoming: crossbeam::queue::SegQueue::new(),
            lcore: core,
            flags: Cell::new(ReactorState::Init),
            sx,
            rx,
            tokio_rt,
        }
    }

    /// this function gets called by DPDK
    extern "C" fn poll(core: *mut c_void) -> i32 {
        debug!("Start polling of reactor {}", core as u32);
        let reactor = Reactors::get_by_core(core as u32).unwrap();
        if reactor.get_state() != ReactorState::Init {
            warn!("calling poll on a reactor who is not in the INIT state");
        }

        if std::env::var("MAYASTOR_DELAY").is_ok() {
            reactor.developer_delayed();
        } else {
            reactor.running();
        }
        // loops
        reactor.poll_reactor();
        0
    }

    /// run the futures received on the channel
    async fn run_futures(&self) {
        let futures = FuturesUnordered::new();
        QUEUE.with(|(_, r)| {
            for f in r {
                futures.push(f);
            }
        });

        futures.collect::<()>().await;
    }

    /// receive futures if any
    fn receive_futures(&self) {
        self.rx.try_iter().for_each(|m| {
            let _ = self.spawn_local(m);
        });
    }

    /// send messages to the core/thread -- similar as spdk_thread_send_msg()
    pub fn send_future<F>(&self, future: F)
    where
        F: Future<Output = ()> + 'static,
    {
        self.sx.send(Box::pin(future)).unwrap();
    }

    pub fn spawn_local<F>(&self, future: F) -> impl Future<Output = F::Output>
    where
        F: Future + 'static,
    {
        let (tx, rx) = oneshot::channel();

        let f = Box::pin(async move {
            let output = future.await;

            // Ignore error as unsuccessful send means the `spawn_local` caller
            // dropped the `rx`.
            let _ = tx.send(output);
        });
        // our scheduling right now is basically non-existent but -- in the
        // future we want to schedule work to cores that are not very
        // busy etc.
        QUEUE.with(|(s, _)| s.send(f).unwrap());

        // the handler typically has no meaning to us unless we want to wait for
        // the spawned future to complete before we continue which is
        // done, in example with ['block_on']
        rx.map(|res| res.expect("oneshot channel sender hung up"))
    }

    /// spawn a future locally on the current core block until the future is
    /// completed. The master core is used.
    pub fn block_on<F>(future: F) -> Option<F::Output>
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        // hold on to the any potential thread we might be running on right now
        let thread = Mthread::current();
        Mthread::get_init().enter();

        struct FutureWrapper<'r, F> {
            future: Pin<Box<F>>,
            reactor: &'r Reactor,
        }
        impl<F> Future for FutureWrapper<'_, F>
        where
            F: Future + 'static,
            F::Output: 'static,
        {
            type Output = F::Output;

            fn poll(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<Self::Output> {
                match Pin::new(&mut self.as_mut().future).poll(cx) {
                    Poll::Pending => {
                        let mut f = Box::pin(self.reactor.poll_once());
                        ready!(Pin::new(&mut f).poll(cx));

                        Poll::Pending
                    }
                    Poll::Ready(output) => Poll::Ready(output),
                }
            }
        }

        let reactor = Reactors::master();
        let future = FutureWrapper {
            future: Box::pin(reactor.spawn_local(future)),
            reactor,
        };

        let ret = reactor.tokio_rt.block_on(future);

        Mthread::get_init().exit();
        if let Some(t) = thread {
            t.enter()
        }

        // FIXME: Why do we return an Option here?
        Some(ret)
    }

    /// set the state of this reactor
    fn set_state(&self, state: ReactorState) {
        match state {
            ReactorState::Init
            | ReactorState::Delayed
            | ReactorState::Shutdown
            | ReactorState::Running => {
                self.flags.set(state);
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
    pub fn shutdown(&self) {
        info!("shutdown requested for core {}", self.lcore);
        self.set_state(ReactorState::Shutdown);
    }

    /// returns the current state of the reactor
    pub fn get_state(&self) -> ReactorState {
        self.flags.get()
    }

    /// returns core number of this reactor
    pub fn core(&self) -> u32 {
        self.lcore
    }

    /// poll this reactor to complete any work that is pending
    pub fn poll_reactor(&self) {
        self.tokio_rt.block_on(async {
            loop {
                match self.get_state() {
                    // running is the default mode for all cores. All cores,
                    // except the master core spin within
                    // this specific loop
                    ReactorState::Running => {
                        self.poll_once().await;
                    }
                    ReactorState::Shutdown => {
                        info!("reactor {} shutdown requested", self.lcore);
                        break;
                    }
                    ReactorState::Delayed => {
                        let _ = tokio::time::timeout(
                            Duration::from_millis(1),
                            std::future::pending::<()>(),
                        )
                        .await;
                        self.poll_once().await;
                    }
                    _ => panic!("invalid reactor state {:?}", self.get_state()),
                }
            }
        });

        debug!("initiating shutdown for core {}", Cores::current());

        if self.lcore == Cores::first() {
            debug!("master core stopped polling");
        }
    }

    /// polls the reactor only once for any work regardless of its state. For
    /// now
    #[inline]
    pub async fn poll_once(&self) {
        self.receive_futures();
        self.run_futures().await;
        let threads = self.threads.borrow();
        threads.iter().for_each(|t| {
            t.poll();
        });

        drop(threads);
        while let Some(i) = self.incoming.pop() {
            self.threads.borrow_mut().push_back(i);
        }
    }

    #[inline]
    pub fn poll_once_blocking(&self) {
        self.tokio_rt.block_on(self.poll_once());
    }

    /// poll the threads n times but only poll the futures queue once and look
    /// for incoming only once.
    ///
    /// We might want to set a flag that we need to run futures and or incoming
    /// queues
    pub async fn poll_times(&self, times: u32) {
        let threads = self.threads.borrow();
        for _ in 0 .. times {
            threads.iter().for_each(|t| {
                t.poll();
            });
        }

        self.receive_futures();
        self.run_futures().await;
        drop(threads);

        while let Some(i) = self.incoming.pop() {
            self.threads.borrow_mut().push_back(i);
        }
    }

    pub fn poll_times_blocking(&self, times: u32) {
        self.tokio_rt.block_on(self.poll_times(times));
    }
}
