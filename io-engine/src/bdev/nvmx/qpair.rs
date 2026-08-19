use std::{
    cell::RefCell,
    cmp::max,
    fmt::{Debug, Formatter},
    mem::size_of,
    rc::Rc,
};

use futures::channel::oneshot;

use spdk_rs::libspdk::{
    nvme_qpair_abort_all_queued_reqs, nvme_transport_qpair_abort_reqs, spdk_nvme_ctrlr,
    spdk_nvme_ctrlr_alloc_io_qpair, spdk_nvme_ctrlr_connect_io_qpair,
    spdk_nvme_ctrlr_disconnect_io_qpair, spdk_nvme_ctrlr_free_io_qpair,
    spdk_nvme_ctrlr_get_default_io_qpair_opts, spdk_nvme_io_qpair_opts, spdk_nvme_qpair,
    spdk_nvme_qpair_set_abort_dnr,
};

use std::mem::zeroed;
#[cfg(feature = "spdk-async-qpair-connect")]
use std::{os::raw::c_void, time::Duration};

#[cfg(feature = "spdk-async-qpair-connect")]
use spdk_rs::{
    libspdk::{
        spdk_nvme_ctrlr_connect_io_qpair_async, spdk_nvme_ctrlr_io_qpair_connect_poll_async,
        spdk_nvme_io_qpair_connect_ctx,
    },
    Poller, PollerBuilder, UnsafeRef,
};

#[cfg(feature = "spdk-async-qpair-connect")]
use nix::errno::Errno;

use crate::core::CoreError;

use super::{nvme_bdev_running_config, SpdkNvmeController};

/// I/O QPair state.
#[derive(Debug, Serialize, Clone, Copy, PartialEq, PartialOrd, strum_macros::Display)]
pub enum QPairState {
    /// QPair is not connected.
    Disconnected,
    /// QPair is connecting asynchronously.
    #[cfg(feature = "spdk-async-qpair-connect")]
    Connecting,
    /// QPair is connected.
    Connected,
    /// QPair is dropped.
    Dropped,
}

/// I/O QPair.
pub struct QPair {
    inner: Rc<RefCell<Inner>>,
}

impl Debug for QPair {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.inner.borrow().fmt(f)
    }
}

impl Drop for QPair {
    fn drop(&mut self) {
        // Only a qpair that has actually completed a connection has
        // anything to disconnect. A qpair still `Disconnected` (never
        // connected) or `Connecting` (async connect still in flight) has no
        // transport-level connection established yet, so issuing a
        // disconnect against it hits a state the transport doesn't expect.
        let was_connected = self.state() == QPairState::Connected;

        unsafe {
            let qpair = self.as_ptr();
            if was_connected {
                spdk_nvme_qpair_set_abort_dnr(qpair, true);
                nvme_qpair_abort_all_queued_reqs(qpair);
                nvme_transport_qpair_abort_reqs(qpair);
                spdk_nvme_ctrlr_disconnect_io_qpair(qpair);
            }
            spdk_nvme_ctrlr_free_io_qpair(qpair);
        }

        trace!(?self, "I/O qpair disconnected");

        {
            // Zero-out pointers to the destroyed objects and set `Dropped`
            // state.
            let mut inner = self.inner.borrow_mut();
            inner.qpair = std::ptr::null_mut();
            inner.ctrlr = std::ptr::null_mut();
            inner.state = QPairState::Dropped;
        }
    }
}

/// Returns default qpair options.
fn get_default_options(ctrlr_handle: SpdkNvmeController) -> spdk_nvme_io_qpair_opts {
    let mut opts: spdk_nvme_io_qpair_opts = unsafe { zeroed() };
    let default_opts = nvme_bdev_running_config();

    unsafe {
        spdk_nvme_ctrlr_get_default_io_qpair_opts(
            ctrlr_handle.as_ptr(),
            &mut opts,
            size_of::<spdk_nvme_io_qpair_opts>() as u64,
        )
    };

    opts.io_queue_requests = max(opts.io_queue_requests, default_opts.io_queue_requests);
    opts.create_only = true;

    // Always assume async_mode is enabled instread of
    // relying on default_opts.async_mode.
    opts.async_mode = true;

    opts
}

impl QPair {
    /// Returns an SPDK qpair object pointer.
    #[inline(always)]
    pub fn as_ptr(&self) -> *mut spdk_nvme_qpair {
        self.inner.borrow().qpair
    }

    /// Returns QPair state.
    #[inline(always)]
    pub fn state(&self) -> QPairState {
        self.inner.borrow().state
    }

    /// Sets QPair state.
    #[inline(always)]
    fn set_state(&self, state: QPairState) {
        self.inner.borrow_mut().state = state;
    }

    /// Creates a qpair with default options for target NVMe controller.
    pub(super) fn create(
        ctrlr_handle: SpdkNvmeController,
        ctrlr_name: &str,
    ) -> Result<Self, CoreError> {
        let qpair_opts = get_default_options(ctrlr_handle);
        let ctrlr = ctrlr_handle.as_ptr();

        let qpair = unsafe {
            spdk_nvme_ctrlr_alloc_io_qpair(
                ctrlr,
                &qpair_opts,
                size_of::<spdk_nvme_io_qpair_opts>() as u64,
            )
        };

        if qpair.is_null() {
            error!(
                ?ctrlr,
                ?ctrlr_name,
                "failed to allocate I/O qpair for controller"
            );

            Err(CoreError::GetIoChannel {
                name: ctrlr_name.to_string(),
            })
        } else {
            let qpair = Self {
                inner: Rc::new(RefCell::new(Inner {
                    qpair,
                    ctrlr,
                    ctrlr_name: ctrlr_name.to_owned(),
                    state: QPairState::Disconnected,
                    waiters: Vec::new(),
                })),
            };

            Ok(qpair)
        }
    }

    /// Connects a qpair synchronously.
    pub(crate) fn connect(&self) -> i32 {
        trace!(?self, "new I/O qpair connection");

        // Check if I/O qpair is already connected to provide idempotency for
        // multiple allocations of the same handle for the same thread, to make
        // sure we don't reconnect every time.
        if self.state() == QPairState::Connected {
            trace!(?self, "I/O qpair already connected");
            return 0;
        }

        // During synchronous connection we shouldn't be preemped by any other
        // SPDK thread, so we can't see QPairState::Connecting.
        assert_eq!(
            self.state(),
            QPairState::Disconnected,
            "Invalid QPair state"
        );

        // Mark qpair as being connected and try to connect.
        let status = self.inner.borrow().ctrlr_connect_sync();

        // Update QPairState according to the connection result.
        self.set_state(if status == 0 {
            QPairState::Connected
        } else {
            QPairState::Disconnected
        });

        trace!(?self, ?status, "I/O qpair connected");

        status
    }
}

#[cfg(feature = "spdk-async-qpair-connect")]
impl QPair {
    /// Connects a qpair asynchronously.
    pub(crate) async fn connect_async(&self) -> Result<(), CoreError> {
        // Take into account other connect requests for this I/O qpair to avoid
        // multiple concurrent connections.
        let recv = match self.state() {
            QPairState::Disconnected => self.start_new_async()?,
            QPairState::Connecting => self.waiting_async()?,
            QPairState::Connected => {
                // Provide idempotency for multiple allocations of the same
                // handle for the same thread, to make sure we don't reconnect
                // every time.
                trace!(?self, "I/O qpair already connected");
                return Ok(());
            }
            QPairState::Dropped => {
                panic!("I/O qpair is in an invalid state: {:?}", self.state());
            }
        };

        let qpair = self.as_ptr();
        let res = recv.await.map_err(|_| {
            // Receiver failure may be caused by qpair having been dropped,
            // so we cannot use `self` here.
            error!(?qpair, "I/O qpair connection canceled");
            CoreError::OpenBdev {
                source: Errno::ECANCELED,
            }
        })?;
        res
    }

    /// Starts a new async connection and returns a receiver for it.
    fn start_new_async(&self) -> Result<ResultReceiver, CoreError> {
        trace!(?self, "new async I/O pair connection");

        assert_eq!(self.state(), QPairState::Disconnected);

        self.set_state(QPairState::Connecting);
        Connection::create(self.inner.clone())
    }

    /// Returns a receiver to wait for a connection already in progress.
    fn waiting_async(&self) -> Result<ResultReceiver, CoreError> {
        trace!(
            ?self,
            "new async I/O pair connection: connection already in progress"
        );

        assert_eq!(self.state(), QPairState::Connecting);

        let (s, r) = oneshot::channel();
        self.inner.borrow_mut().waiters.push(s);
        Ok(r)
    }
}

type ConnectResult = Result<(), CoreError>;

type ResultSender = oneshot::Sender<ConnectResult>;

#[allow(dead_code)]
type ResultReceiver = oneshot::Receiver<ConnectResult>;

/// Inner state of the QPair object.
struct Inner {
    qpair: *mut spdk_nvme_qpair,
    ctrlr: *mut spdk_nvme_ctrlr,
    ctrlr_name: String,
    state: QPairState,
    waiters: Vec<ResultSender>,
}

impl Debug for Inner {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QPair")
            .field("qpair", &self.qpair)
            .field("ctrlr", &self.ctrlr)
            .field("ctrlr_name", &self.ctrlr_name)
            .field("state", &self.state)
            .field("waiters", &self.waiters.len())
            .finish()
    }
}

impl Inner {
    /// Connects synchronously.
    fn ctrlr_connect_sync(&self) -> i32 {
        unsafe { spdk_nvme_ctrlr_connect_io_qpair(self.ctrlr, self.qpair) }
    }

    /// Connects asynchronously.
    #[cfg(feature = "spdk-async-qpair-connect")]
    fn ctrlr_connect_async(
        &self,
        ctx: &mut Connection,
    ) -> Result<*mut spdk_nvme_io_qpair_connect_ctx, CoreError> {
        let res = unsafe {
            spdk_nvme_ctrlr_connect_io_qpair_async(
                self.ctrlr,
                self.qpair,
                Some(qpair_connect_cb),
                ctx as *mut _ as *mut c_void,
            )
        };

        if res.is_null() {
            error!(
                ?self,
                "Failed to initiate asynchronous connection on a qpair"
            );

            Err(CoreError::OpenBdev {
                source: Errno::ENXIO,
            })
        } else {
            Ok(res)
        }
    }
}

/// Async QPair connection.
#[cfg(feature = "spdk-async-qpair-connect")]
struct Connection<'a> {
    inner: Rc<RefCell<Inner>>,
    sender: Option<ResultSender>,
    poller: Option<Poller<'a, UnsafeRef<Self>>>,
    probe: *mut spdk_nvme_io_qpair_connect_ctx,
}

#[cfg(feature = "spdk-async-qpair-connect")]
impl Debug for Connection<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.inner.borrow().fmt(f)
    }
}

#[cfg(feature = "spdk-async-qpair-connect")]
impl Drop for Connection<'_> {
    fn drop(&mut self) {
        trace!(?self, "I/O connection dropped")
    }
}

#[cfg(feature = "spdk-async-qpair-connect")]
impl Connection<'_> {
    /// Creats a new async qpair connection, and returns a receiver to await the
    /// completion.
    fn create(inner: Rc<RefCell<Inner>>) -> Result<ResultReceiver, CoreError> {
        // Create sender-receiver pair to notify about connection completion.
        let (sender, receiver) = oneshot::channel();

        // Create self (boxed).
        let mut ctx = Box::new(Self {
            inner: inner.clone(),
            sender: Some(sender),
            poller: None,
            probe: std::ptr::null_mut(),
        });

        let self_ref = UnsafeRef::new(ctx.as_mut());

        // Start poller for the async connection.
        ctx.poller = Some(
            PollerBuilder::new()
                .with_name("io_qpair_connect_poller")
                .with_interval(Duration::from_millis(1))
                .with_data(UnsafeRef::new(ctx.as_mut()))
                .with_poll_fn(Connection::poll_cb)
                .build(),
        );

        // Start async connection, and create a probe for it.
        ctx.probe = inner.borrow().ctrlr_connect_async(ctx.as_mut())?;

        // Context is now managed by the SPDK async connection and the poller.
        Box::leak(ctx);

        // Drive the connect state machine's first step synchronously, right
        // now, instead of waiting for the poller's first scheduled tick (up
        // to 1ms later). `spdk_nvme_ctrlr_connect_io_qpair_async()` (called
        // above via `ctrlr_connect_async()`) only allocates the probe
        // context -- it does NOT call `nvme_transport_ctrlr_connect_qpair()`,
        // the call that actually kicks off the RDMA transport connect and
        // moves the qpair off the poll group's `disconnected_qpairs` list.
        // That only happens on the first `INIT` step of
        // `spdk_nvme_ctrlr_io_qpair_connect_poll_async()`, driven by
        // `Connection::poll()` below. Until that first step runs, the qpair
        // sits on `disconnected_qpairs` in `NVME_RDMA_QPAIR_STATE_INVALID`
        // (confirmed via core dump). If the channel's own, much faster
        // completion poller ticks during that window, it calls
        // `nvme_rdma_ctrlr_disconnect_qpair_poll()` unconditionally on every
        // qpair on that list, which hits SPDK's `assert(false)` on a qpair
        // that never actually started connecting. Running the same poll
        // callback the timer would run anyway, just synchronously and first,
        // closes that window without touching poll-group registration
        // ordering (which SPDK requires to happen before connect, for SRQ
        // wiring).
        Connection::poll_cb(&self_ref);

        Ok(receiver)
    }

    /// Poller's callback.
    fn poll_cb(arg: &UnsafeRef<Self>) -> i32 {
        let conn = unsafe { arg.as_ref() };
        match conn.poll() {
            Ok(r) => {
                if r {
                    0 // continue polling
                } else {
                    1 // stop the poller
                }
            }
            Err(e) => {
                // Error occured, so SPDK won't call connection callback.
                // Notify about failure and stop the poller.
                let conn = unsafe { Box::from_raw(arg.as_ptr()) };
                conn.complete(Err(CoreError::OpenBdev { source: e }));
                1 // stop the poller
            }
        }
    }

    // Polls the async connection probe.
    // Returns true to continue polling, false to stop.
    fn poll(&self) -> Result<bool, Errno> {
        if self.state() == QPairState::Dropped {
            warn!(
                ?self,
                "I/O qpair instance dropped, stopping polling \
                async connection probe"
            );

            // `QPairState::Dropped` indicates that the QPair instance was
            // dropped, and qpair was destroyed. Free the
            // probe manually here as SPDK won't have a chance to do it, and
            // stop polling.
            unsafe { libc::free(self.probe as *mut _ as *mut c_void) };

            return Err(Errno::ECANCELED);
        }

        // Poll the probe. In the case of a success or an error, the probe will
        // be freed by SPDK.
        let res = unsafe { spdk_nvme_ctrlr_io_qpair_connect_poll_async(self.qpair(), self.probe) };

        match res {
            // Connection is complete, callback has been called and this
            // connection instance already dropped.
            0 => Ok(false),
            // Connection is still in progress, keep polling.
            1 => Ok(true),
            // Error occurred during polling.
            e => {
                let e = Errno::from_raw(e.abs());
                error!(?self, "I/O qpair async connection polling error: {e}");
                Err(e)
            }
        }
    }

    /// Consumes `self` and completes the connection operation with the given
    /// result.
    fn complete(mut self, res: ConnectResult) {
        if let Err(err) = &res {
            error!(?self, ?err, "I/O qpair async connection failed");
        } else {
            trace!(?self, "I/O qpair successfully connected");
        }

        // Stop the poller.
        self.poller.take();

        // Set state.
        if self.state() == QPairState::Connecting {
            self.set_state(if res.is_ok() {
                QPairState::Connected
            } else {
                QPairState::Disconnected
            });
        }

        let waiters = self.take_waiters();

        // Notify the listener.
        self.sender
            .take()
            .expect("I/O pair connection object must have been initialized")
            .send(res.clone())
            .expect("Failed to notify I/O qpair connection listener");

        // Notify the waiters.
        waiters.into_iter().for_each(|w| {
            w.send(res.clone())
                .expect("Failed to notify a connection waiter");
        });
    }

    /// Returns SPDK qpair pointer .
    #[inline]
    fn qpair(&self) -> *mut spdk_nvme_qpair {
        self.inner.borrow().qpair
    }

    /// Returns QPair connection state.
    #[inline]
    fn state(&self) -> QPairState {
        self.inner.borrow().state
    }

    /// Sets QPair connection state.
    #[inline]
    fn set_state(&self, state: QPairState) {
        self.inner.borrow_mut().state = state
    }

    /// Takes out the waiter list to avoid iterating under borrow.
    fn take_waiters(&self) -> Vec<ResultSender> {
        let mut waiters = Vec::new();
        std::mem::swap(&mut self.inner.borrow_mut().waiters, &mut waiters);
        waiters
    }
}

/// Async connection callback.
#[cfg(feature = "spdk-async-qpair-connect")]
extern "C" fn qpair_connect_cb(_qpair: *mut spdk_nvme_qpair, cb_arg: *mut c_void) {
    let ctx = unsafe { Box::from_raw(cb_arg as *mut Connection) };
    ctx.complete(Ok(()));
}
