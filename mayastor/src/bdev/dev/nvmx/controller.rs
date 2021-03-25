//!
//!
//! This file contains the main structures for a NVMe controller
use futures::channel::oneshot;
use merge::Merge;
use nix::errno::Errno;
use once_cell::sync::OnceCell;
use std::{
    convert::From,
    fmt,
    os::raw::c_void,
    ptr::NonNull,
    sync::{Arc, Mutex},
};

use spdk_sys::{
    spdk_for_each_channel,
    spdk_for_each_channel_continue,
    spdk_io_channel_iter,
    spdk_io_channel_iter_get_channel,
    spdk_io_channel_iter_get_ctx,
    spdk_io_device_register,
    spdk_io_device_unregister,
    spdk_nvme_ctrlr,
    spdk_nvme_ctrlr_get_ns,
    spdk_nvme_ctrlr_process_admin_completions,
    spdk_nvme_ctrlr_reset,
    spdk_nvme_detach,
};

use crate::{
    bdev::dev::nvmx::{
        channel::{NvmeControllerIoChannel, NvmeIoChannel, NvmeIoChannelInner},
        controller_inner::TimeoutConfig,
        controller_state::{
            ControllerFailureReason,
            ControllerFlag,
            ControllerStateMachine,
        },
        nvme_bdev_running_config,
        uri::NvmeControllerContext,
        NvmeControllerState,
        NvmeControllerState::*,
        NvmeNamespace,
        NVME_CONTROLLERS,
    },
    core::{
        mempool::MemoryPool,
        poller,
        BlockDeviceIoStats,
        CoreError,
        DeviceEventType,
        OpCompletionCallback,
        OpCompletionCallbackArg,
    },
    ffihelper::{cb_arg, done_cb},
    nexus_uri::NexusBdevError,
};

const RESET_CTX_POOL_SIZE: u64 = 1024 - 1;

// Memory pool for keeping context during controller resets.
static RESET_CTX_POOL: OnceCell<MemoryPool<ResetCtx>> = OnceCell::new();

struct ResetCtx {
    name: String,
    cb: OpCompletionCallback,
    cb_arg: OpCompletionCallbackArg,
    spdk_handle: *mut spdk_nvme_ctrlr,
    io_device: Arc<IoDevice>,
    shutdown_in_progress: bool,
}

struct ShutdownCtx {
    name: String,
    cb: OpCompletionCallback,
    cb_arg: OpCompletionCallbackArg,
}

impl<'a> NvmeControllerInner<'a> {
    fn new(
        ctrlr: NonNull<spdk_nvme_ctrlr>,
        name: String,
        cfg: *mut TimeoutConfig,
    ) -> Self {
        let io_device = Arc::new(IoDevice::create(ctrlr.as_ptr().cast(), name));

        let adminq_poller = poller::Builder::new()
            .with_name("nvme_poll_adminq")
            .with_interval(
                nvme_bdev_running_config().nvme_adminq_poll_period_us,
            )
            .with_poll_fn(move || nvme_poll_adminq(cfg as *mut c_void))
            .build();

        Self {
            ctrlr,
            adminq_poller,
            namespaces: Vec::new(),
            io_device,
        }
    }
}
#[derive(Debug)]
pub struct NvmeControllerInner<'a> {
    namespaces: Vec<Arc<NvmeNamespace>>,
    ctrlr: NonNull<spdk_nvme_ctrlr>,
    adminq_poller: poller::Poller<'a>,
    io_device: Arc<IoDevice>,
}

type EventCallbackList = Vec<fn(DeviceEventType, &str)>;

/*
 * NVME controller implementation.
 */
pub struct NvmeController<'a> {
    pub(crate) name: String,
    id: u64,
    prchk_flags: u32,
    inner: Option<NvmeControllerInner<'a>>,
    state_machine: ControllerStateMachine,
    event_listeners: Mutex<EventCallbackList>,
    /// Timeout config is accessed by SPDK-driven timeout callback handlers,
    /// so it needs to be a raw pointer. Mutable members are made atomic to
    /// eliminate lock contention between API path and callback path.
    pub(crate) timeout_config: *mut TimeoutConfig,
}

impl<'a> fmt::Debug for NvmeController<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NvmeController")
            .field("name", &self.name)
            .field("prchk_flags", &self.prchk_flags)
            .field("state_machine", &self.state_machine)
            .finish()
    }
}

unsafe impl<'a> Send for NvmeController<'a> {}
unsafe impl<'a> Sync for NvmeController<'a> {}

impl<'a> NvmeController<'a> {
    /// Creates a new NVMe controller with the given name.
    pub fn new(name: &str, prchk_flags: u32) -> Option<Self> {
        let l = NvmeController {
            name: String::from(name),
            id: 0,
            prchk_flags,
            state_machine: ControllerStateMachine::new(name),
            inner: None,
            event_listeners: Mutex::new(Vec::<fn(DeviceEventType, &str)>::new()),
            timeout_config: Box::into_raw(Box::new(TimeoutConfig::new(name))),
        };

        debug!("{}: new NVMe controller created", l.name);
        Some(l)
    }

    /// returns the name of the current controller
    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    /// returns the protection flags the controller is created with
    pub fn flags(&self) -> u32 {
        self.prchk_flags
    }

    /// returns the ID of the controller
    pub fn id(&self) -> u64 {
        // If controller is initialized, ID must be set.
        if self.state_machine.current_state() != New {
            assert_ne!(self.id, 0, "Controller ID is not yet initialized");
        }
        self.id
    }

    fn set_id(&mut self, id: u64) -> u64 {
        assert_ne!(id, 0, "Controller ID can't be zero");
        self.id = id;
        debug!("{} ID set to 0x{:X}", self.name, self.id);
        id
    }

    // As of now, only 1 namespace per controller is supported.
    pub fn namespace(&self) -> Option<Arc<NvmeNamespace>> {
        let inner = self
            .inner
            .as_ref()
            .expect("(BUG) no inner NVMe controller defined yet");

        if let Some(ns) = inner.namespaces.get(0) {
            Some(Arc::clone(ns))
        } else {
            debug!("no namespaces associated with the current controller");
            None
        }
    }

    /// we should try to avoid this
    pub fn ctrlr_as_ptr(&self) -> *mut spdk_nvme_ctrlr {
        self.inner.as_ref().map_or(std::ptr::null_mut(), |c| {
            let ptr = c.ctrlr.as_ptr();
            debug!("SPDK handle {:p}", ptr);
            ptr
        })
    }

    /// populate name spaces, current we only populate the first namespace
    fn populate_namespaces(&mut self) {
        let ns = unsafe { spdk_nvme_ctrlr_get_ns(self.ctrlr_as_ptr(), 1) };

        if ns.is_null() {
            warn!(
                "{} no namespaces reported by the NVMe controller",
                self.name
            );
        }

        self.inner.as_mut().unwrap().namespaces =
            vec![Arc::new(NvmeNamespace::from_ptr(ns))]
    }

    /// Get controller state.
    pub fn get_state(&self) -> NvmeControllerState {
        self.state_machine.current_state()
    }

    /// Reset the controller.
    /// Upon reset all pending I/O operations are cancelled and all I/O handles
    /// are reinitialized.
    pub fn reset(
        &mut self,
        cb: OpCompletionCallback,
        cb_arg: OpCompletionCallbackArg,
        failover: bool,
    ) -> Result<(), CoreError> {
        match self.state_machine.current_state() {
            Running | Faulted(_) => {}
            _ => {
                error!(
                    "{} Controller is in '{:?}' state, reset not possible",
                    self.name,
                    self.state_machine.current_state()
                );
                return Err(CoreError::ResetDispatch {
                    source: Errno::EBUSY,
                });
            }
        }

        self.state_machine
            .set_flag_exclusively(ControllerFlag::ResetActive)
            .map_err(|_| {
                error!("{} reset already in progress", self.name);
                CoreError::ResetDispatch {
                    source: Errno::EBUSY,
                }
            })?;

        info!(
            "{} initiating controller reset, failover = {}",
            self.name, failover
        );

        if failover {
            warn!(
                "{} failover is not supported for controller reset",
                self.name
            );
        }

        let io_device = Arc::clone(&self.inner.as_ref().unwrap().io_device);
        let reset_ctx = ResetCtx {
            name: self.name.clone(),
            cb,
            cb_arg,
            spdk_handle: self.ctrlr_as_ptr(),
            io_device,
            shutdown_in_progress: false,
        };

        info!("{}: starting reset", self.name);
        let inner = self.inner.as_mut().unwrap();
        // Iterate over all I/O channels and rrest/econfigure them one by one.
        inner.io_device.traverse_io_channels(
            NvmeController::_reset_destroy_channels,
            NvmeController::_reset_destroy_channels_done,
            reset_ctx,
        );
        Ok(())
    }

    fn _shutdown_channels(
        channel: &mut NvmeIoChannelInner,
        ctx: &mut ShutdownCtx,
    ) -> i32 {
        let rc = channel.shutdown();

        if rc == 0 {
            debug!("{} I/O channel successfully shutdown", ctx.name);
        } else {
            error!(
                "{} failed to shutdown I/O channel, reset aborted",
                ctx.name
            );
        }
        rc
    }

    fn _shutdown_channels_done(result: i32, ctx: ShutdownCtx) {
        info!("{} all I/O channels shutted down", ctx.name);

        let controller = NVME_CONTROLLERS
            .lookup_by_name(&ctx.name)
            .expect("Controller disappeared while being shutdown");
        let mut controller = controller.lock().expect("lock poisoned");

        // In case I/O channels didn't shutdown successfully, mark
        // the controller as Faulted.
        if result != 0 {
            error!("{} failed to shutdown I/O channels, rc = {}. Shutdown aborted.", ctx.name, result);
            controller
                .state_machine
                .transition(Faulted(ControllerFailureReason::ShutdownFailed))
                .expect("failed to transition controller to Faulted state");
            return;
        }

        // Reset the controller to complete all remaining I/O requests after all
        // I/O channels are closed.
        // TODO: fail the controller via spdk_nvme_ctrlr_fail() upon shutdown ?
        debug!("{} resetting NVMe controller", ctx.name);
        let rc = unsafe { spdk_nvme_ctrlr_reset(controller.ctrlr_as_ptr()) };
        if rc != 0 {
            error!("{} failed to reset controller, rc = {}", ctx.name, rc);
        }

        // Finalize controller shutdown and invoke callback.
        controller.clear_namespaces();
        controller
            .state_machine
            .transition(Unconfigured)
            .expect("failed to transition controller to Unconfigured state");

        drop(controller);
        info!("{} shutdown complete, result = {}", ctx.name, result);
        (ctx.cb)(result == 0, ctx.cb_arg);
    }

    fn clear_namespaces(&mut self) {
        let inner = self
            .inner
            .as_mut()
            .expect("(BUG) no inner NVMe controller defined yet");
        inner.namespaces.clear();
        debug!("{} all namespaces removed", self.name);
    }

    /// Get I/O statistics for all I/O channels of the controller.
    pub fn get_io_stats<T: 'static + Sized, F>(
        &self,
        cb: F,
        cb_arg: T,
    ) -> Result<(), CoreError>
    where
        F: Fn(Result<BlockDeviceIoStats, CoreError>, T) + 'static,
    {
        struct IoStatsCtx<V: 'static + Sized> {
            cb: Box<dyn Fn(Result<BlockDeviceIoStats, CoreError>, V) + 'static>,
            cb_arg: V,
            io_stats: BlockDeviceIoStats,
        }

        if self.state_machine.current_state() != Running {
            error!(
                "{} Controller is in '{:?}' state, reset not possible",
                self.name,
                self.state_machine.current_state()
            );
            return Err(CoreError::DeviceStatisticsError {
                source: Errno::EAGAIN,
            });
        }

        let ctx = IoStatsCtx {
            cb: Box::new(cb),
            cb_arg,
            io_stats: BlockDeviceIoStats::default(),
        };

        // Process I/O statistics for a given channel.
        fn account_channel_stats<N>(
            channel: &mut NvmeIoChannelInner,
            ctx: &mut IoStatsCtx<N>,
        ) -> i32 {
            ctx.io_stats
                .merge(channel.get_io_stats_controller().get_io_stats());
            0
        }

        // Pass aggregated I/O statistics back to the caller.
        fn account_channel_stats_done<N>(result: i32, ctx: IoStatsCtx<N>) {
            let stats = if result == 0 {
                Ok(ctx.io_stats)
            } else {
                Err(CoreError::DeviceStatisticsError {
                    source: Errno::EAGAIN,
                })
            };

            (ctx.cb)(stats, ctx.cb_arg)
        }

        self.inner.as_ref().unwrap().io_device.traverse_io_channels(
            account_channel_stats::<T>,
            account_channel_stats_done::<T>,
            ctx,
        );

        Ok(())
    }

    /// Shutdown the controller and all its resources.
    /// This function deallocates all controller's resources (I/O queues, I/O
    /// channels and pollers), aborts all active I/O operations and
    /// unregisters the I/O device associated with the controller.
    pub fn shutdown(
        &mut self,
        cb: OpCompletionCallback,
        cb_arg: OpCompletionCallbackArg,
    ) -> Result<(), CoreError> {
        self.state_machine.transition(Unconfiguring).map_err(|_| {
            error!(
                "{} controller is in {:?} state, cannot shutdown",
                self.name,
                self.state_machine.current_state(),
            );
            CoreError::ResetDispatch {
                source: Errno::EBUSY,
            }
        })?;

        info!("{} shutting down the controller", self.name);

        let ctx = ShutdownCtx {
            name: self.get_name(),
            cb,
            cb_arg,
        };

        // Schedule asynchronous device shutdown and return.
        self.inner.as_ref().unwrap().io_device.traverse_io_channels(
            NvmeController::_shutdown_channels,
            NvmeController::_shutdown_channels_done,
            ctx,
        );

        Ok(())
    }

    fn _complete_reset(reset_ctx: ResetCtx, status: i32) {
        // Lookup controller carefully, as it can be removed while reset
        // in progress.
        let c = NVME_CONTROLLERS.lookup_by_name(reset_ctx.name);
        if let Some(controller) = c {
            let mut controller = controller.lock().expect("lock poisoned");

            // If controller exists, its state must reflect active reset
            // operation, as no other operations are allowed upon
            // reset except device removal.
            controller
                .state_machine
                .clear_flag_exclusively(ControllerFlag::ResetActive)
                .expect("Reset flag improperly cleared during reset");

            if status != 0 {
                // Transition controller into Faulted state, but only if the
                // controller is in Running state, as concurrent
                // shutdown might be in place.
                let _ = controller.state_machine.transition_checked(
                    Running,
                    Faulted(ControllerFailureReason::ResetFailed),
                );
            }

            // Unlock the controller before calling the callback to avoid
            // potential deadlocks.
            drop(controller);
        }

        (reset_ctx.cb)(status == 0, reset_ctx.cb_arg);
    }

    fn _reset_destroy_channels(
        channel: &mut NvmeIoChannelInner,
        ctx: &mut ResetCtx,
    ) -> i32 {
        debug!("Resetting I/O channel");

        // Bail out preliminary if shutdown is active.
        if ctx.shutdown_in_progress {
            return 0;
        }

        // Check in advance for concurrent controller shutdown.
        if channel.is_shutdown() {
            ctx.shutdown_in_progress = true;
            return 0;
        }

        let rc = channel.reset();

        if rc == 0 {
            debug!("I/O channel successfully reset");
        } else {
            error!("failed to reset I/O channel (rc={}), reset aborted", rc);
        }
        rc
    }

    fn _reset_destroy_channels_done(status: i32, reset_ctx: ResetCtx) {
        if status != 0 {
            error!(
                "{}: controller reset failed with status = {}",
                reset_ctx.name, status
            );
            NvmeController::_complete_reset(reset_ctx, status);
            return;
        }

        info!("{}: all I/O channels successfully reset", reset_ctx.name);
        // In case shutdown is active, don't reset the controller as its
        // being removed.
        if reset_ctx.shutdown_in_progress {
            info!(
                "{}: controller shutdown detected, skipping reset",
                reset_ctx.name
            );
            return;
        }

        let rc = unsafe { spdk_nvme_ctrlr_reset(reset_ctx.spdk_handle) };
        if rc != 0 {
            error!(
                "{} failed to reset controller, rc = {}",
                reset_ctx.name, rc
            );

            NvmeController::_complete_reset(reset_ctx, rc);
        } else {
            info!(
                "{} controller successfully reset, reinitializing I/O channels",
                reset_ctx.name
            );

            /* Once controller is successfully reset, schedule another I/O
             * channel traversal to restore all I/O channels.
             */
            let io_device = Arc::clone(&reset_ctx.io_device);
            io_device.traverse_io_channels(
                NvmeController::_reset_create_channels,
                NvmeController::_reset_create_channels_done,
                reset_ctx,
            );
        }
    }

    fn _reset_create_channels(
        channel: &mut NvmeIoChannelInner,
        reset_ctx: &mut ResetCtx,
    ) -> i32 {
        // Make sure no cuncurrent shutdown takes place.
        if channel.is_shutdown() {
            return 0;
        }

        debug!("Reinitializing I/O channel");
        let rc = channel.reinitialize(&reset_ctx.name, reset_ctx.spdk_handle);
        if rc != 0 {
            error!(
                "{} failed to reinitialize I/O channel, rc = {}",
                reset_ctx.name, rc
            );
        } else {
            info!("{} I/O channel successfully reinitialized", reset_ctx.name);
        }
        rc
    }

    fn _reset_create_channels_done(status: i32, reset_ctx: ResetCtx) {
        info!(
            "{} controller reset completed, status = {}",
            reset_ctx.name, status
        );
        NvmeController::_complete_reset(reset_ctx, status);
    }

    fn notify_event(&self, event: DeviceEventType) -> usize {
        // Keep a separate copy of all registered listeners in order to not
        // invoke them with the lock held.
        let listeners = {
            let listeners = self
                .event_listeners
                .lock()
                .expect("event listeners lock poisoned");
            listeners.clone()
        };

        for l in listeners.iter() {
            (*l)(event, &self.name);
        }
        listeners.len()
    }

    /// Register listener to monitor device events related to this controller.
    pub fn add_event_listener(
        &self,
        listener: fn(DeviceEventType, &str),
    ) -> Result<(), CoreError> {
        let mut listeners = self
            .event_listeners
            .lock()
            .expect("event listeners lock poisoned");

        listeners.push(listener);
        debug!("{} added event listener", self.name);
        Ok(())
    }
}

impl<'a> Drop for NvmeController<'a> {
    fn drop(&mut self) {
        let curr_state = self.get_state();
        debug!("{} dropping controller (state={:?})", self.name, curr_state);

        // Controller must be properly unconfigured to prevent dangerous
        // side-effects (like active qpairs referring to not existing
        // controller).
        assert!(
            matches!(curr_state, New | Unconfigured),
            "{} dropping active controller in {:?} state",
            self.name,
            curr_state
        );

        // Inner state might not be yes available.
        if self.inner.is_some() {
            let inner = self.inner.take().expect("NVMe inner already gone");

            debug!(
                ?self.name,
                "detaching NVMe controller"
            );
            let rc = unsafe { spdk_nvme_detach(inner.ctrlr.as_ptr()) };

            debug!(
                ?self.name,
                "stopping admin queue poller"
            );
            inner.adminq_poller.stop();

            assert_eq!(rc, 0, "Failed to detach NVMe controller");
            debug!(
                ?self.name,
                "NVMe controller successfully detached"
            );
        }

        unsafe {
            std::ptr::drop_in_place(self.timeout_config);
        }
    }
}

/// return number of completions processed (maybe 0) or negated on error. -ENXIO
/// in the special case that the qpair is failed at the transport layer.
pub extern "C" fn nvme_poll_adminq(ctx: *mut c_void) -> i32 {
    let timeout_cfg = TimeoutConfig::from_ptr(ctx as *mut TimeoutConfig);

    let rc =
        unsafe { spdk_nvme_ctrlr_process_admin_completions(timeout_cfg.ctrlr) };

    // Reset controller upon failure.
    if rc < 0 {
        timeout_cfg.reset_controller();
    }

    if rc == 0 {
        0
    } else {
        1
    }
}

/// Destroy target controller and notify all listeners about device removal.
pub(crate) async fn destroy_device(name: String) -> Result<(), NexusBdevError> {
    let carc = NVME_CONTROLLERS.lookup_by_name(&name).ok_or(
        NexusBdevError::BdevNotFound {
            name: String::from(&name),
        },
    )?;

    // 1. Initiate controller shutdown, which shuts down all I/O resources
    // of the controller.
    let (s, r) = oneshot::channel::<bool>();
    {
        let mut controller = carc.lock().expect("lock poisoned");

        fn _shutdown_callback(success: bool, ctx: *mut c_void) {
            done_cb(ctx, success);
        }

        controller
            .shutdown(_shutdown_callback, cb_arg(s))
            .map_err(|_| NexusBdevError::DestroyBdev {
                name: String::from(&name),
                source: Errno::EAGAIN,
            })?
    }

    if !r.await.expect("Failed awaiting at shutdown()") {
        error!(?name, "failed to shutdown controller");
        return Err(NexusBdevError::DestroyBdev {
            name: String::from(&name),
            source: Errno::EAGAIN,
        });
    }

    // 2. Remove controller from the list so that a new controller with the
    // same name can be inserted. Note that there may exist other
    // references to the controller before removal, but since all
    // controller's resources have been invalidated, that exposes no
    // risk, as no operations will be possible on such controllers.
    if NVME_CONTROLLERS.remove_by_name(&name).is_err() {
        warn!(?name, "no controller record found, proceeding with removal");
    } else {
        debug!(?name, "removed from controller list");
    }

    // Notify the listeners.
    debug!(?name, "notifying listeners about device removal");
    let controller = carc.lock().unwrap();
    let num_listeners = controller.notify_event(DeviceEventType::DeviceRemoved);
    debug!(
        ?name,
        ?num_listeners,
        "listeners notified about device removal"
    );

    Ok(())
}

pub(crate) fn connected_attached_cb(
    ctx: &mut NvmeControllerContext,
    ctrlr: NonNull<spdk_nvme_ctrlr>,
) {
    ctx.unregister_poller();
    // we use the ctrlr address as the controller id in the global table
    let cid = ctrlr.as_ptr() as u64;

    // get a reference to our controller we created when we kicked of the async
    // attaching process
    let controller = NVME_CONTROLLERS
        .lookup_by_name(&ctx.name())
        .expect("no controller in the list");

    // clone it now such that we can lock the original, and insert it later.
    let ctl = Arc::clone(&controller);
    let mut controller = controller.lock().unwrap();
    controller
        .state_machine
        .transition(Initializing)
        .expect("Failed to transition controller into Initialized state");

    unsafe {
        (*controller.timeout_config).ctrlr = ctrlr.as_ptr();
    }

    controller.set_id(cid);
    controller.inner = Some(NvmeControllerInner::new(
        ctrlr,
        controller.get_name(),
        controller.timeout_config,
    ));

    controller.configure_timeout();
    controller.populate_namespaces();

    // Proactively initialize cache for controller operations.
    RESET_CTX_POOL.get_or_init(|| {
        MemoryPool::<ResetCtx>::create(
            "nvme_ctrlr_reset_ctx",
            RESET_CTX_POOL_SIZE,
        )
        .expect(
            "Failed to create memory pool for NVMe controller reset contexts",
        )
    });

    NVME_CONTROLLERS.insert_controller(cid.to_string(), ctl);

    controller
        .state_machine
        .transition(Running)
        .expect("Failed to transition controller into Running state");

    // Wake up the waiter and complete controller registration.
    ctx.sender()
        .send(Ok(()))
        .expect("done callback receiver side disappeared");
}

pub(crate) mod options {
    use std::mem::size_of;

    use spdk_sys::{
        spdk_nvme_ctrlr_get_default_ctrlr_opts,
        spdk_nvme_ctrlr_opts,
    };

    /// structure that holds the default NVMe controller options. This is
    /// different from ['NvmeBdevOpts'] as it exposes more control over
    /// variables.

    pub struct NvmeControllerOpts(spdk_nvme_ctrlr_opts);
    impl NvmeControllerOpts {
        pub fn as_ptr(&self) -> *const spdk_nvme_ctrlr_opts {
            &self.0
        }
    }

    impl Default for NvmeControllerOpts {
        fn default() -> Self {
            let mut default = spdk_nvme_ctrlr_opts::default();

            unsafe {
                spdk_nvme_ctrlr_get_default_ctrlr_opts(
                    &mut default,
                    size_of::<spdk_nvme_ctrlr_opts>() as u64,
                );
            }

            Self(default)
        }
    }

    #[derive(Debug, Default)]
    pub struct Builder {
        admin_timeout_ms: Option<u32>,
        disable_error_logging: Option<bool>,
        fabrics_connect_timeout_us: Option<u64>,
        transport_retry_count: Option<u8>,
        keep_alive_timeout_ms: Option<u32>,
    }

    #[allow(dead_code)]
    impl Builder {
        pub fn new() -> Self {
            Self::default()
        }

        pub fn with_admin_timeout_ms(mut self, timeout: u32) -> Self {
            self.admin_timeout_ms = Some(timeout);
            self
        }
        pub fn with_fabrics_connect_timeout_us(mut self, timeout: u64) -> Self {
            self.fabrics_connect_timeout_us = Some(timeout);
            self
        }

        pub fn with_transport_retry_count(mut self, count: u8) -> Self {
            self.transport_retry_count = Some(count);
            self
        }

        pub fn with_keep_alive_timeout_ms(mut self, timeout: u32) -> Self {
            self.keep_alive_timeout_ms = Some(timeout);
            self
        }

        pub fn disable_error_logging(mut self, disable: bool) -> Self {
            self.disable_error_logging = Some(disable);
            self
        }

        /// Builder to override default values
        pub fn build(self) -> NvmeControllerOpts {
            let mut opts = NvmeControllerOpts::default();

            if let Some(timeout_ms) = self.admin_timeout_ms {
                opts.0.admin_timeout_ms = timeout_ms;
            }
            if let Some(timeout_us) = self.fabrics_connect_timeout_us {
                opts.0.fabrics_connect_timeout_us = timeout_us;
            }

            if let Some(retries) = self.transport_retry_count {
                opts.0.transport_retry_count = retries;
            }

            if let Some(timeout_ms) = self.keep_alive_timeout_ms {
                opts.0.keep_alive_timeout_ms = timeout_ms;
            }

            opts
        }
    }
    #[cfg(test)]
    mod test {
        use crate::bdev::dev::nvmx::controller::options;

        #[test]
        fn nvme_default_controller_options() {
            let opts = options::Builder::new()
                .with_admin_timeout_ms(1)
                .with_fabrics_connect_timeout_us(1)
                .with_transport_retry_count(1)
                .build();

            assert_eq!(opts.0.admin_timeout_ms, 1);
            assert_eq!(opts.0.fabrics_connect_timeout_us, 1);
            assert_eq!(opts.0.transport_retry_count, 1);
        }
    }
}

#[derive(Debug)]
struct IoDevice(NonNull<c_void>);

/// Wrapper around SPDK I/O device.
impl IoDevice {
    /// Register the controller as an I/O device.
    fn create(devptr: *mut c_void, name: String) -> Self {
        unsafe {
            spdk_io_device_register(
                devptr,
                Some(NvmeControllerIoChannel::create),
                Some(NvmeControllerIoChannel::destroy),
                std::mem::size_of::<NvmeIoChannel>() as u32,
                name.as_ptr() as *const i8,
            )
        }

        debug!("{} I/O device registered at {:p}", name, devptr);

        Self(NonNull::new(devptr).unwrap())
    }

    /// Iterate over all I/O channels associated with this I/O device.
    fn traverse_io_channels<T>(
        &self,
        channel_cb: impl FnMut(&mut NvmeIoChannelInner, &mut T) -> i32 + 'static,
        done_cb: impl FnMut(i32, T) + 'static,
        caller_ctx: T,
    ) {
        struct TraverseCtx<N> {
            channel_cb: Box<
                dyn FnMut(&mut NvmeIoChannelInner, &mut N) -> i32 + 'static,
            >,
            done_cb: Box<dyn FnMut(i32, N) + 'static>,
            ctx: N,
        }

        let traverse_ctx = Box::into_raw(Box::new(TraverseCtx {
            channel_cb: Box::new(channel_cb),
            done_cb: Box::new(done_cb),
            ctx: caller_ctx,
        }));
        assert!(
            !traverse_ctx.is_null(),
            "Failed to allocate contex for I/O channels iteration"
        );

        /// Low-level per-channel visitor to be invoked by SPDK I/O channel
        /// enumeration logic.
        extern "C" fn _visit_channel<V>(i: *mut spdk_io_channel_iter) {
            let traverse_ctx = unsafe {
                let p = spdk_io_channel_iter_get_ctx(i) as *mut TraverseCtx<V>;
                &mut *p
            };
            let io_channel = unsafe {
                let ch = spdk_io_channel_iter_get_channel(i);
                NvmeIoChannel::inner_from_channel(ch)
            };

            let rc =
                (traverse_ctx.channel_cb)(io_channel, &mut traverse_ctx.ctx);

            unsafe {
                spdk_for_each_channel_continue(i, rc);
            }
        }

        /// Low-level completion callback for SPDK I/O channel enumeration
        /// logic.
        extern "C" fn _visit_channel_done<V>(
            i: *mut spdk_io_channel_iter,
            status: i32,
        ) {
            // Reconstruct the context box to let all the resources be properly
            // dropped.
            let mut traverse_ctx = unsafe {
                Box::<TraverseCtx<V>>::from_raw(
                    spdk_io_channel_iter_get_ctx(i) as *mut TraverseCtx<V>
                )
            };

            (traverse_ctx.done_cb)(status, traverse_ctx.ctx);
        }

        // Start I/O channel iteration via SPDK.
        unsafe {
            spdk_for_each_channel(
                self.0.as_ptr(),
                Some(_visit_channel::<T>),
                traverse_ctx as *mut c_void,
                Some(_visit_channel_done::<T>),
            );
        }
    }
}

impl Drop for IoDevice {
    fn drop(&mut self) {
        debug!("unregistering I/O device at {:p}", self.0.as_ptr());

        unsafe {
            spdk_io_device_unregister(self.0.as_ptr(), None);
        }
    }
}

pub(crate) mod transport {
    use libc::c_void;
    use spdk_sys::spdk_nvme_transport_id;
    use std::{ffi::CStr, fmt::Debug, ptr::copy_nonoverlapping};

    pub struct NvmeTransportId(spdk_nvme_transport_id);

    impl Debug for NvmeTransportId {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            writeln!(
                f,
                "Transport ID: {}: {}: {}: {}:",
                self.trtype(),
                self.traddr(),
                self.subnqn(),
                self.svcid()
            )
        }
    }

    impl NvmeTransportId {
        pub fn trtype(&self) -> String {
            unsafe {
                CStr::from_ptr(&self.0.trstring[0])
                    .to_string_lossy()
                    .to_string()
            }
        }

        pub fn traddr(&self) -> String {
            unsafe {
                CStr::from_ptr(&self.0.traddr[0])
                    .to_string_lossy()
                    .to_string()
            }
        }

        pub fn subnqn(&self) -> String {
            unsafe {
                CStr::from_ptr(&self.0.subnqn[0])
                    .to_string_lossy()
                    .to_string()
            }
        }
        pub fn svcid(&self) -> String {
            unsafe {
                CStr::from_ptr(&self.0.trsvcid[0])
                    .to_string_lossy()
                    .to_string()
            }
        }

        pub fn as_ptr(&self) -> *const spdk_nvme_transport_id {
            &self.0
        }
    }

    #[derive(Debug)]
    enum TransportId {
        TCP = 0x3,
    }

    impl Default for TransportId {
        fn default() -> Self {
            Self::TCP
        }
    }

    impl From<TransportId> for String {
        fn from(t: TransportId) -> Self {
            match t {
                TransportId::TCP => String::from("tcp"),
            }
        }
    }

    #[derive(Debug)]
    #[allow(dead_code)]
    pub(crate) enum AdressFamily {
        NvmfAdrfamIpv4 = 0x1,
        NvmfAdrfamIpv6 = 0x2,
        NvmfAdrfamIb = 0x3,
        NvmfAdrfamFc = 0x4,
        NvmfAdrfamLoop = 0xfe,
    }

    impl Default for AdressFamily {
        fn default() -> Self {
            Self::NvmfAdrfamIpv4
        }
    }

    #[derive(Default, Debug)]
    pub struct Builder {
        trid: TransportId,
        adrfam: AdressFamily,
        svcid: String,
        traddr: String,
        subnqn: String,
    }

    impl Builder {
        pub fn new() -> Self {
            Self {
                ..Default::default()
            }
        }

        /// the address to connect to
        pub fn with_traddr(mut self, traddr: &str) -> Self {
            self.traddr = traddr.to_string();
            self
        }
        /// svcid (port) to connect to

        pub fn with_svcid(mut self, svcid: &str) -> Self {
            self.svcid = svcid.to_string();
            self
        }

        /// target nqn
        pub fn with_subnqn(mut self, subnqn: &str) -> Self {
            self.subnqn = subnqn.to_string();
            self
        }

        /// builder for transportID currently defaults to TCP IPv4
        pub fn build(self) -> NvmeTransportId {
            let trtype = String::from(TransportId::TCP);
            let mut trid = spdk_nvme_transport_id {
                adrfam: AdressFamily::NvmfAdrfamIpv4 as u32,
                trtype: TransportId::TCP as u32,
                ..Default::default()
            };

            unsafe {
                copy_nonoverlapping(
                    trtype.as_ptr().cast(),
                    &mut trid.trstring[0] as *const _ as *mut c_void,
                    trtype.len(),
                );

                copy_nonoverlapping(
                    self.traddr.as_ptr().cast(),
                    &mut trid.traddr[0] as *const _ as *mut c_void,
                    self.traddr.len(),
                );
                copy_nonoverlapping(
                    self.svcid.as_ptr() as *const c_void,
                    &mut trid.trsvcid[0] as *const _ as *mut c_void,
                    self.svcid.len(),
                );
                copy_nonoverlapping(
                    self.subnqn.as_ptr() as *const c_void,
                    &mut trid.subnqn[0] as *const _ as *mut c_void,
                    self.subnqn.len(),
                );
            };

            NvmeTransportId(trid)
        }
    }

    #[cfg(test)]
    mod test {
        use crate::bdev::dev::nvmx::controller::transport;

        #[test]
        fn test_transport_id() {
            let transport = transport::Builder::new()
                .with_subnqn("nqn.2021-01-01:test.nqn")
                .with_svcid("4420")
                .with_traddr("127.0.0.1")
                .build();

            assert_eq!(transport.traddr(), "127.0.0.1");
            assert_eq!(transport.subnqn(), "nqn.2021-01-01:test.nqn");
            assert_eq!(transport.svcid(), "4420");
        }
    }
}
