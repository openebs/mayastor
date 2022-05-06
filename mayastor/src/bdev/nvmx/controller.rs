//!
//!
//! This file contains the main structures for a NVMe controller
use std::{
    convert::From,
    fmt,
    os::raw::c_void,
    ptr::NonNull,
    sync::{Arc, Mutex},
};

use futures::channel::oneshot;
use merge::Merge;
use nix::errno::Errno;

use spdk_rs::libspdk::{
    spdk_nvme_async_event_completion,
    spdk_nvme_cpl,
    spdk_nvme_ctrlr,
    spdk_nvme_ctrlr_fail,
    spdk_nvme_ctrlr_get_ns,
    spdk_nvme_ctrlr_is_active_ns,
    spdk_nvme_ctrlr_register_aer_callback,
    spdk_nvme_ctrlr_reset,
    spdk_nvme_detach,
};

use crate::{
    bdev::nvmx::{
        channel::{NvmeControllerIoChannel, NvmeIoChannel, NvmeIoChannelInner},
        controller_inner::{SpdkNvmeController, TimeoutConfig},
        controller_state::{
            ControllerFailureReason,
            ControllerFlag,
            ControllerStateMachine,
        },
        nvme_bdev_running_config,
        uri::NvmeControllerContext,
        utils::{
            nvme_cpl_succeeded,
            NvmeAerInfoNotice,
            NvmeAerInfoNvmCommandSet,
            NvmeAerType,
        },
        NvmeControllerState,
        NvmeControllerState::*,
        NvmeNamespace,
        NVME_CONTROLLERS,
    },
    core::{
        poller,
        BlockDeviceIoStats,
        CoreError,
        DeviceEventDispatcher,
        DeviceEventSink,
        DeviceEventType,
        IoDevice,
        OpCompletionCallback,
        OpCompletionCallbackArg,
    },
    ffihelper::{cb_arg, done_cb},
    nexus_uri::NexusBdevError,
    sleep::mayastor_sleep,
};

#[derive(Debug)]
struct ResetCtx {
    name: String,
    cb: OpCompletionCallback,
    cb_arg: OpCompletionCallbackArg,
    spdk_handle: SpdkNvmeController,
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
        ctrlr: SpdkNvmeController,
        name: String,
        cfg: NonNull<TimeoutConfig>,
    ) -> Self {
        let io_device = Arc::new(IoDevice::new::<NvmeIoChannel>(
            NonNull::new(ctrlr.as_ptr().cast()).unwrap(),
            &name,
            Some(NvmeControllerIoChannel::create),
            Some(NvmeControllerIoChannel::destroy),
        ));

        let adminq_poller = poller::Builder::new()
            .with_name("nvme_poll_adminq")
            .with_interval(
                nvme_bdev_running_config().nvme_adminq_poll_period_us,
            )
            .with_poll_fn(move || nvme_poll_adminq(cfg.as_ptr().cast()))
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
    ctrlr: SpdkNvmeController,
    adminq_poller: poller::Poller<'a>,
    io_device: Arc<IoDevice>,
}

unsafe impl<'a> Send for NvmeControllerInner<'a> {}
unsafe impl<'a> Sync for NvmeControllerInner<'a> {}

/// NVME controller implementation.
/// TODO
pub struct NvmeController<'a> {
    pub(crate) name: String,
    id: u64,
    prchk_flags: u32,
    inner: Option<NvmeControllerInner<'a>>,
    state_machine: ControllerStateMachine,
    event_dispatcher: Mutex<DeviceEventDispatcher>,
    /// Timeout config is accessed by SPDK-driven timeout callback handlers,
    /// so it needs to be a raw pointer. Mutable members are made atomic to
    /// eliminate lock contention between API path and callback path.
    pub(crate) timeout_config: NonNull<TimeoutConfig>,
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
            event_dispatcher: Mutex::new(DeviceEventDispatcher::new()),
            timeout_config: NonNull::new(Box::into_raw(Box::new(
                TimeoutConfig::new(name),
            )))
            .expect("failed to box timeout context"),
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
            Some(ns.clone())
        } else {
            debug!("no namespaces associated with the current controller");
            None
        }
    }

    pub fn controller(&self) -> Option<SpdkNvmeController> {
        self.inner.as_ref().map(|c| c.ctrlr)
    }

    /// we should try to avoid this
    pub fn ctrlr_as_ptr(&self) -> *mut spdk_nvme_ctrlr {
        self.inner.as_ref().map_or(std::ptr::null_mut(), |c| {
            let ptr = c.ctrlr.as_ptr();
            debug!("SPDK handle {:p}", ptr);
            ptr
        })
    }

    /// Register callbacks.
    fn register_callbacks(&mut self) {
        let ctrlr = self.ctrlr_as_ptr();

        unsafe {
            spdk_nvme_ctrlr_register_aer_callback(
                ctrlr,
                Some(aer_cb),
                ctrlr as *mut c_void,
            );
        };
    }

    /// populate name spaces, current we only populate the first namespace
    fn populate_namespaces(&mut self) -> bool {
        let ctrlr = self.ctrlr_as_ptr();
        let mut ctrlr_inner = self.inner.as_mut().unwrap();
        let ns = unsafe { spdk_nvme_ctrlr_get_ns(ctrlr, 1) };
        let ns_active = unsafe { spdk_nvme_ctrlr_is_active_ns(ctrlr, 1) };
        let mut notify_listeners = false;

        // Deactivate existing namespace in case it is no longer active.
        if !ns_active && !ctrlr_inner.namespaces.is_empty() {
            debug!("{}: deactivating existing namespace", self.name);
            notify_listeners = true;
        }

        let namespaces = if ns.is_null() || !ns_active {
            warn!(
                "{}: no active namespaces reported by the NVMe controller",
                self.name
            );
            vec![]
        } else {
            debug!("{}: namespace successfully populated", self.name);
            vec![Arc::new(NvmeNamespace::from_ptr(ns))]
        };

        ctrlr_inner.namespaces = namespaces;

        // Fault the controller in case of inactive namespace.
        if !ns_active {
            self
                .state_machine
                .transition(Faulted(ControllerFailureReason::NamespaceInit))
                .expect("failed to fault controller in response to ns enumeration failure");
        }

        // Notify listeners in case of namespace removal.
        if notify_listeners {
            self.notify_listeners(DeviceEventType::DeviceRemoved);
        }

        ns_active
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

        debug!(
            "{} initiating controller reset, failover = {}",
            self.name, failover
        );

        if failover {
            warn!(
                "{} failover is not supported for controller reset",
                self.name
            );
        }

        let io_device = self.inner.as_ref().unwrap().io_device.clone();
        let reset_ctx = ResetCtx {
            name: self.name.clone(),
            cb,
            cb_arg,
            spdk_handle: self
                .controller()
                .expect("controller is may not be NULL"),
            io_device,
            shutdown_in_progress: false,
        };

        debug!("{}: starting reset", self.name);
        let inner = self.inner.as_mut().unwrap();
        // Iterate over all I/O channels and reset/configure them one by one.
        inner.io_device.traverse_io_channels(
            NvmeController::_reset_destroy_channels,
            NvmeController::_reset_destroy_channels_done,
            NvmeIoChannel::inner_from_channel,
            reset_ctx,
        );
        Ok(())
    }

    fn _shutdown_channels(
        channel: &mut NvmeIoChannelInner,
        ctx: &mut ShutdownCtx,
    ) -> i32 {
        debug!(?ctx.name, "shutting down I/O channel");
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
        debug!("{} all I/O channels shutted down", ctx.name);

        let controller = NVME_CONTROLLERS
            .lookup_by_name(&ctx.name)
            .expect("Controller disappeared while being shutdown");
        let mut controller = controller.lock();

        // In case I/O channels didn't shutdown successfully, mark
        // the controller as Faulted.
        if result != 0 {
            error!("{} failed to shutdown I/O channels, rc = {}. Shutdown aborted.", ctx.name, result);
            controller
                .state_machine
                .transition(Faulted(ControllerFailureReason::Shutdown))
                .expect("failed to transition controller to Faulted state");
            return;
        }

        // Reset the controller to complete all remaining I/O requests after all
        // I/O channels are closed.
        // TODO: fail the controller via spdk_nvme_ctrlr_fail() upon shutdown ?
        //debug!("{} resetting NVMe controller", ctx.name);
        debug!("{} resetting NVMe controller", ctx.name);
        // unsafe {
        //     (*controller.ctrlr_as_ptr()).reinit_after_reset = false;
        // }
        // let rc = unsafe { spdk_nvme_ctrlr_reset(controller.ctrlr_as_ptr()) };
        // if rc != 0 {
        //     error!("{} failed to reset controller, rc = {}", ctx.name, rc);
        // }
        unsafe { spdk_nvme_ctrlr_fail(controller.ctrlr_as_ptr()) };

        // Finalize controller shutdown and invoke callback.
        controller.clear_namespaces();
        controller
            .state_machine
            .transition(Unconfigured)
            .expect("failed to transition controller to Unconfigured state");

        drop(controller);
        debug!("{} shutdown complete, result = {}", ctx.name, result);
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
            NvmeIoChannel::inner_from_channel,
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
        // Prevent racing device destroy
        unsafe {
            self.timeout_config.as_mut().start_device_destroy();
        }

        debug!("{} shutting down the controller", self.name);

        let ctx = ShutdownCtx {
            name: self.get_name(),
            cb,
            cb_arg,
        };

        // Schedule asynchronous device shutdown and return.
        self.inner.as_ref().unwrap().io_device.traverse_io_channels(
            NvmeController::_shutdown_channels,
            NvmeController::_shutdown_channels_done,
            NvmeIoChannel::inner_from_channel,
            ctx,
        );

        Ok(())
    }

    fn _complete_reset(reset_ctx: ResetCtx, status: i32) {
        // Lookup controller carefully, as it can be removed while reset
        // in progress.
        let c = NVME_CONTROLLERS.lookup_by_name(reset_ctx.name);
        if let Some(controller) = c {
            let mut controller = controller.lock();

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
                    Faulted(ControllerFailureReason::Reset),
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
        debug!(?channel, "resetting");

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

    fn hot_remove_channels_done(status: i32, ctx: ResetCtx) {
        trace!(?ctx, ?status, "all I/O channels successfully removed");
    }

    pub fn hot_remove(
        &mut self,
        cb: OpCompletionCallback,
        cb_arg: OpCompletionCallbackArg,
    ) -> Result<(), CoreError> {
        if let Some(c) = self.controller() {
            c.fail()
        }
        let io_device = self.inner.as_ref().unwrap().io_device.clone();
        let reset_ctx = ResetCtx {
            name: self.name.clone(),
            cb,
            cb_arg,
            spdk_handle: self.controller().expect("controller may not be NULL"),
            io_device,
            shutdown_in_progress: false,
        };

        let inner = self.inner.as_mut().unwrap();
        // Iterate over all I/O channels and reset/configure them one by one.
        inner.io_device.traverse_io_channels(
            NvmeController::_reset_destroy_channels,
            NvmeController::hot_remove_channels_done,
            NvmeIoChannel::inner_from_channel,
            reset_ctx,
        );
        Ok(())
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

        debug!("{}: all I/O channels successfully reset", reset_ctx.name);
        // In case shutdown is active, don't reset the controller as its
        // being removed.
        if reset_ctx.shutdown_in_progress {
            warn!(
                "{}: controller shutdown detected, skipping reset",
                reset_ctx.name
            );
            return;
        }

        let rc =
            unsafe { spdk_nvme_ctrlr_reset(reset_ctx.spdk_handle.as_ptr()) };
        if rc != 0 {
            error!(
                "{} failed to reset controller, rc = {}",
                reset_ctx.name, rc
            );

            NvmeController::_complete_reset(reset_ctx, rc);
        } else {
            debug!(
                "{} controller successfully reset, reinitializing I/O channels",
                reset_ctx.name
            );

            // Once controller is successfully reset, schedule another
            //I/O channel traversal to restore all I/O channels.
            let io_device = reset_ctx.io_device.clone();
            io_device.traverse_io_channels(
                NvmeController::_reset_create_channels,
                NvmeController::_reset_create_channels_done,
                NvmeIoChannel::inner_from_channel,
                reset_ctx,
            );
        }
    }

    fn _reset_create_channels(
        channel: &mut NvmeIoChannelInner,
        reset_ctx: &mut ResetCtx,
    ) -> i32 {
        // Make sure no concurrent shutdown takes place.
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
            debug!("{} I/O channel successfully reinitialized", reset_ctx.name);
        }
        rc
    }

    fn _reset_create_channels_done(status: i32, reset_ctx: ResetCtx) {
        debug!(
            "{} controller reset completed, status = {}",
            reset_ctx.name, status
        );
        NvmeController::_complete_reset(reset_ctx, status);
    }

    /// Notifies all listeners of this controller.
    ///
    /// Note: Keep a separate copy of all registered listeners in order to not
    /// invoke them with the lock held.
    fn notify_listeners(&self, event: DeviceEventType) -> usize {
        let mut disp = self
            .event_dispatcher
            .lock()
            .expect("event dispatcher lock poisoned");

        disp.dispatch_event(event, &self.name);
        disp.count()
    }

    /// Register listener to monitor device events related to this controller.
    pub fn register_device_listener(
        &self,
        listener: DeviceEventSink,
    ) -> Result<(), CoreError> {
        let mut listeners = self
            .event_dispatcher
            .lock()
            .expect("event listeners lock poisoned");

        listeners.add_listener(listener);
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
                "stopping admin queue poller"
            );

            inner.adminq_poller.stop();

            drop(inner.io_device);

            debug!(
                ?self.name,
                "detaching NVMe controller"
            );
            let rc = unsafe { spdk_nvme_detach(inner.ctrlr.as_ptr()) };

            assert_eq!(rc, 0, "Failed to detach NVMe controller");
            info!(
                ?self.name,
                "NVMe controller successfully detached"
            );
        }

        unsafe {
            Box::from_raw(self.timeout_config.as_ptr());
        }
    }
}

extern "C" fn aer_cb(ctx: *mut c_void, cpl: *const spdk_nvme_cpl) {
    let mut event = spdk_nvme_async_event_completion::default();

    if !nvme_cpl_succeeded(cpl) {
        warn!("AER request execute failed");
        return;
    }

    event.raw = unsafe { (*cpl).cdw0 };

    let (event_type, event_info) = unsafe {
        (event.bits.async_event_type(), event.bits.async_event_info())
    };

    debug!(
        "Received AER event: event_type={:?}, event_info={:?}",
        event_type, event_info
    );

    // Populate namespaces in response to AER.
    if event_type == NvmeAerType::Notice as u32
        && event_info == NvmeAerInfoNotice::AttrChanged as u32
    {
        let cid = ctx as u64;

        match NVME_CONTROLLERS.lookup_by_name(cid.to_string()) {
            Some(c) => {
                let mut ctrlr = c.lock();
                debug!(
                    "{}: populating namespaces in response to AER",
                    ctrlr.get_name()
                );
                ctrlr.populate_namespaces();
            }
            None => {
                warn!(
                    "No NVMe controller exists with ID 0x{:x}, no namespaces rescanned",
                    cid,
                );
            }
        }
    } else if event_type == NvmeAerType::Io as u32
        && event_info == NvmeAerInfoNvmCommandSet::ReservationLogAvail as u32
    {
        debug!("Reservation log available");
    }
}

/// Poll to process qpair completions on admin queue
/// Returns: 0 (SPDK_POLLER_IDLE) or 1 (SPDK_POLLER_BUSY)
pub extern "C" fn nvme_poll_adminq(ctx: *mut c_void) -> i32 {
    let mut context = NonNull::<TimeoutConfig>::new(ctx.cast())
        .expect("ctx pointer may never be null");
    let context = unsafe { context.as_mut() };

    // returns number of completions processed (maybe 0) or the negated error,
    // which is one of:
    //
    // ENXIO: the qpair is not connected or when the controller is
    // marked as failed.
    //
    // EAGAIN: returned whenever the controller is being reset.
    let result = context.process_adminq();

    if result < 0 {
        if context.start_device_destroy() {
            error!(
                "process adminq: {}: {}",
                context.name,
                Errno::from_i32(result.abs())
            );
            info!("dispatching nexus fault and retire: {}", context.name);
            let dev_name = context.name.to_string();
            let carc = NVME_CONTROLLERS.lookup_by_name(&dev_name).unwrap();
            debug!(
                ?dev_name,
                "notifying listeners of admin command completion failure"
            );
            let controller = carc.lock();
            let num_listeners = controller.notify_listeners(
                DeviceEventType::AdminCommandCompletionFailed,
            );
            debug!(
                ?dev_name,
                ?num_listeners,
                "listeners notified of admin command completion failure"
            );
        }
        return 1;
    }

    if result == 0 {
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
        let mut controller = carc.lock();

        // Skip not-fully initialized controllers.
        if controller.get_state() != NvmeControllerState::New {
            fn _shutdown_callback(success: bool, ctx: *mut c_void) {
                done_cb(ctx, success);
            }

            controller.shutdown(_shutdown_callback, cb_arg(s)).map_err(
                |_| NexusBdevError::DestroyBdev {
                    name: String::from(&name),
                    source: Errno::EAGAIN,
                },
            )?;

            // Release the lock before waiting for controller shutdown.
            drop(controller);

            if !r.await.expect("Failed awaiting at shutdown()") {
                error!(?name, "failed to shutdown controller");
                return Err(NexusBdevError::DestroyBdev {
                    name: String::from(&name),
                    source: Errno::EAGAIN,
                });
            }
        }
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
    {
        let controller = carc.lock();
        let num_listeners =
            controller.notify_listeners(DeviceEventType::DeviceRemoved);
        debug!(
            ?name,
            ?num_listeners,
            "listeners notified about device removal"
        );
    }

    let mut carc = carc;
    loop {
        match Arc::try_unwrap(carc) {
            Ok(i) => {
                drop(i);
                break;
            }
            Err(ret) => {
                warn!(?name, "delaying controller destroy");
                let rx = mayastor_sleep(std::time::Duration::from_millis(250));
                if rx.await.is_err() {
                    error!("failed to wait for mayastor_sleep");
                }
                carc = ret;
            }
        }
    }

    Ok(())
}

pub(crate) fn connected_attached_cb(
    ctx: &mut NvmeControllerContext,
    ctrlr: SpdkNvmeController,
) {
    // we use the ctrlr address as the controller id in the global table
    let cid = ctrlr.as_ptr() as u64;

    // get a reference to our controller we created when we kicked of the async
    // attaching process
    let controller = NVME_CONTROLLERS
        .lookup_by_name(&ctx.name())
        .expect("no controller in the list");

    // clone it now such that we can lock the original, and insert it later.
    let ctl = controller.clone();
    let mut controller = controller.lock();
    controller
        .state_machine
        .transition(Initializing)
        .expect("Failed to transition controller into Initialized state");
    //unsafe { (*ctrlr.as_ptr()).reinit_after_reset = false };
    // set the controller as a pointer within the context of the time out config
    unsafe { controller.timeout_config.as_mut().set_controller(ctrlr) };
    controller.set_id(cid);
    controller.inner = Some(NvmeControllerInner::new(
        ctrlr,
        controller.get_name(),
        controller.timeout_config,
    ));

    controller.configure_timeout();

    if !controller.populate_namespaces() {
        error!("{}: failed to populate namespaces", ctx.name());
        ctx.sender()
            .send(Err(Errno::ENXIO))
            .expect("done callback receiver side disappeared");
        return;
    }

    // Register callbacks.
    controller.register_callbacks();

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
    use std::{mem::size_of, ptr::copy_nonoverlapping};

    use spdk_rs::libspdk::{
        spdk_nvme_ctrlr_get_default_ctrlr_opts,
        spdk_nvme_ctrlr_opts,
    };

    use crate::ffihelper::IntoCString;

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
        ext_host_id: Option<[u8; 16]>,
        host_nqn: Option<String>,
        keep_alive_timeout_ms: Option<u32>,
        transport_retry_count: Option<u8>,
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

        pub fn with_ext_host_id(mut self, ext_host_id: [u8; 16]) -> Self {
            self.ext_host_id = Some(ext_host_id);
            self
        }

        pub fn with_hostnqn<T: Into<String>>(mut self, host_nqn: T) -> Self {
            self.host_nqn = Some(host_nqn.into());
            self
        }

        /// Builder to override default values
        pub fn build(self) -> NvmeControllerOpts {
            let mut opts = NvmeControllerOpts::default();
            opts.0.disable_error_logging = true;

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

            if let Some(ext_host_id) = self.ext_host_id {
                opts.0.extended_host_id = ext_host_id;
            }

            if let Some(host_nqn) = self.host_nqn {
                unsafe {
                    copy_nonoverlapping(
                        host_nqn.into_cstring().as_ptr(),
                        &mut opts.0.hostnqn[0],
                        opts.0.hostnqn.len(),
                    )
                };
            }

            opts
        }
    }
    #[cfg(test)]
    mod test {
        use crate::bdev::nvmx::controller::options;

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

pub(crate) mod transport {
    use std::{ffi::CStr, fmt::Debug, ptr::copy_nonoverlapping};

    use libc::c_void;

    use spdk_rs::libspdk::spdk_nvme_transport_id;

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
    #[allow(clippy::upper_case_acronyms)]
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
    #[allow(dead_code)]
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
        use crate::bdev::nvmx::controller::transport;

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
