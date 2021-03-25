use crossbeam::atomic::AtomicCell;
use std::{
    convert::TryFrom,
    os::raw::c_void,
    ptr::NonNull,
    time::{Duration, Instant},
};

use crate::{
    bdev::dev::nvmx::{
        nvme_bdev_running_config,
        utils::nvme_cpl_succeeded,
        NvmeController,
        NVME_CONTROLLERS,
    },
    core::{CoreError, DeviceIoController, DeviceTimeoutAction},
};

use spdk_sys::{
    spdk_nvme_cmd_cb,
    spdk_nvme_cpl,
    spdk_nvme_ctrlr,
    spdk_nvme_ctrlr_cmd_abort,
    spdk_nvme_ctrlr_get_regs_csts,
    spdk_nvme_ctrlr_register_timeout_callback,
    spdk_nvme_qpair,
    SPDK_BDEV_NVME_TIMEOUT_ACTION_ABORT,
    SPDK_BDEV_NVME_TIMEOUT_ACTION_NONE,
    SPDK_BDEV_NVME_TIMEOUT_ACTION_RESET,
};

impl TryFrom<u32> for DeviceTimeoutAction {
    type Error = String;

    fn try_from(action: u32) -> Result<Self, Self::Error> {
        let a = match action {
            SPDK_BDEV_NVME_TIMEOUT_ACTION_NONE => DeviceTimeoutAction::Ignore,
            SPDK_BDEV_NVME_TIMEOUT_ACTION_RESET => DeviceTimeoutAction::Reset,
            SPDK_BDEV_NVME_TIMEOUT_ACTION_ABORT => DeviceTimeoutAction::Abort,
            _ => {
                return Err(format!(
                    "Invalid timeout action in config: {}",
                    action
                ));
            }
        };

        Ok(a)
    }
}

/// Maximum number of controller reset attempts to be taken in case
/// controller reset fails. Once this limit is reached, the next possible
/// controller reset will be allowed only when reset cooldown interval
/// elapses.
/// This is done to prevent the storm of reset requests in response to
/// frequent I/O errors in a controller (including errors while processing
/// admin queue completions).
const MAX_RESET_ATTEMPTS: u32 = 5;

/// Time to wait till reset attempts can be recharged to maximum
/// after all current reset attempts have been used.
const RESET_COOLDOWN_INTERVAL: u64 = 30;

pub(crate) struct TimeoutConfig {
    pub name: String,
    timeout_action: AtomicCell<DeviceTimeoutAction>,
    reset_in_progress: AtomicCell<bool>,
    pub ctrlr: *mut spdk_nvme_ctrlr,
    reset_attempts: u32,
    next_reset_time: Instant,
}

impl Drop for TimeoutConfig {
    fn drop(&mut self) {
        debug!("{} dropping TimeoutConfig", self.name);
    }
}

/// Structure for holding I/O timeout related configuration settings and
/// providing fast and atomic access to it.
impl TimeoutConfig {
    pub fn new(ctrlr: &str) -> Self {
        Self {
            name: String::from(ctrlr),
            timeout_action: AtomicCell::new(DeviceTimeoutAction::Ignore),
            reset_in_progress: AtomicCell::new(false),
            ctrlr: std::ptr::null_mut(),
            reset_attempts: MAX_RESET_ATTEMPTS,
            next_reset_time: Instant::now(),
        }
    }

    fn reset_cb(success: bool, ctx: *mut c_void) {
        let timeout_ctx = TimeoutConfig::from_ptr(ctx as *mut TimeoutConfig);

        if success {
            info!(
                "{} controller successfully reset in response to I/O timeout",
                timeout_ctx.name
            );
            // In case of successful reset, also reset the allowed number of
            // reset attempts.
            timeout_ctx.reset_attempts = MAX_RESET_ATTEMPTS;
        } else {
            error!(
                "{} failed to reset controller in response to I/O timeout",
                timeout_ctx.name
            );

            // Setup the reset cooldown interval in case of the last
            // failed reset attempt.
            if timeout_ctx.reset_attempts == 0 {
                timeout_ctx.next_reset_time = Instant::now()
                    + Duration::from_secs(RESET_COOLDOWN_INTERVAL);
                info!(
                    "{} reset cooldown interval activated ({} secs)",
                    timeout_ctx.name, RESET_COOLDOWN_INTERVAL,
                );
            }
        }

        // Clear the flag as we are the exclusive owner.
        assert!(
            timeout_ctx.reset_in_progress.compare_and_swap(true, false),
            "non-exclusive access to controller reset flag"
        );
    }

    /// Resets controller exclusively, taking into account existing active
    /// resets related to I/O timeout.
    pub(crate) fn reset_controller(&mut self) {
        // Make sure no other resets are in progress.
        if self.reset_in_progress.compare_and_swap(false, true) {
            return;
        }

        // Check if the maximum number of resets exceeded and we need
        // to adjust the number of attempts based on time reset cooldown period.
        if self.reset_attempts == 0 {
            if Instant::now() >= self.next_reset_time {
                self.reset_attempts = MAX_RESET_ATTEMPTS;
                info!(
                    "{} reset cooldown period elapsed, reset enabled.",
                    self.name,
                );
            }
        }

        if self.reset_attempts > 0 {
            // Account reset attempt.
            self.reset_attempts -= 1;

            if let Some(c) =
                NVME_CONTROLLERS.lookup_by_name(self.name.to_string())
            {
                let mut c = c.lock().expect("controller lock poisoned");
                if let Err(e) = c.reset(
                    TimeoutConfig::reset_cb,
                    self as *mut TimeoutConfig as *mut c_void,
                    false,
                ) {
                    error!(
                        "{}: failed to initiate controller reset: {}",
                        self.name, e
                    );
                } else {
                    info!(
                        "{} controller reset initiated ({} reset attempts left)",
                        self.name, self.reset_attempts
                    );
                    return;
                }
            } else {
                error!(
                    "No controller instance found for {}, reset not possible",
                    self.name
                );
            }
        }

        // Clear the flag as we are the exclusive owner.
        assert!(
            self.reset_in_progress.compare_and_swap(true, false),
            "non-exclusive access to controller reset flag"
        );
    }

    /// Set new I/O timeout action.
    pub fn set_timeout_action(&mut self, action: DeviceTimeoutAction) {
        self.timeout_action.store(action);
    }

    /// Get current I/O timeout action.
    pub fn get_timeout_action(&self) -> DeviceTimeoutAction {
        self.timeout_action.load()
    }

    pub fn from_ptr(ptr: *mut TimeoutConfig) -> &'static mut TimeoutConfig {
        unsafe { &mut *(ptr as *mut TimeoutConfig) }
    }
}

pub(crate) struct SpdkNvmeController(NonNull<spdk_nvme_ctrlr>);

/// Wrapper around SPDK controller object to abstract low-level library API.
impl SpdkNvmeController {
    /// Transform SPDK NVMe controller object into a wrapper instance.
    pub fn from_ptr(ctrlr: *mut spdk_nvme_ctrlr) -> Option<SpdkNvmeController> {
        NonNull::new(ctrlr).map(SpdkNvmeController)
    }

    /// Check Controller Fatal Status flag.
    pub fn check_cfs(&self) -> bool {
        unsafe {
            let csts = spdk_nvme_ctrlr_get_regs_csts(self.0.as_ptr());
            csts.bits.cfs() != 0
        }
    }

    /// Abort command on a given I/O qpair.
    pub fn abort_queued_command(
        &self,
        qpair: *mut spdk_nvme_qpair,
        cid: u16,
        cb: spdk_nvme_cmd_cb,
        cb_arg: *mut c_void,
    ) -> i32 {
        unsafe {
            spdk_nvme_ctrlr_cmd_abort(self.0.as_ptr(), qpair, cid, cb, cb_arg)
        }
    }
}

impl From<*mut spdk_nvme_ctrlr> for SpdkNvmeController {
    fn from(ctrlr: *mut spdk_nvme_ctrlr) -> Self {
        Self::from_ptr(ctrlr)
            .expect("nullptr dereference while accessing NVME controller")
    }
}

// I/O device controller API.
impl<'a> DeviceIoController for NvmeController<'a> {
    /// Get current I/O timeout action.
    fn get_timeout_action(&self) -> Result<DeviceTimeoutAction, CoreError> {
        Ok(TimeoutConfig::from_ptr(self.timeout_config).get_timeout_action())
    }

    /// Set current I/O timeout action.
    fn set_timeout_action(
        &mut self,
        action: DeviceTimeoutAction,
    ) -> Result<(), CoreError> {
        TimeoutConfig::from_ptr(self.timeout_config).set_timeout_action(action);
        info!("{} timeout action set to {:?}", self.name, action);
        Ok(())
    }
}

// I/O timeout handling for NVMe controller.
impl<'a> NvmeController<'a> {
    extern "C" fn command_abort_handler(
        ctx: *mut c_void,
        cpl: *const spdk_nvme_cpl,
    ) {
        let timeout_ctx = TimeoutConfig::from_ptr(ctx as *mut TimeoutConfig);

        if nvme_cpl_succeeded(cpl) {
            info!("{} CID abort succeeded for controller.", timeout_ctx.name);
        } else {
            error!(
                "{} CID abort failed, resetting the controller.",
                timeout_ctx.name
            );
            timeout_ctx.reset_controller();
        }
    }

    extern "C" fn io_timeout_handler(
        cb_arg: *mut c_void,
        ctrlr: *mut spdk_nvme_ctrlr,
        qpair: *mut spdk_nvme_qpair,
        cid: u16,
    ) {
        let spdk_ctrlr = SpdkNvmeController::from(ctrlr);
        let timeout_cfg = TimeoutConfig::from_ptr(cb_arg as *mut TimeoutConfig);
        let mut timeout_action = timeout_cfg.timeout_action.load();

        error!(
            "{}: detected timeout: qpair={:p}, cid={}, action={:?}",
            timeout_cfg.name, qpair, cid, timeout_action
        );

        // Check Controller Fatal Status for non-admin commands only to avoid
        // endless command resubmission in case of disconnected qpair.
        if !qpair.is_null() && spdk_ctrlr.check_cfs() {
            error!(
                "{}: controller Fatal Status set, reset required",
                timeout_cfg.name
            );
            timeout_action = DeviceTimeoutAction::Reset;
        }

        // Handle timeout based on the action.
        match timeout_action {
            DeviceTimeoutAction::Abort | DeviceTimeoutAction::Reset => {
                if timeout_action == DeviceTimeoutAction::Abort {
                    // Abort commands only for non-admin queue, fallthrough
                    // to reset otherwise.
                    if !qpair.is_null() {
                        error!("{}: aborting CID {}", timeout_cfg.name, cid);
                        let rc = spdk_ctrlr.abort_queued_command(
                            qpair,
                            cid,
                            Some(NvmeController::command_abort_handler),
                            cb_arg,
                        );
                        if rc == 0 {
                            info!(
                                "{}: initiated abort for CID {}",
                                timeout_cfg.name, cid
                            );
                            return;
                        }
                        error!(
                            "{}: unable to abort CID {}, reset required",
                            timeout_cfg.name, cid
                        );
                    } else {
                        info!(
                            "{}: skipping Abort timeout action for admin qpair",
                            timeout_cfg.name
                        );
                    }
                    // Fallthrough to perform controller reset in case abort
                    // fails.
                }
                info!(
                    "{} resetting controller in response to I/O timeout",
                    timeout_cfg.name
                );
                timeout_cfg.reset_controller();
            }
            DeviceTimeoutAction::Ignore => {
                info!(
                    "{}: no I/O timeout action defined, timeout ignored",
                    timeout_cfg.name
                );
            }
        }
    }

    pub(crate) fn configure_timeout(&mut self) {
        let device_defaults = nvme_bdev_running_config();

        if device_defaults.timeout_us == 0 {
            warn!(
                "{} no timeout configured for NVMe controller, I/O timeout handling disabled.",
                self.name
            );
            self.set_timeout_action(DeviceTimeoutAction::Ignore)
                .unwrap();
            return;
        }

        let action = match DeviceTimeoutAction::try_from(
            device_defaults.action_on_timeout,
        ) {
            Ok(action) => action,
            Err(e) => {
                error!(
                    "{}: can not apply requested I/O timeout action: {}, falling back to Ignore",
                    self.name, e
                );
                DeviceTimeoutAction::Ignore
            }
        };

        self.set_timeout_action(action).unwrap();

        unsafe {
            spdk_nvme_ctrlr_register_timeout_callback(
                self.ctrlr_as_ptr(),
                device_defaults.timeout_us,
                Some(NvmeController::io_timeout_handler),
                self.timeout_config as *mut c_void,
            );
        }
        info!(
            "{} I/O timeout set to {} us",
            self.name, device_defaults.timeout_us
        );
    }
}
