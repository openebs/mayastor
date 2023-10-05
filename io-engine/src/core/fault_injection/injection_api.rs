#![cfg(feature = "fault-injection")]

use nix::errno::Errno;
use once_cell::sync::OnceCell;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::core::{CoreError, IoCompletionStatus};

use super::{
    add_bdev_io_injection,
    FaultDomain,
    FaultInjectionError,
    FaultIoStage,
    InjectIoCtx,
    Injection,
};

/// A list of fault injections.
struct Injections {
    items: Vec<Injection>,
}

static INJECTIONS: OnceCell<parking_lot::Mutex<Injections>> = OnceCell::new();

impl Injections {
    fn new() -> Self {
        Self {
            items: Vec::new(),
        }
    }

    #[inline(always)]
    fn get() -> parking_lot::MutexGuard<'static, Self> {
        INJECTIONS
            .get_or_init(|| parking_lot::Mutex::new(Injections::new()))
            .lock()
    }

    /// Adds an injection.
    pub fn add(&mut self, inj: Injection) -> Result<(), FaultInjectionError> {
        if inj.domain == FaultDomain::BdevIo {
            add_bdev_io_injection(&inj)?;
        }

        info!("Adding injected fault: '{inj:?}'");
        self.items.push(inj);

        Ok(())
    }

    /// Removes all injections matching the URI.
    pub fn remove(&mut self, uri: &str) -> Result<(), FaultInjectionError> {
        info!("Removing injected fault: '{uri}'");
        self.items.retain(|inj| inj.uri() != uri);
        Ok(())
    }

    /// Returns a copy of the injection list.
    pub fn list(&self) -> Vec<Injection> {
        self.items.clone()
    }

    /// TODO
    #[inline(always)]
    fn inject(
        &self,
        stage: FaultIoStage,
        op: &InjectIoCtx,
    ) -> Option<IoCompletionStatus> {
        self.items.iter().find_map(|inj| inj.inject(stage, op))
    }
}

static INJECTIONS_ENABLED: AtomicBool = AtomicBool::new(false);

/// Checks if fault injection is globally enabled.
/// This method is fast and can used in I/O code path to quick check
/// before checking if an injection has to be applied to a particular
/// device.
#[inline]
pub fn injections_enabled() -> bool {
    INJECTIONS_ENABLED.load(Ordering::SeqCst)
}

/// Enables fault injections globally.
#[inline]
fn enable_fault_injections() {
    if INJECTIONS_ENABLED
        .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
        .is_ok()
    {
        warn!("Enabling fault injection globally");
    }
}
/// Adds an fault injection.
pub fn add_fault_injection(inj: Injection) -> Result<(), FaultInjectionError> {
    enable_fault_injections();
    Injections::get().add(inj)
}

/// Removes all injections matching the URI.
pub fn remove_fault_injection(uri: &str) -> Result<(), FaultInjectionError> {
    Injections::get().remove(uri)
}

/// Lists fault injections. A clone of current state of injection is returned.
pub fn list_fault_injections() -> Vec<Injection> {
    Injections::get().list()
}

/// Finds and injects a fault for the given I/O context, at the submission I/O
/// stage. In the case a fault is injected, returns the corresponding
/// `CoreError`.
#[inline]
pub fn inject_submission_error(ctx: &InjectIoCtx) -> Result<(), CoreError> {
    if !injections_enabled() || !ctx.is_valid() {
        return Ok(());
    }

    match Injections::get().inject(FaultIoStage::Submission, ctx) {
        None => Ok(()),
        Some(IoCompletionStatus::Success) => Ok(()),
        Some(_) => Err(crate::bdev::device::io_type_to_err(
            ctx.io_type,
            Errno::ENXIO,
            ctx.range.start,
            ctx.range.end - ctx.range.start,
        )),
    }
}

/// Finds and injects a fault for the given I/O context, at the completion I/O
/// stage.
/// In the case a fault is injected, returns the corresponding
/// `IoCompletionStatus`.
#[inline]
pub fn inject_completion_error(
    ctx: &InjectIoCtx,
    status: IoCompletionStatus,
) -> IoCompletionStatus {
    if !injections_enabled()
        || !ctx.is_valid()
        || status != IoCompletionStatus::Success
    {
        return status;
    }

    match Injections::get().inject(FaultIoStage::Completion, ctx) {
        Some(inj) => inj,
        None => IoCompletionStatus::Success,
    }
}
