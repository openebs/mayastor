#![cfg(feature = "fault-injection")]

use nix::errno::Errno;
use once_cell::sync::OnceCell;
use std::{
    convert::TryInto,
    sync::atomic::{AtomicBool, Ordering},
};

use crate::core::{CoreError, IoCompletionStatus};

use super::{
    FaultDomain,
    FaultInjection,
    FaultIoStage,
    FaultIoType,
    InjectIoCtx,
};

/// A list of fault injections.
struct Injections {
    items: Vec<FaultInjection>,
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
    pub fn add(&mut self, inj: FaultInjection) {
        info!("Adding injected fault: '{uri}'", uri = inj.uri);
        self.items.push(inj);
    }

    /// Removes all injections matching the URI.
    pub fn remove(&mut self, uri: &str) {
        info!("Removing injected fault: '{uri}'");
        self.items.retain(|inj| inj.uri != uri);
    }

    /// Returns a copy of the injection list.
    pub fn list(&self) -> Vec<FaultInjection> {
        self.items.clone()
    }

    /// TODO
    #[inline(always)]
    fn inject(
        &mut self,
        domain: FaultDomain,
        fault_io_type: FaultIoType,
        fault_io_stage: FaultIoStage,
        op: &InjectIoCtx,
    ) -> Option<IoCompletionStatus> {
        self.items.iter_mut().find_map(|inj| {
            inj.inject(domain, fault_io_type, fault_io_stage, op)
        })
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
pub fn add_fault_injection(inj: FaultInjection) {
    enable_fault_injections();
    Injections::get().add(inj);
}

/// Removes all injections matching the URI.
pub fn remove_fault_injection(uri: &str) {
    Injections::get().remove(uri);
}

/// Lists fault injections. A clone of current state of injection is returned.
pub fn list_fault_injections() -> Vec<FaultInjection> {
    Injections::get().list()
}

/// Finds and injects a fault for the given I/O context, at the submission I/O
/// stage. In the case a fault is injected, returns the corresponding
/// `CoreError`.
#[inline]
pub fn inject_submission_error(
    domain: FaultDomain,
    ctx: &InjectIoCtx,
) -> Result<(), CoreError> {
    if !injections_enabled() || !ctx.is_valid() {
        return Ok(());
    }

    let Ok(fault_io_type) = ctx.io_type.try_into() else {
        return Ok(());
    };

    match Injections::get().inject(
        domain,
        fault_io_type,
        FaultIoStage::Submission,
        ctx,
    ) {
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
    domain: FaultDomain,
    ctx: &InjectIoCtx,
    status: IoCompletionStatus,
) -> IoCompletionStatus {
    if !injections_enabled()
        || !ctx.is_valid()
        || status != IoCompletionStatus::Success
    {
        return status;
    }

    let Ok(fault_io_type) = ctx.io_type.try_into() else {
        return status;
    };

    match Injections::get().inject(
        domain,
        fault_io_type,
        FaultIoStage::Completion,
        ctx,
    ) {
        Some(inj) => inj,
        None => IoCompletionStatus::Success,
    }
}
