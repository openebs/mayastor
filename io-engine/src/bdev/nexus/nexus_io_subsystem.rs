use std::{
    collections::VecDeque,
    fmt::{Debug, Display, Formatter},
    sync::atomic::{AtomicU32, Ordering},
};

use crossbeam::atomic::AtomicCell;
use futures::channel::oneshot;

use super::{Error, Nexus};

use crate::{
    core::{Bdev, Cores, Protocol, Share},
    subsys::NvmfSubsystem,
};

/// Nexus pause states.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum NexusPauseState {
    /// When subsystem is stopping/stopped but not destroyed, there's a race where a resume will
    /// restart the subsystem!
    Unpaused,
    Pausing,
    /// When subsystem is stopping/stopped but not destroyed, there's a race where a pause will fail.
    /// We'll still consider it as paused in this case.
    Paused,
    Frozen,
    Unpausing,
}

impl Display for NexusPauseState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let fmt = match self {
            Self::Unpaused => "unpaused",
            Self::Pausing => "pausing",
            Self::Paused => "paused",
            Self::Frozen => "frozen",
            Self::Unpausing => "unpausing",
        };
        write!(f, "{fmt}")
    }
}

/// Abstraction for managing pausing/unpausing I/O on NVMe subsystem, allowing
/// concurrent pause/resume calls by serializing low-level SPDK calls.
pub(super) struct NexusIoSubsystem<'n> {
    /// Subsystem name.
    name: String,
    /// Nexus Bdev associated with the subsystem.
    bdev: &'n mut Bdev<Nexus<'n>>,
    /// Subsystem pause state.
    pause_state: AtomicCell<NexusPauseState>,
    /// Pause waiters.
    pause_waiters: VecDeque<oneshot::Sender<i32>>,
    /// Pause counter.
    pause_cnt: AtomicU32,
}

impl Debug for NexusIoSubsystem<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:?}: I/O subsystem [{}]",
            self.bdev.data(),
            self.pause_state.load()
        )
    }
}

impl<'n> NexusIoSubsystem<'n> {
    /// Create a new instance of Nexus I/O subsystem for a given nexus name and
    /// block device.
    pub(super) fn new(name: String, bdev: &'n mut Bdev<Nexus<'n>>) -> Self {
        Self {
            pause_state: AtomicCell::new(NexusPauseState::Unpaused),
            pause_waiters: VecDeque::with_capacity(8), /* Default number of
                                                        * replicas */
            pause_cnt: AtomicU32::new(0),
            name,
            bdev,
        }
    }

    /// Get the subsystem pause state.
    pub(super) fn pause_state(&self) -> NexusPauseState {
        self.pause_state.load()
    }

    /// Suspend any incoming IO to the bdev pausing the controller allows us to
    /// handle internal events and which is a protocol feature.
    /// In case concurrent pause requests take place, the other callers
    /// will wait till the nexus is resumed and will continue execution
    /// with the nexus paused once they are awakened via resume().
    /// Note: in order to handle concurrent pauses properly, this function must
    /// be called only from the master core.
    pub(super) async fn suspend(&mut self) -> Result<(), Error> {
        assert_eq!(
            Cores::current(),
            Cores::first(),
            "NexusIoSubsystem::suspend() must called on the first core"
        );

        trace!("{self:?}: pausing I/O...");

        loop {
            let state = self
                .pause_state
                .compare_exchange(NexusPauseState::Unpaused, NexusPauseState::Pausing);

            match state {
                Ok(NexusPauseState::Unpaused) => {
                    // Pause subsystem. The only acceptable counter transition
                    // is: 0 -> 1.
                    assert_eq!(
                        self.pause_cnt.fetch_add(1, Ordering::SeqCst),
                        0,
                        "Corrupted subsystem pause counter"
                    );

                    if let Some(Protocol::Nvmf) = self.bdev.shared() {
                        if let Some(subsystem) = NvmfSubsystem::nqn_lookup(&self.name) {
                            let nqn = subsystem.get_nqn();
                            trace!("{self:?}: pausing subsystem '{nqn}'...");

                            let result = subsystem.pause().await;
                            if let Err(ref error) = result {
                                // todo: handle error instead of panic, but in practice only ENOMEM can be seen here?
                                if error.errno() != nix::Error::EPERM {
                                    panic!("Failed to pause subsystem: {}", error);
                                }
                                // Can't pause a stopped subsystem, but we're essentially paused anyway.
                                // However, there's a race here as, resume may resume a stopped subsystem.
                                // We should prevent this on the spdk nvmf subsystem state mgmt.
                            }

                            if result.is_ok() {
                                trace!("{self:?}: subsystem '{nqn}' paused");
                            } else {
                                warn!("{self:?}: subsystem '{nqn}' stopped");
                            }
                        }
                    }

                    // Mark subsystem as paused after it has been paused.
                    self.pause_state
                        .compare_exchange(NexusPauseState::Pausing, NexusPauseState::Paused)
                        .expect("Failed to mark subsystem as Paused");
                    break;
                }
                // Subsystem is already paused, increment number of paused.
                Err(NexusPauseState::Paused | NexusPauseState::Frozen) => {
                    trace!("{self:?}: nexus is already paused, incrementing pause count");
                    self.pause_cnt.fetch_add(1, Ordering::SeqCst);
                    break;
                }
                // Wait till the subsystem has completed transition and retry operation.
                Err(NexusPauseState::Unpausing) | Err(NexusPauseState::Pausing) => {
                    trace!("{self:?}: nexus is in intermediate state, deferring pause operation");

                    let nex = format!("{self:?}");

                    let (s, r) = oneshot::channel::<i32>();
                    self.pause_waiters.push_back(s);
                    if r.await.is_err() {
                        error!("{nex}: I/O subsystem is gone while waiting");
                        return Ok(());
                    }

                    trace!("{self:?}: nexus completed state transition, retrying pause operation");
                }
                state => {
                    panic!("Corrupted I/O subsystem state: {:?}", state);
                }
            }
        }

        // Resume one waiter in case there are any.
        if let Some(w) = self.pause_waiters.pop_front() {
            trace!("{self:?}: resuming the first pause waiter");
            w.send(0).expect("I/O subsystem pause waiter disappeared");
        }

        trace!("{self:?}: I/O paused");
        Ok(())
    }

    /// Resume IO to the bdev.
    /// # NOTE
    /// In order to handle concurrent resumes properly, this function must
    /// be called only from the master core.
    /// # Warning
    /// This function may return whilst the subsystem is still paused if other requests for pause
    /// have been accepted.
    /// The subsystem is only truly resumed when the last pause is undone, which may happen after
    /// this function completes.
    pub(super) async fn resume(&mut self, freeze: bool) -> Result<(), Error> {
        assert_eq!(
            Cores::current(),
            Cores::first(),
            "NexusIoSubsystem::resume() must called on the first core"
        );

        trace!("{self:?}: resuming I/O...");

        loop {
            let state = self.pause_state.load();
            match state {
                // Already unpaused, bail out.
                NexusPauseState::Unpaused => {
                    break;
                }
                // Simultaneous pausing/unpausing: wait till the subsystem has
                // completed transition and retry operation.
                NexusPauseState::Pausing | NexusPauseState::Unpausing => {
                    trace!("{self:?}: nexus is in intermediate state, deferring resume operation");

                    let nex = format!("{self:?}");

                    debug_assert_eq!(
                        state,
                        NexusPauseState::Unpausing,
                        "{nex}: resuming whilst pausing ??"
                    );

                    let (s, r) = oneshot::channel::<i32>();
                    self.pause_waiters.push_back(s);
                    if r.await.is_err() {
                        error!("{nex}: I/O subsystem is gone while waiting");
                        return Ok(());
                    }

                    trace!("{self:?}: completed state transition, retrying resume operation");
                }
                // Unpause the subsystem, taking into account the overall number
                // of pauses, or leave it frozen.
                NexusPauseState::Paused | NexusPauseState::Frozen => {
                    let v = self.pause_cnt.fetch_sub(1, Ordering::SeqCst);

                    if v != 1 {
                        break;
                    } // In case the last pause discarded, resume the subsystem.

                    if state == NexusPauseState::Frozen || freeze {
                        if let Some(subsystem) = NvmfSubsystem::nqn_lookup(&self.name) {
                            trace!(
                                "{self:?}: subsystem '{}' not being resumed",
                                subsystem.get_nqn()
                            );
                        }
                        self.pause_state.store(NexusPauseState::Frozen);
                    } else {
                        if let Some(subsystem) = NvmfSubsystem::nqn_lookup(&self.name) {
                            self.pause_state.store(NexusPauseState::Unpausing);
                            trace!("{self:?}: resuming subsystem '{}'...", subsystem.get_nqn());
                            if let Err(error) = subsystem.resume().await {
                                // todo: handle error instead of panic, but in practice only ENOMEM can be seen here?
                                panic!(
                                    "Failed to resume subsystem '{}: {}",
                                    subsystem.get_nqn(),
                                    error
                                );
                            }

                            trace!("{self:?}: subsystem '{}' resumed", subsystem.get_nqn());
                        }
                        // todo: we may have received a Stop request whilst resuming
                        self.pause_state.store(NexusPauseState::Unpaused);
                    }
                    break;
                }
            }
        }

        // Resume one waiter in case there are any.
        if !self.pause_waiters.is_empty() {
            trace!("{self:?}: resuming the first resume waiter");
            let w = self.pause_waiters.pop_front().unwrap();
            w.send(0).expect("I/O subsystem resume waiter disappeared");
        }

        trace!("{self:?}: I/O resumed");
        Ok(())
    }
}
