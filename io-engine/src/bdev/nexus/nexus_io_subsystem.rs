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
        if Cores::current() != Cores::first() {
            return Err(Error::OperationNotAllowed {
                reason: "NexusIoSubsystem::suspend() must be called on the first core".into(),
            });
        }

        trace!("{self:?}: pausing I/O...");

        loop {
            let state = self
                .pause_state
                .compare_exchange(NexusPauseState::Unpaused, NexusPauseState::Pausing);

            match state {
                Ok(NexusPauseState::Unpaused) => {
                    // Pause subsystem. The only acceptable counter transition
                    // is: 0 -> 1. A pause can't be in effect while unpaused, so a
                    // corrupted counter is reset rather than trusted.
                    let count = self.pause_cnt.swap(1, Ordering::SeqCst);
                    if count != 0 {
                        error!("{self:?}: corrupted subsystem pause counter {count}, reset");
                    }

                    if let Some(Protocol::Nvmf) = self.bdev.shared() {
                        if let Some(subsystem) = NvmfSubsystem::nqn_lookup(&self.name) {
                            let nqn = subsystem.get_nqn();
                            trace!("{self:?}: pausing subsystem '{nqn}'...");

                            match subsystem.pause().await {
                                Ok(_) => trace!("{self:?}: subsystem '{nqn}' paused"),
                                // todo: handle error instead of panic, but in practice only ENOMEM can be seen here?
                                // ENODEV can be seen when nexus is being unshared concurrently
                                Err(error)
                                    if matches!(
                                        error.errno(),
                                        nix::Error::EPERM | nix::Error::ECANCELED
                                    ) =>
                                {
                                    warn!("{self:?}: subsystem '{nqn}' not paused: {error}")
                                }
                                Err(error) => {
                                    // undo the pause and let any waiter retry
                                    error!("{self:?}: failed to pause subsystem '{nqn}': {error}");
                                    self.pause_cnt.store(0, Ordering::SeqCst);
                                    self.pause_state.store(NexusPauseState::Unpaused);
                                    self.wake_waiter();
                                    return Err(Error::SubsysNvmf {
                                        e: format!("failed to pause subsystem '{nqn}': {error}"),
                                    });
                                }
                            }
                        }
                    }

                    // Mark subsystem as paused after it has been paused.
                    if let Err(state) = self
                        .pause_state
                        .compare_exchange(NexusPauseState::Pausing, NexusPauseState::Paused)
                    {
                        error!("{self:?}: unexpected state {state:?} whilst pausing, marking as Paused");
                        self.pause_state.store(NexusPauseState::Paused);
                    }
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
                    error!("{self:?}: corrupted I/O subsystem state: {state:?}");
                    return Err(Error::OperationNotAllowed {
                        reason: format!("corrupted I/O subsystem state: {state:?}"),
                    });
                }
            }
        }

        // Resume one waiter in case there are any.
        self.wake_waiter();

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
        if Cores::current() != Cores::first() {
            return Err(Error::OperationNotAllowed {
                reason: "NexusIoSubsystem::resume() must be called on the first core".into(),
            });
        }

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

                    if state == NexusPauseState::Pausing {
                        warn!("{nex}: resuming whilst pausing");
                    }

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
                    let Ok(v) =
                        self.pause_cnt
                            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |count| {
                                count.checked_sub(1)
                            })
                    else {
                        error!("{self:?}: resume without a matching pause");
                        return Err(Error::OperationNotAllowed {
                            reason: "resume without a matching pause".into(),
                        });
                    };

                    if v != 1 {
                        break;
                    } // In case the last pause discarded, resume the subsystem.

                    if state == NexusPauseState::Frozen || freeze {
                        if let Some(subsystem) = NvmfSubsystem::nqn_lookup(&self.name) {
                            trace!(
                                "{self:?}: subsystem '{}' not being resumed",
                                subsystem.nqn_str()
                            );
                        }
                        self.pause_state.store(NexusPauseState::Frozen);
                    } else {
                        if let Some(subsystem) = NvmfSubsystem::nqn_lookup(&self.name) {
                            let nqn = subsystem.get_nqn();
                            self.pause_state.store(NexusPauseState::Unpausing);
                            trace!("{self:?}: resuming subsystem '{nqn}'...");
                            match subsystem.resume().await {
                                Ok(_) => trace!("{self:?}: subsystem '{nqn}' resumed"),
                                Err(error)
                                    if matches!(
                                        error.errno(),
                                        nix::Error::EPERM | nix::Error::ECANCELED
                                    ) =>
                                {
                                    warn!("{self:?}: subsystem '{nqn}' not resumed: {error}");
                                }
                                Err(error) => {
                                    // still paused: restore the pause so it can be retried
                                    error!("{self:?}: failed to resume subsystem '{nqn}': {error}");
                                    self.pause_cnt.store(1, Ordering::SeqCst);
                                    self.pause_state.store(NexusPauseState::Paused);
                                    self.wake_waiter();
                                    return Err(Error::SubsysNvmf {
                                        e: format!("failed to resume subsystem '{nqn}': {error}"),
                                    });
                                }
                            }
                        }
                        // todo: we may have received a Stop request whilst resuming
                        self.pause_state.store(NexusPauseState::Unpaused);
                    }
                    break;
                }
            }
        }

        // Resume one waiter in case there are any.
        self.wake_waiter();

        trace!("{self:?}: I/O resumed");
        Ok(())
    }

    /// Wake up the first waiter which is still waiting, if any.
    fn wake_waiter(&mut self) {
        while let Some(waiter) = self.pause_waiters.pop_front() {
            if waiter.send(0).is_ok() {
                trace!("{self:?}: resumed the first waiter");
                break;
            }
        }
    }
}
