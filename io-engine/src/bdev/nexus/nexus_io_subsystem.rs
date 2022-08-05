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
    Unpaused,
    Pausing,
    Paused,
    Unpausing,
}

impl Display for NexusPauseState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Unpaused => "unpaused",
                Self::Pausing => "pausing",
                Self::Paused => "paused",
                Self::Unpausing => "unpausing",
            }
        )
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

impl<'n> Debug for NexusIoSubsystem<'n> {
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

        trace!("{:?}: pausing I/O...", self);

        loop {
            let state = self.pause_state.compare_exchange(
                NexusPauseState::Unpaused,
                NexusPauseState::Pausing,
            );

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
                        if let Some(subsystem) =
                            NvmfSubsystem::nqn_lookup(&self.name)
                        {
                            trace!(
                                "{:?}: pausing subsystem '{}'...",
                                self,
                                subsystem.get_nqn()
                            );

                            subsystem.pause().await.unwrap();

                            trace!(
                                "{:?}: subsystem '{}' paused",
                                self,
                                subsystem.get_nqn()
                            );
                        }
                    }

                    // Mark subsystem as paused after it has been paused.
                    self.pause_state
                        .compare_exchange(
                            NexusPauseState::Pausing,
                            NexusPauseState::Paused,
                        )
                        .expect("Failed to mark subsystem as Paused");
                    break;
                }
                // Subsystem is already paused, increment number of paused.
                Err(NexusPauseState::Paused) => {
                    trace!(
                        "{:?}: nexus is already paused, \
                        incrementing pause count",
                        self
                    );
                    self.pause_cnt.fetch_add(1, Ordering::SeqCst);
                    break;
                }
                // Wait till the subsystem has completed transition and retry
                // operation.
                Err(NexusPauseState::Unpausing)
                | Err(NexusPauseState::Pausing) => {
                    trace!(
                        "{:?}: nexus is in intermediate state, \
                            deferring pause operation",
                        self
                    );

                    let (s, r) = oneshot::channel::<i32>();
                    self.pause_waiters.push_back(s);
                    r.await.unwrap();

                    trace!(
                        "{:?}: nexus completed state transition, \
                        retrying pause operation",
                        self
                    );
                }
                _ => {
                    panic!("Corrupted I/O subsystem state");
                }
            };
        }

        // Resume one waiter in case there are any.
        if let Some(w) = self.pause_waiters.pop_front() {
            trace!("{:?}: resuming the first pause waiter", self);
            w.send(0).expect("I/O subsystem pause waiter disappeared");
        }

        trace!("{:?}: I/O paused", self);
        Ok(())
    }

    /// Resume IO to the bdev.
    /// Note: in order to handle concurrent resumes properly, this function must
    /// be called only from the master core.
    pub(super) async fn resume(&mut self) -> Result<(), Error> {
        assert_eq!(
            Cores::current(),
            Cores::first(),
            "NexusIoSubsystem::resume() must called on the first core"
        );

        trace!("{:?}: resuming I/O...", self);

        loop {
            match self.pause_state.load() {
                // Already unpaused, bail out.
                NexusPauseState::Unpaused => {
                    break;
                }
                // Simultaneous pausing/unpausing: wait till the subsystem has
                // completed transition and retry operation.
                NexusPauseState::Pausing | NexusPauseState::Unpausing => {
                    trace!(
                        "{:?}: nexus is in intermediate state, \
                        deferring resume operation",
                        self
                    );

                    let (s, r) = oneshot::channel::<i32>();
                    self.pause_waiters.push_back(s);
                    r.await.unwrap();

                    trace!(
                        "{:?}: completed state transition, \
                        retrying resume operation",
                        self
                    );
                }
                // Unpause the subsystem, taking into account the overall number
                // of pauses.
                NexusPauseState::Paused => {
                    let v = self.pause_cnt.fetch_sub(1, Ordering::SeqCst);
                    // In case the last pause discarded, resume the subsystem.
                    if v == 1 {
                        if let Some(subsystem) =
                            NvmfSubsystem::nqn_lookup(&self.name)
                        {
                            self.pause_state.store(NexusPauseState::Unpausing);
                            trace!(
                                "{:?}: resuming subsystem '{}'...",
                                self,
                                subsystem.get_nqn()
                            );

                            subsystem.resume().await.unwrap();

                            trace!(
                                "{:?}: subsystem '{}' resumed",
                                self,
                                subsystem.get_nqn()
                            );
                        }
                        self.pause_state.store(NexusPauseState::Unpaused);
                    }
                    break;
                }
            }
        }

        // Resume one waiter in case there are any.
        if !self.pause_waiters.is_empty() {
            trace!("{:?}: resuming the first resume waiter", self);
            let w = self.pause_waiters.pop_front().unwrap();
            w.send(0).expect("I/O subsystem resume waiter disappeared");
        }

        trace!("{:?}: I/O resumed", self);
        Ok(())
    }
}
