use std::{
    collections::VecDeque,
    sync::atomic::{AtomicU32, Ordering},
};

use crossbeam::atomic::AtomicCell;
use futures::channel::oneshot;

use crate::{
    bdev::{nexus::nexus_bdev::Error as NexusError, Nexus},
    core::{Bdev, Cores, Protocol, Share},
    subsys::NvmfSubsystem,
};

/// TODO
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum NexusPauseState {
    Unpaused,
    Pausing,
    Paused,
    Unpausing,
}

/// Abstraction for managing pausing/unpausing I/O on NVMe subsystem, allowing
/// concurrent pause/resume calls by serializing low-level SPDK calls.
#[derive(Debug)]
pub(super) struct NexusIoSubsystem<'n> {
    /// TODO
    name: String,
    /// TODO
    bdev: &'n mut Bdev<Nexus<'n>>,
    /// TODO
    pause_state: AtomicCell<NexusPauseState>,
    /// TODO
    pause_waiters: VecDeque<oneshot::Sender<i32>>,
    /// TODO
    pause_cnt: AtomicU32,
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
    pub(super) async fn suspend(&mut self) -> Result<(), NexusError> {
        assert_eq!(Cores::current(), Cores::first());

        trace!(?self.name, "pausing nexus I/O");

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
                            trace!(nexus=%self.name, nqn=%subsystem.get_nqn(), "pausing subsystem");
                            subsystem.pause().await.unwrap();
                            trace!(nexus=%self.name, nqn=%subsystem.get_nqn(), "subsystem paused");
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
                    trace!(nexus=%self.name, "nexus is already paused, incrementing refcount");
                    self.pause_cnt.fetch_add(1, Ordering::SeqCst);
                    break;
                }
                // Wait till the subsystem has completed transition and retry
                // operation.
                Err(NexusPauseState::Unpausing)
                | Err(NexusPauseState::Pausing) => {
                    trace!(nexus=%self.name, "nexus is in intermediate state, deferring Pause operation");
                    let (s, r) = oneshot::channel::<i32>();
                    self.pause_waiters.push_back(s);
                    r.await.unwrap();
                    trace!(nexus=%self.name, "nexus completed state transition, retrying Pause operation");
                }
                _ => {
                    panic!("Corrupted I/O subsystem state");
                }
            };
        }

        // Resume one waiter in case there are any.
        if let Some(w) = self.pause_waiters.pop_front() {
            trace!(nexus=%self.name, "resuming the first Pause waiter");
            w.send(0).expect("I/O subsystem pause waiter disappeared");
        }

        trace!(?self.name, "nexus I/O paused");
        Ok(())
    }

    /// Resume IO to the bdev.
    /// Note: in order to handle concurrent resumes properly, this function must
    /// be called only from the master core.
    pub(super) async fn resume(&mut self) -> Result<(), NexusError> {
        assert_eq!(Cores::current(), Cores::first());

        trace!(?self.name, "resuming nexus I/O");

        loop {
            match self.pause_state.load() {
                // Already unpaused, bail out.
                NexusPauseState::Unpaused => {
                    break;
                }
                // Simultaneous pausing/unpausing: wait till the subsystem has
                // completed transition and retry operation.
                NexusPauseState::Pausing | NexusPauseState::Unpausing => {
                    trace!(?self.name, "nexus is in intermediate state, deferring Resume operation");
                    let (s, r) = oneshot::channel::<i32>();
                    self.pause_waiters.push_back(s);
                    r.await.unwrap();
                    trace!(?self.name, "completed state transition, retrying Resume operation");
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
                            trace!(nexus=%self.name, nqn=%subsystem.get_nqn(), "resuming subsystem");
                            subsystem.resume().await.unwrap();
                            trace!(nexus=%self.name, nqn=%subsystem.get_nqn(), "subsystem resumed");
                        }
                        self.pause_state.store(NexusPauseState::Unpaused);
                    }
                    break;
                }
            }
        }

        // Resume one waiter in case there are any.
        if !self.pause_waiters.is_empty() {
            trace!(nexus=%self.name, "resuming the first Resume waiter");
            let w = self.pause_waiters.pop_front().unwrap();
            w.send(0).expect("I/O subsystem resume waiter disappeared");
        }

        trace!(?self.name, "nexus I/O resumed");
        Ok(())
    }
}
