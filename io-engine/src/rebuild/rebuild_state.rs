use super::{RebuildError, RebuildOperation};
use crate::rebuild::RebuildStats;

/// Allowed states for a rebuild job.
#[derive(Default, Debug, PartialEq, Copy, Clone)]
pub enum RebuildState {
    /// Init when the job is newly created
    #[default]
    Init,
    /// Running when the job is rebuilding
    Running,
    /// Stopped when the job is halted as requested through stop
    /// and pending its removal
    Stopped,
    /// Paused when the job is paused as requested through pause
    Paused,
    /// Failed when an IO (R/W) operation was failed
    /// there are no retries as it currently stands
    Failed,
    /// Completed when the rebuild was successfully completed
    Completed,
}

impl std::fmt::Display for RebuildState {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            RebuildState::Init => write!(f, "init"),
            RebuildState::Running => write!(f, "running"),
            RebuildState::Stopped => write!(f, "stopped"),
            RebuildState::Paused => write!(f, "paused"),
            RebuildState::Failed => write!(f, "failed"),
            RebuildState::Completed => write!(f, "completed"),
        }
    }
}

impl RebuildState {
    /// Final update for a rebuild job
    pub fn done(self) -> bool {
        matches!(self, Self::Stopped | Self::Failed | Self::Completed)
    }
    /// Chugging along and rebuilding segments.
    pub fn running(&self) -> bool {
        matches!(self, Self::Running)
    }
}

/// Rebuild state information containing the current and pending states.
/// The pending state is used when trying to move to the desired state, eg:
/// stopping a rebuild but the rebuild cannot be stopped right away if it has
/// ongoing active tasks.
#[derive(Debug, Default, Clone)]
pub(super) struct RebuildStates {
    /// Current state of the rebuild job.
    pub current: RebuildState,
    /// Pending state for the rebuild job.
    pub(super) pending: Option<RebuildState>,
    /// Last rebuild error, if any.
    pub(super) error: Option<RebuildError>,
    final_stats: Option<RebuildStats>,
}

impl std::fmt::Display for RebuildStates {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl RebuildStates {
    /// Get the final rebuild statistics.
    pub(super) fn final_stats(&self) -> &Option<RebuildStats> {
        &self.final_stats
    }
    /// Set the final rebuild statistics.
    pub(super) fn set_final_stats(&mut self, stats: RebuildStats) {
        self.final_stats = Some(stats);
    }

    /// Set's the next pending state
    /// if one is already set then override only if flag is set
    pub(super) fn set_pending(
        &mut self,
        state: RebuildState,
        override_pending: bool,
    ) -> Result<(), RebuildError> {
        match self.pending {
            Some(pending) if !override_pending && (pending != state) => {
                Err(RebuildError::StatePending {
                    state: pending.to_string(),
                })
            }
            _ => {
                if self.current != state {
                    self.pending = Some(state);
                } else {
                    self.pending = None;
                }
                Ok(())
            }
        }
    }

    /// Reconcile the pending state into the current state.
    pub(super) fn reconcile(&mut self) -> RebuildState {
        if let Some(pending) = self.pending.take() {
            self.current = pending;
        }

        self.current
    }

    /// Single state machine where all operations are handled.
    /// Returns bool indicating whether a change has happened or not.
    pub(super) fn exec_op(
        &mut self,
        op: RebuildOperation,
        override_pending: bool,
    ) -> Result<bool, RebuildError> {
        type S = RebuildState;
        let e = RebuildError::OpError {
            operation: op.to_string(),
            state: self.to_string(),
        };

        debug!(
            "Executing operation {} with override {}, {:?}",
            op, override_pending, self
        );

        match op {
            RebuildOperation::Start => {
                match self.current {
                    // start only allowed when... starting
                    S::Stopped | S::Paused | S::Failed | S::Completed => Err(e),
                    // for idempotence sake
                    S::Running => Ok(false),
                    S::Init => {
                        self.set_pending(S::Running, false)?;
                        Ok(true)
                    }
                }
            }
            RebuildOperation::Stop => {
                match self.current {
                    // We're already stopping anyway, so all is well
                    S::Failed | S::Completed => Err(e),
                    // for idempotence sake
                    S::Stopped => Ok(false),
                    S::Running => {
                        self.set_pending(S::Stopped, override_pending)?;
                        Ok(false)
                    }
                    S::Init | S::Paused => {
                        self.set_pending(S::Stopped, override_pending)?;
                        Ok(true)
                    }
                }
            }
            RebuildOperation::Pause => match self.current {
                S::Stopped | S::Failed | S::Completed => Err(e),
                S::Init | S::Running | S::Paused => {
                    self.set_pending(S::Paused, false)?;
                    Ok(false)
                }
            },
            RebuildOperation::Resume => match self.current {
                S::Init | S::Stopped | S::Failed | S::Completed => Err(e),
                S::Running | S::Paused => {
                    self.set_pending(S::Running, false)?;
                    Ok(true)
                }
            },
            RebuildOperation::Fail => match self.current {
                S::Init | S::Stopped | S::Paused | S::Completed => Err(e),
                // for idempotence sake
                S::Failed => Ok(false),
                S::Running => {
                    self.set_pending(S::Failed, override_pending)?;
                    Ok(false)
                }
            },
            RebuildOperation::Complete => match self.current {
                S::Init | S::Paused | S::Stopped | S::Failed | S::Completed => {
                    Err(e)
                }
                S::Running => {
                    self.set_pending(S::Completed, override_pending)?;
                    Ok(false)
                }
            },
        }
    }
}
