use super::RebuildError;
use std::fmt;

/// Allowed states for a rebuild job.
#[derive(Debug, PartialEq, Copy, Clone)]
pub enum RebuildState {
    /// Init when the job is newly created
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

impl fmt::Display for RebuildState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
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
}

/// TODO
#[derive(Debug, Default)]
pub(super) struct RebuildStates {
    /// Current state of the rebuild job
    pub current: RebuildState,
    /// Pending state for the rebuild job
    pub(super) pending: Option<RebuildState>,
}

impl std::fmt::Display for RebuildStates {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Default for RebuildState {
    fn default() -> Self {
        RebuildState::Init
    }
}

impl RebuildStates {
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

    /// a change to `state` is pending
    pub(super) fn pending_equals(&self, state: RebuildState) -> bool {
        self.pending == Some(state)
    }

    /// reconcile the pending state into the current state
    pub(super) fn reconcile(&mut self) -> RebuildState {
        if let Some(pending) = self.pending {
            self.current = pending;
            self.pending = None;
        }

        self.current
    }
}
