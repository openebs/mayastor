use crossbeam::atomic::AtomicCell;
use snafu::Snafu;

use NvmeControllerState::*;

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum NvmeControllerState {
    New,
    Initializing,
    Running,
    Faulted(ControllerFailureReason),
    Unconfiguring,
    Unconfigured,
}
#[derive(Debug, PartialEq, Copy, Clone)]
pub enum ControllerFailureReason {
    Reset,
    Shutdown,
    NamespaceInit,
}

impl ToString for NvmeControllerState {
    fn to_string(&self) -> String {
        match *self {
            Self::New => "New",
            Self::Initializing => "Initializing",
            Self::Running => "Running",
            Self::Unconfiguring => "Unconfiguring",
            Self::Unconfigured => "Unconfigured",
            Self::Faulted(_) => "Faulted",
        }
        .to_string()
    }
}

#[derive(Eq, PartialEq, Copy, Clone, Debug)]
pub enum ControllerFlag {
    ResetActive,
}

impl ToString for ControllerFlag {
    fn to_string(&self) -> String {
        match *self {
            ControllerFlag::ResetActive => "ResetActive",
        }
        .to_string()
    }
}
/// Entity that manages the following components of every NVMe controller:
/// - controller states
/// - controller flags
///
/// 1. Controller state checks.
/// Every NVMe controller passes through different states during its lifetime,
/// which makes it important to control state transitions and disallow invalid
/// state changes. Controller state machine enforces state checks based on the
/// controller state diagram.
/// 2. Controller flags.
/// Controller flags are a set of boolean variables that carry some extra
/// information required for the controller to operate properly.
#[derive(Debug)]
pub struct ControllerStateMachine {
    name: String,
    current_state: NvmeControllerState,
    // TODO: As of now we have only one flag (ResetActive), so just one
    // variable is fine. Later we need to implement array of flags in a proper
    // way.
    flag: AtomicCell<bool>,
}

#[derive(Debug, Snafu, Clone)]
#[snafu(visibility = "pub")]
pub enum ControllerStateMachineError {
    #[snafu(display(
        "invalid transition from {:?} to {:?}",
        current_state,
        new_state
    ))]
    ControllerStateTransitionError {
        current_state: NvmeControllerState,
        new_state: NvmeControllerState,
    },
    #[snafu(display(
        "failed to exclusively update flag {:?} to {} from {}",
        flag,
        current_value,
        new_value
    ))]
    ControllerFlagUpdateError {
        flag: ControllerFlag,
        current_value: bool,
        new_value: bool,
    },
}

/// Check if a transition exists between two given states.
/// Initial state: New, final state: Unconfigured.
fn check_transition(
    from: NvmeControllerState,
    to: NvmeControllerState,
) -> bool {
    match from {
        New => matches!(to, Initializing),
        Initializing => matches!(to, Running | Faulted(_)),
        Running => matches!(to, Unconfiguring | Faulted(_)),
        Unconfiguring => matches!(to, Unconfigured | Faulted(_)),
        Faulted(_) => matches!(to, Running | Unconfiguring | Faulted(_)),
        // Final state, no further transitions possible.
        Unconfigured => false,
    }
}

impl ControllerStateMachine {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            current_state: New,
            flag: AtomicCell::new(false),
        }
    }

    /// Unconditionally transition from the current state to a new state.
    pub fn transition(
        &mut self,
        new_state: NvmeControllerState,
    ) -> Result<(), ControllerStateMachineError> {
        if check_transition(self.current_state, new_state) {
            info!(
                "{} transitioned from state {:?} to {:?}",
                self.name, self.current_state, new_state
            );
            self.current_state = new_state;
            Ok(())
        } else {
            Err(
                ControllerStateMachineError::ControllerStateTransitionError {
                    current_state: self.current_state,
                    new_state,
                },
            )
        }
    }

    /// Transition to a new state only if ID of the current state matches.
    pub fn transition_checked(
        &mut self,
        current_state: NvmeControllerState,
        new_state: NvmeControllerState,
    ) -> Result<(), ControllerStateMachineError> {
        if self.current_state != current_state {
            return Err(
                ControllerStateMachineError::ControllerStateTransitionError {
                    current_state,
                    new_state,
                },
            );
        }

        self.transition(new_state)
    }

    /// Get surrent state.
    pub fn current_state(&self) -> NvmeControllerState {
        self.current_state
    }

    /// Sets the flag only if it is not set.
    pub fn set_flag_exclusively(
        &self,
        flag: ControllerFlag,
    ) -> Result<(), ControllerStateMachineError> {
        let f = self.lookup_flag(flag);

        if let Err(current) = f.compare_exchange(false, true) {
            Err(ControllerStateMachineError::ControllerFlagUpdateError {
                flag,
                current_value: current,
                new_value: true,
            })
        } else {
            Ok(())
        }
    }

    /// Clears the flag only if it is set.
    pub fn clear_flag_exclusively(
        &self,
        flag: ControllerFlag,
    ) -> Result<(), ControllerStateMachineError> {
        let f = self.lookup_flag(flag);

        if let Err(current) = f.compare_exchange(true, false) {
            Err(ControllerStateMachineError::ControllerFlagUpdateError {
                flag,
                current_value: current,
                new_value: false,
            })
        } else {
            Ok(())
        }
    }

    // Private functions.
    fn lookup_flag(&self, _flag: ControllerFlag) -> &AtomicCell<bool> {
        // TODO: Implement name-based, array-based flag lookup later.
        &self.flag
    }
}
