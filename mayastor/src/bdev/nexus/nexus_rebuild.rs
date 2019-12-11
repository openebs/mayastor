//!
//! This file contains the main routines that implement the rebuild process of
//! an nexus instance.

use crate::{
    bdev::nexus::{
        nexus_bdev::{Nexus, NexusState},
        nexus_channel::DREvent,
        nexus_child::ChildState,
    },
    descriptor::Descriptor,
    event,
    rebuild::{RebuildState, RebuildTask},
};
use snafu::{ResultExt, Snafu};
use std::rc::Rc;

#[derive(Debug, Snafu)]
pub enum RebuildError {
    #[snafu(display(
        "Can only do one rebuild per nexus {} at the same time",
        name
    ))]
    OnlyOneRebuild { name: String },
    #[snafu(display(
        "There is no rebuild task in progress for the nexus {}",
        name
    ))]
    NoRebuildTask { name: String },
    #[snafu(display("Cannot find a rebuild solution for nexus {}", name))]
    NoSolution { name: String },
    #[snafu(display("Unable to spawn rebuild task for nexus {}", name))]
    SpawnRebuild { source: event::Error, name: String },
    #[snafu(display("Rebuild task for nexus {} disappeared halfway", name))]
    Disappeared { name: String },
    #[snafu(display("Rebuild for nexus {} failed; sender is gone", name))]
    SenderIsGone { name: String },
}

impl Nexus {
    /// find any child that requires a rebuild. Children in the faulted state
    /// are eligible for a rebuild closed children are not and must be
    /// opened first.
    fn find_rebuild_target(&mut self) -> Option<Rc<Descriptor>> {
        if self.state != NexusState::Degraded {
            trace!(
                "{}: does not require any rebuild operation as its state: {}",
                self.name,
                self.state.to_string()
            );
        }

        for child in &mut self.children {
            if child.state == ChildState::Faulted {
                trace!(
                    "{}: child {} selected as rebuild target",
                    self.name,
                    child.name
                );
                child.repairing = true;
                return Some(child.descriptor.as_ref()?.clone());
            }
        }
        None
    }

    /// find a child which can be used as a rebuild source
    fn find_rebuild_source(&mut self) -> Option<Rc<Descriptor>> {
        if self.children.len() == 1 {
            trace!("{}: not enough children to initiate rebuild", self.name);
            return None;
        }

        for child in &self.children {
            if child.state == ChildState::Open {
                trace!(
                    "{}: child {} selected as rebuild source",
                    self.name,
                    child.name
                );
                return Some(child.descriptor.as_ref()?.clone());
            }
        }
        None
    }

    pub fn start_rebuild(
        &mut self,
        core: u32,
    ) -> Result<NexusState, RebuildError> {
        if self.state == NexusState::Remuling {
            assert_eq!(true, self.children.iter().any(|c| c.repairing));
            return Err(RebuildError::OnlyOneRebuild {
                name: self.name.clone(),
            });
        }

        let target = self.find_rebuild_target();
        let source = self.find_rebuild_source();

        if target.is_none() || source.is_none() {
            return Err(RebuildError::NoSolution {
                name: self.name.clone(),
            });
        }

        let rebuild_task =
            RebuildTask::new(source.unwrap(), target.unwrap()).unwrap();

        let ctx = event::spawn_on_core(core, rebuild_task, |task| task.run())
            .context(SpawnRebuild {
            name: self.name.clone(),
        })?;

        self.rebuild_handle = Some(ctx);
        Ok(self.set_state(NexusState::Remuling))
    }

    /// await the completion of the rebuild.
    pub async fn rebuild_completion(
        &mut self,
    ) -> Result<RebuildState, RebuildError> {
        if let Some(task) = self.rebuild_handle.as_mut() {
            // get a hold of the child that is in the repairing state
            let mut child = match self.children.iter_mut().find(|c| c.repairing)
            {
                Some(c) => c,
                None => {
                    // we were rebuilding, but for some reason it errored out
                    // in case we did not handle properly.
                    // The task is gone, but the state has not been updated
                    // properly.
                    return Err(RebuildError::Disappeared {
                        name: self.name.clone(),
                    });
                }
            };

            child.repairing = false;

            let result = match task.completed().await {
                Ok(state) => {
                    // mark the child that has been completed as healthy
                    if state == RebuildState::Completed {
                        trace!("setting child {} state to online", child.name);
                        child.state = ChildState::Open;
                        self.reconfigure(DREvent::ChildOnline).await;
                    }

                    if state == RebuildState::Completed && self.is_healthy() {
                        self.set_state(NexusState::Online);
                    } else {
                        self.set_state(NexusState::Degraded);
                    }
                    Ok(state)
                }

                Err(_) => {
                    self.set_state(NexusState::Degraded);
                    Err(RebuildError::SenderIsGone {
                        name: self.name.clone(),
                    })
                }
            };
            let rebuild = self.rebuild_handle.take();
            drop(rebuild);
            result
        } else {
            Err(RebuildError::NoRebuildTask {
                name: self.name.clone(),
            })
        }
    }

    pub fn rebuild_suspend(&mut self) -> Result<RebuildState, RebuildError> {
        if let Some(task) = self.rebuild_handle.as_mut() {
            Ok(task.suspend().unwrap())
        } else {
            Err(RebuildError::NoRebuildTask {
                name: self.name.clone(),
            })
        }
    }

    pub fn rebuild_resume(&mut self) -> Result<RebuildState, RebuildError> {
        if let Some(task) = self.rebuild_handle.as_mut() {
            Ok(task.resume().unwrap())
        } else {
            Err(RebuildError::NoRebuildTask {
                name: self.name.clone(),
            })
        }
    }

    pub fn rebuild_get_current(&mut self) -> Result<u64, RebuildError> {
        if let Some(task) = self.rebuild_handle.as_ref() {
            Ok(task.current())
        } else {
            Err(RebuildError::NoRebuildTask {
                name: self.name.clone(),
            })
        }
    }

    pub fn log_progress(&mut self) {
        if let Some(hdl) = self.rebuild_handle.as_ref() {
            info!(":{} {:?} {}", self.name, self.state, hdl.current());
        }
    }
}
