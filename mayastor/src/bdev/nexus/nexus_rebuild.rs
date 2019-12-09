//!
//! This file contains the main routines that implement the rebuild process of
//! an nexus instance.

use crate::{
    bdev::nexus::{
        nexus_bdev::{Nexus, NexusState},
        nexus_channel::DREvent,
        nexus_child::ChildState,
        Error,
    },
    descriptor::Descriptor,
    event::spawn_on_core,
    rebuild::{RebuildState, RebuildTask},
};
use std::rc::Rc;
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

    pub fn start_rebuild(&mut self, core: u32) -> Result<NexusState, Error> {
        if self.state == NexusState::Remuling {
            assert_eq!(true, self.children.iter().any(|c| c.repairing));
            return Err(Error::Invalid(
                "can only do one rebuild per nexus at the same time for now"
                    .into(),
            ));
        }

        let target = self.find_rebuild_target();
        let source = self.find_rebuild_source();

        if target.is_none() || source.is_none() {
            return Err(Error::Internal(
                "{}: cannot construct rebuild solution".into(),
            ));
        }

        let rebuild_task =
            RebuildTask::new(source.unwrap(), target.unwrap()).unwrap();

        let ctx = spawn_on_core(core, rebuild_task, |task| task.run());

        if let Ok(ctx) = ctx {
            self.rebuild_handle = Some(ctx);
            Ok(self.set_state(NexusState::Remuling))
        } else {
            Err(Error::Internal("unable to start rebuild".into()))
        }
    }

    /// await the completion of the rebuild.
    pub async fn rebuild_completion(&mut self) -> Result<RebuildState, Error> {
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
                    return Err(Error::Internal(
                        "rebuild process disappeared halfway".into(),
                    ));
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
                    Err(Error::Internal(
                        "rebuild failed; sender is gone".into(),
                    ))
                }
            };
            let rebuild = self.rebuild_handle.take();
            drop(rebuild);
            result
        } else {
            Err(Error::Invalid(
                "No rebuild task registered or rebuild already completed"
                    .into(),
            ))
        }
    }

    pub fn rebuild_suspend(&mut self) -> Result<RebuildState, Error> {
        if let Some(task) = self.rebuild_handle.as_mut() {
            Ok(task.suspend().unwrap())
        } else {
            Err(Error::Invalid("no rebuild task configured".into()))
        }
    }

    pub fn rebuild_resume(&mut self) -> Result<RebuildState, Error> {
        if let Some(task) = self.rebuild_handle.as_mut() {
            Ok(task.resume().unwrap())
        } else {
            Err(Error::Invalid("no rebuild task configured".into()))
        }
    }

    pub fn rebuild_get_current(&mut self) -> Result<u64, Error> {
        if let Some(task) = self.rebuild_handle.as_ref() {
            Ok(task.current())
        } else {
            Err(Error::Invalid("no rebuild task configured".into()))
        }
    }

    pub fn log_progress(&mut self) {
        if let Some(hdl) = self.rebuild_handle.as_ref() {
            info!(":{} {:?} {}", self.name, self.state, hdl.current());
        }
    }
}
