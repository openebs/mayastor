//!
//! This file contains the main routines that implement the rebuild process of
//! an nexus instance.

use crate::{
    bdev::nexus::{
        nexus_bdev::{Nexus, NexusState},
        nexus_child::ChildState,
        Error,
    },
    descriptor::Descriptor,
    event::spawm_on_core,
    rebuild::RebuildTask,
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

        for child in &self.children {
            if child.state == ChildState::Faulted {
                trace!(
                    "{}: child {} selected as rebuild target",
                    self.name,
                    child.name
                );
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
        let target = self.find_rebuild_target();
        let source = self.find_rebuild_source();

        if target.is_none() || source.is_none() {
            return Err(Error::Internal(
                "{}: cannot construct rebuild solution".into(),
            ));
        }

        let copy_task =
            RebuildTask::new(source.unwrap(), target.unwrap()).unwrap();

        let ctx = spawm_on_core(core, copy_task, |task| task.run());

        if let Ok(ctx) = ctx {
            self.rebuild_handle = Some(ctx);
            Ok(self.set_state(NexusState::Remuling))
        } else {
            Err(Error::Internal("unable to start rebuild".into()))
        }
    }

    pub async fn rebuild_completion(&mut self) -> Result<bool, Error> {
        if let Some(task) = self.rebuild_handle.as_mut() {
            if let Ok(r) = task.completed().await {
                let _ = self.rebuild_handle.take();
                Ok(r)
            } else {
                Ok(false)
            }
        } else {
            Err(Error::Invalid("No rebuild task registered".into()))
        }
    }

    pub fn rebuild_suspend(&mut self) -> Result<(), Error> {
        if let Some(mut task) = self.rebuild_handle.take() {
            let _state = task.suspend().unwrap();
            self.rebuild_handle = Some(task);
            Ok(())
        } else {
            Err(Error::Invalid("no rebuild task configured".into()))
        }
    }

    pub fn rebuild_resume(&mut self) -> Result<(), Error> {
        if let Some(mut task) = self.rebuild_handle.take() {
            let _state = task.resume().unwrap();
            self.rebuild_handle = Some(task);
            Ok(())
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
