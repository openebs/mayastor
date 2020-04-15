use crossbeam::channel::Receiver;
use rpc::mayastor::RebuildStateReply;
use snafu::ResultExt;

use crate::{
    bdev::nexus::{
        nexus_bdev::{
            nexus_lookup,
            CreateRebuildError,
            Error,
            Nexus,
            NexusState,
            RebuildJobNotFound,
            RebuildOperationError,
            RemoveRebuildJob,
        },
        nexus_channel::DREvent,
        nexus_child::ChildState,
    },
    core::Reactors,
    rebuild::{RebuildJob, RebuildOperations, RebuildState},
};

impl Nexus {
    /// Starts a rebuild job in the background
    pub async fn start_rebuild_rpc(&mut self, name: &str) -> Result<(), Error> {
        self.start_rebuild(name).await?;
        Ok(())
    }

    /// Starts a rebuild job and returns a receiver channel
    /// which can be used to await the rebuild completion
    pub async fn start_rebuild(
        &mut self,
        name: &str,
    ) -> Result<Receiver<RebuildState>, Error> {
        trace!("{}: start rebuild request for {}", self.name, name);

        let src_child_name =
            match self.children.iter().find(|c| c.state == ChildState::Open) {
                Some(child) => Ok(child.name.clone()),
                None => Err(Error::NoRebuildSource {
                    name: self.name.clone(),
                }),
            }?;

        let dst_child = match self.children.iter_mut().find(|c| c.name == name)
        {
            Some(c) => Ok(c),
            None => Err(Error::ChildNotFound {
                child: name.to_owned(),
                name: self.name.clone(),
            }),
        }?;

        let job = RebuildJob::create(
            &self.name,
            &src_child_name,
            &dst_child.name,
            self.data_ent_offset,
            self.bdev.num_blocks() + self.data_ent_offset,
            |nexus, job| {
                Reactors::current().send_future(async move {
                    Nexus::complete_rebuild(nexus, job).await;
                });
            },
        )
        .context(CreateRebuildError {
            child: name.to_string(),
            name: self.name.clone(),
        })?;

        dst_child.repairing = true;

        Ok(job.start())
    }

    /// Stop a rebuild job in the background
    pub async fn stop_rebuild(&self, name: &str) -> Result<(), Error> {
        match self.get_rebuild_job(name) {
            Ok(rt) => rt.stop().context(RebuildOperationError {}),
            // If a rebuild task is not found return ok
            // as we were just going to remove it anyway.
            Err(_) => Ok(()),
        }
    }

    /// Pause a rebuild job in the background
    pub async fn pause_rebuild(&mut self, name: &str) -> Result<(), Error> {
        let rt = self.get_rebuild_job(name)?;
        rt.pause().context(RebuildOperationError {})
    }

    /// Resume a rebuild job in the background
    pub async fn resume_rebuild(&mut self, name: &str) -> Result<(), Error> {
        let rt = self.get_rebuild_job(name)?;
        rt.resume().context(RebuildOperationError {})
    }

    /// Return the state of a rebuild job
    pub async fn get_rebuild_state(
        &mut self,
        name: &str,
    ) -> Result<RebuildStateReply, Error> {
        let rt = self.get_rebuild_job(name)?;
        Ok(RebuildStateReply {
            state: rt.state.to_string(),
        })
    }

    /// Cancels all rebuilds jobs associated with the child
    /// If any job is found with the child as a destination then the job is
    /// stopped. If any job is found with the child as a source then
    /// the job is replaced with a new one with another healthy child
    /// as src, if found
    /// todo: how to proceed if not healthy child is found?
    pub async fn cancel_child_rebuild_jobs(&mut self, name: &str) {
        let mut src_jobs = self.get_rebuild_job_src(name);

        // Issues a stop to all jobs with the child as a source
        src_jobs
            .iter_mut()
            .for_each(|j| j.stop().unwrap_or_else(|_| {}));

        // todo: before we can start a new rebuild we need to wait
        // for the previous rebuild to complete - cas-194
        for job in src_jobs {
            if let Err(e) = self.start_rebuild(&job.destination).await {
                error!("Failed to recreate rebuild: {}", e);
            }
        }

        // stops the only possible job with the child as a destination
        self.stop_rebuild(name).await.ok();
    }

    /// Return rebuild job associated with the src child name.
    /// Return error if no rebuild job associated with it.
    fn get_rebuild_job_src<'a>(
        &mut self,
        name: &'a str,
    ) -> Vec<&'a mut RebuildJob> {
        let jobs = RebuildJob::lookup_src(&name);

        jobs.iter().for_each(|job| assert!(job.nexus == self.name));
        jobs
    }

    /// Return rebuild job associated with the dest child name.
    /// Return error if no rebuild job associated with it.
    fn get_rebuild_job<'a>(
        &self,
        name: &'a str,
    ) -> Result<&'a mut RebuildJob, Error> {
        let job = RebuildJob::lookup(&name).context(RebuildJobNotFound {
            child: name.to_owned(),
            name: self.name.clone(),
        })?;

        assert!(job.nexus == self.name);
        Ok(job)
    }

    /// On rebuild job completion it updates the child and the nexus
    /// based on the rebuild job's final state
    async fn on_rebuild_complete_job(
        &mut self,
        job: &RebuildJob,
    ) -> Result<(), Error> {
        let recovered_child = self.get_child_by_name(&job.destination)?;

        recovered_child.repairing = false;

        if job.state == RebuildState::Completed {
            recovered_child.state = ChildState::Open;

            // Actually we'd have to check if all other children are healthy
            // and if not maybe we can start the other rebuild's?
            self.set_state(NexusState::Online);

            // child can now be part of the IO path
            self.reconfigure(DREvent::ChildOnline).await;
        } else {
            error!(
                "Rebuild job for child {} of nexus {} failed with state {:?}",
                &job.destination, &self.name, job.state
            );
        }

        Ok(())
    }

    async fn on_rebuild_complete(&mut self, job: String) -> Result<(), Error> {
        let j = RebuildJob::lookup(&job).context(RebuildJobNotFound {
            child: job.clone(),
            name: self.name.clone(),
        })?;

        if j.state == RebuildState::Paused {
            // Leave all states as they are
            return Ok(());
        }

        let job = RebuildJob::remove(&job).context(RemoveRebuildJob {
            child: job.clone(),
            name: self.name.clone(),
        })?;

        self.on_rebuild_complete_job(&job).await
    }

    /// Rebuild Complete callback when a rebuild job completes
    async fn complete_rebuild(nexus: String, job: String) {
        info!("nexus {} received complete_rebuild from job {}", nexus, job);

        if let Some(nexus) = nexus_lookup(&nexus) {
            if let Err(e) = nexus.on_rebuild_complete(job).await {
                error!("Failed to complete the rebuild with error {}", e);
            }
        } else {
            error!("Failed to find nexus {} for rebuild job {}", nexus, job);
        }
    }
}
