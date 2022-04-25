use futures::channel::oneshot::Receiver;
use snafu::ResultExt;
use std::pin::Pin;

use super::{
    nexus_lookup_mut,
    ChildState,
    CreateRebuild,
    DrEvent,
    Error,
    Nexus,
    Reason,
    RebuildJobNotFound,
    RebuildOperation,
    RemoveRebuildJob,
    VerboseError,
};

use crate::{
    bdev::nexus::nexus_persistence::PersistOp,
    core::Reactors,
    rebuild::{
        ClientOperations,
        RebuildError,
        RebuildJob,
        RebuildState,
        RebuildStats,
    },
};

impl<'n> Nexus<'n> {
    /// Starts a rebuild job and returns a receiver channel
    /// which can be used to await the rebuild completion
    pub async fn start_rebuild(
        self: Pin<&mut Self>,
        name: &str,
    ) -> Result<Receiver<RebuildState>, Error> {
        trace!("{}: start rebuild request for {}", self.name, name);

        let src_child_name = match self
            .children
            .iter()
            .find(|c| c.state() == ChildState::Open && c.get_name() != name)
        {
            Some(child) => Ok(child.name.clone()),
            None => Err(Error::NoRebuildSource {
                name: self.name.clone(),
            }),
        }?;

        let dst_child_name =
            match self.children.iter().find(|c| c.get_name() == name) {
                Some(c)
                    if c.state() == ChildState::Faulted(Reason::OutOfSync) =>
                {
                    Ok(c.name.clone())
                }
                Some(c) => Err(Error::ChildNotDegraded {
                    child: name.to_owned(),
                    name: self.name.clone(),
                    state: c.state().to_string(),
                }),
                None => Err(Error::ChildNotFound {
                    child: name.to_owned(),
                    name: self.name.clone(),
                }),
            }?;

        let job = RebuildJob::create(
            &self.name,
            &src_child_name,
            &dst_child_name,
            std::ops::Range::<u64> {
                start: self.data_ent_offset,
                end: self.num_blocks() + self.data_ent_offset,
            },
            |nexus, job| {
                Reactors::current().send_future(async move {
                    Nexus::notify_rebuild(nexus, job).await;
                });
            },
        )
        .context(CreateRebuild {
            child: name.to_owned(),
            name: self.name.clone(),
        })?;

        // We're now rebuilding the `dst_child` which means it HAS to become an
        // active participant in the frontend nexus bdev for Writes.
        // This is because the rebuild job copies from src to target child
        // sequentially, from start to the end.
        // This means any Write frontend IO to a range which has already been
        // rebuilt would then need to be rebuilt again.
        // Ensuring that the dst child receives all frontend Write IO keeps all
        // rebuilt ranges in sync with the other children.
        self.reconfigure(DrEvent::ChildRebuild).await;

        job.as_client().start().context(RebuildOperation {
            job: name.to_owned(),
            name: self.name.clone(),
        })
    }

    /// Terminates a rebuild in the background
    /// used for shutdown operations and
    /// unlike the client operation stop, this command does not fail
    /// as it overrides the previous client operations
    async fn terminate_rebuild(&self, name: &str) {
        // If a rebuild job is not found that's ok
        // as we were just going to remove it anyway.
        if let Ok(rj) = self.get_rebuild_job(name) {
            let ch = rj.as_client().terminate();
            if let Err(e) = ch.await {
                error!(
                    "Failed to wait on rebuild job for child {} to terminate with error {}", name,
                    e.verbose()
                );
            }
        }
    }

    /// Stop a rebuild job in the background
    pub async fn stop_rebuild(&self, name: &str) -> Result<(), Error> {
        match self.get_rebuild_job(name) {
            Ok(rj) => rj.as_client().stop().context(RebuildOperation {
                job: name.to_owned(),
                name: self.name.clone(),
            }),
            // If a rebuild task is not found return ok
            // as we were just going to remove it anyway.
            Err(_) => Ok(()),
        }
    }

    /// Pause a rebuild job in the background
    pub async fn pause_rebuild(
        self: Pin<&mut Self>,
        name: &str,
    ) -> Result<(), Error> {
        let rj = self.get_rebuild_job(name)?.as_client();
        rj.pause().context(RebuildOperation {
            job: name.to_owned(),
            name: self.name.clone(),
        })
    }

    /// Resume a rebuild job in the background
    pub async fn resume_rebuild(
        self: Pin<&mut Self>,
        name: &str,
    ) -> Result<(), Error> {
        let rj = self.get_rebuild_job(name)?.as_client();
        rj.resume().context(RebuildOperation {
            job: name.to_owned(),
            name: self.name.clone(),
        })
    }

    /// Return the state of a rebuild job
    pub async fn get_rebuild_state(
        self: Pin<&mut Self>,
        name: &str,
    ) -> Result<RebuildState, Error> {
        let rj = self.get_rebuild_job(name)?;
        Ok(rj.state())
    }

    /// Return the stats of a rebuild job
    pub async fn get_rebuild_stats(
        self: Pin<&mut Self>,
        name: &str,
    ) -> Result<RebuildStats, Error> {
        let rj = self.get_rebuild_job(name)?;
        Ok(rj.stats())
    }

    /// Returns the rebuild progress of child target `name`
    pub fn get_rebuild_progress(&self, name: &str) -> Result<u32, Error> {
        let rj = self.get_rebuild_job(name)?;

        Ok(rj.as_client().stats().progress as u32)
    }

    /// Cancels all rebuilds jobs associated with the child.
    /// Returns a list of rebuilding children whose rebuild job was cancelled.
    pub async fn cancel_child_rebuild_jobs(&self, name: &str) -> Vec<String> {
        let mut src_jobs = self.get_rebuild_job_src(name);
        let mut terminated_jobs = Vec::new();
        let mut rebuilding_children = Vec::new();

        // terminate all jobs with the child as a source
        src_jobs.iter_mut().for_each(|j| {
            terminated_jobs.push(j.as_client().terminate());
            rebuilding_children.push(j.destination.clone());
        });

        // wait for the jobs to complete terminating
        for job in terminated_jobs {
            if let Err(e) = job.await {
                error!("Error {} when waiting for the job to terminate", e);
            }
        }

        // terminate the only possible job with the child as a destination
        self.terminate_rebuild(name).await;
        rebuilding_children
    }

    /// Start a rebuild for each of the children
    /// todo: how to proceed if no healthy child is found?
    pub async fn start_rebuild_jobs(
        mut self: Pin<&mut Self>,
        child_names: Vec<String>,
    ) {
        for name in child_names {
            if let Err(e) = self.as_mut().start_rebuild(&name).await {
                error!("Failed to start rebuild: {}", e.verbose());
            }
        }
    }

    /// Return rebuild job associated with the src child name.
    /// Return error if no rebuild job associated with it.
    fn get_rebuild_job_src<'a>(
        &self,
        name: &'a str,
    ) -> Vec<&'a mut RebuildJob> {
        let jobs = RebuildJob::lookup_src(name);

        jobs.iter().for_each(|job| assert_eq!(job.nexus, self.name));
        jobs
    }

    /// Return rebuild job associated with the dest child name.
    /// Return error if no rebuild job associated with it.
    fn get_rebuild_job<'a>(
        &self,
        name: &'a str,
    ) -> Result<&'a mut RebuildJob, Error> {
        let job = RebuildJob::lookup(name).context(RebuildJobNotFound {
            child: name.to_owned(),
            name: self.name.clone(),
        })?;

        assert_eq!(job.nexus, self.name);
        Ok(job)
    }

    /// On rebuild job completion it updates the child and the nexus
    /// based on the rebuild job's final state
    async fn on_rebuild_complete_job(
        mut self: Pin<&mut Self>,
        job: &RebuildJob,
    ) -> Result<(), Error> {
        let recovering_child =
            self.as_mut().get_child_by_name(&job.destination)?;

        match job.state() {
            RebuildState::Completed => {
                recovering_child.set_state(ChildState::Open);
                info!(
                    "Child {} has been rebuilt successfully",
                    recovering_child.get_name()
                );
                let child_name = recovering_child.get_name().to_string();
                let child_state = recovering_child.state();
                self.persist(PersistOp::Update((child_name, child_state)))
                    .await;
            }
            RebuildState::Stopped => {
                info!(
                    "Rebuild job for child {} of nexus {} stopped",
                    &job.destination, &self.name,
                );
            }
            RebuildState::Failed => {
                // rebuild has failed so we need to set the child as faulted
                // allowing the control plane to replace it with another
                if let Some(RebuildError::ReadIoError {
                    ..
                }) = job.error
                {
                    // todo: retry rebuild using another child as source?
                }
                recovering_child.fault(Reason::RebuildFailed).await;
                error!(
                    "Rebuild job for child {} of nexus {} failed, error: {}",
                    &job.destination,
                    &self.name,
                    job.error_desc(),
                );
            }
            _ => {
                recovering_child.fault(Reason::RebuildFailed).await;
                error!(
                    "Rebuild job for child {} of nexus {} failed with state {:?}",
                    &job.destination,
                    &self.name,
                    job.state(),
                );
            }
        }

        self.reconfigure(DrEvent::ChildRebuild).await;
        Ok(())
    }

    async fn on_rebuild_update(
        mut self: Pin<&mut Self>,
        job: String,
    ) -> Result<(), Error> {
        let j = RebuildJob::lookup(&job).context(RebuildJobNotFound {
            child: job.clone(),
            name: self.name.clone(),
        })?;

        if !j.state().done() {
            // Leave all states as they are
            return Ok(());
        }

        let complete_err = self.as_mut().on_rebuild_complete_job(j).await;
        let remove_err = RebuildJob::remove(&job)
            .context(RemoveRebuildJob {
                child: job,
                name: self.name.clone(),
            })
            .map(|_| ());

        complete_err.and(remove_err)
    }

    /// Rebuild updated callback when a rebuild job state updates
    async fn notify_rebuild(nexus: String, job: String) {
        info!("nexus {} received notify_rebuild from job {}", nexus, job);

        if let Some(nexus) = nexus_lookup_mut(&nexus) {
            if let Err(e) = nexus.on_rebuild_update(job).await {
                error!(
                    "Failed to complete the rebuild with error {}",
                    e.verbose()
                );
            }
        } else {
            error!("Failed to find nexus {} for rebuild job {}", nexus, job);
        }
    }
}
