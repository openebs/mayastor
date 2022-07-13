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
    rebuild::{RebuildError, RebuildJob, RebuildState, RebuildStats},
};

impl<'n> Nexus<'n> {
    /// Starts a rebuild job and returns a receiver channel
    /// which can be used to await the rebuild completion
    pub async fn start_rebuild(
        self: Pin<&mut Self>,
        child_uri: &str,
    ) -> Result<Receiver<RebuildState>, Error> {
        trace!("{}: start rebuild request for {}", self.name, child_uri);

        let src_child_uri = match self
            .children
            .iter()
            .find(|c| c.state() == ChildState::Open && c.uri() != child_uri)
        {
            Some(child) => Ok(child.uri().to_owned()),
            None => Err(Error::NoRebuildSource {
                name: self.name.clone(),
            }),
        }?;

        let dst_child_uri =
            match self.children.iter().find(|c| c.uri() == child_uri) {
                Some(c)
                    if c.state() == ChildState::Faulted(Reason::OutOfSync) =>
                {
                    Ok(c.uri().to_owned())
                }
                Some(c) => Err(Error::ChildNotDegraded {
                    child: child_uri.to_owned(),
                    name: self.name.clone(),
                    state: c.state().to_string(),
                }),
                None => Err(Error::ChildNotFound {
                    child: child_uri.to_owned(),
                    name: self.name.clone(),
                }),
            }?;

        let job = RebuildJob::create(
            &self.name,
            &src_child_uri,
            &dst_child_uri,
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
            child: child_uri.to_owned(),
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

        job.start().context(RebuildOperation {
            job: child_uri.to_owned(),
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
            let ch = rj.terminate();
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
            Ok(rj) => rj.stop().context(RebuildOperation {
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
        let rj = self.get_rebuild_job(name)?;
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
        let rj = self.get_rebuild_job(name)?;
        rj.resume().context(RebuildOperation {
            job: name.to_owned(),
            name: self.name.clone(),
        })
    }

    /// Return the state of a rebuild job for the given destination.
    pub async fn get_rebuild_state(
        self: Pin<&mut Self>,
        dst_uri: &str,
    ) -> Result<RebuildState, Error> {
        let rj = self.get_rebuild_job(dst_uri)?;
        Ok(rj.state())
    }

    /// Return the stats of a rebuild job for the given destination.
    pub async fn get_rebuild_stats(
        self: Pin<&mut Self>,
        dst_uri: &str,
    ) -> Result<RebuildStats, Error> {
        let rj = self.get_rebuild_job(dst_uri)?;
        Ok(rj.stats())
    }

    /// Returns the rebuild progress of a rebuild job for the given destination.
    pub fn get_rebuild_progress(&self, dst_uri: &str) -> Result<u32, Error> {
        let rj = self.get_rebuild_job(dst_uri)?;
        Ok(rj.stats().progress as u32)
    }

    /// Cancels all rebuilds jobs associated with the child.
    /// Returns a list of rebuilding children whose rebuild job was cancelled.
    pub async fn cancel_child_rebuild_jobs(
        &self,
        src_uri: &str,
    ) -> Vec<String> {
        let mut src_jobs = self.get_rebuild_job_src(src_uri);
        let mut terminated_jobs = Vec::new();
        let mut rebuilding_children = Vec::new();

        // terminate all jobs with the child as a source
        src_jobs.iter_mut().for_each(|j| {
            terminated_jobs.push(j.terminate());
            rebuilding_children.push(j.dst_uri.clone());
        });

        // wait for the jobs to complete terminating
        for job in terminated_jobs {
            if let Err(e) = job.await {
                error!("Error {} when waiting for the job to terminate", e);
            }
        }

        // terminate the only possible job with the child as a destination
        self.terminate_rebuild(src_uri).await;
        rebuilding_children
    }

    /// Start a rebuild for each of the children
    /// todo: how to proceed if no healthy child is found?
    pub async fn start_rebuild_jobs(
        mut self: Pin<&mut Self>,
        child_uris: Vec<String>,
    ) {
        for uri in child_uris {
            if let Err(e) = self.as_mut().start_rebuild(&uri).await {
                error!("Failed to start rebuild: {}", e.verbose());
            }
        }
    }

    /// Return rebuild jobs associated with the src child name.
    fn get_rebuild_job_src<'a>(
        &self,
        src_child_uri: &'a str,
    ) -> Vec<&'a mut RebuildJob> {
        let jobs = RebuildJob::lookup_src(src_child_uri);

        jobs.iter()
            .for_each(|job| assert_eq!(job.nexus_name, self.name));
        jobs
    }

    /// Return rebuild job associated with the dest child name.
    /// Return error if no rebuild job associated with it.
    fn get_rebuild_job<'a>(
        &self,
        dst_child_uri: &'a str,
    ) -> Result<&'a mut RebuildJob, Error> {
        let job =
            RebuildJob::lookup(dst_child_uri).context(RebuildJobNotFound {
                child: dst_child_uri.to_owned(),
                name: self.name.clone(),
            })?;

        assert_eq!(job.nexus_name, self.name);
        Ok(job)
    }

    /// On rebuild job completion it updates the child and the nexus
    /// based on the rebuild job's final state
    async fn on_rebuild_complete_job(
        mut self: Pin<&mut Self>,
        job: &RebuildJob<'n>,
    ) -> Result<(), Error> {
        let recovering_child = self.as_mut().get_child_by_name(&job.dst_uri)?;

        match job.state() {
            RebuildState::Completed => {
                recovering_child.set_state(ChildState::Open);
                info!(
                    "Child {} has been rebuilt successfully",
                    recovering_child.uri()
                );
                let child_name = recovering_child.uri().to_string();
                let child_state = recovering_child.state();
                self.persist(PersistOp::Update((child_name, child_state)))
                    .await;
            }
            RebuildState::Stopped => {
                info!(
                    "Rebuild job for child {} of nexus {} stopped",
                    &job.dst_uri, &self.name,
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
                    &job.dst_uri,
                    &self.name,
                    job.error_desc(),
                );
            }
            _ => {
                recovering_child.fault(Reason::RebuildFailed).await;
                error!(
                    "Rebuild job for child {} of nexus {} failed with state {:?}",
                    &job.dst_uri,
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
