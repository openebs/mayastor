use futures::channel::oneshot::Receiver;
use snafu::ResultExt;
use std::{marker::PhantomData, pin::Pin, sync::Arc};

use super::{
    nexus_err,
    nexus_lookup_mut,
    nexus_persistence::PersistOp,
    ChildSyncState,
    DrEvent,
    Error,
    FaultReason,
    Nexus,
};

use crate::{
    core::{Reactors, VerboseError},
    rebuild::{
        HistoryRecord,
        RebuildError,
        RebuildJob,
        RebuildState,
        RebuildStats,
    },
};

/// Rebuild pause guard ensures rebuild jobs are resumed before it is dropped.
pub(crate) struct RebuildPauseGuard<'a> {
    /// Nexus name.
    nexus_name: String,
    /// Cancelled rebuilding children.
    cancelled: Vec<String>,
    /// Indicates that rebuilds were started.
    restarted: bool,
    /// Nexus life time.
    _a: PhantomData<&'a ()>,
}

impl<'a> Drop for RebuildPauseGuard<'a> {
    fn drop(&mut self) {
        assert!(self.restarted);
    }
}

impl<'a> RebuildPauseGuard<'a> {
    /// Creates a rebuild pause guard for the given children.
    fn new(nexus_name: String, cancelled: Vec<String>) -> Self {
        Self {
            nexus_name,
            cancelled,
            restarted: false,
            _a: Default::default(),
        }
    }

    /// Consumes the rebuild cancel guard and starts rebuilding the children
    /// that previously had their rebuild jobs cancelled, in spite of whether or
    /// not the child was correctly faulted.
    pub(super) async fn resume(mut self) {
        assert!(!self.restarted);
        self.restarted = true;

        if let Some(nexus) = nexus_lookup_mut(&self.nexus_name) {
            nexus.start_rebuild_jobs(&self.cancelled).await;
        } else {
            warn!(
                "Nexus '{}': not found on resume cancelled rebuild jobs",
                self.nexus_name
            );
        }
    }
}

impl<'n> Nexus<'n> {
    /// Starts a rebuild job and returns a receiver channel
    /// which can be used to await the rebuild completion
    pub async fn start_rebuild(
        self: Pin<&mut Self>,
        child_uri: &str,
    ) -> Result<Receiver<RebuildState>, Error> {
        let name = self.name.clone();
        info!("{name}: start rebuild request for {child_uri}");

        // Find a healthy child to rebuild from.
        let src_child_uri = match self
            .children_iter()
            .find(|c| c.is_healthy() && c.uri() != child_uri)
        {
            Some(child) => Ok(child.uri().to_owned()),
            None => Err(Error::NoRebuildSource {
                name: name.clone(),
            }),
        }?;

        let dst_child_uri = match self.lookup_child(child_uri) {
            Some(c) if c.is_opened_unsync() => {
                if c.rebuild_job().is_some() {
                    Err(Error::RebuildJobAlreadyExists {
                        child: child_uri.to_owned(),
                        name: name.clone(),
                    })
                } else {
                    Ok(c.uri().to_owned())
                }
            }
            Some(c) => Err(Error::ChildNotDegraded {
                child: child_uri.to_owned(),
                name: self.name.clone(),
                state: c.state().to_string(),
            }),
            None => Err(Error::ChildNotFound {
                child: child_uri.to_owned(),
                name: name.clone(),
            }),
        }?;

        self.create_rebuild_job(&src_child_uri, &dst_child_uri)
            .await?;

        // We're now rebuilding the `dst_child` which means it HAS to become an
        // active participant in the frontend nexus bdev for Writes.
        // This is because the rebuild job copies from src to target child
        // sequentially, from start to the end.
        // This means any Write frontend IO to a range which has already been
        // rebuilt would then need to be rebuilt again.
        // Ensuring that the dst child receives all frontend Write IO keeps all
        // rebuilt ranges in sync with the other children.
        self.reconfigure(DrEvent::ChildRebuild).await;

        self.rebuild_job_mut(&dst_child_uri)?.start().context(
            nexus_err::RebuildOperation {
                job: child_uri.to_owned(),
                name: name.clone(),
            },
        )
    }

    /// TODO
    async fn create_rebuild_job(
        &self,
        src_child_uri: &str,
        dst_child_uri: &str,
    ) -> Result<(), Error> {
        RebuildJob::new(
            &self.name,
            src_child_uri,
            dst_child_uri,
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
        .await
        .and_then(RebuildJob::store)
        .context(nexus_err::CreateRebuild {
            child: dst_child_uri.to_owned(),
            name: self.name.clone(),
        })
    }

    /// Translates the job into a new history record and pushes into
    /// the history.
    fn create_history_record(mut self: Pin<&mut Self>, job: Arc<RebuildJob>) {
        let Some(rec) = job.history_record() else {
            error!("{self:?}: try to get history record on unfinished job");
            return;
        };

        unsafe {
            self.as_mut().get_unchecked_mut().rebuild_history.push(rec);
        }

        debug!(
            "{self:?}: new rebuild history record for '{dst}'; \
            total {num} records",
            dst = job.dst_uri,
            num = self.rebuild_history.len()
        );
    }

    /// Terminates a rebuild in the background.
    /// Used for shutdown operations and
    /// unlike the client operation stop, this command does not fail
    /// as it overrides the previous client operations.
    async fn terminate_rebuild(self: Pin<&mut Self>, child_uri: &str) {
        // If a rebuild job is not found that's ok
        // as we were just going to remove it anyway.
        if let Ok(rj) = self.rebuild_job_mut(child_uri) {
            let ch = rj.terminate();
            if let Err(e) = ch.await {
                error!(
                    "Failed to wait on rebuild job for child {child_uri} \
                    to terminate with error {}",
                    e.verbose()
                );
            }
        }
    }

    /// Stops a rebuild job in the background.
    pub async fn stop_rebuild(
        self: Pin<&mut Self>,
        dst_uri: &str,
    ) -> Result<(), Error> {
        let name = self.name.clone();
        match self.rebuild_job_mut(dst_uri) {
            Ok(rj) => rj.stop().context(nexus_err::RebuildOperation {
                job: dst_uri.to_owned(),
                name,
            }),
            // If a rebuild task is not found return ok
            // as we were just going to remove it anyway.
            Err(_) => Ok(()),
        }
    }

    /// Pauses a rebuild job in the background.
    pub async fn pause_rebuild(
        self: Pin<&mut Self>,
        dst_uri: &str,
    ) -> Result<(), Error> {
        let name = self.name.clone();
        let rj = self.rebuild_job_mut(dst_uri)?;
        rj.pause().context(nexus_err::RebuildOperation {
            job: dst_uri.to_owned(),
            name,
        })
    }

    /// Resumes a rebuild job in the background.
    pub async fn resume_rebuild(
        self: Pin<&mut Self>,
        dst_uri: &str,
    ) -> Result<(), Error> {
        let name = self.name.clone();
        let rj = self.rebuild_job_mut(dst_uri)?;
        rj.resume().context(nexus_err::RebuildOperation {
            job: dst_uri.to_owned(),
            name,
        })
    }

    /// Returns the state of a rebuild job for the given destination.
    pub fn rebuild_state(&self, dst_uri: &str) -> Result<RebuildState, Error> {
        let rj = self.rebuild_job(dst_uri)?;
        Ok(rj.state())
    }

    /// Return the stats of a rebuild job for the given destination.
    pub(crate) async fn rebuild_stats(
        self: Pin<&mut Self>,
        dst_uri: &str,
    ) -> Result<RebuildStats, Error> {
        let rj = self.rebuild_job(dst_uri)?;
        Ok(rj.stats().await)
    }

    /// Iterate over the replica rebuild history for this nexus
    /// and return it as output.
    pub fn rebuild_history(&self) -> &Vec<HistoryRecord> {
        &self.rebuild_history
    }

    /// Returns the rebuild progress of a rebuild job for the given destination.
    pub(crate) async fn rebuild_progress(
        self: Pin<&mut Self>,
        dst_uri: &str,
    ) -> Result<u32, Error> {
        self.rebuild_stats(dst_uri).await.map(|s| s.progress as u32)
    }

    /// Pauses rebuild jobs, returning rebuild pause guard.
    pub(super) async fn pause_rebuild_jobs<'a>(
        mut self: Pin<&mut Self>,
        src_uri: &str,
    ) -> RebuildPauseGuard<'a> {
        let cancelled = self.as_mut().cancel_rebuild_jobs(src_uri).await;

        RebuildPauseGuard::new(self.nexus_name().to_owned(), cancelled)
    }

    /// Cancels all rebuilds jobs associated with the child.
    /// Returns a list of rebuilding children whose rebuild job was cancelled.
    pub async fn cancel_rebuild_jobs(
        self: Pin<&mut Self>,
        src_uri: &str,
    ) -> Vec<String> {
        info!("{:?}: cancel rebuild jobs from '{}'...", self, src_uri);

        let src_jobs = RebuildJob::lookup_src(src_uri);
        let mut terminated_jobs = Vec::new();
        let mut rebuilding_children = Vec::new();

        // terminate all jobs with the child as a source
        src_jobs.into_iter().for_each(|j| {
            terminated_jobs.push(j.terminate());
            rebuilding_children.push(j.dst_uri.clone());
        });

        // wait for the jobs to complete terminating
        for job in terminated_jobs {
            if let Err(e) = job.await {
                error!(
                    "{:?}: error when waiting for the rebuild job \
                    to terminate: {}",
                    self,
                    e.verbose()
                );
            }
        }

        // terminate the only possible job with the child as a destination
        self.terminate_rebuild(src_uri).await;
        rebuilding_children
    }

    /// Start a rebuild for each of the children.
    /// TODO: how to proceed if no healthy child is found?
    pub async fn start_rebuild_jobs(
        mut self: Pin<&mut Self>,
        child_uris: &[String],
    ) {
        for uri in child_uris {
            if let Err(e) = self.as_mut().start_rebuild(uri).await {
                error!(
                    "{:?}: failed to start rebuild of '{}': {}",
                    self,
                    uri,
                    e.verbose()
                );
            }
        }
    }

    /// Returns rebuild job associated with the destination child URI.
    /// Returns error if no rebuild job associated with it.
    pub(crate) fn rebuild_job(
        &self,
        dst_child_uri: &str,
    ) -> Result<std::sync::Arc<RebuildJob>, Error> {
        RebuildJob::lookup(dst_child_uri).map_err(|_| {
            Error::RebuildJobNotFound {
                child: dst_child_uri.to_owned(),
                name: self.name.to_owned(),
            }
        })
    }

    /// Returns rebuild job associated with the destination child URI.
    /// Returns error if no rebuild job associated with it.
    pub(crate) fn rebuild_job_mut(
        &self,
        dst_child_uri: &str,
    ) -> Result<Arc<RebuildJob>, Error> {
        let name = self.name.clone();
        RebuildJob::lookup(dst_child_uri).map_err(|_| {
            Error::RebuildJobNotFound {
                child: dst_child_uri.to_owned(),
                name,
            }
        })
    }

    /// Returns number of rebuild jobs on all children.
    pub fn count_rebuild_jobs(&self) -> usize {
        self.children_iter().fold(0, |acc, c| {
            if c.rebuild_job().is_some() {
                acc + 1
            } else {
                acc
            }
        })
    }

    /// On rebuild job completion it updates the child and the nexus
    /// based on the rebuild job's final state
    async fn on_rebuild_update(
        mut self: Pin<&mut Self>,
        child_uri: &str,
    ) -> Result<(), Error> {
        let job = self.rebuild_job(child_uri)?;
        let job_state = job.state();
        if !job_state.done() {
            tracing::info!(
                rebuild.target = child_uri,
                rebuild.state = ?job_state,
                "Rebuild update notification"
            );
            // Leave all states as they are
            return Ok(());
        }

        let dst_child = self.as_mut().child_mut(child_uri)?;

        match job_state {
            RebuildState::Completed => {
                dst_child.sync_state = ChildSyncState::Synced;
                if dst_child.is_healthy() {
                    info!("Child {} has been rebuilt successfully", child_uri);
                    let child_uri = child_uri.to_owned();
                    let healthy = dst_child.is_healthy();
                    // todo: shouldn't we do this with the subsystem paused?
                    self.persist(PersistOp::Update {
                        child_uri,
                        healthy,
                    })
                    .await;
                } else {
                    warn!("Child {} has been rebuilt successfully but it's not in the expected state", child_uri);
                }
            }
            RebuildState::Stopped => {
                info!(
                    "Rebuild job for child {} of nexus {} stopped",
                    child_uri, self.name,
                );
            }
            RebuildState::Failed => {
                // rebuild has failed so we need to set the child as faulted
                // allowing the control plane to replace it with another

                if let Some(RebuildError::ReadIoFailed {
                    ..
                }) = job.error()
                {
                    // todo: retry rebuild using another child as source?
                }

                dst_child.close_faulted(FaultReason::RebuildFailed).await;
                error!(
                    "Rebuild job for child {} of nexus {} failed, error: {}",
                    child_uri,
                    &self.name,
                    job.error_desc(),
                );
            }
            _ => {
                dst_child.close_faulted(FaultReason::RebuildFailed).await;
                error!(
                    "Rebuild job for child {} of nexus {} failed with state {:?}",
                    child_uri,
                    &self.name,
                    job_state,
                );
            }
        }

        let dst_child = self.as_mut().child_mut(child_uri)?;
        // TODO: Should this be done only after reconfigure?
        // Reason being if we remove the rebuild job then another rebuild could
        // potentially be triggered even though we haven't reconfigured
        // yet.
        // However in order to do this we'll have to change how rebuilding
        // children are added as WO in the nexus channel reconnection.
        match dst_child.remove_rebuild_job() {
            None => {
                error!("{:?}: inconsistent rebuild job state", dst_child);
                return Ok(());
            }
            Some(job) => {
                self.as_mut().create_history_record(job);
            }
        }

        self.reconfigure(DrEvent::ChildRebuild).await;

        Ok(())
    }

    /// Rebuild updated callback when a rebuild job state updates
    async fn notify_rebuild(nexus: String, job: String) {
        info!("nexus {} received notify_rebuild from job {}", nexus, job);

        if let Some(nexus) = nexus_lookup_mut(&nexus) {
            if let Err(e) = nexus.on_rebuild_update(&job).await {
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
