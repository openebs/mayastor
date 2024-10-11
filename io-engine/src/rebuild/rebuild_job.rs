use std::sync::{Arc, Weak};

use chrono::Utc;
use futures::channel::oneshot;

use super::{
    HistoryRecord,
    RebuildError,
    RebuildJobBackendManager,
    RebuildJobRequest,
    RebuildState,
    RebuildStates,
    RebuildStats,
};
use crate::{
    core::{Reactors, ReadOptions, VerboseError},
    rebuild::{
        rebuild_descriptor::RebuildDescriptor,
        rebuild_job_backend::{RebuildBackend, RebuildJobManager},
    },
};

/// Rebuild I/O verification mode.
#[derive(Debug, Clone, Default)]
pub enum RebuildVerifyMode {
    /// Do not verify rebuild I/Os.
    #[default]
    None,
    /// Fail rebuild job if I/O verification fails.
    Fail,
    /// Panic if I/O verification fails.
    Panic,
}

/// Rebuild job options.
#[derive(Debug, Default)]
pub struct RebuildJobOptions {
    pub verify_mode: RebuildVerifyMode,
    pub read_opts: ReadOptions,
}
impl RebuildJobOptions {
    /// Use the given `ReadOptions`.
    pub fn with_read_opts(mut self, read_opts: ReadOptions) -> Self {
        self.read_opts = read_opts;
        self
    }
}

/// Operations used to control the state of the job.
#[derive(Debug)]
pub(super) enum RebuildOperation {
    /// Client Operations
    ///
    /// Starts the job for the first time
    Start,
    /// Stops the job (eg, child being removed)
    Stop,
    /// Pauses the job
    Pause,
    /// Resumes the previously paused job
    Resume,
    /// Internal Operations
    ///
    /// an IO error has occurred
    Fail,
    /// rebuild completed successfully
    Complete,
}

impl std::fmt::Display for RebuildOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

/// A rebuild job is responsible for managing a rebuild (copy) which reads
/// from source_hdl and writes into destination_hdl from specified start to end.
/// This is a frontend interface that communicates with a backend runner which
/// is the one responsible for the read/writing of the data.
#[derive(Debug)]
pub struct RebuildJob {
    /// Source URI of the healthy child to rebuild from.
    src_uri: String,
    /// Target URI of the out of sync child in need of a rebuild.
    pub(crate) dst_uri: String,
    /// Frontend to backend channel.
    comms: RebuildFBendChan,
    /// Current state of the rebuild job.
    states: Arc<parking_lot::RwLock<RebuildStates>>,
    /// Channel used to Notify rebuild updates when the state changes.
    notify_chan: crossbeam::channel::Receiver<RebuildState>,
    /// Channel used to Notify when rebuild completes.
    complete_chan: Weak<parking_lot::Mutex<Vec<oneshot::Sender<RebuildState>>>>,
}

impl RebuildJob {
    /// Creates a new RebuildJob taking a specific backend implementation and
    /// running the generic backend manager.
    pub(super) async fn from_backend(
        backend: impl RebuildBackend + 'static,
    ) -> Result<Self, RebuildError> {
        let desc = backend.common_desc();
        let src_uri = desc.src_uri.to_string();
        let dst_uri = desc.dst_uri.to_string();
        let manager = RebuildJobBackendManager::new(backend);
        let frontend = Self {
            src_uri,
            dst_uri,
            states: manager.states.clone(),
            comms: RebuildFBendChan::from(&manager.info_chan),
            complete_chan: Arc::downgrade(&manager.complete_chan),
            notify_chan: manager.notify_chan.1.clone(),
        };

        // Kick off the rebuild task where it will "live" and await for
        // commands.
        manager.schedule().await;

        Ok(frontend)
    }

    /// Creates a new RebuildJob taking a specific backend implementation and
    /// running the generic backend manager.
    pub(super) fn from_manager(
        manager: &RebuildJobManager,
        desc: &RebuildDescriptor,
    ) -> Self {
        Self {
            src_uri: desc.src_uri.to_string(),
            dst_uri: desc.dst_uri.to_string(),
            states: manager.states.clone(),
            comms: RebuildFBendChan::from(&manager.info_chan),
            complete_chan: Arc::downgrade(&manager.complete_chan),
            notify_chan: manager.notify_chan.1.clone(),
        }
    }

    /// Schedules the job to start in a future and returns a complete channel
    /// which can be waited on.
    pub async fn start(
        &self,
    ) -> Result<oneshot::Receiver<RebuildState>, RebuildError> {
        self.exec_client_op(RebuildOperation::Start)?;
        self.add_completion_listener()
    }

    /// Stops the job which then triggers the completion hooks.
    pub fn stop(&self) -> Result<(), RebuildError> {
        self.exec_client_op(RebuildOperation::Stop)
    }

    /// Pauses the job which can then be later resumed.
    pub fn pause(&self) -> Result<(), RebuildError> {
        self.exec_client_op(RebuildOperation::Pause)
    }

    /// Resumes a previously paused job
    /// this could be used to mitigate excess load on the source bdev, eg
    /// too much contention with frontend IO.
    pub fn resume(&self) -> Result<(), RebuildError> {
        self.exec_client_op(RebuildOperation::Resume)
    }

    /// Forcefully stops the job, overriding any pending client operation
    /// returns an async channel which can be used to await for termination.
    pub(crate) fn force_stop(
        &self,
    ) -> either::Either<oneshot::Receiver<RebuildState>, RebuildState> {
        self.force_terminate(RebuildOperation::Stop)
    }

    /// Forcefully fails the job, overriding any pending client operation
    /// returns an async channel which can be used to await for termination.
    pub(crate) fn force_fail(
        &self,
    ) -> either::Either<oneshot::Receiver<RebuildState>, RebuildState> {
        self.force_terminate(RebuildOperation::Fail)
    }

    /// Forcefully terminates the job with the given operation, overriding any
    /// pending client operation returns an async channel which can be used
    /// to await for termination.
    fn force_terminate(
        &self,
        op: RebuildOperation,
    ) -> either::Either<oneshot::Receiver<RebuildState>, RebuildState> {
        self.exec_internal_op(op).ok();

        match self.add_completion_listener() {
            Ok(chan) => either::Either::Left(chan),
            Err(_) => either::Either::Right(self.state()),
        }
    }

    /// Get the rebuild stats.
    pub async fn stats(&self) -> RebuildStats {
        let (s, r) = oneshot::channel::<RebuildStats>();
        self.comms.send(RebuildJobRequest::GetStats(s)).await.ok();
        match r.await {
            Ok(stats) => stats,
            Err(_) => match self.final_stats() {
                Some(stats) => {
                    debug!(
                        rebuild.target = self.dst_uri,
                        "Using final rebuild stats: {stats:?}"
                    );

                    stats
                }
                _ => {
                    error!(
                        rebuild.target = self.dst_uri,
                        "Rebuild backend terminated without setting \
                        final rebuild stats"
                    );

                    Default::default()
                }
            },
        }
    }

    /// TODO
    pub(crate) fn history_record(&self) -> Option<HistoryRecord> {
        self.final_stats().map(|final_stats| HistoryRecord {
            child_uri: self.dst_uri.to_string(),
            src_uri: self.src_uri.to_string(),
            final_stats,
            state: self.state(),
            end_time: Utc::now(),
        })
    }

    /// Get the last error.
    pub fn error(&self) -> Option<RebuildError> {
        self.states.read().error.clone()
    }

    /// Get the last error description.
    pub fn error_desc(&self) -> String {
        match self.error() {
            Some(e) => e.verbose(),
            _ => "".to_string(),
        }
    }

    /// Gets the current rebuild state.
    pub fn state(&self) -> RebuildState {
        self.states.read().current
    }

    /// Get a channel to listen on for rebuild notifications.
    pub fn notify_chan(&self) -> crossbeam::channel::Receiver<RebuildState> {
        self.notify_chan.clone()
    }

    /// Get the uri of the rebuild source.
    pub fn src_uri(&self) -> &str {
        &self.src_uri
    }

    /// Get the name of this rebuild job (ie the rebuild target).
    pub fn name(&self) -> &str {
        self.dst_uri()
    }

    /// Get the uri of the rebuild destination.
    pub fn dst_uri(&self) -> &str {
        &self.dst_uri
    }

    /// Get the final rebuild statistics.
    fn final_stats(&self) -> Option<RebuildStats> {
        self.states.read().final_stats().clone()
    }

    /// Client operations are now allowed to skip over previous operations.
    fn exec_client_op(&self, op: RebuildOperation) -> Result<(), RebuildError> {
        self.exec_op(op, false)
    }

    /// Internal operations can bypass previous pending operations.
    fn exec_internal_op(
        &self,
        op: RebuildOperation,
    ) -> Result<(), RebuildError> {
        self.exec_op(op, true)
    }

    /// Single state machine where all operations are handled.
    fn exec_op(
        &self,
        op: RebuildOperation,
        override_pending: bool,
    ) -> Result<(), RebuildError> {
        let wake_up = self.states.write().exec_op(op, override_pending)?;
        if wake_up {
            self.wake_up();
        }
        Ok(())
    }

    fn wake_up(&self) {
        let sender = self.comms.send_clone();
        let dst_uri = self.dst_uri.clone();
        Reactors::master().send_future(async move {
            if let Err(error) = sender.send(RebuildJobRequest::WakeUp).await {
                error!(
                    ?error,
                    rebuild.target = dst_uri,
                    "Failed to wake up rebuild backend, it has been dropped",
                );
            }
        });
    }

    fn add_completion_listener(
        &self,
    ) -> Result<oneshot::Receiver<RebuildState>, RebuildError> {
        let (sender, receiver) = oneshot::channel();
        let list = match self.complete_chan.upgrade() {
            None => Err(RebuildError::BackendGone),
            Some(chan) => Ok(chan),
        }?;
        list.lock().push(sender);
        Ok(receiver)
    }
}

#[derive(Debug)]
struct RebuildFBendChan {
    sender: async_channel::Sender<RebuildJobRequest>,
}
impl RebuildFBendChan {
    /// Forward the given request to the backend job.
    async fn send(&self, req: RebuildJobRequest) -> Result<(), RebuildError> {
        self.sender
            .send(req)
            .await
            .map_err(|_| RebuildError::BackendGone)
    }
    /// Get a clone of the sender channel.
    fn send_clone(&self) -> async_channel::Sender<RebuildJobRequest> {
        self.sender.clone()
    }
}
impl From<&super::RebuildFBendChan> for RebuildFBendChan {
    fn from(value: &super::RebuildFBendChan) -> Self {
        Self {
            sender: value.sender_clone(),
        }
    }
}
