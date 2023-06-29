use std::{
    collections::HashMap,
    ops::Range,
    sync::{Arc, Weak},
};

use chrono::Utc;
use futures::channel::oneshot;
use once_cell::sync::OnceCell;
use spdk_rs::Thread;

use super::{
    HistoryRecord,
    RebuildError,
    RebuildJobBackend,
    RebuildJobRequest,
    RebuildMap,
    RebuildState,
    RebuildStates,
    RebuildStats,
};
use crate::core::{Reactors, VerboseError};

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
    /// Name of the nexus associated with the rebuild job.
    pub nexus_name: String,
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
    /// Creates a new RebuildJob which rebuilds from source URI to target URI
    /// from start to end (of the data partition); notify_fn callback is called
    /// when the rebuild state is updated - with the nexus and destination
    /// URI as arguments.
    pub async fn new(
        nexus_name: &str,
        src_uri: &str,
        dst_uri: &str,
        range: Range<u64>,
        notify_fn: fn(String, String) -> (),
    ) -> Result<Self, RebuildError> {
        // Allocate an instance of the rebuild back-end.
        let backend = RebuildJobBackend::new(
            nexus_name,
            src_uri,
            dst_uri,
            range.clone(),
            notify_fn,
        )
        .await?;

        let frontend = Self {
            nexus_name: backend.nexus_name.clone(),
            src_uri: backend.src_uri.clone(),
            dst_uri: backend.dst_uri.clone(),
            states: backend.states.clone(),
            comms: RebuildFBendChan::from(&backend.info_chan),
            complete_chan: Arc::downgrade(&backend.complete_chan),
            notify_chan: backend.notify_chan.1.clone(),
        };

        // Kick off the rebuild task where it will "live" and await for
        // commands.
        backend.schedule().await;

        Ok(frontend)
    }

    /// Returns number of all rebuild jobs on the system.
    pub fn count() -> usize {
        Self::get_instances().len()
    }

    /// Lookup a rebuild job by its destination uri then remove and drop it.
    pub fn remove(name: &str) -> Result<Arc<Self>, RebuildError> {
        match Self::get_instances().remove(name) {
            Some(job) => Ok(job),
            None => Err(RebuildError::JobNotFound {
                job: name.to_owned(),
            }),
        }
    }

    /// Stores a rebuild job in the rebuild job list.
    pub fn store(self) -> Result<(), RebuildError> {
        let mut rebuild_list = Self::get_instances();

        if rebuild_list.contains_key(&self.dst_uri) {
            Err(RebuildError::JobAlreadyExists {
                job: self.dst_uri,
            })
        } else {
            let _ = rebuild_list.insert(self.dst_uri.clone(), Arc::new(self));
            Ok(())
        }
    }

    /// Lookup a rebuild job by its destination uri and return it.
    pub fn lookup(dst_uri: &str) -> Result<Arc<Self>, RebuildError> {
        if let Some(job) = Self::get_instances().get(dst_uri) {
            Ok(job.clone())
        } else {
            Err(RebuildError::JobNotFound {
                job: dst_uri.to_owned(),
            })
        }
    }

    /// Lookup all rebuilds jobs with name as its source.
    pub fn lookup_src(src_uri: &str) -> Vec<Arc<Self>> {
        Self::get_instances()
            .iter_mut()
            .filter_map(|j| {
                if j.1.src_uri == src_uri {
                    Some(j.1.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    /// Schedules the job to start in a future and returns a complete channel
    /// which can be waited on.
    pub(crate) async fn start(
        &self,
        map: Option<RebuildMap>,
    ) -> Result<oneshot::Receiver<RebuildState>, RebuildError> {
        if let Some(map) = map {
            let (s, r) = oneshot::channel();
            self.comms
                .send(RebuildJobRequest::SetRebuildMap((map, s)))
                .await
                .ok();
            if let Err(e) = r.await {
                error!(
                    "{uri}: failed to set rebuild map: {e}",
                    uri = self.dst_uri
                );
            }
        }

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
    pub(crate) fn force_stop(&self) -> oneshot::Receiver<RebuildState> {
        self.force_terminate(RebuildOperation::Stop)
    }

    /// Forcefully fails the job, overriding any pending client operation
    /// returns an async channel which can be used to await for termination.
    pub(crate) fn force_fail(&self) -> oneshot::Receiver<RebuildState> {
        self.force_terminate(RebuildOperation::Fail)
    }

    /// Forcefully terminates the job with the given operation, overriding any
    /// pending client operation returns an async channel which can be used
    /// to await for termination.
    fn force_terminate(
        &self,
        op: RebuildOperation,
    ) -> oneshot::Receiver<RebuildState> {
        self.exec_internal_op(op).ok();
        self.add_completion_listener()
            .unwrap_or_else(|_| oneshot::channel().1)
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

    /// Get the uri of the rebuild destination.
    pub fn dst_uri(&self) -> &str {
        &self.dst_uri
    }

    /// Get the final rebuild statistics.
    fn final_stats(&self) -> Option<RebuildStats> {
        self.states.read().final_stats().clone()
    }

    /// Get the rebuild job instances container, we ensure that this can only
    /// ever be called on a properly allocated thread
    fn get_instances<'a>() -> parking_lot::MutexGuard<'a, RebuildJobInstances> {
        assert!(Thread::is_spdk_thread(), "not called from SPDK thread");

        static REBUILD_INSTANCES: OnceCell<
            parking_lot::Mutex<RebuildJobInstances>,
        > = OnceCell::new();

        REBUILD_INSTANCES
            .get_or_init(|| parking_lot::Mutex::new(HashMap::new()))
            .lock()
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

/// List of rebuild jobs indexed by the destination's replica uri.
type RebuildJobInstances = HashMap<String, Arc<RebuildJob>>;

#[derive(Debug, Clone)]
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
