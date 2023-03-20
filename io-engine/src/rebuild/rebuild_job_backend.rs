use crossbeam::channel::{unbounded, Receiver, Sender};
use futures::{
    channel::{mpsc, oneshot},
    FutureExt,
    StreamExt,
};
use snafu::ResultExt;
use std::sync::Arc;

use super::{
    rebuild_error::{BdevInvalidUri, BdevNotFound, NoCopyBuffer},
    RebuildDescriptor,
    RebuildError,
    RebuildState,
    RebuildStates,
    RebuildStats,
    RebuildTask,
    RebuildTasks,
    TaskResult,
    Within,
    SEGMENT_SIZE,
    SEGMENT_TASKS,
};
use crate::{
    bdev::device_open,
    bdev_api::bdev_get_name,
    core::{BlockDevice, Reactors, UntypedBdev},
};

/// Request between frontend and backend.
#[derive(Debug)]
pub(super) enum RebuildJobRequest {
    /// Wake up the rebuild backend to check for latest state information.
    WakeUp,
    /// Get the rebuild stats from the backend.
    Stats(oneshot::Sender<RebuildStats>),
}

/// Channel to share information between frontend and backend.
#[derive(Debug, Clone)]
pub(super) struct RebuildFBendChan {
    sender: async_channel::Sender<RebuildJobRequest>,
    receiver: async_channel::Receiver<RebuildJobRequest>,
}
impl RebuildFBendChan {
    fn new() -> Self {
        let (sender, receiver) = async_channel::unbounded();
        Self {
            sender,
            receiver,
        }
    }
    async fn recv(&mut self) -> Result<RebuildJobRequest, RebuildError> {
        self.receiver
            .recv()
            .await
            .map_err(|_| RebuildError::FrontendGone {})
    }

    /// Get a clone of the receive channel.
    pub(super) fn recv_clone(
        &self,
    ) -> async_channel::Receiver<RebuildJobRequest> {
        self.receiver.clone()
    }
    /// Get a clone of the send channel.
    pub(super) fn sender_clone(
        &self,
    ) -> async_channel::Sender<RebuildJobRequest> {
        self.sender.clone()
    }
}

/// A rebuild job is responsible for managing a rebuild (copy) which reads
/// from source_hdl and writes into destination_hdl from specified start to end.
pub(super) struct RebuildJobBackend {
    /// Name of the nexus associated with the rebuild job.
    pub nexus_name: String,
    /// Source URI of the healthy child to rebuild from.
    pub src_uri: String,
    /// Target URI of the out of sync child in need of a rebuild.
    pub dst_uri: String,
    /// The next block to be rebuilt.
    pub(super) next: u64,
    /// A pool of tasks which perform the actual data rebuild.
    pub(super) task_pool: RebuildTasks,
    /// Notification as a `fn` callback.
    pub(super) notify_fn: fn(String, String) -> (),
    /// Channel used to signal rebuild update.
    pub notify_chan: (Sender<RebuildState>, Receiver<RebuildState>),
    /// Current state of the rebuild job.
    pub(super) states: Arc<parking_lot::RwLock<RebuildStates>>,
    /// Channel list which allows the await of the rebuild.
    pub(super) complete_chan:
        Arc<parking_lot::Mutex<Vec<oneshot::Sender<RebuildState>>>>,
    /// Channel to share information between frontend and backend.
    pub(super) info_chan: RebuildFBendChan,
    /// All the rebuild related descriptors.
    pub(super) descriptor: Arc<RebuildDescriptor>,
}

impl std::fmt::Debug for RebuildJobBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RebuildJob")
            .field("nexus", &self.nexus_name)
            .field("source", &self.src_uri)
            .field("destination", &self.dst_uri)
            .finish()
    }
}

impl RebuildJobBackend {
    /// Creates a new RebuildJob which rebuilds from source URI to target URI
    /// from start to end (of the data partition); notify_fn callback is called
    /// when the rebuild state is updated - with the nexus and destination
    /// URI as arguments.
    pub async fn new(
        nexus_name: &str,
        src_uri: &str,
        dst_uri: &str,
        range: std::ops::Range<u64>,
        notify_fn: fn(String, String) -> (),
    ) -> Result<Self, RebuildError> {
        let src_descriptor = device_open(
            &bdev_get_name(src_uri).context(BdevInvalidUri {
                uri: src_uri.to_string(),
            })?,
            false,
        )
        .map_err(|e| RebuildError::BdevNotFound {
            source: e,
            bdev: src_uri.to_string(),
        })?;

        let dst_descriptor = device_open(
            &bdev_get_name(dst_uri).context(BdevInvalidUri {
                uri: dst_uri.to_string(),
            })?,
            true,
        )
        .map_err(|e| RebuildError::BdevNotFound {
            source: e,
            bdev: dst_uri.to_string(),
        })?;

        let source_hdl = RebuildDescriptor::io_handle(&*src_descriptor).await?;
        let destination_hdl =
            RebuildDescriptor::io_handle(&*dst_descriptor).await?;

        if !Self::validate(
            source_hdl.get_device(),
            destination_hdl.get_device(),
            &range,
        ) {
            return Err(RebuildError::InvalidParameters {});
        };

        // validation passed, block size is the same for both
        let block_size = destination_hdl.get_device().block_len();
        let segment_size_blks = SEGMENT_SIZE / block_size;

        let mut tasks = RebuildTasks {
            tasks: Default::default(),
            // only sending one message per channel at a time so we don't need
            // the extra buffer
            channel: mpsc::channel(0),
            active: 0,
            total: SEGMENT_TASKS,
            segments_done: Default::default(),
        };

        for _ in 0 .. tasks.total {
            let copy_buffer = destination_hdl
                .dma_malloc(segment_size_blks * block_size)
                .context(NoCopyBuffer {})?;
            tasks.push(RebuildTask {
                buffer: copy_buffer,
                sender: tasks.channel.0.clone(),
                error: None,
            });
        }

        let nexus_descriptor = UntypedBdev::open_by_name(nexus_name, false)
            .context(BdevNotFound {
                bdev: nexus_name.to_string(),
            })?;

        Ok(Self {
            nexus_name: nexus_name.to_string(),
            src_uri: src_uri.to_string(),
            dst_uri: dst_uri.to_string(),
            task_pool: tasks,
            next: range.start,
            notify_fn,
            notify_chan: unbounded::<RebuildState>(),
            states: Default::default(),
            complete_chan: Default::default(),
            info_chan: RebuildFBendChan::new(),
            descriptor: Arc::new(RebuildDescriptor {
                src_uri: src_uri.to_string(),
                dst_uri: dst_uri.to_string(),
                range,
                block_size,
                segment_size_blks,
                src_descriptor,
                dst_descriptor,
                nexus_descriptor,
            }),
        })
    }

    /// State of the rebuild job
    fn state(&self) -> RebuildState {
        self.states.read().current
    }

    /// Reply back to the requester with the rebuild statistics.
    async fn reply_stats(
        &mut self,
        requester: oneshot::Sender<RebuildStats>,
    ) -> Result<(), RebuildStats> {
        requester.send(self.stats())?;
        Ok(())
    }

    /// Moves the rebuild job runner and runs until completion.
    pub(super) async fn schedule(self) {
        let mut job = self;
        Reactors::master().send_future(async move { job.run().await });
    }

    /// Runs the management async task and listens for requests from the
    /// frontend side of the rebuild, example: get statistics.
    async fn run(&mut self) {
        while !self.reconcile().done() {
            if !self.state().running() {
                match self.info_chan.recv().await {
                    Ok(RebuildJobRequest::WakeUp) => {}
                    Ok(RebuildJobRequest::Stats(reply)) => {
                        self.reply_stats(reply).await.ok();
                    }
                    Err(error) => {
                        self.fail_with(error);
                    }
                }
                continue;
            }

            self.start_all_tasks();

            let mut recv = self.info_chan.recv_clone();
            while self.task_pool.running() {
                futures::select! {
                    message = recv.next() => match message {
                        Some(RebuildJobRequest::WakeUp) => { }
                        Some(RebuildJobRequest::Stats(reply)) => {
                            self.reply_stats(reply).await.ok();
                        }
                        None => {
                            // The frontend is gone (dropped), this should not happen, but let's
                            // be defensive and simply cancel the rebuild.
                            self.fail_with(RebuildError::FrontendGone);
                            self.manage_tasks().await;
                            break;
                        }
                    },
                    _ = self.manage_tasks().fuse() => {},
                }
            }
        }
    }

    /// Runs the management async task that kicks off N rebuild copy tasks and
    /// awaits each completion. When any task completes it kicks off another
    /// until the destination is fully rebuilt.
    async fn manage_tasks(&mut self) {
        while self.task_pool.active > 0 {
            match self.await_one_task().await {
                Some(r) => match r.error {
                    None => {
                        let state = self.states.read().clone();
                        match state.pending {
                            None | Some(RebuildState::Running) => {
                                self.start_task_by_id(r.id);
                            }
                            _ => {
                                // await all active tasks as we might still have
                                // ongoing IO. do we need a timeout?
                                self.await_all_tasks().await;
                                break;
                            }
                        }
                    }
                    Some(e) => {
                        error!("Failed to rebuild segment id {} block {} with error: {}", r.id, r.blk, e);
                        self.fail_with(e);
                        self.await_all_tasks().await;
                        break;
                    }
                },
                None => {
                    // all senders have disconnected, out of place termination?
                    self.task_sync_fail();
                    break;
                }
            }
        }
    }

    /// Log the statistics and send a notification to the listeners.
    fn notify(&mut self) {
        self.stats();
        self.send_notify();
    }

    /// Calls the job's registered notify fn callback and notify sender channel
    fn send_notify(&mut self) {
        // should this return a status before we notify the sender channel?
        (self.notify_fn)(self.nexus_name.clone(), self.dst_uri.clone());
        if let Err(e) = self.notify_chan.0.send(self.state()) {
            error!("Rebuild Job {} of nexus {} failed to send complete via the unbound channel with err {}", self.dst_uri, self.nexus_name, e);
        }
    }

    /// Check if the source and destination block devices are compatible for
    /// rebuild
    fn validate(
        source: &dyn BlockDevice,
        destination: &dyn BlockDevice,
        range: &std::ops::Range<u64>,
    ) -> bool {
        // todo: make sure we don't overwrite the labels
        let data_partition_start = 0;
        range.within(data_partition_start .. source.num_blocks())
            && range.within(data_partition_start .. destination.num_blocks())
            && source.block_len() == destination.block_len()
    }

    /// Reconciles the pending state to the current and clear the pending.
    fn reconcile(&mut self) -> RebuildState {
        let (old, new) = {
            let mut state = self.states.write();
            let old = state.current;
            let new = state.reconcile();
            (old, new)
        };

        if old != new {
            info!(
                "Rebuild job {}: changing state from {:?} to {:?}",
                self.dst_uri, old, new
            );
            self.notify();
        }

        new
    }

    /// Collects statistics from the job
    pub fn stats(&self) -> RebuildStats {
        let blocks_total =
            self.descriptor.range.end - self.descriptor.range.start;

        // segment size may not be aligned to the total size
        let blocks_recovered = std::cmp::min(
            self.task_pool.segments_done * self.descriptor.segment_size_blks,
            blocks_total,
        );

        let progress = (blocks_recovered * 100) / blocks_total;

        info!(
            "State: {}, Nexus: {}, Src: {}, Dst: {}, range: {:?}, next: {}, \
            block_size: {}, segment_sz: {}, recovered_blks: {}, progress: {}%, TaskPool: {:?}",
            self.state(),
            self.nexus_name,
            self.src_uri,
            self.dst_uri,
            self.descriptor.range,
            self.next,
            self.descriptor.block_size,
            self.descriptor.segment_size_blks,
            blocks_recovered,
            progress,
            self.task_pool,
        );

        RebuildStats {
            blocks_total,
            blocks_recovered,
            progress,
            segment_size_blks: self.descriptor.segment_size_blks,
            block_size: self.descriptor.block_size,
            tasks_total: self.task_pool.total as u64,
            tasks_active: self.task_pool.active as u64,
        }
    }

    /// Fails the job, overriding any pending client operation
    fn fail(&self) {
        self.exec_internal_op(super::RebuildOperation::Fail).ok();
    }

    /// Fails the job, with the given error.
    fn fail_with<E: Into<Option<RebuildError>>>(&mut self, error: E) {
        self.fail();
        self.states.write().error = error.into();
    }

    fn task_sync_fail(&mut self) {
        let active = self.task_pool.active;
        error!(
            "Failed to wait for {} rebuild tasks due to task channel failure",
            active
        );
        self.fail_with(RebuildError::RebuildTasksChannel {
            active,
        });
    }

    /// Completes the job, overriding any pending operation
    fn complete(&self) {
        self.exec_internal_op(super::RebuildOperation::Complete)
            .ok();
    }

    /// Internal operations can bypass previous pending operations.
    fn exec_internal_op(
        &self,
        op: super::RebuildOperation,
    ) -> Result<bool, RebuildError> {
        self.states.write().exec_op(op, true)
    }

    /// Kicks off all rebuild tasks in the background, or as many as necessary
    /// to complete the rebuild.
    fn start_all_tasks(&mut self) {
        assert_eq!(
            self.task_pool.active, 0,
            "{} active tasks",
            self.task_pool.active
        );

        for n in 0 .. self.task_pool.total {
            if !self.start_task_by_id(n) {
                break;
            }
        }
        // Nothing to rebuild, in case we paused but the rebuild is complete
        if self.task_pool.active == 0 {
            self.complete();
        }

        self.stats();
    }

    /// Tries to kick off a task by its identifier and returns result.
    /// todo: there's no need to use id's, just use a task from the pool.
    fn start_task_by_id(&mut self, id: usize) -> bool {
        match self.send_segment_task(id) {
            Some(next) => {
                self.task_pool.active += 1;
                self.next = next;
                true
            }
            // we've already got enough tasks to rebuild the destination
            None => {
                if self.task_pool.active == 0 {
                    self.complete();
                }
                false
            }
        }
    }

    /// Awaits for one rebuild task to complete and collect the task's result.
    async fn await_one_task(&mut self) -> Option<TaskResult> {
        self.task_pool.await_one_task().await
    }

    /// Awaits for all active rebuild tasks to complete.
    async fn await_all_tasks(&mut self) {
        debug!(
            "Awaiting all active tasks({}) for rebuild {}",
            self.task_pool.active, self.dst_uri
        );

        while self.task_pool.active > 0 {
            if self.await_one_task().await.is_none() {
                // this should never happen, but just in case..
                self.task_sync_fail();
                return;
            }
        }
        debug!("Finished awaiting all tasks for rebuild {}", self.dst_uri);
    }

    /// Sends one segment worth of data in a reactor future and notifies the
    /// management channel. Returns the next segment offset to rebuild, if any.
    fn send_segment_task(&mut self, id: usize) -> Option<u64> {
        if self.next >= self.descriptor.range.end {
            None
        } else {
            let blk = self.next;
            let next = std::cmp::min(
                self.next + self.descriptor.segment_size_blks,
                self.descriptor.range.end,
            );

            self.task_pool
                .send_segment(id, blk, self.descriptor.clone());

            Some(next)
        }
    }
}

impl Drop for RebuildJobBackend {
    fn drop(&mut self) {
        let stats = self.stats();
        self.states.write().set_final_stats(stats);

        tracing::info!(
            rebuild.target = self.dst_uri,
            "RebuildJobBackend being dropped with done({})",
            self.state().done()
        );
    }
}
