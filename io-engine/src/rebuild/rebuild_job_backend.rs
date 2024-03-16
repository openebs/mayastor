use std::{
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use crossbeam::channel::{unbounded, Receiver, Sender};
use futures::{channel::oneshot, FutureExt, StreamExt};

use super::{
    RebuildDescriptor,
    RebuildError,
    RebuildState,
    RebuildStates,
    RebuildStats,
    RebuildTasks,
    TaskResult,
};

use crate::core::Reactors;

/// Request between frontend and backend.
#[derive(Debug)]
pub(super) enum RebuildJobRequest {
    /// Wake up the rebuild backend to check for latest state information.
    WakeUp,
    /// Get the rebuild stats from the backend.
    GetStats(oneshot::Sender<RebuildStats>),
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
    async fn recv(&mut self) -> Option<RebuildJobRequest> {
        self.receiver.recv().await.ok()
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

/// Shared interface for different implementations of the rebuild.
/// A rebuild backend must implement this trait allowing it to
/// be used by the `RebuildJobManager`.
#[async_trait::async_trait(?Send)]
pub(super) trait RebuildBackend:
    std::fmt::Debug + std::fmt::Display
{
    /// Callback for rebuild state change notifications.
    fn on_state_change(&mut self);

    /// Get a reference to the common rebuild descriptor.
    fn common_desc(&self) -> &RebuildDescriptor;

    /// Get the remaining blocks we have yet to be rebuilt.
    fn blocks_remaining(&self) -> u64;
    /// Check if this is a partial rebuild.
    fn is_partial(&self) -> bool;

    /// Get a reference to the tasks pool.
    fn task_pool(&self) -> &RebuildTasks;
    /// Schedule new work on the given task by its id.
    /// Returns false if no further work is required.
    fn schedule_task_by_id(&mut self, id: usize) -> bool;
    /// Wait for the completion of a task and get the result.
    /// Each task's completion must be awaited, to ensure that no in-progress IO
    /// remains when we complete a rebuild.
    async fn await_one_task(&mut self) -> Option<TaskResult>;
}

/// A rebuild job is responsible for managing a rebuild (copy) which reads
/// from source_hdl and writes into destination_hdl from specified start to end.
pub(super) struct RebuildJobManager {
    /// Channel used to signal rebuild update.
    pub notify_chan: (Sender<RebuildState>, Receiver<RebuildState>),
    /// Current state of the rebuild job.
    pub(super) states: Arc<parking_lot::RwLock<RebuildStates>>,
    /// Channel list which allows the await of the rebuild.
    pub(super) complete_chan:
        Arc<parking_lot::Mutex<Vec<oneshot::Sender<RebuildState>>>>,
    /// Channel to share information between frontend and backend.
    pub(super) info_chan: RebuildFBendChan,
    /// Job serial number.
    serial: u64,
}

/// A rebuild job is responsible for managing a rebuild (copy) which reads
/// from source_hdl and writes into destination_hdl from specified start to end.
pub(super) struct RebuildJobBackendManager {
    manager: RebuildJobManager,
    /// The rebuild backend runner which implements the `RebuildBackend` and
    /// performs a specific type of rebuild copy.
    backend: Box<dyn RebuildBackend>,
}

impl Deref for RebuildJobBackendManager {
    type Target = RebuildJobManager;

    fn deref(&self) -> &Self::Target {
        &self.manager
    }
}
impl DerefMut for RebuildJobBackendManager {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.manager
    }
}

impl std::fmt::Debug for RebuildJobBackendManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RebuildJob")
            .field("backend", &self.backend)
            .field("serial", &self.serial)
            .finish()
    }
}

impl std::fmt::Display for RebuildJobBackendManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Rebuild job #{s} ({state}{done}) '{src}' -> '{dst}' {back}",
            s = self.serial,
            state = self.state(),
            done = if self.state().done() { ": done" } else { "" },
            src = self.backend.common_desc().src_uri,
            dst = self.backend.common_desc().dst_uri,
            back = self.backend,
        )
    }
}

impl RebuildJobManager {
    pub fn new() -> Self {
        // Job serial numbers.
        static SERIAL: AtomicU64 = AtomicU64::new(1);

        let serial = SERIAL.fetch_add(1, Ordering::SeqCst);
        Self {
            notify_chan: unbounded::<RebuildState>(),
            states: Default::default(),
            complete_chan: Default::default(),
            info_chan: RebuildFBendChan::new(),
            serial,
        }
    }
    pub fn into_backend(
        self,
        backend: impl RebuildBackend + 'static,
    ) -> RebuildJobBackendManager {
        RebuildJobBackendManager {
            manager: self,
            backend: Box::new(backend),
        }
    }
}

impl RebuildJobBackendManager {
    /// Creates a new RebuildJob which rebuilds from source URI to target URI
    /// from start to end (of the data partition); notify_fn callback is called
    /// when the rebuild state is updated - with the nexus and destination
    /// URI as arguments.
    pub fn new(backend: impl RebuildBackend + 'static) -> Self {
        let be = Self {
            manager: RebuildJobManager::new(),
            backend: Box::new(backend),
        };
        info!("{be}: backend created");
        be
    }

    /// Moves the rebuild job manager and runs until completion.
    pub(super) async fn schedule(self) {
        let mut job = self;
        Reactors::master().send_future(async move { job.run().await });
    }

    /// Runs the management async task and listens for requests from the
    /// frontend side of the rebuild, example: get statistics.
    async fn run(&mut self) {
        while !self.reconcile().done() {
            if !self.state().running() {
                let message = self.info_chan.recv().await;
                self.handle_message(message).await;
                continue;
            }

            // todo: is there a bug here if we fail above?
            self.start_all_tasks();

            let mut recv = self.info_chan.recv_clone();
            while self.task_pool().running() {
                futures::select! {
                    message = recv.next() => if !self.handle_message(message).await {
                        // The frontend is gone (dropped), this should not happen, but let's
                        // be defensive and simply cancel the rebuild.
                        self.manage_tasks().await;
                        break;
                    },
                    _ = self.manage_tasks().fuse() => {},
                }
            }
        }
    }

    /// State Management

    /// Reconciles the pending state to the current and clear the pending.
    fn reconcile(&mut self) -> RebuildState {
        let (old, new) = {
            let mut state = self.states.write();
            let old = state.current;
            let new = state.reconcile();
            (old, new)
        };

        if old != new {
            // Log the statistics and send a notification to the listeners.
            let s = self.stats();
            info!(
                "{self}: changing state from {old:?} to {new:?}; \
                current stats: {s:?}"
            );
            self.on_state_change();
        }

        new
    }
    /// Calls the job's registered notify fn callback and notify sender channel
    fn on_state_change(&mut self) {
        self.backend.on_state_change();

        if let Err(e) = self.notify_chan.0.send(self.state()) {
            error!(
                "{self}: failed to send complete via the unbound channel \
                with error: {e}"
            );
        }
    }

    /// State of the rebuild job
    fn state(&self) -> RebuildState {
        self.states.read().current
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

    /// Generic Rebuild Statistics

    /// Collects generic statistics from the job.
    pub fn stats(&self) -> RebuildStats {
        let descriptor = self.backend.common_desc();
        let blocks_total = descriptor.range.end - descriptor.range.start;

        // segment size may not be aligned to the total size
        let blocks_recovered = std::cmp::min(
            self.task_pool().segments_done * descriptor.segment_size_blks,
            blocks_total,
        );

        let blocks_transferred = std::cmp::min(
            self.task_pool().segments_transferred
                * descriptor.segment_size_blks,
            blocks_total,
        );

        let blocks_remaining = self.backend.blocks_remaining();

        let progress = (blocks_recovered * 100) / blocks_total;
        assert!(
            progress < 100 || blocks_remaining == 0,
            "progress is {}% but there are {} blocks remaining",
            progress,
            blocks_remaining
        );

        RebuildStats {
            start_time: descriptor.start_time,
            is_partial: self.backend.is_partial(),
            blocks_total,
            blocks_recovered,
            blocks_transferred,
            blocks_remaining,
            progress,
            blocks_per_task: descriptor.segment_size_blks,
            block_size: descriptor.block_size,
            tasks_total: self.task_pool().total as u64,
            tasks_active: self.task_pool().active as u64,
            end_time: None,
        }
    }

    /// Reply back to the requester with the generic rebuild stats.
    async fn reply_stats(
        &mut self,
        requester: oneshot::Sender<RebuildStats>,
    ) -> Result<(), RebuildStats> {
        let s = self.stats();
        trace!("{self}: current stats: {s:?}");
        requester.send(s)?;
        Ok(())
    }

    /// Rebuild Tasks Management

    fn task_sync_fail(&mut self) {
        let active = self.task_pool().active;
        error!(
            "{self}: failed to wait for {active} rebuild tasks \
            due to task channel failure"
        );
        self.fail_with(RebuildError::RebuildTasksChannel {
            active,
        });
    }
    fn task_pool(&self) -> &RebuildTasks {
        self.backend.task_pool()
    }

    /// Kicks off all rebuild tasks in the background, or as many as necessary
    /// to complete the rebuild.
    fn start_all_tasks(&mut self) {
        assert_eq!(
            self.task_pool().active,
            0,
            "{} active tasks",
            self.task_pool().active
        );

        for n in 0 .. self.task_pool().total {
            if !self.start_task_by_id(n) {
                break;
            }
        }

        // Nothing to rebuild, in case we paused but the rebuild is complete
        if self.task_pool().active == 0 {
            self.complete();
        }

        let s = self.stats();
        debug!("{self}: started all tasks; current stats: {s:?}");
    }

    /// Tries to kick off a task by its identifier and returns result.
    /// todo: there's no need to use id's, just use a task from the pool.
    fn start_task_by_id(&mut self, id: usize) -> bool {
        if !self.backend.schedule_task_by_id(id) {
            if self.task_pool().active == 0 {
                self.complete();
            }
            false
        } else {
            true
        }
    }

    /// Awaits for one rebuild task to complete and collect the task's result.
    async fn await_one_task(&mut self) -> Option<TaskResult> {
        self.backend.await_one_task().await
    }

    /// Awaits for all active rebuild tasks to complete.
    async fn await_all_tasks(&mut self) {
        debug!(
            "{self}: awaiting all active tasks ({})",
            self.task_pool().active
        );

        while self.task_pool().active > 0 {
            if self.await_one_task().await.is_none() {
                // this should never happen, but just in case..
                self.task_sync_fail();
                return;
            }
        }

        debug!("{self}: finished awaiting all tasks");
    }

    /// Runs the management async task which kicks off N rebuild copy tasks and
    /// awaits each completion.
    /// When any task completes, it kicks off another until the destination is
    /// fully rebuilt.
    async fn manage_tasks(&mut self) {
        while self.task_pool().active > 0 {
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
                        error!(
                            "{self}: failed to rebuild segment \
                            id={sid} block={blk} with error: {e}",
                            sid = r.id,
                            blk = r.blk
                        );
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

    /// Handles a request messages replying to it if necessary.
    /// Returns false if the message was empty (ie the frontend is gone)
    async fn handle_message(
        &mut self,
        message: Option<RebuildJobRequest>,
    ) -> bool {
        match message {
            Some(RebuildJobRequest::WakeUp) => {}
            Some(RebuildJobRequest::GetStats(reply)) => {
                self.reply_stats(reply).await.ok();
            }
            None => {
                self.fail_with(RebuildError::FrontendGone);
                return false;
            }
        }
        true
    }
}

impl Drop for RebuildJobBackendManager {
    fn drop(&mut self) {
        let stats = self.stats();
        info!("{self}: backend dropped; final stats: {stats:?}");
        self.states.write().set_final_stats(stats);
        for sender in self.complete_chan.lock().drain(..) {
            sender.send(self.state()).ok();
        }
    }
}
