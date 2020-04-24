#![warn(missing_docs)]

use crate::core::{Bdev, BdevHandle, DmaBuf, Reactors};
use crossbeam::channel::unbounded;
use once_cell::sync::OnceCell;
use snafu::ResultExt;
use spdk_sys::spdk_get_thread;
use std::{cell::UnsafeCell, collections::HashMap};

use futures::{
    channel::{mpsc, oneshot},
    StreamExt,
};

use super::rebuild_api::*;

/// Global list of rebuild jobs using a static OnceCell
pub(super) struct RebuildInstances {
    inner: UnsafeCell<HashMap<String, RebuildJob>>,
}

unsafe impl Sync for RebuildInstances {}
unsafe impl Send for RebuildInstances {}

/// Result returned by each segment task worker
/// used to communicate with the management task indicating that the
/// segment task worker is ready to copy another segment
struct TaskResult {
    /// block that was being rebuilt
    blk: u64,
    /// id of the task
    id: u64,
    /// encountered error, if any
    error: Option<RebuildError>,
}

/// Number of concurrent copy tasks per rebuild job
const SEGMENT_TASKS: u64 = 4;
/// Size of each segment used by the copy task
const SEGMENT_SIZE: u64 = 10 * 1024; // 10KiB

/// Each rebuild task needs a unique buffer to read/write from source to target
/// a mpsc channel is used to communicate with the management task and each
/// task used a clone of the sender allowing the management to poll a single
/// receiver
pub(super) struct RebuildTasks {
    buffers: Vec<DmaBuf>,
    senders: Vec<mpsc::Sender<TaskResult>>,

    channel: (mpsc::Sender<TaskResult>, mpsc::Receiver<TaskResult>),
    active: u64,
    total: u64,
}

impl RebuildJob {
    /// Stores a rebuild job in the rebuild job list
    pub(super) fn store(self: Self) -> Result<(), RebuildError> {
        let rebuild_list = Self::get_instances();

        if rebuild_list.contains_key(&self.destination) {
            Err(RebuildError::JobAlreadyExists {
                job: self.destination,
            })
        } else {
            let _ = rebuild_list.insert(self.destination.clone(), self);
            Ok(())
        }
    }

    /// Returns a new rebuild job based on the parameters
    pub(super) fn new(
        nexus: &str,
        source: &str,
        destination: &str,
        start: u64,
        end: u64,
        notify_fn: fn(String, String) -> (),
    ) -> Result<Self, RebuildError> {
        let source_hdl =
            BdevHandle::open(source, false, false).context(NoBdevHandle {
                bdev: source,
            })?;
        let destination_hdl = BdevHandle::open(destination, true, false)
            .context(NoBdevHandle {
                bdev: destination,
            })?;

        if !Self::validate(&source_hdl.get_bdev(), &destination_hdl.get_bdev())
        {
            return Err(RebuildError::InvalidParameters {});
        };

        // validation passed, block size is the same for both
        let block_size = destination_hdl.get_bdev().block_len() as u64;
        let segment_size_blks = (SEGMENT_SIZE / block_size) as u64;

        let mut tasks = RebuildTasks {
            buffers: Vec::new(),
            senders: Vec::new(),
            // only sending one message per channel at a time so we don't need
            // the extra buffer
            channel: mpsc::channel(0),
            active: 0,
            total: SEGMENT_TASKS,
        };

        for _ in 0 .. tasks.total {
            let copy_buffer = source_hdl
                .dma_malloc((segment_size_blks * block_size) as usize)
                .context(NoCopyBuffer {})?;
            tasks.buffers.push(copy_buffer);
            tasks.senders.push(tasks.channel.0.clone());
        }

        let (source, destination, nexus) = (
            source.to_string(),
            destination.to_string(),
            nexus.to_string(),
        );

        Ok(Self {
            nexus,
            source,
            source_hdl,
            destination,
            destination_hdl,
            start,
            end,
            next: start,
            block_size,
            segment_size_blks,
            tasks,
            notify_fn,
            notify_chan: unbounded::<RebuildState>(),
            states: Default::default(),
            complete_chan: Vec::new(),
        })
    }

    // Runs the management async task that kicks off N rebuild copy tasks and
    // awaits each completion. When any task completes it kicks off another
    // until the bdev is fully rebuilt
    async fn run(&mut self) {
        self.start_all_tasks();
        while self.tasks.active > 0 {
            match self.await_one_task().await {
                Some(r) => match r.error {
                    None => {
                        match self.states.pending {
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
                        self.fail();
                        self.await_all_tasks().await;
                        break;
                    }
                },
                None => {
                    // all senders have disconnected, out of place termination?
                    error!("Out of place termination with potentially {} active tasks", self.tasks.active);
                    let _ = self.terminate();
                    break;
                }
            }
        }
        self.reconcile();
    }

    /// Copies one segment worth of data from source into destination
    async fn copy_one(
        &mut self,
        id: u64,
        blk: u64,
    ) -> Result<(), RebuildError> {
        let mut copy_buffer: DmaBuf;

        let mut copy_buffer = if (blk + self.segment_size_blks) > self.end {
            let segment_size_blks = self.end - blk;

            trace!(
                    "Adjusting last segment size from {} to {}. offset: {}, start: {}, end: {}",
                    self.segment_size_blks, segment_size_blks, blk, self.start, self.end,
                );

            copy_buffer = self
                .source_hdl
                .dma_malloc((segment_size_blks * self.block_size) as usize)
                .context(NoCopyBuffer {})?;

            &mut copy_buffer
        } else {
            &mut self.tasks.buffers[id as usize]
        };

        self.source_hdl
            .read_at(blk * self.block_size, &mut copy_buffer)
            .await
            .context(IoError {
                bdev: &self.source,
            })?;

        self.destination_hdl
            .write_at(blk * self.block_size, &copy_buffer)
            .await
            .context(IoError {
                bdev: &self.destination,
            })?;

        Ok(())
    }

    fn notify(&mut self) {
        self.stats();
        self.send_notify();
    }

    /// Calls the job's registered notify fn callback and notify sender channel
    fn send_notify(&mut self) {
        // should this return a status before we notify the sender channel?
        (self.notify_fn)(self.nexus.clone(), self.destination.clone());
        if let Err(e) = self.notify_chan.0.send(self.state()) {
            error!("Rebuild Job {} of nexus {} failed to send complete via the unbound channel with err {}", self.destination, self.nexus, e);
        }
    }

    /// Check if the source and destination block devices are compatible for
    /// rebuild
    fn validate(source: &Bdev, destination: &Bdev) -> bool {
        !(source.size_in_bytes() != destination.size_in_bytes()
            || source.block_len() != destination.block_len())
    }

    /// reconcile the pending state to the current and clear the pending
    fn reconcile(&mut self) {
        let old = self.state();
        let new = self.states.reconcile();

        if old != new {
            info!(
                "Rebuild job {}: changing state from {:?} to {:?}",
                self.destination, old, new
            );
            self.notify();
        }
    }

    /// reconciles to state if it's the same as the pending value
    fn reconcile_to_state(&mut self, state: RebuildState) -> bool {
        if self.states.pending_equals(state) {
            self.reconcile();
            true
        } else {
            false
        }
    }
    fn schedule(&self) {
        match self.state() {
            RebuildState::Paused | RebuildState::Init => {
                let destination = self.destination.clone();
                Reactors::master().send_future(async move {
                    let job = match RebuildJob::lookup(&destination) {
                        Ok(job) => job,
                        Err(_) => {
                            return error!(
                                "Failed to find and start the rebuild job {}",
                                destination
                            );
                        }
                    };

                    if job.reconcile_to_state(RebuildState::Running) {
                        job.run().await;
                    }
                });
            }
            _ => {}
        }
    }

    /// Get the rebuild job instances container, we ensure that this can only
    /// ever be called on a properly allocated thread
    pub(super) fn get_instances() -> &'static mut HashMap<String, Self> {
        let thread = unsafe { spdk_get_thread() };
        if thread.is_null() {
            panic!("not called from SPDK thread")
        }

        static REBUILD_INSTANCES: OnceCell<RebuildInstances> = OnceCell::new();

        let global_instances =
            REBUILD_INSTANCES.get_or_init(|| RebuildInstances {
                inner: UnsafeCell::new(HashMap::new()),
            });

        unsafe { &mut *global_instances.inner.get() }
    }
}

#[derive(Debug)]
/// Operations used to control the state of the job
enum RebuildOperation {
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
        write!(f, "{:?}", self)
    }
}

impl ClientOperations for RebuildJob {
    fn stats(&self) -> Option<RebuildStats> {
        info!(
            "State: {:#}, Src: {}, Dst: {}, start: {}, end: {}, next: {}, block: {}",
            self.state(), self.source, self.destination,
            self.start, self.end, self.next, self.block_size
        );

        None
    }

    fn start(
        &mut self,
    ) -> Result<oneshot::Receiver<RebuildState>, RebuildError> {
        self.exec_client_op(RebuildOperation::Start)?;
        let end_channel = oneshot::channel();
        self.complete_chan.push(end_channel.0);
        Ok(end_channel.1)
    }

    fn stop(&mut self) -> Result<(), RebuildError> {
        self.exec_client_op(RebuildOperation::Stop)
    }

    fn pause(&mut self) -> Result<(), RebuildError> {
        self.exec_client_op(RebuildOperation::Pause)
    }

    fn resume(&mut self) -> Result<(), RebuildError> {
        self.exec_client_op(RebuildOperation::Resume)
    }

    fn terminate(&mut self) -> oneshot::Receiver<RebuildState> {
        self.exec_internal_op(RebuildOperation::Stop).ok();
        let end_channel = oneshot::channel();
        self.complete_chan.push(end_channel.0);
        end_channel.1
    }
}

/// Internal facing operations on a Rebuild Job
trait InternalOperations {
    /// Fails the job, overriding any pending client operation
    fn fail(&mut self);
    /// Completes the job, overriding any pending operation
    fn complete(&mut self);
}

impl InternalOperations for RebuildJob {
    fn fail(&mut self) {
        self.exec_internal_op(RebuildOperation::Fail).ok();
    }

    fn complete(&mut self) {
        self.exec_internal_op(RebuildOperation::Complete).ok();
    }
}

impl RebuildJob {
    fn start_all_tasks(&mut self) {
        assert_eq!(self.tasks.active, 0, "{} active tasks", self.tasks.active);

        for n in 0 .. self.tasks.total {
            self.next = match self.send_segment_task(n) {
                Some(next) => {
                    self.tasks.active += 1;
                    next
                }
                None => break, /* we've already got enough tasks to rebuild
                                * the bdev */
            };
        }
    }

    fn start_task_by_id(&mut self, id: u64) {
        match self.send_segment_task(id) {
            Some(next) => {
                self.tasks.active += 1;
                self.next = next;
            }
            None => {
                if self.tasks.active == 0 {
                    self.complete();
                }
            }
        };
    }

    async fn await_one_task(&mut self) -> Option<TaskResult> {
        self.tasks.channel.1.next().await.map(|f| {
            self.tasks.active -= 1;
            f
        })
    }

    async fn await_all_tasks(&mut self) {
        while self.await_one_task().await.is_some() {
            if self.tasks.active == 0 {
                break;
            }
        }
    }

    /// Sends one segment worth of data in a reactor future and notifies the
    /// management channel. Returns the next segment offset to rebuild, if any
    fn send_segment_task(&self, id: u64) -> Option<u64> {
        if self.next >= self.end {
            None
        } else {
            let blk = self.next;
            let next =
                std::cmp::min(self.next + self.segment_size_blks, self.end);
            let name = self.destination.clone();

            Reactors::current().send_future(async move {
                let job = Self::lookup(&name).unwrap();

                let r = TaskResult {
                    blk,
                    id,
                    error: job.copy_one(id, blk).await.err(),
                };

                if let Err(e) = job.tasks.senders[id as usize].start_send(r) {
                    error!("Failed to notify job of segment id: {} blk: {} completion, err: {}", id, blk, e);
                }
            });

            Some(next)
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct RebuildStates {
    /// Current state of the rebuild job
    pub current: RebuildState,

    /// Pending state for the rebuild job
    pending: Option<RebuildState>,
}

impl std::fmt::Display for RebuildStates {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Default for RebuildState {
    fn default() -> Self {
        RebuildState::Init
    }
}

impl RebuildStates {
    /// Set's the next pending state
    /// if one is already set then override only if flag is set
    pub(self) fn set_pending(
        &mut self,
        state: RebuildState,
        override_pending: bool,
    ) -> Result<(), RebuildError> {
        match self.pending {
            Some(pending) if !override_pending => {
                Err(RebuildError::StatePending {
                    state: pending.to_string(),
                })
            }
            _ => {
                if self.current != state {
                    self.pending = Some(state);
                } else {
                    self.pending = None;
                }
                Ok(())
            }
        }
    }

    /// a change to `state` is pending
    fn pending_equals(&self, state: RebuildState) -> bool {
        self.pending == Some(state)
    }

    /// reconcile the pending state into the current state
    fn reconcile(&mut self) -> RebuildState {
        if let Some(pending) = self.pending {
            self.current = pending;
            self.pending = None;
        }

        self.current
    }
}

impl RebuildJob {
    /// Client operations are now allowed to skip over previous operations
    fn exec_client_op(
        &mut self,
        op: RebuildOperation,
    ) -> Result<(), RebuildError> {
        self.exec_op(op, false)
    }
    fn exec_internal_op(
        &mut self,
        op: RebuildOperation,
    ) -> Result<(), RebuildError> {
        self.exec_op(op, true)
    }

    /// Single state machine where all operations are handled
    fn exec_op(
        &mut self,
        op: RebuildOperation,
        override_pending: bool,
    ) -> Result<(), RebuildError> {
        type S = RebuildState;
        let e = RebuildError::OpError {
            operation: op.to_string(),
            state: self.states.to_string(),
        };

        trace!(
            "Executing operation {} with override {}",
            op,
            override_pending
        );

        match op {
            RebuildOperation::Start => {
                match self.state() {
                    // start only allowed when... starting
                    S::Stopped | S::Paused | S::Failed | S::Completed => Err(e),
                    // for idempotence sake
                    S::Running => Ok(()),
                    S::Init => {
                        self.states.set_pending(S::Running, false)?;
                        self.schedule();
                        Ok(())
                    }
                }
            }
            RebuildOperation::Stop => {
                match self.state() {
                    // We're already stopping anyway, so all is well
                    S::Failed | S::Completed => Err(e),
                    // for idempotence sake
                    S::Stopped => Ok(()),
                    S::Running => {
                        self.states
                            .set_pending(S::Stopped, override_pending)?;
                        Ok(())
                    }
                    S::Init | S::Paused => {
                        self.states
                            .set_pending(S::Stopped, override_pending)?;

                        // The rebuild is not running so we need to reconcile
                        self.reconcile();
                        Ok(())
                    }
                }
            }
            RebuildOperation::Pause => match self.state() {
                S::Stopped | S::Failed | S::Completed => Err(e),
                // for idempotence sake
                S::Paused => Ok(()),
                S::Init | S::Running => {
                    self.states.set_pending(S::Paused, false)?;
                    Ok(())
                }
            },
            RebuildOperation::Resume => match self.state() {
                S::Init | S::Stopped | S::Failed | S::Completed => Err(e),
                // for idempotence sake
                S::Running => Ok(()),
                S::Paused => {
                    self.states.set_pending(S::Running, false)?;
                    self.schedule();
                    Ok(())
                }
            },
            RebuildOperation::Fail => match self.state() {
                S::Init | S::Stopped | S::Paused | S::Completed => Err(e),
                // for idempotence sake
                S::Failed => Ok(()),
                S::Running => {
                    self.states.set_pending(S::Failed, override_pending)?;
                    Ok(())
                }
            },
            RebuildOperation::Complete => match self.state() {
                S::Init | S::Paused | S::Stopped | S::Failed | S::Completed => {
                    Err(e)
                }
                S::Running => {
                    self.states.set_pending(S::Completed, override_pending)?;
                    Ok(())
                }
            },
        }
    }
}
