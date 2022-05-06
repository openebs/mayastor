#![warn(missing_docs)]

use std::{cell::UnsafeCell, collections::HashMap};

use crossbeam::channel::unbounded;
use futures::{
    channel::{mpsc, oneshot},
    StreamExt,
};
use once_cell::sync::OnceCell;
use snafu::ResultExt;

use spdk_rs::{
    libspdk::{spdk_get_thread, SPDK_BDEV_LARGE_BUF_MAX_SIZE},
    DmaBuf,
};

use crate::{
    bdev::{device_open, nexus::VerboseError},
    core::{
        BlockDevice,
        BlockDeviceDescriptor,
        BlockDeviceHandle,
        RangeContext,
        Reactors,
        UntypedBdev,
    },
    nexus_uri::bdev_get_name,
};

use super::rebuild_api::*;

/// Global list of rebuild jobs using a static OnceCell
pub(super) struct RebuildInstances {
    inner: UnsafeCell<HashMap<String, Box<RebuildJob>>>,
}

unsafe impl Sync for RebuildInstances {}
unsafe impl Send for RebuildInstances {}

/// Result returned by each segment task worker
/// used to communicate with the management task indicating that the
/// segment task worker is ready to copy another segment
#[derive(Debug, Clone)]
struct TaskResult {
    /// block that was being rebuilt
    blk: u64,
    /// id of the task
    id: usize,
    /// encountered error, if any
    error: Option<RebuildError>,
}

/// Number of concurrent copy tasks per rebuild job
const SEGMENT_TASKS: usize = 16;
/// Size of each segment used by the copy task
pub const SEGMENT_SIZE: u64 = SPDK_BDEV_LARGE_BUF_MAX_SIZE as u64;

/// Each rebuild task needs a unique buffer to read/write from source to target
/// A mpsc channel is used to communicate with the management task
#[derive(Debug)]
struct RebuildTask {
    buffer: DmaBuf,
    sender: mpsc::Sender<TaskResult>,
    error: Option<TaskResult>,
}

/// Pool of rebuild tasks and progress tracking
/// Each task uses a clone of the sender allowing the management task to poll a
/// single receiver
#[derive(Debug)]
pub(super) struct RebuildTasks {
    tasks: Vec<RebuildTask>,

    channel: (mpsc::Sender<TaskResult>, mpsc::Receiver<TaskResult>),
    active: usize,
    total: usize,

    segments_done: u64,
}

/// Checks whether a range is contained within another range
pub trait Within<T> {
    /// True if `self` is contained within `right`, otherwise false
    fn within(&self, right: std::ops::Range<T>) -> bool;
}

impl Within<u64> for std::ops::Range<u64> {
    fn within(&self, right: std::ops::Range<u64>) -> bool {
        // also make sure ranges don't overflow
        self.start < self.end
            && right.start < right.end
            && self.start >= right.start
            && self.end <= right.end
    }
}

impl RebuildJob {
    /// Stores a rebuild job in the rebuild job list
    pub(super) fn store(self) -> Result<(), RebuildError> {
        let rebuild_list = Self::get_instances();

        if rebuild_list.contains_key(&self.destination) {
            Err(RebuildError::JobAlreadyExists {
                job: self.destination,
            })
        } else {
            let _ =
                rebuild_list.insert(self.destination.clone(), Box::new(self));
            Ok(())
        }
    }

    /// Returns a new rebuild job based on the parameters
    #[allow(clippy::same_item_push)]
    pub(super) fn new(
        nexus: &str,
        source: &str,
        destination: &str,
        range: std::ops::Range<u64>,
        notify_fn: fn(String, String) -> (),
    ) -> Result<Self, RebuildError> {
        let src_descriptor = device_open(
            &bdev_get_name(source).context(BdevInvalidUri {
                uri: source.to_string(),
            })?,
            false,
        )
        .map_err(|e| RebuildError::BdevNotFound {
            source: e,
            bdev: source.to_string(),
        })?;

        let dst_descriptor = device_open(
            &bdev_get_name(destination).context(BdevInvalidUri {
                uri: destination.to_string(),
            })?,
            true,
        )
        .map_err(|e| RebuildError::BdevNotFound {
            source: e,
            bdev: destination.to_string(),
        })?;

        let source_hdl = Self::get_io_handle(&*src_descriptor)?;
        let destination_hdl = Self::get_io_handle(&*dst_descriptor)?;

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
            tasks: Vec::new(),
            // only sending one message per channel at a time so we don't need
            // the extra buffer
            channel: mpsc::channel(0),
            active: 0,
            total: SEGMENT_TASKS,
            segments_done: 0,
        };

        for _ in 0 .. tasks.total {
            let copy_buffer = destination_hdl
                .dma_malloc(segment_size_blks * block_size)
                .context(NoCopyBuffer {})?;
            tasks.tasks.push(RebuildTask {
                buffer: copy_buffer,
                sender: tasks.channel.0.clone(),
                error: None,
            });
        }

        let (source, destination, nexus) = (
            source.to_string(),
            destination.to_string(),
            nexus.to_string(),
        );

        let nexus_descriptor = UntypedBdev::open_by_name(&nexus, false)
            .context(BdevNotFound {
                bdev: nexus.to_string(),
            })?;

        Ok(Self {
            nexus,
            nexus_descriptor,
            source,
            destination,
            next: range.start,
            range,
            block_size,
            segment_size_blks,
            task_pool: tasks,
            notify_fn,
            notify_chan: unbounded::<RebuildState>(),
            states: Default::default(),
            complete_chan: Vec::new(),
            error: None,
            src_descriptor,
            dst_descriptor,
        })
    }

    // Runs the management async task that kicks off N rebuild copy tasks and
    // awaits each completion. When any task completes it kicks off another
    // until the bdev is fully rebuilt
    async fn run(&mut self) {
        self.start_all_tasks();
        while self.task_pool.active > 0 {
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
                        self.error = Some(e);
                        break;
                    }
                },
                None => {
                    // all senders have disconnected, out of place termination?
                    error!("Out of place termination with potentially {} active tasks", self.task_pool.active);
                    let _ = self.terminate();
                    break;
                }
            }
        }
        self.reconcile();
    }

    /// Return the size of the segment to be copied.
    fn get_segment_size_blks(&self, blk: u64) -> u64 {
        // Adjust the segments size for the last segment
        if (blk + self.segment_size_blks) > self.range.end {
            return self.range.end - blk;
        }
        self.segment_size_blks
    }

    /// Copies one segment worth of data from source into destination. During
    /// this time the LBA range being copied is locked so that there cannot be
    /// front end I/O to the same LBA range.
    ///
    /// # Safety
    ///
    /// The lock and unlock functions internally reference the RangeContext as a
    /// raw pointer, so rust cannot correctly manage its lifetime. The
    /// RangeContext MUST NOT be dropped until after the lock and unlock have
    /// completed.
    ///
    /// The use of RangeContext here is safe because it is stored on the stack
    /// for the duration of the calls to lock and unlock.
    async fn locked_copy_one(
        &mut self,
        id: usize,
        blk: u64,
    ) -> Result<(), RebuildError> {
        let len = self.get_segment_size_blks(blk);
        // The nexus children have metadata and data partitions, whereas the
        // nexus has a data partition only. Because we are locking the range on
        // the nexus, we need to calculate the offset from the start of the data
        // partition.
        let mut ctx = RangeContext::new(blk - self.range.start, len);
        let ch = self
            .nexus_descriptor
            .get_channel()
            .expect("Failed to get nexus channel");

        // Wait for LBA range to be locked.
        // This prevents other I/Os being issued to this LBA range whilst it is
        // being rebuilt.
        self.nexus_descriptor
            .lock_lba_range(&mut ctx, &ch)
            .await
            .context(RangeLockError {
                blk,
                len,
            })?;

        // Perform the copy
        let result = self.copy_one(id, blk).await;

        // Wait for the LBA range to be unlocked.
        // This allows others I/Os to be issued to this LBA range once again.
        self.nexus_descriptor
            .unlock_lba_range(&mut ctx, &ch)
            .await
            .context(RangeUnLockError {
                blk,
                len,
            })?;

        result
    }

    /// Copies one segment worth of data from source into destination.
    async fn copy_one(
        &mut self,
        id: usize,
        blk: u64,
    ) -> Result<(), RebuildError> {
        let mut copy_buffer: DmaBuf;
        let source_hdl = Self::get_io_handle(&*self.src_descriptor)?;
        let destination_hdl = Self::get_io_handle(&*self.dst_descriptor)?;

        let copy_buffer = if self.get_segment_size_blks(blk)
            == self.segment_size_blks
        {
            &mut self.task_pool.tasks[id].buffer
        } else {
            let segment_size_blks = self.range.end - blk;

            trace!(
                    "Adjusting last segment size from {} to {}. offset: {}, range: {:?}",
                    self.segment_size_blks, segment_size_blks, blk, self.range,
                );

            copy_buffer = destination_hdl
                .dma_malloc(segment_size_blks * self.block_size)
                .context(NoCopyBuffer {})?;

            &mut copy_buffer
        };

        source_hdl
            .read_at(blk * self.block_size, copy_buffer)
            .await
            .context(ReadIoError {
                bdev: &self.source,
            })?;

        destination_hdl
            .write_at(blk * self.block_size, copy_buffer)
            .await
            .context(WriteIoError {
                bdev: &self.destination,
            })?;

        Ok(())
    }

    fn get_io_handle(
        descriptor: &dyn BlockDeviceDescriptor,
    ) -> Result<Box<dyn BlockDeviceHandle>, RebuildError> {
        descriptor
            .get_io_handle()
            .map_err(|e| RebuildError::NoBdevHandle {
                source: e,
                bdev: descriptor.get_device().device_name(),
            })
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
    pub(super) fn get_instances() -> &'static mut HashMap<String, Box<Self>> {
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
    fn stats(&self) -> RebuildStats {
        let blocks_total = self.range.end - self.range.start;

        // segment size may not be aligned to the total size
        let blocks_recovered = std::cmp::min(
            self.task_pool.segments_done * self.segment_size_blks,
            blocks_total,
        );

        let progress = (blocks_recovered * 100) / blocks_total;

        info!(
            "State: {}, Src: {}, Dst: {}, range: {:?}, next: {}, \
             block_size: {}, segment_sz: {}, recovered_blks: {}, progress: {}%",
            self.state(),
            self.source,
            self.destination,
            self.range,
            self.next,
            self.block_size,
            self.segment_size_blks,
            blocks_recovered,
            progress,
        );

        RebuildStats {
            blocks_total,
            blocks_recovered,
            progress,
            segment_size_blks: self.segment_size_blks,
            block_size: self.block_size,
            tasks_total: self.task_pool.total as u64,
            tasks_active: self.task_pool.active as u64,
        }
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
        assert_eq!(
            self.task_pool.active, 0,
            "{} active tasks",
            self.task_pool.active
        );

        for n in 0 .. self.task_pool.total {
            self.next = match self.send_segment_task(n) {
                Some(next) => {
                    self.task_pool.active += 1;
                    next
                }
                None => break, /* we've already got enough tasks to rebuild
                                * the bdev */
            };
        }
    }

    fn start_task_by_id(&mut self, id: usize) {
        match self.send_segment_task(id) {
            Some(next) => {
                self.task_pool.active += 1;
                self.next = next;
            }
            None => {
                if self.task_pool.active == 0 {
                    self.complete();
                }
            }
        };
    }

    async fn await_one_task(&mut self) -> Option<TaskResult> {
        self.task_pool.channel.1.next().await.map(|f| {
            self.task_pool.active -= 1;
            if f.error.is_none() {
                self.task_pool.segments_done += 1;
            } else {
                self.task_pool.tasks[f.id].error = Some(f.clone());
            }
            f
        })
    }

    async fn await_all_tasks(&mut self) {
        debug!(
            "Awaiting all active tasks({}) for rebuild {}",
            self.task_pool.active, self.destination
        );
        while self.task_pool.active > 0 {
            if self.await_one_task().await.is_none() {
                error!("Failed to wait for {} rebuild tasks due mpsc channel failure.", self.task_pool.active);
                self.fail();
                return;
            }
        }
        debug!(
            "Finished awaiting all tasks for rebuild {}",
            self.destination
        );
    }

    /// Sends one segment worth of data in a reactor future and notifies the
    /// management channel. Returns the next segment offset to rebuild, if any
    fn send_segment_task(&self, id: usize) -> Option<u64> {
        if self.next >= self.range.end {
            None
        } else {
            let blk = self.next;
            let next = std::cmp::min(
                self.next + self.segment_size_blks,
                self.range.end,
            );
            let name = self.destination.clone();

            Reactors::current().send_future(async move {
                let job = Self::lookup(&name).unwrap();

                let r = TaskResult {
                    blk,
                    id,
                    error: job.locked_copy_one(id, blk).await.err(),
                };

                let task = &mut job.task_pool.tasks[id];
                if let Err(e) = task.sender.start_send(r) {
                    error!("Failed to notify job of segment id: {} blk: {} completion, err: {}", id, blk, e.verbose());
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
            Some(pending) if !override_pending && (pending != state) => {
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
                S::Init | S::Running | S::Paused => {
                    self.states.set_pending(S::Paused, false)?;
                    Ok(())
                }
            },
            RebuildOperation::Resume => match self.state() {
                S::Init | S::Stopped | S::Failed | S::Completed => Err(e),
                S::Running | S::Paused => {
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
