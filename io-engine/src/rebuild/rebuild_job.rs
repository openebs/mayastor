use std::{cell::UnsafeCell, collections::HashMap, fmt};

use crossbeam::channel::{unbounded, Receiver, Sender};

use futures::channel::{mpsc, oneshot};
use once_cell::sync::OnceCell;
use snafu::ResultExt;

use spdk_rs::{DmaBuf, LbaRange, Thread};

use crate::{
    bdev::{device_open, nexus::VerboseError, Nexus},
    core::{
        Bdev,
        BlockDevice,
        BlockDeviceDescriptor,
        BlockDeviceHandle,
        DescriptorGuard,
        Reactors,
    },
    nexus_uri::bdev_get_name,
};

use super::{
    rebuild_error::*,
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

/// Global list of rebuild jobs using a static OnceCell
pub(super) struct RebuildInstances {
    inner: UnsafeCell<HashMap<String, Box<RebuildJob<'static>>>>,
}

unsafe impl Sync for RebuildInstances {}
unsafe impl Send for RebuildInstances {}

/// A rebuild job is responsible for managing a rebuild (copy) which reads
/// from source_hdl and writes into destination_hdl from specified start to end
pub struct RebuildJob<'n> {
    /// name of the nexus associated with the rebuild job
    pub nexus_name: String,
    /// descriptor for the nexus
    pub(super) nexus_descriptor: DescriptorGuard<Nexus<'n>>,
    /// source URI of the healthy child to rebuild from
    pub src_uri: String,
    /// target URI of the out of sync child in need of a rebuild
    pub dst_uri: String,
    /// TODO
    pub(super) block_size: u64,
    /// TODO
    pub(super) range: std::ops::Range<u64>,
    /// TODO
    pub(super) next: u64,
    /// TODO
    pub(super) segment_size_blks: u64,
    /// TODO
    pub(super) task_pool: RebuildTasks,
    /// TODO
    pub(super) notify_fn: fn(String, String) -> (),
    /// channel used to signal rebuild update
    pub notify_chan: (Sender<RebuildState>, Receiver<RebuildState>),
    /// current state of the rebuild job
    pub(super) states: RebuildStates,
    /// channel list which allows the await of the rebuild
    pub(super) complete_chan: Vec<oneshot::Sender<RebuildState>>,
    /// rebuild copy error, if any
    pub error: Option<RebuildError>,

    // Pre-opened descriptors for source/destination block device.
    pub(super) src_descriptor: Box<dyn BlockDeviceDescriptor>,
    pub(super) dst_descriptor: Box<dyn BlockDeviceDescriptor>,
}

// TODO: is `RebuildJob` really a Send type?
unsafe impl Send for RebuildJob<'_> {}

impl<'n> fmt::Debug for RebuildJob<'n> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RebuildJob")
            .field("nexus", &self.nexus_name)
            .field("source", &self.src_uri)
            .field("destination", &self.dst_uri)
            .finish()
    }
}

impl<'n> RebuildJob<'n> {
    /// Get the rebuild job instances container, we ensure that this can only
    /// ever be called on a properly allocated thread
    fn get_instances() -> &'static mut HashMap<String, Box<RebuildJob<'static>>>
    {
        if !Thread::is_spdk_thread() {
            panic!("not called from SPDK thread")
        }

        static REBUILD_INSTANCES: OnceCell<RebuildInstances> = OnceCell::new();

        let global_instances =
            REBUILD_INSTANCES.get_or_init(|| RebuildInstances {
                inner: UnsafeCell::new(HashMap::new()),
            });

        unsafe { &mut *global_instances.inner.get() }
    }

    /// Stores a rebuild job in the rebuild job list
    fn store(self) -> Result<(), RebuildError> {
        let rebuild_list = Self::get_instances();

        if rebuild_list.contains_key(&self.dst_uri) {
            Err(RebuildError::JobAlreadyExists {
                job: self.dst_uri,
            })
        } else {
            let _ = rebuild_list
                .insert(self.dst_uri.clone(), Box::new(self.into_static()));
            Ok(())
        }
    }

    /// TODO
    fn into_static(self) -> RebuildJob<'static> {
        unsafe { std::mem::transmute::<_, RebuildJob<'static>>(self) }
    }

    /// TODO
    fn from_static<'a>(
        job: &mut RebuildJob<'static>,
    ) -> &'a mut RebuildJob<'a> {
        unsafe { std::mem::transmute::<_, &'a mut RebuildJob<'a>>(job) }
    }

    /// Creates a new RebuildJob which rebuilds from source URI to target URI
    /// from start to end (of the data partition); notify_fn callback is called
    /// when the rebuild state is updated - with the nexus and destination
    /// URI as arguments
    pub fn create<'a>(
        nexus_name: &str,
        src_uri: &str,
        dst_uri: &'a str,
        range: std::ops::Range<u64>,
        notify_fn: fn(String, String) -> (),
    ) -> Result<&'a mut Self, RebuildError> {
        Self::new(nexus_name, src_uri, dst_uri, range, notify_fn)?.store()?;

        Self::lookup(dst_uri)
    }

    /// Returns a new rebuild job based on the parameters
    #[allow(clippy::same_item_push)]
    fn new(
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

        let nexus_descriptor = Bdev::<Nexus>::open_by_name(nexus_name, false)
            .context(BdevNotFound {
            bdev: nexus_name.to_string(),
        })?;

        Ok(Self {
            nexus_name: nexus_name.to_string(),
            nexus_descriptor,
            src_uri: src_uri.to_string(),
            dst_uri: dst_uri.to_string(),
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

    /// Lookup a rebuild job by its destination uri and return it
    pub fn lookup<'a>(
        dst_uri: &str,
    ) -> Result<&'a mut RebuildJob<'a>, RebuildError> {
        if let Some(job) = Self::get_instances().get_mut(dst_uri) {
            Ok(Self::from_static(job))
        } else {
            Err(RebuildError::JobNotFound {
                job: dst_uri.to_owned(),
            })
        }
    }

    /// Lookup all rebuilds jobs with name as its source.
    pub fn lookup_src<'a>(src_uri: &str) -> Vec<&'a mut RebuildJob<'a>> {
        Self::get_instances()
            .iter_mut()
            .filter_map(|j| {
                if j.1.src_uri == src_uri {
                    Some(Self::from_static(j.1.as_mut()))
                } else {
                    None
                }
            })
            .collect()
    }

    /// Lookup a rebuild job by its destination uri then remove and drop it.
    pub fn remove(name: &str) -> Result<(), RebuildError> {
        match Self::get_instances().remove(name) {
            Some(_) => Ok(()),
            None => Err(RebuildError::JobNotFound {
                job: name.to_owned(),
            }),
        }
    }

    /// Number of rebuild job instances
    pub fn count() -> usize {
        Self::get_instances().len()
    }

    /// State of the rebuild job
    pub fn state(&self) -> RebuildState {
        self.states.current
    }

    /// Error description
    pub fn error_desc(&self) -> String {
        match self.error.as_ref() {
            Some(e) => e.verbose(),
            _ => "".to_string(),
        }
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
        let r = LbaRange::new(blk - self.range.start, len);

        // Wait for LBA range to be locked.
        // This prevents other I/Os being issued to this LBA range whilst it is
        // being rebuilt.
        let lock = self.nexus_descriptor.lock_lba_range(r).await.context(
            RangeLockError {
                blk,
                len,
            },
        )?;

        // Perform the copy
        let result = self.copy_one(id, blk).await;

        // Wait for the LBA range to be unlocked.
        // This allows others I/Os to be issued to this LBA range once again.
        self.nexus_descriptor.unlock_lba_range(lock).await.context(
            RangeUnLockError {
                blk,
                len,
            },
        )?;

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
                bdev: &self.src_uri,
            })?;

        destination_hdl
            .write_at(blk * self.block_size, copy_buffer)
            .await
            .context(WriteIoError {
                bdev: &self.dst_uri,
            })?;

        Ok(())
    }

    /// TODO
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

    /// TODO
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
    fn reconcile(&mut self) {
        let old = self.state();
        let new = self.states.reconcile();

        if old != new {
            info!(
                "Rebuild job {}: changing state from {:?} to {:?}",
                self.dst_uri, old, new
            );
            self.notify();
        }
    }

    /// Reconciles to state if it's the same as the pending value.
    fn reconcile_to_state(&mut self, state: RebuildState) -> bool {
        if self.states.pending_equals(state) {
            self.reconcile();
            true
        } else {
            false
        }
    }

    /// TODO
    fn schedule(&self) {
        match self.state() {
            RebuildState::Paused | RebuildState::Init => {
                let dst_uri = self.dst_uri.clone();
                Reactors::master().send_future(async move {
                    let job = match RebuildJob::lookup(&dst_uri) {
                        Ok(job) => job,
                        Err(_) => {
                            return error!(
                                "Failed to find and start the rebuild job {}",
                                dst_uri
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

    /// Collects statistics from the job
    pub fn stats(&self) -> RebuildStats {
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
            self.src_uri,
            self.dst_uri,
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

    /// Schedules the job to start in a future and returns a complete channel
    /// which can be waited on.
    pub fn start(
        &mut self,
    ) -> Result<oneshot::Receiver<RebuildState>, RebuildError> {
        self.exec_client_op(RebuildOperation::Start)?;
        let end_channel = oneshot::channel();
        self.complete_chan.push(end_channel.0);
        Ok(end_channel.1)
    }

    /// Stops the job which then triggers the completion hooks.
    pub fn stop(&mut self) -> Result<(), RebuildError> {
        self.exec_client_op(RebuildOperation::Stop)
    }

    /// Pauses the job which can then be later resumed.
    pub fn pause(&mut self) -> Result<(), RebuildError> {
        self.exec_client_op(RebuildOperation::Pause)
    }

    /// Resumes a previously paused job
    /// this could be used to mitigate excess load on the source bdev, eg
    /// too much contention with frontend IO.
    pub fn resume(&mut self) -> Result<(), RebuildError> {
        self.exec_client_op(RebuildOperation::Resume)
    }

    /// Forcefully terminates the job, overriding any pending client operation
    /// returns an async channel which can be used to await for termination/
    pub fn terminate(&mut self) -> oneshot::Receiver<RebuildState> {
        self.exec_internal_op(RebuildOperation::Stop).ok();
        let end_channel = oneshot::channel();
        self.complete_chan.push(end_channel.0);
        end_channel.1
    }

    /// Fails the job, overriding any pending client operation
    fn fail(&mut self) {
        self.exec_internal_op(RebuildOperation::Fail).ok();
    }

    /// Completes the job, overriding any pending operation
    fn complete(&mut self) {
        self.exec_internal_op(RebuildOperation::Complete).ok();
    }

    /// TODO
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

    /// TODO
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

    /// TODO
    async fn await_one_task(&mut self) -> Option<TaskResult> {
        self.task_pool.await_one_task().await
    }

    /// TODO
    async fn await_all_tasks(&mut self) {
        debug!(
            "Awaiting all active tasks({}) for rebuild {}",
            self.task_pool.active, self.dst_uri
        );

        while self.task_pool.active > 0 {
            if self.await_one_task().await.is_none() {
                error!("Failed to wait for {} rebuild tasks due mpsc channel failure.", self.task_pool.active);
                self.fail();
                return;
            }
        }
        debug!("Finished awaiting all tasks for rebuild {}", self.dst_uri);
    }

    /// Sends one segment worth of data in a reactor future and notifies the
    /// management channel. Returns the next segment offset to rebuild, if any.
    fn send_segment_task(&self, id: usize) -> Option<u64> {
        if self.next >= self.range.end {
            None
        } else {
            let blk = self.next;
            let next = std::cmp::min(
                self.next + self.segment_size_blks,
                self.range.end,
            );
            let name = self.dst_uri.clone();

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

impl<'n> RebuildJob<'n> {
    /// Client operations are now allowed to skip over previous operations.
    fn exec_client_op(
        &mut self,
        op: RebuildOperation,
    ) -> Result<(), RebuildError> {
        self.exec_op(op, false)
    }

    /// TODO.
    fn exec_internal_op(
        &mut self,
        op: RebuildOperation,
    ) -> Result<(), RebuildError> {
        self.exec_op(op, true)
    }

    /// Single state machine where all operations are handled.
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
