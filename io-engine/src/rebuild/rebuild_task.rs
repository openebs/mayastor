use futures::{channel::mpsc, stream::FusedStream, SinkExt, StreamExt};
use parking_lot::Mutex;

use spdk_rs::DmaBuf;
use std::{rc::Rc, sync::Arc};

use crate::{
    core::{Reactors, VerboseError},
    rebuild::SEGMENT_SIZE,
};

use super::{RebuildDescriptor, RebuildError, RebuildVerifyMode};

/// Result returned by each segment task worker.
/// Used to communicate with the management task indicating that the
/// segment task worker is ready to copy another segment.
#[derive(Debug, Clone)]
pub(super) struct TaskResult {
    /// Id of the rebuild task.
    pub(super) id: usize,
    /// Block that was being rebuilt.
    pub(super) blk: u64,
    /// Encountered error, if any.
    pub(super) error: Option<RebuildError>,
    /// Indicates if the segment was actually transferred (partial rebuild may
    /// skip segments).
    is_transferred: bool,
}

/// Each rebuild task needs a unique buffer to read/write from source to target.
/// An mpsc channel is used to communicate with the management task.
#[derive(Debug)]
pub(super) struct RebuildTask {
    /// The pre-allocated buffers used to read/write.
    buffer: DmaBuf,
    /// The channel used to notify when the task completes/fails.
    sender: mpsc::Sender<TaskResult>,
    /// Last error seen by this particular task.
    error: Option<TaskResult>,
}

impl RebuildTask {
    pub(super) fn new(
        buffer: DmaBuf,
        sender: mpsc::Sender<TaskResult>,
    ) -> Self {
        Self {
            buffer,
            sender,
            error: None,
        }
    }

    /// Copies one segment worth of data from source into destination.
    /// Returns true if write transfer took place, false otherwise.
    pub(super) async fn copy_one(
        &mut self,
        offset_blk: u64,
        desc: &RebuildDescriptor,
    ) -> Result<bool, RebuildError> {
        let iov = desc.adjusted_iov(&self.buffer, offset_blk);
        let iovs = &mut [iov];

        if !desc.read_src_segment(offset_blk, iovs).await? {
            // Segment is not allocated in the source, skip the write.
            return Ok(false);
        }
        desc.write_dst_segment(offset_blk, iovs).await?;

        if !matches!(desc.options.verify_mode, RebuildVerifyMode::None) {
            desc.verify_segment(offset_blk, iovs).await?;
        }

        Ok(true)
    }
}

/// Pool of rebuild tasks and progress tracking.
/// Each task uses a clone of the sender allowing the management task to poll a
/// single receiver.
pub(super) struct RebuildTasks {
    /// All tasks managed by this entity.
    /// Each task can run off on its own, and thus why each is protected
    /// by a lock.
    pub(super) tasks: Vec<Arc<Mutex<RebuildTask>>>,
    /// The channel is used to communicate with the tasks.
    pub(super) channel: (mpsc::Sender<TaskResult>, mpsc::Receiver<TaskResult>),
    /// How many active tasks at present.
    pub(super) active: usize,
    /// How many tasks in total.
    pub(super) total: usize,
    /// How many segments have been rebuilt so far, regardless if they were
    /// actually copied or the target segment was already in sync.
    /// In other words, how many rebuild tasks have successfully completed.
    pub(super) segments_done: u64,
    /// How many segments have been actually transferred so far.
    pub(super) segments_transferred: u64,
}

impl std::fmt::Debug for RebuildTasks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RebuildTasks")
            .field("active", &self.active)
            .field("total", &self.total)
            .field("segments_done", &self.segments_done)
            .finish()
    }
}

impl RebuildTasks {
    /// Create a rebuild tasks pool for the given rebuild descriptor.
    /// Each task can be schedule to run concurrently, and each task
    /// gets its own `DmaBuf` from where it reads and writes from.
    pub(super) fn new(
        task_count: usize,
        desc: &RebuildDescriptor,
    ) -> Result<Self, RebuildError> {
        // only sending one message per channel at a time so we don't need
        // the extra buffer
        let channel = mpsc::channel(0);
        let tasks = (0 .. task_count).map(|_| {
            let buffer = desc.dma_malloc(SEGMENT_SIZE)?;
            let task = RebuildTask::new(buffer, channel.0.clone());
            Ok(Arc::new(Mutex::new(task)))
        });
        assert_eq!(tasks.len(), task_count);

        Ok(RebuildTasks {
            total: tasks.len(),
            tasks: tasks.collect::<Result<_, _>>()?,
            channel,
            active: 0,
            segments_done: 0,
            segments_transferred: 0,
        })
    }

    /// Check if there's at least one task still running.
    pub(super) fn running(&self) -> bool {
        self.active > 0 && !self.channel.1.is_terminated()
    }
    /// Await for one task to complete and update the task complete count.
    pub(super) async fn await_one_task(&mut self) -> Option<TaskResult> {
        self.channel.1.next().await.map(|f| {
            self.active -= 1;
            if f.error.is_none() {
                self.segments_done += 1;
                if f.is_transferred {
                    self.segments_transferred += 1;
                }
            }
            f
        })
    }
    /// Schedules the run of a task by its id. It will copy the segment size
    /// starting at the given block address from source to destination.
    /// todo: don't use a specific task, simply get the next from the pool.
    pub(super) fn schedule_segment_rebuild(
        &mut self,
        id: usize,
        blk: u64,
        copier: Rc<impl RebuildTaskCopier + 'static>,
    ) {
        let task = self.tasks[id].clone();

        Reactors::current().send_future(async move {
            // No other thread/task will acquire the mutex at the same time.
            let mut task = task.lock();

            // Could we make this the option, rather than the descriptor itself?
            let result = copier.copy_segment(blk, &mut task).await;

            let is_transferred = *result.as_ref().unwrap_or(&false);
            let error = TaskResult {
                id,
                blk,
                error: result.err(),
                is_transferred,
            };
            task.error = Some(error.clone());
            if let Err(e) = task.sender.send(error).await {
                error!(
                    "Failed to notify job of segment id: {id} blk: {blk} \
                    completion, err: {err}",
                    err = e.verbose()
                );
            }
        });
    }
}

/// Interface to allow for different implementations of a single task copy
/// operation.
/// Currently allows for only the copy of a single segment, though this
/// can be expanded for sub-segment copies.
#[async_trait::async_trait(?Send)]
pub(super) trait RebuildTaskCopier {
    /// Copies an entire segment at the given block address, from source to
    /// target using a `DmaBuf`.
    async fn copy_segment(
        &self,
        blk: u64,
        task: &mut RebuildTask,
    ) -> Result<bool, RebuildError>;
}

#[async_trait::async_trait(?Send)]
impl RebuildTaskCopier for RebuildDescriptor {
    /// Copies one segment worth of data from source into destination.
    async fn copy_segment(
        &self,
        blk: u64,
        task: &mut RebuildTask,
    ) -> Result<bool, RebuildError> {
        // todo: move the map out of the descriptor, into the specific backends.
        if self.is_blk_sync(blk) {
            return Ok(false);
        }

        // Perform the copy.
        let result = task.copy_one(blk, self).await;

        // In the case of success, mark the segment as already transferred.
        if result.is_ok() {
            self.blk_synced(blk);
        }

        result
    }
}
