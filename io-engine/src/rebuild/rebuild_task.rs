use futures::{channel::mpsc, stream::FusedStream, SinkExt, StreamExt};
use parking_lot::Mutex;
use snafu::ResultExt;
use spdk_rs::{DmaBuf, LbaRange};
use std::{rc::Rc, sync::Arc};

use crate::core::{Reactors, VerboseError};

use super::{
    rebuild_error::{RangeLockFailed, RangeUnlockFailed},
    RebuildDescriptor,
    RebuildError,
    RebuildVerifyMode,
};

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
        blk: u64,
        descriptor: &RebuildDescriptor,
    ) -> Result<bool, RebuildError> {
        if descriptor.is_blk_sync(blk) {
            return Ok(false);
        }

        let len = descriptor.get_segment_size_blks(blk);
        // The nexus children have metadata and data partitions, whereas the
        // nexus has a data partition only. Because we are locking the range on
        // the nexus, we need to calculate the offset from the start of the data
        // partition.
        let r = LbaRange::new(blk - descriptor.range.start, len);

        // Wait for LBA range to be locked.
        // This prevents other I/Os being issued to this LBA range whilst it is
        // being rebuilt.
        let lock = descriptor
            .nexus_descriptor
            .lock_lba_range(r)
            .await
            .context(RangeLockFailed {
                blk,
                len,
            })?;

        // Perform the copy.
        let result = self.copy_one(blk, descriptor).await;

        // Wait for the LBA range to be unlocked.
        // This allows others I/Os to be issued to this LBA range once again.
        descriptor
            .nexus_descriptor
            .unlock_lba_range(lock)
            .await
            .context(RangeUnlockFailed {
                blk,
                len,
            })?;

        // In the case of success, mark the segment as already transferred.
        if result.is_ok() {
            descriptor.blk_synced(blk);
        }

        result
    }

    /// Copies one segment worth of data from source into destination.
    /// Returns true if write transfer took place, false otherwise.
    async fn copy_one(
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
    /// Add the given `RebuildTask` to the task pool.
    pub(super) fn push(&mut self, task: RebuildTask) {
        self.tasks.push(Arc::new(Mutex::new(task)));
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
    pub(super) fn send_segment(
        &mut self,
        id: usize,
        blk: u64,
        descriptor: Rc<RebuildDescriptor>,
    ) {
        let task = self.tasks[id].clone();

        Reactors::current().send_future(async move {
            // No other thread/task will acquire the mutex at the same time.
            let mut task = task.lock();
            let result = task.locked_copy_one(blk, &descriptor).await;
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
