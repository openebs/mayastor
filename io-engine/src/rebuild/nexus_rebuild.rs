use snafu::ResultExt;
use spdk_rs::LbaRange;
use std::{
    ops::{Deref, Range},
    rc::Rc,
};

use crate::{
    core::{DescriptorGuard, UntypedBdev},
    gen_rebuild_instances,
    rebuild::{
        rebuild_error::{RangeLockFailed, RangeUnlockFailed},
        rebuild_task::{RebuildTask, RebuildTaskCopier},
    },
};

use super::{
    rebuild_descriptor::RebuildDescriptor,
    rebuild_error::{BdevNotFound, RebuildError},
    rebuild_job::RebuildJob,
    rebuild_job_backend::RebuildBackend,
    rebuild_task::{RebuildTasks, TaskResult},
    RebuildJobOptions,
    SEGMENT_TASKS,
};

/// A Nexus rebuild job is responsible for managing a rebuild (copy) which reads
/// from source_hdl and writes into destination_hdl from specified start to end.
/// Each copy is synchronized with the nexus bdev using ranged locks to ensure
/// that there is no concurrent between the nexus and the rebuild.
/// This is a frontend interface that communicates with a backend runner which
/// is the one responsible for the read/writing of the data.
pub struct NexusRebuildJob(RebuildJob);

impl std::fmt::Debug for NexusRebuildJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl Deref for NexusRebuildJob {
    type Target = RebuildJob;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl NexusRebuildJob {
    /// Creates a new RebuildJob which rebuilds from source URI to target URI
    /// from start to end (of the data partition); notify_fn callback is called
    /// when the rebuild state is updated - with the nexus and destination
    /// URI as arguments.
    /// todo: Should we use a builder? Example:
    /// NexusRebuild:
    /// Builder::new(src, srd).with_range().with_options().with_nexus().build()
    /// GenericRebuild:
    /// Builder::new(src, srd).with_range().with_options().build()
    pub async fn new(
        nexus_name: &str,
        src_uri: &str,
        dst_uri: &str,
        range: Range<u64>,
        options: RebuildJobOptions,
        notify_fn: fn(String, String) -> (),
    ) -> Result<Self, RebuildError> {
        let descriptor =
            RebuildDescriptor::new(src_uri, dst_uri, Some(range), options)
                .await?;
        let tasks = RebuildTasks::new(SEGMENT_TASKS, &descriptor)?;

        let backend = NexusRebuildJobBackend::new(
            nexus_name, tasks, notify_fn, descriptor,
        )
        .await?;

        RebuildJob::from_backend(backend).await.map(Self)
    }
}

gen_rebuild_instances!(NexusRebuildJob);

/// Contains all descriptors and their associated information which allows the
/// tasks to copy/rebuild data from source to destination.
pub(super) struct NexusRebuildDescriptor {
    /// Name of the nexus associated with the rebuild job.
    pub nexus_name: String,
    /// Nexus Descriptor so we can lock its ranges when rebuilding a segment.
    pub(super) nexus: DescriptorGuard<()>,
    /// The generic rebuild descriptor for copying from source to target.
    pub(super) common: RebuildDescriptor,
}
impl Deref for NexusRebuildDescriptor {
    type Target = RebuildDescriptor;

    fn deref(&self) -> &Self::Target {
        &self.common
    }
}

/// A nexus-specific rebuild job which is responsible for rebuilding
/// the common `RebuildDescriptor` with the addition of the nexus guard
/// as a means of locking the range which is being rebuilt ensuring
/// there are no concurrent writes to the same range between the
/// user IO (through the nexus) and the rebuild itself.
pub(super) struct NexusRebuildJobBackend {
    /// The next block to be rebuilt.
    next: u64,
    /// A pool of tasks which perform the actual data rebuild.
    task_pool: RebuildTasks,
    /// A nexus rebuild specific descriptor.
    descriptor: Rc<NexusRebuildDescriptor>,
    /// Notification callback which existing nexus uses to sync
    /// with rebuild updates.
    notify_fn: fn(String, String) -> (),
}

#[async_trait::async_trait(?Send)]
impl RebuildBackend for NexusRebuildJobBackend {
    fn on_state_change(&mut self) {
        (self.notify_fn)(
            self.descriptor.nexus_name.clone(),
            self.descriptor.dst_uri.clone(),
        );
    }

    fn common_desc(&self) -> &RebuildDescriptor {
        &self.descriptor
    }

    fn task_pool(&self) -> &RebuildTasks {
        &self.task_pool
    }

    fn schedule_task_by_id(&mut self, id: usize) -> bool {
        match self.send_segment_task(id) {
            Some(next) => {
                self.task_pool.active += 1;
                self.next = next;
                true
            }
            // we've already got enough tasks to rebuild the destination
            None => false,
        }
    }
    async fn await_one_task(&mut self) -> Option<TaskResult> {
        self.task_pool.await_one_task().await
    }
}

impl std::fmt::Debug for NexusRebuildJobBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NexusRebuildJob")
            .field("nexus", &self.descriptor.nexus_name)
            .field("next", &self.next)
            .finish()
    }
}
impl std::fmt::Display for NexusRebuildJobBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "on nexus '{nex}'", nex = self.descriptor.nexus_name)
    }
}

impl NexusRebuildJobBackend {
    /// Creates a new RebuildJob which rebuilds from source URI to target URI
    /// from start to end (of the data partition); notify_fn callback is called
    /// when the rebuild state is updated - with the nexus and destination
    /// URI as arguments.
    pub async fn new(
        nexus_name: &str,
        task_pool: RebuildTasks,
        notify_fn: fn(String, String) -> (),
        descriptor: RebuildDescriptor,
    ) -> Result<Self, RebuildError> {
        let nexus_descriptor = UntypedBdev::open_by_name(nexus_name, false)
            .context(BdevNotFound {
                bdev: nexus_name.to_string(),
            })?;

        let be = Self {
            next: descriptor.range.start,
            task_pool,
            descriptor: Rc::new(NexusRebuildDescriptor {
                nexus: nexus_descriptor,
                nexus_name: nexus_name.to_string(),
                common: descriptor,
            }),
            notify_fn,
        };

        info!("{be}: backend created");

        Ok(be)
    }

    /// Sends one segment worth of data in a reactor future and notifies the
    /// management channel. Returns the next segment offset to rebuild, if any.
    fn send_segment_task(&mut self, id: usize) -> Option<u64> {
        if self.next >= self.descriptor.range.end {
            None
        } else {
            let next = std::cmp::min(
                self.next + self.descriptor.segment_size_blks,
                self.descriptor.range.end,
            );
            self.task_pool.schedule_segment_rebuild(
                id,
                self.next,
                self.descriptor.clone(),
            );
            Some(next)
        }
    }
}

#[async_trait::async_trait(?Send)]
impl RebuildTaskCopier for NexusRebuildDescriptor {
    fn descriptor(&self) -> &RebuildDescriptor {
        &self.common
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
    async fn copy_segment(
        &self,
        blk: u64,
        task: &mut RebuildTask,
    ) -> Result<bool, RebuildError> {
        if self.is_blk_sync(blk) {
            return Ok(false);
        }

        let len = self.get_segment_size_blks(blk);
        // The nexus children have metadata and data partitions, whereas the
        // nexus has a data partition only. Because we are locking the range on
        // the nexus, we need to calculate the offset from the start of the data
        // partition.
        let r = LbaRange::new(blk - self.range.start, len);

        // Wait for LBA range to be locked.
        // This prevents other I/Os being issued to this LBA range whilst it is
        // being rebuilt.
        let lock =
            self.nexus
                .lock_lba_range(r)
                .await
                .context(RangeLockFailed {
                    blk,
                    len,
                })?;

        // Perform the copy.
        let result = task.copy_one(blk, self).await;

        // Wait for the LBA range to be unlocked.
        // This allows others I/Os to be issued to this LBA range once again.
        self.nexus
            .unlock_lba_range(lock)
            .await
            .context(RangeUnlockFailed {
                blk,
                len,
            })?;

        // In the case of success, mark the segment as already transferred.
        if result.is_ok() {
            self.blk_synced(blk);
        }

        result
    }
}
