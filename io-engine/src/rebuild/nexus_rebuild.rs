use futures::channel::oneshot;
use snafu::ResultExt;
use spdk_rs::LbaRange;
use std::ops::{Deref, Range};

use crate::{
    core::{DescriptorGuard, UntypedBdev},
    gen_rebuild_instances,
    rebuild::{
        rebuild_error::{RangeLockFailed, RangeUnlockFailed},
        rebuild_job_backend::RebuildJobManager,
        rebuild_task::{RebuildTask, RebuildTaskCopier},
        rebuilders::{
            FullRebuild,
            PartialSeqCopier,
            PartialSeqRebuild,
            RangeRebuilder,
        },
        RebuildMap,
        RebuildState,
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
pub struct NexusRebuildJob {
    job: RebuildJob,
}

/// Nexus supports both full and partial rebuilds. In case of a partial rebuild
/// we have to provide the nexus rebuild job with a `RebuildMap`.
/// However, this can only be provided after we've created the rebuild as taking
/// the map from the nexus children is an operation which cannot be undone
/// today.
/// Therefore we use a rebuild job starter which creates the initial bits
/// required for the rebuild job and which can be started later with or without
/// the `RebuildMap`.
pub struct NexusRebuildJobStarter {
    /// The job itself is optional because it gets taken when we want to store.
    /// After the job is taken, we then can schedule either a full or a partial
    /// rebuild with the backend.
    job: Option<NexusRebuildJob>,
    manager: RebuildJobManager,
    backend: NexusRebuildJobBackendStarter,
}

impl std::fmt::Debug for NexusRebuildJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.job.fmt(f)
    }
}
impl Deref for NexusRebuildJob {
    type Target = RebuildJob;

    fn deref(&self) -> &Self::Target {
        &self.job
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
    pub async fn new_starter(
        nexus_name: &str,
        src_uri: &str,
        dst_uri: &str,
        range: Range<u64>,
        options: RebuildJobOptions,
        notify_fn: fn(String, String) -> (),
    ) -> Result<NexusRebuildJobStarter, RebuildError> {
        let descriptor =
            RebuildDescriptor::new(src_uri, dst_uri, Some(range), options)
                .await?;
        let tasks = RebuildTasks::new(SEGMENT_TASKS, &descriptor)?;

        let backend = NexusRebuildJobBackendStarter::new(
            nexus_name, tasks, notify_fn, descriptor,
        )
        .await?;

        let manager = RebuildJobManager::new();

        Ok(NexusRebuildJobStarter {
            job: Some(Self {
                job: RebuildJob::from_manager(&manager, &backend.descriptor),
            }),
            manager,
            backend,
        })
    }
}
impl NexusRebuildJobStarter {
    /// Store the inner rebuild job in the rebuild job list.
    pub fn store(mut self) -> Result<Self, RebuildError> {
        if let Some(job) = self.job.take() {
            job.store()?;
        }
        Ok(self)
    }
    /// Schedules the job to start in a future and returns a complete channel
    /// which can be waited on.
    pub async fn start(
        self,
        job: std::sync::Arc<NexusRebuildJob>,
        map: Option<RebuildMap>,
    ) -> Result<oneshot::Receiver<RebuildState>, RebuildError> {
        match map {
            None => {
                self.manager
                    .into_backend(self.backend.into_full())
                    .schedule()
                    .await;
            }
            Some(map) => {
                self.manager
                    .into_backend(self.backend.into_partial_seq(map))
                    .schedule()
                    .await;
            }
        }
        job.start().await
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
pub(super) struct NexusRebuildJobBackend<
    T: RebuildTaskCopier,
    R: RangeRebuilder<T>,
> {
    /// A pool of tasks which perform the actual data rebuild.
    task_pool: RebuildTasks,
    /// The range rebuilder which walks and copies the segments.
    copier: R,
    /// Notification callback which existing nexus uses to sync
    /// with rebuild updates.
    notify_fn: fn(String, String) -> (),
    /// The name of the nexus this pertains to.
    nexus_name: String,
    _p: std::marker::PhantomData<T>,
}

/// A Nexus rebuild job backend starter.
struct NexusRebuildJobBackendStarter {
    /// A pool of tasks which perform the actual data rebuild.
    task_pool: RebuildTasks,
    /// A nexus rebuild specific descriptor.
    descriptor: NexusRebuildDescriptor,
    /// Notification callback which existing nexus uses to sync
    /// with rebuild updates.
    notify_fn: fn(String, String) -> (),
}
impl NexusRebuildJobBackendStarter {
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

        let descriptor = NexusRebuildDescriptor {
            nexus: nexus_descriptor,
            nexus_name: nexus_name.to_string(),
            common: descriptor,
        };
        Ok(Self {
            descriptor,
            task_pool,
            notify_fn,
        })
    }

    fn into_partial_seq(
        self,
        map: RebuildMap,
    ) -> NexusRebuildJobBackend<
        PartialSeqCopier<NexusRebuildDescriptor>,
        PartialSeqRebuild<NexusRebuildDescriptor>,
    > {
        NexusRebuildJobBackend {
            task_pool: self.task_pool,
            notify_fn: self.notify_fn,
            nexus_name: self.descriptor.nexus_name.clone(),
            copier: PartialSeqRebuild::new(map, self.descriptor),
            _p: Default::default(),
        }
    }
    fn into_full(
        self,
    ) -> NexusRebuildJobBackend<
        NexusRebuildDescriptor,
        FullRebuild<NexusRebuildDescriptor>,
    > {
        NexusRebuildJobBackend {
            task_pool: self.task_pool,
            notify_fn: self.notify_fn,
            nexus_name: self.descriptor.nexus_name.clone(),
            copier: FullRebuild::new(self.descriptor),
            _p: Default::default(),
        }
    }
}

#[async_trait::async_trait(?Send)]
impl<T: RebuildTaskCopier + 'static, R: RangeRebuilder<T>> RebuildBackend
    for NexusRebuildJobBackend<T, R>
{
    fn on_state_change(&mut self) {
        (self.notify_fn)(
            self.nexus_name.clone(),
            self.common_desc().dst_uri.clone(),
        );
    }

    fn common_desc(&self) -> &RebuildDescriptor {
        self.copier.desc()
    }

    fn blocks_remaining(&self) -> u64 {
        self.copier.blocks_remaining()
    }
    fn is_partial(&self) -> bool {
        self.copier.is_partial()
    }

    fn task_pool(&self) -> &RebuildTasks {
        &self.task_pool
    }

    fn schedule_task_by_id(&mut self, id: usize) -> bool {
        self.copier
            .next()
            .map(|blk| {
                self.task_pool.schedule_segment_rebuild(
                    id,
                    blk,
                    self.copier.copier(),
                );
                self.task_pool.active += 1;
                true
            })
            .unwrap_or_default()
    }
    async fn await_one_task(&mut self) -> Option<TaskResult> {
        self.task_pool.await_one_task().await
    }
}

impl<T: RebuildTaskCopier + 'static, R: RangeRebuilder<T>> std::fmt::Debug
    for NexusRebuildJobBackend<T, R>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NexusRebuildJob")
            .field("nexus", &self.nexus_name)
            .field("next", &self.copier.peek_next())
            .finish()
    }
}
impl<T: RebuildTaskCopier + 'static, R: RangeRebuilder<T>> std::fmt::Display
    for NexusRebuildJobBackend<T, R>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "on nexus '{nex}'", nex = self.nexus_name)
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
    #[inline]
    async fn copy_segment(
        &self,
        blk: u64,
        task: &mut RebuildTask,
    ) -> Result<bool, RebuildError> {
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

        result
    }
}
