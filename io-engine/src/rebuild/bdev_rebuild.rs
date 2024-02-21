use std::ops::{Deref, Range};

use super::{
    rebuild_descriptor::RebuildDescriptor,
    rebuild_error::RebuildError,
    rebuild_job_backend::RebuildBackend,
    rebuild_task::{RebuildTasks, TaskResult},
    RebuildJob,
    RebuildJobOptions,
    SEGMENT_TASKS,
};

use crate::{
    gen_rebuild_instances,
    rebuild::rebuilders::{FullRebuild, RangeRebuilder},
};

/// A Bdev rebuild job is responsible for managing a rebuild (copy) which reads
/// from source_hdl and writes into destination_hdl from specified start to end.
pub struct BdevRebuildJob(RebuildJob);

impl std::fmt::Debug for BdevRebuildJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl Deref for BdevRebuildJob {
    type Target = RebuildJob;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl BdevRebuildJob {
    /// Creates a new RebuildJob which rebuilds from source URI to target URI
    /// from start to end (of the data partition); notify_fn callback is called
    /// when the rebuild state is updated - with the source and destination
    /// bdev URI's as arguments.
    pub async fn new(
        src_uri: &str,
        dst_uri: &str,
        range: Option<Range<u64>>,
        options: RebuildJobOptions,
        notify_fn: fn(&str, &str) -> (),
    ) -> Result<Self, RebuildError> {
        let descriptor =
            RebuildDescriptor::new(src_uri, dst_uri, range, options).await?;
        let tasks = RebuildTasks::new(SEGMENT_TASKS, &descriptor)?;
        let backend =
            BdevRebuildJobBackend::new(tasks, notify_fn, descriptor).await?;

        RebuildJob::from_backend(backend).await.map(Self)
    }
}

gen_rebuild_instances!(BdevRebuildJob);

/// A rebuild job which is responsible for rebuilding from
/// source to target of the `RebuildDescriptor`.
pub(super) struct BdevRebuildJobBackend {
    /// A pool of tasks which perform the actual data rebuild.
    task_pool: RebuildTasks,
    /// A generic rebuild descriptor.
    copier: FullRebuild<RebuildDescriptor>,
    /// Notification callback with src and dst uri's.
    notify_fn: fn(&str, &str) -> (),
}

#[async_trait::async_trait(?Send)]
impl RebuildBackend for BdevRebuildJobBackend {
    fn on_state_change(&mut self) {
        let desc = self.common_desc();
        (self.notify_fn)(&desc.src_uri, &desc.dst_uri);
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

impl std::fmt::Debug for BdevRebuildJobBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BdevRebuildJob")
            .field("next", &self.copier.peek_next())
            .finish()
    }
}
impl std::fmt::Display for BdevRebuildJobBackend {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

impl BdevRebuildJobBackend {
    /// Creates a new RebuildJob which rebuilds from source URI to target URI
    /// from start to end (of the data partition); notify_fn callback is called
    /// when the rebuild state is updated - with the source and destination
    /// URI as arguments.
    pub async fn new(
        task_pool: RebuildTasks,
        notify_fn: fn(&str, &str) -> (),
        descriptor: RebuildDescriptor,
    ) -> Result<Self, RebuildError> {
        Ok(Self {
            task_pool,
            copier: FullRebuild::new(descriptor),
            notify_fn,
        })
    }
}
