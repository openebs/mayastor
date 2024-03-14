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
    core::SegmentMap,
    gen_rebuild_instances,
    rebuild::rebuilders::{FullRebuild, PartialRebuild, RangeRebuilder},
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

/// Builder for the `BdevRebuildJob`, allowing for custom bdev to bdev rebuilds.
#[derive(Default)]
pub struct BdevRebuildJobBuilder {
    range: Option<Range<u64>>,
    options: RebuildJobOptions,
    notify_fn: Option<fn(&str, &str) -> ()>,
    rebuild_map: Option<SegmentMap>,
}
impl BdevRebuildJobBuilder {
    /// Specify a particular range.
    pub fn with_range(mut self, range: Range<u64>) -> Self {
        self.range = Some(range);
        self
    }
    /// Specify the rebuild options.
    pub fn with_option(mut self, options: RebuildJobOptions) -> Self {
        self.options = options;
        self
    }
    /// Specify a notification function.
    pub fn with_notify_fn(mut self, notify_fn: fn(&str, &str) -> ()) -> Self {
        self.notify_fn = Some(notify_fn);
        self
    }
    /// Specify a rebuild map, turning it into a partial rebuild.
    pub fn with_bitmap(mut self, rebuild_map: SegmentMap) -> Self {
        self.rebuild_map = Some(rebuild_map);
        self
    }
    /// Builds a `BdevRebuildJob` which can be started and which will then
    /// rebuild from source to destination.
    pub async fn build(
        self,
        src_uri: &str,
        dst_uri: &str,
    ) -> Result<BdevRebuildJob, RebuildError> {
        let descriptor =
            RebuildDescriptor::new(src_uri, dst_uri, self.range, self.options)
                .await?;
        let task_pool = RebuildTasks::new(SEGMENT_TASKS, &descriptor)?;
        let notify_fn = self.notify_fn.unwrap_or(|_, _| {});
        match self.rebuild_map {
            Some(map) => {
                descriptor.validate_map(&map)?;
                let backend = BdevRebuildJobBackend {
                    task_pool,
                    notify_fn,
                    copier: PartialRebuild::new(map, descriptor),
                };
                RebuildJob::from_backend(backend).await.map(BdevRebuildJob)
            }
            None => {
                let backend = BdevRebuildJobBackend {
                    task_pool,
                    notify_fn,
                    copier: FullRebuild::new(descriptor),
                };
                RebuildJob::from_backend(backend).await.map(BdevRebuildJob)
            }
        }
    }
}

impl BdevRebuildJob {
    /// Helps create a `Self` using a builder: `BdevRebuildJobBuilder`.
    pub fn builder() -> BdevRebuildJobBuilder {
        BdevRebuildJobBuilder::default()
    }
}

gen_rebuild_instances!(BdevRebuildJob);

/// A rebuild job which is responsible for rebuilding from
/// source to target of the `RebuildDescriptor`.
pub(super) struct BdevRebuildJobBackend<R: RangeRebuilder<RebuildDescriptor>> {
    /// A pool of tasks which perform the actual data rebuild.
    task_pool: RebuildTasks,
    /// A generic rebuild descriptor.
    copier: R,
    /// Notification callback with src and dst uri's.
    notify_fn: fn(&str, &str) -> (),
}

#[async_trait::async_trait(?Send)]
impl<R: RangeRebuilder<RebuildDescriptor>> RebuildBackend
    for BdevRebuildJobBackend<R>
{
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

impl<R: RangeRebuilder<RebuildDescriptor>> std::fmt::Debug
    for BdevRebuildJobBackend<R>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BdevRebuildJob")
            .field("next", &self.copier.peek_next())
            .finish()
    }
}
impl<R: RangeRebuilder<RebuildDescriptor>> std::fmt::Display
    for BdevRebuildJobBackend<R>
{
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}
