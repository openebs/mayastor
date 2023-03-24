use snafu::ResultExt;

use super::rebuild_error::{NoBdevHandle, RebuildError};
use crate::core::{BlockDeviceDescriptor, BlockDeviceHandle, DescriptorGuard};
use chrono::{DateTime, Utc};

/// Contains all descriptors and their associated information which allows the
/// tasks to copy/rebuild data from source to destination.
pub(super) struct RebuildDescriptor {
    /// The block size of the src and dst.
    /// todo: allow for differences?
    pub(super) block_size: u64,
    /// The range of the entire rebuild.
    pub(super) range: std::ops::Range<u64>,
    /// Segment size in blocks (number of segments divided by device block
    /// size).
    pub(super) segment_size_blks: u64,
    /// Source URI of the healthy child to rebuild from.
    pub src_uri: String,
    /// Target URI of the out of sync child to rebuild.
    pub dst_uri: String,
    /// Pre-opened descriptor for the source block device.
    #[allow(clippy::non_send_fields_in_send_ty)]
    pub(super) src_descriptor: Box<dyn BlockDeviceDescriptor>,
    /// Pre-opened descriptor for destination block device.
    #[allow(clippy::non_send_fields_in_send_ty)]
    pub(super) dst_descriptor: Box<dyn BlockDeviceDescriptor>,
    /// Nexus Descriptor so we can lock its ranges when rebuilding a segment.
    pub(super) nexus_descriptor: DescriptorGuard<()>,
    /// Start time of this rebuild.
    pub(super) start_time: DateTime<Utc>,
}

impl RebuildDescriptor {
    /// Return the size of the segment to be copied.
    pub(super) fn get_segment_size_blks(&self, blk: u64) -> u64 {
        // Adjust the segments size for the last segment
        if (blk + self.segment_size_blks) > self.range.end {
            return self.range.end - blk;
        }
        self.segment_size_blks
    }
    /// Get a `BlockDeviceHandle` for the source.
    pub(super) async fn src_io_handle(
        &self,
    ) -> Result<Box<dyn BlockDeviceHandle>, RebuildError> {
        Self::io_handle(&*self.src_descriptor).await
    }
    /// Get a `BlockDeviceHandle` for the destination.
    pub(super) async fn dst_io_handle(
        &self,
    ) -> Result<Box<dyn BlockDeviceHandle>, RebuildError> {
        Self::io_handle(&*self.dst_descriptor).await
    }

    /// Get a `BlockDeviceHandle` for the given block device descriptor.
    pub(super) async fn io_handle(
        descriptor: &dyn BlockDeviceDescriptor,
    ) -> Result<Box<dyn BlockDeviceHandle>, RebuildError> {
        descriptor
            .get_io_handle_nonblock()
            .await
            .context(NoBdevHandle {
                bdev: descriptor.get_device().device_name(),
            })
    }
}
