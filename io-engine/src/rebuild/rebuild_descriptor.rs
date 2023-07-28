use chrono::{DateTime, Utc};
use spdk_rs::{DmaBuf, IoVec, MediaErrorStatusCode, NvmeStatus};
use std::sync::Arc;

use crate::core::{
    BlockDeviceDescriptor,
    BlockDeviceHandle,
    CoreError,
    DescriptorGuard,
    IoCompletionStatus,
    ReadOptions,
};

use super::{RebuildError, RebuildJobOptions, RebuildMap, RebuildVerifyMode};

/// Contains all descriptors and their associated information which allows the
/// tasks to copy/rebuild data from source to destination.
pub(super) struct RebuildDescriptor {
    /// The block size of the src and dst.
    /// todo: allow for differences?
    pub(super) block_size: u64,
    /// The range of the entire rebuild.
    pub(super) range: std::ops::Range<u64>,
    /// Rebuild job options.
    pub(super) options: RebuildJobOptions,
    /// Segment size in blocks (number of segments divided by device block
    /// size).
    pub(super) segment_size_blks: u64,
    /// Source URI of the healthy child to rebuild from.
    pub(super) src_uri: String,
    /// Target URI of the out of sync child to rebuild.
    pub(super) dst_uri: String,
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
    /// Rebuild map.
    pub(super) rebuild_map: Arc<parking_lot::Mutex<Option<RebuildMap>>>,
}

impl RebuildDescriptor {
    /// Return the size of the segment to be copied.
    #[inline(always)]
    pub(super) fn get_segment_size_blks(&self, blk: u64) -> u64 {
        // Adjust the segments size for the last segment
        if (blk + self.segment_size_blks) > self.range.end {
            return self.range.end - blk;
        }
        self.segment_size_blks
    }

    /// Get a `BlockDeviceHandle` for the source.
    #[inline(always)]
    pub(super) async fn src_io_handle(
        &self,
    ) -> Result<Box<dyn BlockDeviceHandle>, RebuildError> {
        Self::io_handle(&*self.src_descriptor).await
    }

    /// Get a `BlockDeviceHandle` for the destination.
    #[inline(always)]
    pub(super) async fn dst_io_handle(
        &self,
    ) -> Result<Box<dyn BlockDeviceHandle>, RebuildError> {
        Self::io_handle(&*self.dst_descriptor).await
    }

    /// Get a `BlockDeviceHandle` for the given block device descriptor.
    #[inline(always)]
    pub(super) async fn io_handle(
        descriptor: &dyn BlockDeviceDescriptor,
    ) -> Result<Box<dyn BlockDeviceHandle>, RebuildError> {
        descriptor.get_io_handle_nonblock().await.map_err(|e| {
            error!(
                "{dev}: failed to get I/O handle: {e}",
                dev = descriptor.device_name()
            );
            RebuildError::NoBdevHandle {
                source: e,
                bdev: descriptor.get_device().device_name(),
            }
        })
    }

    /// Checks if the block has to be transferred.
    /// If no rebuild map is present, all blocks are considered unsynced.
    #[inline(always)]
    pub(super) fn is_blk_sync(&self, blk: u64) -> bool {
        self.rebuild_map
            .lock()
            .as_ref()
            .map_or(false, |m| m.is_blk_clean(blk))
    }

    /// Marks the rebuild segment starting from the given logical block as
    /// already transferred.
    #[inline(always)]
    pub(super) fn blk_synced(&self, blk: u64) {
        if let Some(map) = self.rebuild_map.lock().as_mut() {
            map.blk_clean(blk);
        }
    }

    /// Returns `IoVec` for the givem `DmaBuf`, with length adjusted to the copy
    /// size for the given offset. Given `DmaBuf` must be large enough.
    #[inline(always)]
    pub(super) fn adjusted_iov(
        &self,
        buffer: &DmaBuf,
        offset_blk: u64,
    ) -> IoVec {
        let mut iov = buffer.to_io_vec();

        let iov_len = self.get_segment_size_blks(offset_blk) * self.block_size;
        assert!(iov_len <= iov.len()); // TODO: realloc buffer
        unsafe { iov.set_len(iov_len) };
        iov
    }

    /// Reads a rebuild segment at the given offset from the source replica.
    /// In the case the segment is not allocated on the source, returns false,
    /// and true otherwise.
    pub(super) async fn read_src_segment(
        &self,
        offset_blk: u64,
        iovs: &mut [IoVec],
    ) -> Result<bool, RebuildError> {
        match self
            .src_io_handle()
            .await?
            .readv_blocks_async(
                iovs,
                offset_blk,
                self.get_segment_size_blks(offset_blk),
                ReadOptions::UnwrittenFail,
            )
            .await
        {
            // Read is okay, data has to be copied to the destination.
            Ok(_) => Ok(true),

            // Read from an unallocated block occured, no need to copy it.
            Err(CoreError::ReadFailed {
                status, ..
            }) if matches!(
                status,
                IoCompletionStatus::NvmeError(NvmeStatus::MediaError(
                    MediaErrorStatusCode::DeallocatedOrUnwrittenBlock
                ))
            ) =>
            {
                Ok(false)
            }

            // Read error.
            Err(err) => Err(RebuildError::ReadIoFailed {
                source: err,
                bdev: self.src_uri.clone(),
            }),
        }
    }

    /// Writes the given buffer to the destionation replica.
    pub(super) async fn write_dst_segment(
        &self,
        offset_blk: u64,
        iovs: &[IoVec],
    ) -> Result<(), RebuildError> {
        self.dst_io_handle()
            .await?
            .writev_blocks_async(
                iovs,
                offset_blk,
                self.get_segment_size_blks(offset_blk),
            )
            .await
            .map_err(|err| RebuildError::WriteIoFailed {
                source: err,
                bdev: self.dst_uri.clone(),
            })
    }

    /// Verify segment copy operation by reading destination, and comparing with
    /// the source.
    pub(super) async fn verify_segment(
        &self,
        offset_blk: u64,
        iovs: &mut [IoVec],
    ) -> Result<(), RebuildError> {
        // Read the source again.
        self.src_io_handle()
            .await?
            .readv_blocks_async(
                iovs,
                offset_blk,
                self.get_segment_size_blks(offset_blk),
                ReadOptions::None,
            )
            .await
            .map_err(|err| RebuildError::VerifyIoFailed {
                source: err,
                bdev: self.dst_uri.clone(),
            })?;

        match self
            .dst_io_handle()
            .await?
            .comparev_blocks_async(
                iovs,
                offset_blk,
                self.get_segment_size_blks(offset_blk),
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(CoreError::CompareFailed {
                status, ..
            }) if matches!(
                status,
                IoCompletionStatus::NvmeError(NvmeStatus::MediaError(
                    MediaErrorStatusCode::CompareFailure
                ))
            ) =>
            {
                self.verify_failure(offset_blk)
            }
            Err(err) => Err(RebuildError::VerifyIoFailed {
                source: err,
                bdev: self.dst_uri.clone(),
            }),
        }
    }

    /// Handles verification failure.
    fn verify_failure(&self, offset_blk: u64) -> Result<(), RebuildError> {
        let msg = format!(
            "Rebuild job '{src}' -> '{dst}': verification failed \
            at segment {offset_blk}",
            src = self.src_uri,
            dst = self.dst_uri
        );

        match self.options.verify_mode {
            RebuildVerifyMode::None => {
                error!("{msg}: ignoring");
                Ok(())
            }
            RebuildVerifyMode::Fail => {
                error!("{msg}: failing rebuild");
                Err(RebuildError::VerifyCompareFailed {
                    bdev: self.dst_uri.clone(),
                    verify_message: msg,
                })
            }
            RebuildVerifyMode::Panic => {
                error!("{msg}: will panic");
                panic!("{}", msg);
            }
        }
    }
}
