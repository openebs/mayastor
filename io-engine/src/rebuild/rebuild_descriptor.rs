use chrono::{DateTime, Utc};
use snafu::ResultExt;
use spdk_rs::{
    libspdk::SPDK_NVME_SC_COMPARE_FAILURE,
    DmaBuf,
    IoVec,
    NvmeStatus,
};

use crate::{
    bdev::device_open,
    bdev_api::bdev_get_name,
    core::{
        BlockDevice,
        BlockDeviceDescriptor,
        BlockDeviceHandle,
        CoreError,
        IoCompletionStatus,
        ReadOptions,
        SegmentMap,
    },
    rebuild::{
        rebuild_error::{BdevInvalidUri, NoCopyBuffer},
        WithinRange,
        SEGMENT_SIZE,
    },
};

use super::{RebuildError, RebuildJobOptions, RebuildVerifyMode};

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
    pub(super) src_handle: Box<dyn BlockDeviceHandle>,
    /// Pre-opened descriptor for destination block device.
    #[allow(clippy::non_send_fields_in_send_ty)]
    pub(super) dst_descriptor: Box<dyn BlockDeviceDescriptor>,
    pub(super) dst_handle: Box<dyn BlockDeviceHandle>,
    /// Start time of this rebuild.
    pub(super) start_time: DateTime<Utc>,
}

impl RebuildDescriptor {
    pub(super) async fn new(
        src_uri: &str,
        dst_uri: &str,
        range: Option<std::ops::Range<u64>>,
        options: RebuildJobOptions,
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

        if src_descriptor.device_name() == dst_descriptor.device_name() {
            return Err(RebuildError::SameBdev {
                bdev: src_descriptor.device_name(),
            });
        }

        let src_handle = RebuildDescriptor::io_handle(&*src_descriptor).await?;
        let dst_handle = RebuildDescriptor::io_handle(&*dst_descriptor).await?;

        let range = match range {
            None => {
                let dst_size = dst_descriptor.get_device().size_in_bytes();
                let dst_blk_size = dst_descriptor.get_device().block_len();

                0 .. dst_size / dst_blk_size
            }
            Some(range) => range,
        };

        if !Self::validate(
            src_handle.get_device(),
            dst_handle.get_device(),
            &range,
        ) {
            return Err(RebuildError::InvalidSrcDstRange {});
        }

        let block_size = dst_descriptor.get_device().block_len();
        let segment_size_blks = SEGMENT_SIZE / block_size;

        Ok(Self {
            src_uri: src_uri.to_string(),
            dst_uri: dst_uri.to_string(),
            range,
            options,
            block_size,
            segment_size_blks,
            src_descriptor,
            src_handle,
            dst_descriptor,
            dst_handle,
            start_time: Utc::now(),
        })
    }

    /// Check if the source and destination block devices are compatible for
    /// rebuild.
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

    /// Check if the rebuild range is compatible with the rebuild segment map.
    pub(super) fn validate_map(
        &self,
        map: &SegmentMap,
    ) -> Result<(), RebuildError> {
        if map.size_blks() > self.range.end {
            return Err(RebuildError::InvalidMapRange {});
        }
        Ok(())
    }

    /// Return the size of the segment to be copied.
    #[inline(always)]
    pub(super) fn get_segment_size_blks(&self, blk: u64) -> u64 {
        // Adjust the segments size for the last segment
        if (blk + self.segment_size_blks) > self.range.end {
            return self.range.end - blk;
        }
        self.segment_size_blks
    }

    /// Allocate memory from the memory pool (the mem is zeroed out)
    /// with given size and proper alignment for the bdev.
    pub(super) fn dma_malloc(&self, size: u64) -> Result<DmaBuf, RebuildError> {
        let src_align = self.src_descriptor.get_device().alignment();
        let dst_align = self.dst_descriptor.get_device().alignment();
        DmaBuf::new(size, src_align.max(dst_align)).context(NoCopyBuffer)
    }

    /// Get a `BlockDeviceHandle` for the source.
    #[inline(always)]
    pub(super) fn src_io_handle(&self) -> &dyn BlockDeviceHandle {
        self.src_handle.as_ref()
    }

    /// Get a `BlockDeviceHandle` for the destination.
    #[inline(always)]
    pub(super) fn dst_io_handle(&self) -> &dyn BlockDeviceHandle {
        self.dst_handle.as_ref()
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
        opts: ReadOptions,
    ) -> Result<bool, RebuildError> {
        match self
            .src_io_handle()
            .readv_blocks_async(
                iovs,
                offset_blk,
                self.get_segment_size_blks(offset_blk),
                opts,
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
                IoCompletionStatus::NvmeError(NvmeStatus::UNWRITTEN_BLOCK)
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
                IoCompletionStatus::NvmeError(NvmeStatus::Media(
                    SPDK_NVME_SC_COMPARE_FAILURE
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
