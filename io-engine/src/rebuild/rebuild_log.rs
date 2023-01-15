use bit_vec::BitVec;
use parking_lot::Mutex;
use spdk_rs::IoType;
use std::{
    fmt::{Debug, Formatter},
    sync::Arc,
};

use super::SEGMENT_SIZE;

// Returns ceil of an integer division.
fn div_ceil(a: u64, b: u64) -> u64 {
    (a + b - 1) / b
}

/// Rebuild log data structure.
pub struct RebuildLog {
    /// Name of the underlying device, stored for diagnostic purposes.
    device_name: String,
    /// Segment bitmap. Ones indicate modified segments.
    segments: BitVec,
    /// Device size in segments.
    num_segments: u64,
    /// Device size in block.
    num_blocks: u64,
    /// Size of block in bytes.
    block_len: u64,
}

impl Debug for RebuildLog {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Rebuild log: '{dev}' ({segs} segments: {blks} blocks x {blklen})",
            dev = self.device_name,
            segs = self.num_segments,
            blks = self.num_blocks,
            blklen = self.block_len
        )
    }
}

impl RebuildLog {
    /// Creates a new rebuild log instance for the given device.
    pub fn new(device_name: &str, num_blocks: u64, block_len: u64) -> Self {
        let num_segments = div_ceil(num_blocks * block_len, SEGMENT_SIZE);
        let mut segments = BitVec::new();
        segments.grow(num_segments as usize, false);
        Self {
            device_name: device_name.to_owned(),
            segments,
            num_segments,
            num_blocks,
            block_len,
        }
    }

    /// Logs the given operation, marking the corresponding segment as modified
    /// in the case of a write operation.
    ///
    /// # Arguments
    ///
    /// * `_io_type`: IoType of the operation to log.
    /// * `lbn`: Logical block number.
    /// * `lbn_cnt`: Number of logical blocks affected by the operation.
    fn log_op(&mut self, _io_type: IoType, lbn: u64, lbn_cnt: u64) {
        let start_seg = self.lbn_to_seg(lbn);
        let end_seg = self.lbn_to_seg(lbn + lbn_cnt);
        for i in start_seg ..= end_seg {
            self.segments.set(i, true);
        }
    }

    /// Determines if the given logical block was modified by a write-like
    /// operation.
    ///
    /// # Arguments
    ///
    /// * `lbn`: Logical block number.
    fn is_modified(&self, block_offset: u64) -> bool {
        let seg = self.lbn_to_seg(block_offset);
        if seg < self.segments.len() {
            self.segments[seg]
        } else {
            error!(
                "{self:?}: accessing rebuild log beyond its segment \
                range: {block_offset}"
            );
            true
        }
    }

    /// Marks the rebuild segment starting from the given logical block as
    /// already transferred.
    ///
    /// # Arguments
    ///
    /// * `lbn`: Logical block number.
    fn mark_segment_transferred(&mut self, block_offset: u64) {
        let seg = self.lbn_to_seg(block_offset);
        if seg < self.segments.len() {
            self.segments.set(seg, false);
        }
    }

    /// Calculates the index of segment corresponding to the given logical
    /// block.
    fn lbn_to_seg(&self, lbn: u64) -> usize {
        (lbn * self.block_len / SEGMENT_SIZE) as usize
    }

    /// Counts the total number of modified blocks.
    pub fn count_modified_blocks(&self) -> u64 {
        let segs = self.segments.iter().filter(|i| *i).count() as u64;
        segs * SEGMENT_SIZE / self.block_len
    }
}

/// A handle object for a rebuild log instance.
/// A rebuild log instance allows to have multiple handle to access it.
#[derive(Clone)]
pub struct RebuildLogHandle {
    /// Share log reference.
    log: Arc<Mutex<RebuildLog>>,
    /// Name of the underlying device.
    /// Keep it here to avoid locking just to access the device name,
    /// and cloning the name when accessing it.
    device_name: String,
}

impl From<RebuildLog> for RebuildLogHandle {
    fn from(log: RebuildLog) -> Self {
        let device_name = log.device_name.clone();
        Self {
            log: Arc::new(Mutex::new(log)),
            device_name,
        }
    }
}

impl Debug for RebuildLogHandle {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.log.lock().fmt(f)
    }
}

impl RebuildLogHandle {
    /// Returns the device name of the rebuild log.
    pub fn device_name(&self) -> &str {
        &self.device_name
    }

    /// Logs the given operation, marking the corresponding segment as modified
    /// in the case of a write operation.
    ///
    /// # Arguments
    ///
    /// * `io_type`: IoType of the operation to log.
    /// * `lbn`: Logical block number.
    /// * `lbn_cnt`: Number of logical blocks affected by the operation.
    pub fn log_op(&self, io_type: IoType, lbn: u64, lbn_cnt: u64) {
        if matches!(io_type, IoType::Write | IoType::WriteZeros | IoType::Unmap)
        {
            self.log.lock().log_op(io_type, lbn, lbn_cnt)
        }
    }

    /// Determines if the given logical block was modified by a write-like
    /// operation.
    ///
    /// # Arguments
    ///
    /// * `lbn`: Logical block number.
    pub fn is_modified(&self, lbn: u64) -> bool {
        self.log.lock().is_modified(lbn)
    }

    /// Marks the rebuild segment starting from the given logical block as
    /// already transferred.
    ///
    /// # Arguments
    ///
    /// * `lbn`: Logical block number.
    pub fn mark_segment_transferred(&self, lbn: u64) {
        self.log.lock().mark_segment_transferred(lbn)
    }

    /// Counts the total number of modified blocks.
    pub fn count_modified_blocks(&self) -> u64 {
        self.log.lock().count_modified_blocks()
    }
}
