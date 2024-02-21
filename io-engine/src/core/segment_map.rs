use bit_vec::BitVec;
use std::fmt::{Debug, Formatter};

// Returns ceil of an integer division.
fn div_ceil(a: u64, b: u64) -> u64 {
    (a + b - 1) / b
}

/// Map of rebuild segments of a block device.
/// It marks every segment as a clean (no need to rebuild, or already
/// transferred), or dirty (need to transfer from a healthy device).
#[derive(Clone)]
pub(crate) struct SegmentMap {
    /// Bitmap of rebuild segments of a device. Zeros indicate clean segments,
    /// ones mark dirty ones.
    segments: BitVec,
    /// Device size in segments.
    num_segments: u64,
    /// Device size in blocks.
    num_blocks: u64,
    /// Size of block in bytes.
    block_len: u64,
    /// Segment size in bytes.
    segment_size: u64,
}

impl Debug for SegmentMap {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{segs} segments / {dirty} dirty: {blks} blocks x {blklen}",
            segs = self.num_segments,
            blks = self.num_blocks,
            blklen = self.block_len,
            dirty = self.count_ones(),
        )
    }
}

impl SegmentMap {
    /// Creates a new segment map with the given parameters.
    pub(crate) fn new(
        num_blocks: u64,
        block_len: u64,
        segment_size: u64,
    ) -> Self {
        let num_segments = div_ceil(num_blocks * block_len, segment_size);
        let mut segments = BitVec::new();
        segments.grow(num_segments as usize, false);
        Self {
            segments,
            num_segments,
            num_blocks,
            block_len,
            segment_size,
        }
    }

    /// Merges (bitwise OR) this map with another.
    pub(crate) fn merge(mut self, other: &SegmentMap) -> Self {
        self.segments.or(&other.segments);
        self
    }

    /// Sets a segment bit corresponding to the given logical block, to the
    /// given value.
    pub(crate) fn set(&mut self, lbn: u64, lbn_cnt: u64, value: bool) {
        assert_ne!(self.num_blocks, 0);

        let start_seg = self.lbn_to_seg(lbn);
        // when `lbn_cnt` is 1 means we write only the `lbn` blk, not `lbn` + 1
        let end_seg = self.lbn_to_seg(lbn + lbn_cnt - 1);
        for i in start_seg ..= end_seg {
            self.segments.set(i, value);
        }
    }

    /// Returns value of segment bit corresponding to the given logical block.
    pub(crate) fn get(&self, lbn: u64) -> Option<bool> {
        let seg = self.lbn_to_seg(lbn);
        self.segments.get(seg)
    }

    /// Calculates the index of segment corresponding to the given logical
    /// block.
    fn lbn_to_seg(&self, lbn: u64) -> usize {
        (lbn * self.block_len / self.segment_size) as usize
    }

    /// Counts the total number of bits set to one.
    fn count_ones(&self) -> u64 {
        self.segments.iter().filter(|i| *i).count() as u64
    }

    /// Counts the total number of dirty blocks.
    pub(crate) fn count_dirty_blks(&self) -> u64 {
        self.count_ones() * self.segment_size / self.block_len
    }

    /// Get the segment size in blocks.
    pub(crate) fn segment_size_blks(&self) -> u64 {
        self.segment_size / self.block_len
    }
}

impl From<SegmentMap> for BitVec {
    fn from(value: SegmentMap) -> Self {
        value.segments
    }
}
