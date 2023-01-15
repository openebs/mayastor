use bit_vec::BitVec;
use parking_lot::Mutex;
use spdk_rs::IoType;
use std::{
    fmt::{Debug, Formatter},
    sync::Arc,
};

// use super::SEGMENT_SIZE;

/// TODO
pub struct RebuildLog {
    device_name: String,
    segments: BitVec,
    num_blocks: u64,
    block_len: u64,
}

impl Debug for RebuildLog {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Rebuild log: '{}' ({} blocks x {})",
            self.device_name, self.num_blocks, self.block_len
        )
    }
}

impl RebuildLog {
    /// TODO
    pub fn new(device_name: &str, num_blocks: u64, block_len: u64) -> Self {
        let mut segments = BitVec::new();
        segments.grow(num_blocks as usize, false);
        Self {
            device_name: device_name.to_owned(),
            segments,
            num_blocks,
            block_len,
        }
    }

    /// TODO
    pub fn log_op(
        &mut self,
        io_type: IoType,
        block_offset: u64,
        num_blocks: u64,
    ) {
        // let offset_seg = (block_offset * block_len / SEGMENT_SIZE) as usize;
        //
        // let size = num_blocks * block_len;
        //
        // let num_segments = 1;
        // // let num_segments = size / SEGMENT_SIZE;
        // // let num_segments = if num_segments * SEGMENT_SIZE < size {
        // //     num_segments + 1
        // // } else {
        // //     num_segments
        // // } as usize;
        //
        // if offset_seg >= self.segments.len() {
        //     self.segments.grow(offset_seg + 1, false);
        // }
        //
        // for i in 0 .. num_segments {
        //     self.segments.set(offset_seg + i, true);
        // }
        //
        // println!(
        //     "++++ {:?}: {:?}: {} seg ({} blk) ==> {} segs ({} blks)",
        //     self, io_type, offset_seg, block_offset, num_segments, num_blocks
        // );

        let cur = spdk_rs::Cores::current();

        let block_offset = block_offset as usize;
        for i in 0 .. num_blocks as usize {
            self.segments.set(block_offset + i, true);
            // println!(
            //     "++++ [{}] {:?}: {:?}: {} +",
            //     cur,
            //     self,
            //     io_type,
            //     block_offset + i
            // );
        }
    }

    /// TODO
    pub fn need_copy(&self, block_offset: u64) -> bool {
        // let seg = (block_offset * block_len / SEGMENT_SIZE) as usize;
        // if seg >= self.segments.len() {
        //     return false;
        // }
        // println!(
        //     "++++ ?? {} seg ({} blk): {}",
        //     seg,
        //     block_offset,
        //     if self.segments[seg] { "+" } else { "-" }
        // );
        // self.segments[seg]
        let block_offset = block_offset as usize;
        // println!(
        //     "++++ ?? {}: {}",
        //     block_offset,
        //     if self.segments[block_offset] {
        //         "+"
        //     } else {
        //         "-"
        //     }
        // );
        self.segments[block_offset]
    }
}

/// TODO
#[derive(Clone)]
pub struct RebuildLogHandle {
    log: Arc<Mutex<RebuildLog>>,
}

impl From<RebuildLog> for RebuildLogHandle {
    fn from(log: RebuildLog) -> Self {
        let s = Self {
            log: Arc::new(Mutex::new(log)),
        };
        warn!("++++ {:?}: new log | {} seg_size", s, super::SEGMENT_SIZE);
        s
    }
}

impl Debug for RebuildLogHandle {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.log.lock().fmt(f)
    }
}

impl RebuildLogHandle {
    /// TODO
    pub fn log(&self) -> &Arc<Mutex<RebuildLog>> {
        &self.log
    }

    /// TODO
    pub fn device_name(&self) -> String {
        self.log.lock().device_name.clone()
    }

    /// TODO
    pub fn log_op(&self, io_type: IoType, block_offset: u64, num_blocks: u64) {
        if matches!(io_type, IoType::Write | IoType::WriteZeros | IoType::Unmap)
        {
            self.log.lock().log_op(io_type, block_offset, num_blocks)
        }
    }

    /// TODO
    pub fn need_copy(&self, block_offset: u64) -> bool {
        self.log.lock().need_copy(block_offset)
    }
}
