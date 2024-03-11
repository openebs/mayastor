use crate::{
    core::SegmentMap,
    rebuild::{
        rebuild_descriptor::RebuildDescriptor,
        rebuild_task::{RebuildTask, RebuildTaskCopier},
        RebuildError,
        RebuildMap,
    },
};
use bit_vec::BitVec;
use std::{ops::Range, rc::Rc};

/// A rebuild may rebuild a device by walking it differently, for example:
/// 1. full rebuild - walk the entire device range and copy every segment
///    (current nexus full rebuild behaviour).
/// 2. partial rebuild - walk the allocated segments only and copy them.
/// 3. partial seq rebuild - walk the entire device range and copy only
///    allocated segments (current nexus partial rebuild behaviour).
pub(super) trait RangeRebuilder<T: RebuildTaskCopier> {
    /// Fetch the next block to rebuild.
    fn next(&mut self) -> Option<u64>;
    /// Peek the next block to rebuild.
    fn peek_next(&self) -> Option<u64>;
    /// Get the remaining blocks we have yet to be rebuilt.
    fn blocks_remaining(&self) -> u64;
    /// Check if this is a partial rebuild.
    fn is_partial(&self) -> bool;
    /// Get the rebuild descriptor reference.
    fn desc(&self) -> &RebuildDescriptor;
    /// Get the copier which can copy a segment.
    fn copier(&self) -> Rc<T>;
}

/// The range is the full range of the request, in steps of segment size.
pub(super) struct FullRebuild<T: RebuildTaskCopier> {
    range: PeekableIterator<std::iter::StepBy<Range<u64>>>,
    copier: Rc<T>,
}
impl<T: RebuildTaskCopier> FullRebuild<T> {
    /// Create a full rebuild with the given copier.
    pub(super) fn new(copier: T) -> Self {
        let desc = copier.descriptor();
        let range = desc.range.clone();
        Self {
            range: PeekableIterator::new(
                range.step_by(desc.segment_size_blks as usize),
            ),
            copier: Rc::new(copier),
        }
    }
}
impl<T: RebuildTaskCopier> RangeRebuilder<T> for FullRebuild<T> {
    fn next(&mut self) -> Option<u64> {
        self.range.next()
    }
    fn peek_next(&self) -> Option<u64> {
        self.range.peek().cloned()
    }

    fn blocks_remaining(&self) -> u64 {
        self.peek_next()
            .map(|r| self.desc().range.end.max(r) - r)
            .unwrap_or_default()
    }
    fn is_partial(&self) -> bool {
        false
    }

    fn desc(&self) -> &RebuildDescriptor {
        self.copier.descriptor()
    }
    fn copier(&self) -> Rc<T> {
        self.copier.clone()
    }
}

/// A partial rebuild range which steps through each segment but triggers
/// the copy only if the segment dirty bit is set.
pub(super) struct PartialRebuild<T: RebuildTaskCopier> {
    range: PeekableIterator<std::iter::Enumerate<bit_vec::IntoIter>>,
    segment_size_blks: u64,
    total_blks: u64,
    rebuilt_blks: u64,
    copier: Rc<T>,
}
impl<T: RebuildTaskCopier> PartialRebuild<T> {
    /// Create a partial sequential rebuild with the given copier and segment
    /// map.
    #[allow(dead_code)]
    pub(super) fn new(map: SegmentMap, copier: T) -> Self {
        let total_blks = map.count_dirty_blks();
        let segment_size_blks = map.segment_size_blks();
        let bit_vec: BitVec = map.into();
        Self {
            range: PeekableIterator::new(bit_vec.into_iter().enumerate()),
            total_blks,
            rebuilt_blks: 0,
            segment_size_blks,
            copier: Rc::new(copier),
        }
    }
}
impl<T: RebuildTaskCopier> RangeRebuilder<T> for PartialRebuild<T> {
    fn next(&mut self) -> Option<u64> {
        for (blk, is_set) in self.range.by_ref() {
            if is_set {
                self.rebuilt_blks += self.segment_size_blks;
                return Some(blk as u64);
            }
        }
        None
    }
    fn peek_next(&self) -> Option<u64> {
        // todo: should we add a wrapper to ensure we peek only set bits?
        self.range.peek().map(|(blk, _)| *blk as u64)
    }

    fn blocks_remaining(&self) -> u64 {
        self.total_blks - self.rebuilt_blks
    }
    fn is_partial(&self) -> bool {
        false
    }

    fn desc(&self) -> &RebuildDescriptor {
        self.copier.descriptor()
    }
    fn copier(&self) -> Rc<T> {
        self.copier.clone()
    }
}

/// The range is the full range of the request, in steps of segment size
/// and a copy is triggered for each segment.
/// However, during the copy itself, clean segments are skipped.
pub(super) struct PartialSeqRebuild<T: RebuildTaskCopier> {
    range: PeekableIterator<std::iter::StepBy<Range<u64>>>,
    copier: Rc<PartialSeqCopier<T>>,
}
impl<T: RebuildTaskCopier> PartialSeqRebuild<T> {
    /// Create a partial sequential rebuild with the given copier and segment
    /// map.
    pub(super) fn new(map: RebuildMap, copier: T) -> Self {
        let desc = copier.descriptor();
        let range = desc.range.clone();
        Self {
            range: PeekableIterator::new(
                range.step_by(desc.segment_size_blks as usize),
            ),
            copier: Rc::new(PartialSeqCopier::new(map, copier)),
        }
    }
}
impl<T: RebuildTaskCopier> RangeRebuilder<PartialSeqCopier<T>>
    for PartialSeqRebuild<T>
{
    fn next(&mut self) -> Option<u64> {
        self.range.next()
    }
    fn peek_next(&self) -> Option<u64> {
        self.range.peek().cloned()
    }

    fn blocks_remaining(&self) -> u64 {
        self.copier.map.lock().count_dirty_blks()
    }
    fn is_partial(&self) -> bool {
        true
    }

    fn desc(&self) -> &RebuildDescriptor {
        self.copier.descriptor()
    }
    fn copier(&self) -> Rc<PartialSeqCopier<T>> {
        self.copier.clone()
    }
}
/// The partial sequential rebuild copier, which uses a bitmap to determine if a
/// particular block range must be copied.
pub(super) struct PartialSeqCopier<T: RebuildTaskCopier> {
    map: parking_lot::Mutex<RebuildMap>,
    copier: T,
}
impl<T: RebuildTaskCopier> PartialSeqCopier<T> {
    fn new(map: RebuildMap, copier: T) -> Self {
        Self {
            map: parking_lot::Mutex::new(map),
            copier,
        }
    }
    /// Checks if the block has to be transferred.
    /// If no rebuild map is present, all blocks are considered unsynced.
    #[inline(always)]
    fn is_blk_sync(&self, blk: u64) -> bool {
        self.map.lock().is_blk_clean(blk)
    }

    /// Marks the rebuild segment starting from the given logical block as
    /// already transferred.
    #[inline(always)]
    fn blk_synced(&self, blk: u64) {
        self.map.lock().blk_clean(blk);
    }
}
#[async_trait::async_trait(?Send)]
impl<T: RebuildTaskCopier> RebuildTaskCopier for PartialSeqCopier<T> {
    fn descriptor(&self) -> &RebuildDescriptor {
        self.copier.descriptor()
    }

    /// Copies one segment worth of data from source into destination.
    async fn copy_segment(
        &self,
        blk: u64,
        task: &mut RebuildTask,
    ) -> Result<bool, RebuildError> {
        if self.is_blk_sync(blk) {
            return Ok(false);
        }

        let result = self.copier.copy_segment(blk, task).await;

        // In the case of success, mark the segment as already transferred.
        if result.is_ok() {
            self.blk_synced(blk);
        }

        result
    }
}

/// Adds peekable functionality to a generic iterator.
/// > Note: the peekable from the std library is not sufficient here because it
/// > requires a mutable reference to peek. We get around this limitation by
/// > always setting the peek at a small performance cost.
struct PeekableIterator<I: Iterator> {
    iter: I,
    peek: Option<I::Item>,
}
impl<I: Iterator> PeekableIterator<I> {
    fn new(mut iter: I) -> Self {
        Self {
            peek: iter.next(),
            iter,
        }
    }
    /// Peek into the future for the next value which next would yield.
    fn peek(&self) -> Option<&I::Item> {
        self.peek.as_ref()
    }
}
impl<I: Iterator> Iterator for PeekableIterator<I> {
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        std::mem::replace(&mut self.peek, self.iter.next())
    }
}
