use std::fmt::{Debug, Formatter};

use crate::core::SegmentMap;

/// Map of segments to be rebuilt.
pub(crate) struct RebuildMap {
    /// Name of the underlying block device.
    device_name: String,
    /// Map of device segments.
    segments: SegmentMap,
}

impl Debug for RebuildMap {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Rebuild map: '{dev}' ({segs:?})",
            dev = self.device_name,
            segs = self.segments,
        )
    }
}

impl RebuildMap {
    /// Creates new rebuild map from the given segment map.
    pub(crate) fn new(device_name: &str, segments: SegmentMap) -> Self {
        Self {
            device_name: device_name.to_string(),
            segments,
        }
    }

    /// Determines if the given logical block is clean (no need to transfer).
    ///
    /// # Arguments
    ///
    /// * `lbn`: Logical block number.
    pub(crate) fn is_blk_clean(&self, lbn: u64) -> bool {
        match self.segments.get(lbn) {
            Some(v) => !v,
            None => {
                error!(
                    "{self:?}: accessing rebuild map beyond its segment \
                    range: {lbn}"
                );
                false
            }
        }
    }

    /// Marks the given logical block as clean (e.g. already transferred).
    ///
    /// # Arguments
    ///
    /// * `lbn`: Logical block number.
    pub(crate) fn blk_clean(&mut self, lbn: u64) {
        self.segments.set(lbn, 1, false);
    }

    /// Counts the total number of dirty (to be transferred) blocks.
    pub(crate) fn count_dirty_blks(&self) -> u64 {
        self.segments.count_dirty_blks()
    }
}
