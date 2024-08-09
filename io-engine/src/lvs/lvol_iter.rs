use super::Lvol;
use crate::core::BdevIter;

/// Iterator over available Lvs Lvol's.
pub(crate) struct LvolIter(BdevIter<()>);

impl LvolIter {
    /// Returns a new Lvol iterator.
    pub(crate) fn new() -> Self {
        Self(BdevIter::new())
    }
}

impl Iterator for LvolIter {
    type Item = Lvol;

    fn next(&mut self) -> Option<Self::Item> {
        // notice we're hiding a potential inner loop here
        // only way around this would be to have the iterator return an
        // Option<Option<>> which perhaps is a bit confusing
        for bdev in self.0.by_ref() {
            if let Some(lvol) = Lvol::ok_from(bdev) {
                return Some(lvol);
            }
        }
        None
    }
}
