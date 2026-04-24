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

/// Iterator over [`Lvol`] belonging to a specific [`crate::lvs::Lvs`].
/// # Safety
/// The list must not be modified whilst we are iterating over it.
/// This means you should not run any async code, or any other which would
/// lead to adding or removing lvols from the list.
pub struct LvsLvolIter {
    next: *mut spdk_rs::libspdk::spdk_lvol,
}

impl LvsLvolIter {
    /// Create a new [`LvsLvolIter`] for the specified [`crate::lvs::Lvs`].
    pub fn new(lvs: &crate::lvs::Lvs) -> Self {
        Self {
            next: lvs.as_inner_ref().lvols.tqh_first,
        }
    }
}

impl Iterator for LvsLvolIter {
    type Item = Lvol;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next.is_null() {
            return None;
        }
        let lvol = self.next;
        self.next = unsafe { *self.next }.link.tqe_next;
        Some(Lvol::from_inner_ptr(lvol))
    }
}
