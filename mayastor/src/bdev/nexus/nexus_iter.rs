use crate::bdev::{nexus::nexus_module::NexusModule, Nexus};
use spdk_rs::BdevModuleIter;

/// Returns an immutable iterator for Nexus instances.
pub fn nexus_iter() -> NexusIter {
    NexusIter::new()
}

/// Returns an iterator for Nexus instances that allows
/// modifying an Nexus object.
pub fn nexus_iter_mut() -> NexusIterMut {
    NexusIterMut::new()
}

/// Looks up a Nexus by its name, and returns a reference to it.
pub fn nexus_lookup(name: &str) -> Option<&'static Nexus> {
    NexusIter::new().find(|n| n.name == name)
}

/// Looks up a Nexus by its name, and returns a mutable reference to it.
pub fn nexus_lookup_mut(name: &str) -> Option<&'static mut Nexus> {
    NexusIterMut::new().find(|n| n.name == name)
}

/// TODO
pub struct NexusIter {
    iter: BdevModuleIter<Nexus>,
}

impl Iterator for NexusIter {
    type Item = &'static Nexus;

    fn next(&mut self) -> Option<&'static Nexus> {
        self.iter.next().map(|b| b.data())
    }
}

impl NexusIter {
    fn new() -> Self {
        Self {
            iter: NexusModule::current().iter_bdevs(),
        }
    }
}

/// TODO
pub struct NexusIterMut {
    iter: BdevModuleIter<Nexus>,
}

impl Iterator for NexusIterMut {
    type Item = &'static mut Nexus;

    fn next(&mut self) -> Option<&'static mut Nexus> {
        self.iter.next().map(|mut b| b.data_mut())
    }
}

impl NexusIterMut {
    fn new() -> Self {
        Self {
            iter: NexusModule::current().iter_bdevs(),
        }
    }
}
