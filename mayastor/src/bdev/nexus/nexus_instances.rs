use crate::{
    bdev::{nexus::nexus_module::NexusModule, Nexus},
    core::singleton::{Singleton, SingletonCell},
};
use spdk::{BdevModuleIter, Thread};
use std::ptr::NonNull;

/// TODO
#[derive(Debug)]
pub struct NexusInstances {
    nexuses: Vec<NonNull<Nexus>>,
}

impl Default for NexusInstances {
    fn default() -> Self {
        Self {
            nexuses: Vec::new(),
        }
    }
}

singleton!(NexusInstances);

impl NexusInstances {
    /// Returns instances, we ensure that this can only ever be called on a
    /// properly allocated thread.
    fn get_or_init() -> &'static mut Self {
        if let None = Thread::current() {
            panic!("Nexus instances must be accessed from an SPDK thread")
        }

        Singleton::get_or_init()
    }

    /// TODO
    pub fn add(n: NonNull<Nexus>) {
        let slf = Self::get_or_init();
        slf.nexuses.push(n);
    }

    /// TODO
    pub fn remove_by_name(name: &str) {
        let slf = Self::get_or_init();
        for (idx, p) in slf.nexuses.iter().enumerate() {
            if unsafe { p.as_ref() }.name != name {
                continue;
            }

            // unsafe { Box::from_raw(p.as_ptr()) };
            slf.nexuses.remove(idx);
            return;
        }
    }
}

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
