use crate::{
    bdev::Nexus,
    core::singleton::{Singleton, SingletonCell},
};
use spdk::Thread;

/// TODO
#[derive(Debug)]
pub struct NexusInstances {
    nexuses: Vec<Box<Nexus>>,
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
    pub fn as_ref() -> &'static NexusInstances {
        Self::get_or_init()
    }

    /// TODO
    pub fn as_mut() -> &'static mut NexusInstances {
        Self::get_or_init()
    }

    /// Returns an immutable iterator for Nexus instances.
    pub fn iter(&self) -> NexusIter {
        NexusIter {
            n: 0,
        }
    }

    /// Returns an iterator for Nexus instances that allows
    /// modifying an Nexus object.
    pub fn iter_mut(&mut self) -> NexusIterMut {
        NexusIterMut {
            n: 0,
        }
    }

    /// TODO
    pub fn add(&mut self, n: Box<Nexus>) -> &mut Nexus {
        self.nexuses.push(n);
        self.nexuses.last_mut().unwrap()
    }

    /// Lookups a nexus by its name and returns a reference to it.
    pub fn lookup(&self, name: &str) -> Option<&Nexus> {
        self.iter()
            .find(|n| n.name == name)
    }

    /// Lookups a nexus by its name and returns a mutable reference to it.
    pub fn lookup_mut(&mut self, name: &str) -> Option<&mut Nexus> {
        self.iter_mut()
            .find(|n| n.name == name)
    }

    /// TODO
    pub fn clear(&mut self) {
        self.nexuses.clear();
    }

    /// TODO
    pub fn remove_by_name(&mut self, name: &str) {
        self.nexuses.retain(|x| x.name != name);
    }
}

/// TODO
pub struct NexusIter {
    n: usize,
}

impl Iterator for NexusIter {
    type Item = &'static Nexus;

    fn next(&mut self) -> Option<&'static Nexus> {
        let inst = NexusInstances::get_or_init();
        if self.n < inst.nexuses.len() {
            let i = self.n;
            self.n += 1;
            Some(&mut inst.nexuses[i])
        } else {
            None
        }
    }
}

/// TODO
pub struct NexusIterMut {
    n: usize,
}

impl Iterator for NexusIterMut {
    type Item = &'static mut Nexus;

    fn next(&mut self) -> Option<&'static mut Nexus> {
        let inst = NexusInstances::get_or_init();
        if self.n < inst.nexuses.len() {
            let i = self.n;
            self.n += 1;
            Some(&mut inst.nexuses[i])
        } else {
            None
        }
    }
}

/// Lookup a nexus by its name (currently used only by test functions).
pub fn nexus_lookup(name: &str) -> Option<&mut Nexus> {
    NexusInstances::as_mut().lookup_mut(name)
}
