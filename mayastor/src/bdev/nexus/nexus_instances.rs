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
    pub fn as_ref() -> &'static NexusInstances {
        Self::get_or_init()
    }

    /// TODO
    pub fn as_mut() -> &'static mut NexusInstances {
        Self::get_or_init()
    }

    /// Returns an immutable iterator for Nexus instances.
    pub fn iter(&self) -> NexusIter {
        NexusIter::new()
    }

    /// Returns an iterator for Nexus instances that allows
    /// modifying an Nexus object.
    pub fn iter_mut(&mut self) -> NexusIterMut {
        NexusIterMut::new()
    }

    /// TODO
    pub fn add(&mut self, n: NonNull<Nexus>) -> &mut Nexus {
        self.nexuses.push(n);
        unsafe { self.nexuses.last_mut().unwrap().as_mut() }
    }

    /// Lookups a nexus by its name and returns a reference to it.
    pub fn lookup(&self, name: &str) -> Option<&Nexus> {
        self.iter().find(|n| n.name == name)
    }

    /// Lookups a nexus by its name and returns a mutable reference to it.
    pub fn lookup_mut(&mut self, name: &str) -> Option<&mut Nexus> {
        info!("^^^^ lookup_mut: {}", name);
        unsafe {
            for (i, n) in self.nexuses.iter().enumerate() {
                info!("    -- {} :: {}", i, n.as_ref().name);
            }

            info!("-- count: {}", self.iter_mut().count());

            let r = self.iter_mut().find(|n| {
                info!("  -- check {}", n.name);
                n.name == name
            });

            match &r {
                Some(_) => info!("    ++ !"),
                None => info!("    ++ ?"),
            }

            r
        }
    }

    /// TODO
    pub fn clear(&mut self) {
        self.nexuses.clear();
    }

    /// TODO
    pub fn remove_by_name(&mut self, name: &str) {
        info!("^^^^ Removing Nexus from list: {}", name);
        for (idx, p) in self.nexuses.iter().enumerate() {
            if unsafe { p.as_ref() }.name != name {
                continue;
            }

            // unsafe { Box::from_raw(p.as_ptr()) };
            self.nexuses.remove(idx);
            return;
        }

        warn!("^^^^ Nexus to be removed not found: {}!", name);
    }
}

/// Lookup a nexus by its name (currently used only by test functions).
pub fn nexus_lookup(name: &str) -> Option<&mut Nexus> {
    NexusInstances::as_mut().lookup_mut(name)
}



//////////////////////////////////////////////////////////////////////////////

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
            Some(unsafe { inst.nexuses[i].as_ref() })
        } else {
            None
        }
    }
}

impl NexusIter {
    fn new() -> Self {
        info!("^^^^ new iter");
        Self {
            n: 0,
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
            Some(unsafe { inst.nexuses[i].as_mut() })
        } else {
            None
        }
    }
}

impl NexusIterMut {
    fn new() -> Self {
        info!("^^^^ new iter mut");
        Self {
            n: 0,
        }
    }
}

//------------------------------------------------------------------------------

/*

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
        info!("^^^^ new iter");
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
        info!("^^^^ new iter mut");
        Self {
            iter: NexusModule::current().iter_bdevs(),
        }
    }
}

*/
