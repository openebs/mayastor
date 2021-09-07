use std::cell::UnsafeCell;
use once_cell::sync::OnceCell;

use crate::bdev::Nexus;
use spdk::Thread;

/// TODO
#[derive(Default, Debug)]
pub struct NexusInstances {
    inner: UnsafeCell<Vec<Box<Nexus>>>,
}

unsafe impl Sync for NexusInstances {}
unsafe impl Send for NexusInstances {}

impl NexusInstances {
    /// Returns instances, we ensure that this can only ever be called on a
    /// properly allocated thread.
    fn get_or_init() -> &'static mut Vec<Box<Nexus>> {
        if let None = Thread::current() {
            panic!("Not called from an SPDK thread")
        }

        static NEXUS_INSTANCES: OnceCell<NexusInstances> = OnceCell::new();

        let global_instances = NEXUS_INSTANCES.get_or_init(|| NexusInstances {
            inner: UnsafeCell::new(Vec::new()),
        });

        unsafe { &mut *global_instances.inner.get() }
    }

    /// Returns a reference to the global Nexus list.
    pub fn as_ref() -> &'static Vec<Box<Nexus>> {
        Self::get_or_init()
    }

    /// Returns a mutable reference to the global Nexus list.
    pub fn as_mut() -> &'static mut Vec<Box<Nexus>> {
        Self::get_or_init()
    }
}

/// Lookup a nexus by its name (currently used only by test functions).
pub fn nexus_lookup(name: &str) -> Option<&mut Nexus> {
    NexusInstances::as_mut()
        .iter_mut()
        .find(|n| n.name == name)
        .map(AsMut::as_mut)
}
