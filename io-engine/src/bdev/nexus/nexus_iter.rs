use crate::bdev::{nexus::nexus_module::NexusModule, Nexus};
use spdk_rs::BdevModuleIter;
use std::pin::Pin;

/// Returns an immutable iterator for Nexus instances.
pub fn nexus_iter<'n>() -> NexusIter<'n> {
    NexusIter::new()
}

/// Returns an iterator for Nexus instances that allows
/// modifying an Nexus object.
pub fn nexus_iter_mut<'n>() -> NexusIterMut<'n> {
    NexusIterMut::new()
}

/// Looks up a Nexus by its name, and returns a reference to it.
pub fn nexus_lookup<'n>(
    name: &str,
) -> Option<<NexusIter<'n> as Iterator>::Item> {
    NexusIter::new().find(|n| n.name == name)
}

/// Looks up a Nexus by its name, and returns a mutable reference to it.
pub fn nexus_lookup_mut<'n>(
    name: &str,
) -> Option<<NexusIterMut<'n> as Iterator>::Item> {
    NexusIterMut::new().find(|n| n.name == name)
}

/// Looks up a Nexus by its name or uuid, and returns a reference to it.
pub fn nexus_lookup_name_uuid<'n>(
    name: &str,
    nexus_uuid: Option<uuid::Uuid>,
) -> Option<<NexusIter<'n> as Iterator>::Item> {
    NexusIter::new().find(|n| {
        n.name == name || (nexus_uuid.is_some() && Some(n.uuid()) == nexus_uuid)
    })
}

/// Looks up a Nexus by its uuid, and returns a mutable reference to it.
pub fn nexus_lookup_uuid_mut<'n>(
    uuid: &str,
) -> Option<<NexusIterMut<'n> as Iterator>::Item> {
    NexusIterMut::new().find(|n| n.uuid().to_string() == uuid)
}

/// Tries to extract nexus name from an NQN.
fn try_nqn_to_nexus_name(nqn: &str) -> Option<String> {
    let vec: Vec<&str> = nqn.split(':').collect();
    vec.get(1).map(ToString::to_string)
}

/// Looks up a Nexus by its subsystem NQN, and returns a reference to it.
pub fn nexus_lookup_nqn<'n>(
    nqn: &str,
) -> Option<<NexusIter<'n> as Iterator>::Item> {
    try_nqn_to_nexus_name(nqn).and_then(|n| nexus_lookup(&n))
}

/// Looks up a Nexus by its subsystem NQN, and returns a mutable reference to
/// it.
pub fn nexus_lookup_nqn_mut<'n>(
    nqn: &str,
) -> Option<<NexusIterMut<'n> as Iterator>::Item> {
    try_nqn_to_nexus_name(nqn).and_then(|n| nexus_lookup_mut(&n))
}

/// TODO
pub struct NexusIter<'n> {
    iter: BdevModuleIter<Nexus<'n>>,
}

impl<'n> Iterator for NexusIter<'n> {
    type Item = &'n Nexus<'n>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|b| b.data())
    }
}

impl NexusIter<'_> {
    fn new() -> Self {
        Self {
            iter: NexusModule::current().iter_bdevs(),
        }
    }
}

/// TODO
pub struct NexusIterMut<'n> {
    iter: BdevModuleIter<Nexus<'n>>,
}

impl<'n> Iterator for NexusIterMut<'n> {
    type Item = Pin<&'n mut Nexus<'n>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|mut b| b.data_mut())
    }
}

impl NexusIterMut<'_> {
    fn new() -> Self {
        Self {
            iter: NexusModule::current().iter_bdevs(),
        }
    }
}
