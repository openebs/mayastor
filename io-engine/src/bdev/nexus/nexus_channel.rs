//!
//! IO is driven by means of so called channels.
use std::{fmt::Debug, pin::Pin};

use super::{ChildState, Nexus, Reason};

use crate::core::{BlockDeviceHandle, Cores, Mthread};

#[repr(C)]
struct Inner<'n> {
    writers: Vec<Box<dyn BlockDeviceHandle>>,
    readers: Vec<Box<dyn BlockDeviceHandle>>,
    previous: usize,
    fail_fast: u32,
    nexus: Pin<&'n mut Nexus<'n>>,
}

/// io channel, per core
#[repr(C)]
pub struct NexusChannel<'n> {
    inner: Box<Inner<'n>>,
}

impl<'n> Debug for NexusChannel<'n> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "readers = {}, writers = {}",
            self.inner.readers.len(),
            self.inner.writers.len()
        )
    }
}

#[derive(Debug)]
#[allow(clippy::enum_variant_names)]
/// Dynamic Reconfiguration Events occur when a child is added or removed
pub enum DrEvent {
    /// Child offline reconfiguration event
    ChildOffline,
    /// mark the child as faulted
    ChildFault,
    /// Child remove reconfiguration event
    ChildRemove,
    /// Child rebuild event
    ChildRebuild,
}

/// Mark nexus child as faulted based on its device name
pub(crate) fn fault_nexus_child(
    nexus: Pin<&mut Nexus>,
    device_name: &str,
) -> bool {
    nexus
        .children_iter()
        .filter(|c| c.state() == ChildState::Open)
        .filter(|c| {
            // If there were previous retires, we do not have a reference
            // to a BlockDevice. We do however, know it can't be the device
            // we are attempting to retire in the first place so this
            // condition is fine.
            if let Ok(child) = c.get_device().as_ref() {
                child.device_name() == device_name
            } else {
                false
            }
        })
        .any(|c| {
            Ok(ChildState::Open)
                == c.state.compare_exchange(
                    ChildState::Open,
                    ChildState::Faulted(Reason::IoError),
                )
        })
}

impl<'n> NexusChannel<'n> {
    /// TODO
    pub(crate) fn new(mut nexus: Pin<&mut Nexus<'n>>) -> Self {
        let mut writers = Vec::new();
        let mut readers = Vec::new();

        unsafe {
            nexus.as_mut().children_iter_mut()
                .filter(|c| c.state() == ChildState::Open)
                .for_each(|c| match (c.get_io_handle(), c.get_io_handle()) {
                    (Ok(w), Ok(r)) => {
                        writers.push(w);
                        readers.push(r);
                    }
                    _ => {
                        c.set_state(ChildState::Faulted(Reason::CantOpen));
                        error!("Failed to get I/O handle for {}, skipping block device", c.uri())
                    }
                });
        }

        let inner = Box::new(Inner {
            writers,
            readers,
            previous: 0,
            nexus: unsafe { nexus.pinned_mut() },
            fail_fast: 0,
        });

        Self {
            inner,
        }
    }

    /// TODO
    pub(crate) fn destroy(mut self) {
        self.inner.writers.clear();
        self.inner.readers.clear();
    }

    /// Returns reference to channel's Nexus.
    #[inline(always)]
    fn nexus(&self) -> &Nexus<'n> {
        &self.inner.nexus
    }

    /// Returns mutable reference to channel's Nexus.
    #[inline(always)]
    fn nexus_mut(&mut self) -> Pin<&mut Nexus<'n>> {
        self.inner.nexus.as_mut()
    }

    /// helper routine to get a channel to read from
    #[inline(always)]
    pub(super) fn reader_at(&self, i: usize) -> &dyn BlockDeviceHandle {
        &*self.inner.readers[i]
    }

    /// TODO
    #[inline(always)]
    pub(super) fn writers(&self) -> &Vec<Box<dyn BlockDeviceHandle>> {
        &self.inner.writers
    }

    /// very simplistic routine to rotate between children for read operations
    /// note that the channels can be None during a reconfigure; this is usually
    /// not the case but a side effect of using the async. As we poll
    /// threads more often depending on what core we are on etc, we might be
    /// "awaiting' while the thread is already trying to submit IO.
    pub(crate) fn child_select(&mut self) -> Option<usize> {
        if self.inner.readers.is_empty() {
            None
        } else {
            if self.inner.previous < self.inner.readers.len() - 1 {
                self.inner.previous += 1;
            } else {
                self.inner.previous = 0;
            }
            Some(self.inner.previous)
        }
    }

    /// Removes a child device from the readers and writers.
    pub fn remove_device(&mut self, device_name: &str) -> bool {
        self.inner.previous = 0;
        trace!(
            ?device_name,
            "core: {} thread: {} removing from during submission channels",
            Cores::current(),
            Mthread::current().unwrap().name()
        );
        trace!(
            "{}: Current number of IO channels write: {} read: {}",
            self.nexus().name,
            self.inner.writers.len(),
            self.inner.readers.len(),
        );
        self.inner
            .readers
            .retain(|c| c.get_device().device_name() != device_name);
        self.inner
            .writers
            .retain(|c| c.get_device().device_name() != device_name);

        trace!(?device_name,
            "core: {} thread: {}: New number of IO channels write:{} read:{} out of {} children",
            Cores::current(),
            Mthread::current().unwrap().name(),
            self.inner.writers.len(),
            self.inner.readers.len(),
            self.nexus().child_count()
        );

        self.fault_device(device_name)
    }

    /// Marks a child device as faulted.
    /// Returns true if the child was in open state, false otherwise.
    pub fn fault_device(&mut self, device_name: &str) -> bool {
        fault_nexus_child(self.nexus_mut(), device_name)
    }

    /// Refreshing our channels simply means that we either have a child going
    /// online or offline. We don't know which child has gone, or was added, so
    /// we simply put back all the channels, and reopen the bdevs that are in
    /// the online state.
    pub(crate) fn refresh(&mut self) {
        info!(
            "{}(thread:{:?}), refreshing IO channels",
            self.nexus().name,
            Mthread::current().unwrap().name(),
        );

        trace!(
            "{}: Current number of IO channels write: {} read: {}",
            self.nexus().name,
            self.inner.writers.len(),
            self.inner.readers.len(),
        );

        // clear the vector of channels and reset other internal values,
        // clearing the values will drop any existing handles in the
        // channel
        self.inner.previous = 0;

        // nvmx will drop the IO qpairs which is different from all other
        // bdevs we might be dealing with. So instead of clearing and refreshing
        // which had no side effects before, we create a new vector and
        // swap them out later

        let mut writers = Vec::new();
        let mut readers = Vec::new();

        // iterate over all our children which are in the open state
        unsafe {
            self.nexus_mut()
                .children_iter_mut()
                .filter(|c| c.state() == ChildState::Open)
                .for_each(|c| match (c.get_io_handle(), c.get_io_handle()) {
                    (Ok(w), Ok(r)) => {
                        writers.push(w);
                        readers.push(r);
                    }
                    _ => {
                        c.set_state(ChildState::Faulted(Reason::CantOpen));
                        error!("failed to get I/O handle for {}", c.uri());
                    }
                });
        }

        // then add write-only children
        if !self.inner.readers.is_empty() {
            unsafe {
                self.nexus_mut()
                    .children_iter_mut()
                    .filter(|c| c.rebuilding())
                    .for_each(|c| {
                        if let Ok(hdl) = c.get_io_handle() {
                            writers.push(hdl);
                        } else {
                            c.set_state(ChildState::Faulted(Reason::CantOpen));
                            error!("failed to get I/O handle for {}", c.uri());
                        }
                    });
            }
        }

        self.inner.writers.clear();
        self.inner.readers.clear();

        self.inner.writers = writers;
        self.inner.readers = readers;

        trace!(
            "{}: New number of IO channels write:{} read:{} out of {} children",
            self.nexus().name,
            self.inner.writers.len(),
            self.inner.readers.len(),
            self.nexus().child_count()
        );
    }
}
