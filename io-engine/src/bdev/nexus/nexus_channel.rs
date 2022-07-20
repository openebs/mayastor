//!
//! IO is driven by means of so called channels.
use std::{cell::UnsafeCell, fmt::Debug, pin::Pin};

use super::{ChildState, Nexus, Reason};

use crate::core::{BlockDeviceHandle, CoreError, Cores, Mthread};

/// io channel, per core
#[repr(C)]
pub struct NexusChannel<'n> {
    writers: Vec<Box<dyn BlockDeviceHandle>>,
    readers: Vec<Box<dyn BlockDeviceHandle>>,
    previous_reader: UnsafeCell<usize>,
    fail_fast: u32,
    nexus: Pin<&'n mut Nexus<'n>>,
}

impl<'n> Debug for NexusChannel<'n> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "readers = {}, writers = {}",
            self.readers.len(),
            self.writers.len()
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

        Self {
            writers,
            readers,
            previous_reader: UnsafeCell::new(0),
            nexus: unsafe { nexus.pinned_mut() },
            fail_fast: 0,
        }
    }

    /// TODO
    pub(crate) fn destroy(mut self) {
        self.writers.clear();
        self.readers.clear();
    }

    /// Returns reference to channel's Nexus.
    #[inline(always)]
    pub(super) fn nexus(&self) -> &Nexus<'n> {
        &self.nexus
    }

    /// Returns mutable reference to channel's Nexus.
    #[inline(always)]
    pub(super) fn nexus_mut(&mut self) -> Pin<&mut Nexus<'n>> {
        self.nexus.as_mut()
    }

    /// TODO
    #[inline(always)]
    pub(super) fn for_each_writer<F>(&self, mut f: F) -> Result<(), CoreError>
    where
        F: FnMut(&dyn BlockDeviceHandle) -> Result<(), CoreError>,
    {
        self.writers.iter().try_for_each(|h| f(h.as_ref()))
    }

    /// very simplistic routine to rotate between children for read operations
    /// note that the channels can be None during a reconfigure; this is usually
    /// not the case but a side effect of using the async. As we poll
    /// threads more often depending on what core we are on etc, we might be
    /// "awaiting' while the thread is already trying to submit IO.
    pub(crate) fn select_reader(&self) -> Option<&dyn BlockDeviceHandle> {
        if self.readers.is_empty() {
            None
        } else {
            let idx = unsafe {
                let idx = &mut *self.previous_reader.get();
                if *idx < self.readers.len() - 1 {
                    *idx += 1;
                } else {
                    *idx = 0;
                }
                *idx
            };
            Some(self.readers[idx].as_ref())
        }
    }

    /// Disconnects a child device from the I/O path.
    pub fn disconnect_device(&mut self, device_name: &str) {
        self.previous_reader = UnsafeCell::new(0);

        trace!(
            ?device_name,
            "core: {} thread: {} removing from during submission channels",
            Cores::current(),
            Mthread::current().unwrap().name()
        );

        trace!(
            "{}: Current number of IO channels write: {} read: {}",
            self.nexus().name,
            self.writers.len(),
            self.readers.len(),
        );

        self.readers
            .retain(|c| c.get_device().device_name() != device_name);
        self.writers
            .retain(|c| c.get_device().device_name() != device_name);

        trace!(?device_name,
            "core: {} thread: {}: New number of IO channels write:{} read:{} out of {} children",
            Cores::current(),
            Mthread::current().unwrap().name(),
            self.writers.len(),
            self.readers.len(),
            self.nexus().child_count()
        );
    }

    /// Refreshing our channels simply means that we either have a child going
    /// online or offline. We don't know which child has gone, or was added, so
    /// we simply put back all the channels, and reopen the bdevs that are in
    /// the online state.
    pub(crate) fn reconnect_all(&mut self) {
        info!(
            "{}(thread:{:?}), refreshing IO channels",
            self.nexus().name,
            Mthread::current().unwrap().name(),
        );

        trace!(
            "{}: Current number of IO channels write: {} read: {}",
            self.nexus().name,
            self.writers.len(),
            self.readers.len(),
        );

        // clear the vector of channels and reset other internal values,
        // clearing the values will drop any existing handles in the
        // channel
        self.previous_reader = UnsafeCell::new(0);

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
        if !self.readers.is_empty() {
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

        self.writers.clear();
        self.readers.clear();

        self.writers = writers;
        self.readers = readers;

        trace!(
            "{}: New number of IO channels write:{} read:{} out of {} children",
            self.nexus().name,
            self.writers.len(),
            self.readers.len(),
            self.nexus().child_count()
        );
    }
}
