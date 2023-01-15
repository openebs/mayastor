//!
//! IO is driven by means of so called channels.
use std::{
    cell::UnsafeCell,
    fmt::{Debug, Display, Formatter},
    pin::Pin,
};

use super::{ChildState, Nexus, Reason};

use crate::{
    core::{BlockDeviceHandle, CoreError, Cores},
    rebuild::RebuildLogHandle,
};

/// io channel, per core
#[repr(C)]
pub struct NexusChannel<'n> {
    writers: Vec<Box<dyn BlockDeviceHandle>>,
    readers: Vec<Box<dyn BlockDeviceHandle>>,
    rebuild_logs: Vec<RebuildLogHandle>,
    previous_reader: UnsafeCell<usize>,
    fail_fast: u32,
    nexus: Pin<&'n mut Nexus<'n>>,
    core: u32,
}

impl<'n> Debug for NexusChannel<'n> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Channel '{} @ {}({})' [r:{}/w:{}/l:{}/c:{}]",
            self.nexus.nexus_name(),
            self.core,
            Cores::current(),
            self.readers.len(),
            self.writers.len(),
            self.rebuild_logs.len(),
            self.nexus.child_count(),
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
    /// Child unplug reconfiguration event
    ChildUnplug,
    /// Child rebuild event
    ChildRebuild,
}

impl Display for DrEvent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::ChildOffline => "offline",
                Self::ChildFault => "fault",
                Self::ChildUnplug => "unplug",
                Self::ChildRebuild => "rebuild",
            }
        )
    }
}

impl<'n> NexusChannel<'n> {
    /// TODO
    pub(crate) fn new(mut nexus: Pin<&mut Nexus<'n>>) -> Self {
        debug!("{:?}: new channel on core {}", nexus, Cores::current());

        let mut writers = Vec::new();
        let mut readers = Vec::new();
        let mut rebuild_logs = Vec::new();

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

        // add all active rebuild logs.
        unsafe {
            nexus
                .as_mut()
                .children_iter_mut()
                .filter(|c| c.is_logging())
                .for_each(|c| {
                    if let Some(log) = c.rebuild_log() {
                        rebuild_logs.push(log);
                    }
                });
        }

        Self {
            writers,
            readers,
            rebuild_logs,
            previous_reader: UnsafeCell::new(0),
            nexus: unsafe { nexus.pinned_mut() },
            fail_fast: 0,
            core: Cores::current(),
        }
    }

    /// TODO
    pub(crate) fn destroy(mut self) {
        debug!(
            "{:?}: destroying IO channel on core {}",
            self.nexus, self.core
        );
        self.writers.clear();
        self.readers.clear();
        self.rebuild_logs.clear();
    }

    /// Returns reference to channel's Nexus.
    #[inline(always)]
    #[allow(dead_code)]
    pub(super) fn nexus(&self) -> &Nexus<'n> {
        &self.nexus
    }

    /// Returns mutable reference to channel's Nexus.
    #[inline(always)]
    pub(super) fn nexus_mut(&mut self) -> Pin<&mut Nexus<'n>> {
        self.nexus.as_mut()
    }

    /// Calls the given callback for each active writer.
    #[inline(always)]
    pub(super) fn for_each_writer<F>(&self, mut f: F) -> Result<(), CoreError>
    where
        F: FnMut(&dyn BlockDeviceHandle) -> Result<(), CoreError>,
    {
        self.writers.iter().try_for_each(|h| f(h.as_ref()))
    }

    /// Calls the given callback for each active rebuild log.
    #[inline(always)]
    pub(super) fn for_each_rebuild_log<F>(&self, f: F)
    where
        F: FnMut(&RebuildLogHandle),
    {
        self.rebuild_logs.iter().for_each(f)
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

        self.readers
            .retain(|c| c.get_device().device_name() != device_name);
        self.writers
            .retain(|c| c.get_device().device_name() != device_name);

        debug!("{:?}: device '{}' disconnected", self, device_name);
    }

    /// Refreshing our channels simply means that we either have a child going
    /// online or offline. We don't know which child has gone, or was added, so
    /// we simply put back all the channels, and reopen the bdevs that are in
    /// the online state.
    pub(crate) fn reconnect_all(&mut self) {
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
        let mut rebuild_logs = Vec::new();

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
                    .filter(|c| c.is_rebuilding())
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

        // add all active rebuild logs.
        unsafe {
            self.nexus_mut()
                .children_iter_mut()
                .filter(|c| c.is_logging())
                .for_each(|c| {
                    if let Some(log) = c.rebuild_log() {
                        rebuild_logs.push(log);
                    }
                });
        }

        self.writers.clear();
        self.readers.clear();
        self.rebuild_logs.clear();

        self.writers = writers;
        self.readers = readers;
        self.rebuild_logs = rebuild_logs;

        debug!("{:?}: child devices reconnected", self);
    }

    /// Faults the devives by its name, with the given fault reason.
    /// The faulted device is scheduled to be retired.
    pub(super) fn fault_device(&mut self, child_device: &str, reason: Reason) {
        self.nexus_mut()
            .fault_child_device(child_device, reason, true);
    }

    /// Attaches a new rebuild log to write to.
    pub(super) fn attach_rebuild_log(&mut self, new_log: RebuildLogHandle) {
        if self
            .rebuild_logs
            .iter()
            .all(|log| log.device_name() != new_log.device_name())
        {
            self.rebuild_logs.push(new_log);
        }
    }

    /// Returns core on which channel was created.
    pub fn core(&self) -> u32 {
        self.core
    }
}
