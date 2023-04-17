//!
//! IO is driven by means of so called channels.
use std::{
    cell::UnsafeCell,
    fmt::{Debug, Display, Formatter},
    pin::Pin,
};

use super::{FaultReason, Nexus};

use crate::{
    core::{BlockDeviceHandle, CoreError, Cores},
    rebuild::RebuildLogHandle,
};

/// I/O channel, per core.
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
            "I/O chan '{nex}' core:{core}({cur}) [R:{r} W:{w} L:{l} C:{c}]",
            nex = self.nexus.nexus_name(),
            core = self.core,
            cur = Cores::current(),
            r = self.readers.len(),
            w = self.writers.len(),
            l = self.rebuild_logs.len(),
            c = self.nexus.child_count(),
        )
    }
}

#[derive(Debug)]
#[allow(clippy::enum_variant_names)]
/// Dynamic Reconfiguration Events occur when a child is added or removed
pub enum DrEvent {
    /// Child unplug reconfiguration event.
    ChildUnplug,
    /// Child rebuild event.
    ChildRebuild,
}

impl Display for DrEvent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::ChildUnplug => "unplug",
                Self::ChildRebuild => "rebuild",
            }
        )
    }
}

impl<'n> NexusChannel<'n> {
    /// TODO
    pub(crate) fn new(nexus: Pin<&mut Nexus<'n>>) -> Self {
        debug!("{nexus:?}: new channel on core {c}", c = Cores::current());

        let mut writers = Vec::new();
        let mut readers = Vec::new();
        let mut rebuild_logs = Vec::new();

        nexus
            .children_iter()
            .filter(|c| c.is_healthy())
            .for_each(|c| match (c.get_io_handle(), c.get_io_handle()) {
                (Ok(w), Ok(r)) => {
                    writers.push(w);
                    readers.push(r);
                }
                _ => {
                    c.set_faulted_state(FaultReason::CantOpen);
                    error!(
                        "Failed to get I/O handle for {c}, \
                        skipping block device",
                        c = c.uri()
                    )
                }
            });

        // Add all active rebuild logs.
        nexus
            .children_iter()
            .filter(|c| !c.is_rebuilding())
            .for_each(|c| {
                if let Some(log) = c.rebuild_log() {
                    rebuild_logs.push(log);
                }
            });

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
            "{nex:?}: destroying I/O channel on core {core}",
            nex = self.nexus,
            core = self.core
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

    /// Returns the total number of available readers in this channel.
    pub(crate) fn num_readers(&self) -> usize {
        self.readers.len()
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

    /// Very simplistic routine to rotate between children for read operations
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

        debug!("{self:?}: device '{device_name}' disconnected");
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

        // nvmx will drop the I/O qpairs which is different from all other
        // bdevs we might be dealing with. So instead of clearing and refreshing
        // which had no side effects before, we create a new vector and
        // swap them out later

        let mut writers = Vec::new();
        let mut readers = Vec::new();
        let mut rebuild_logs = Vec::new();

        // iterate over all our children which are in the healthy state
        self.nexus()
            .children_iter()
            .filter(|c| c.is_healthy())
            .for_each(|c| match (c.get_io_handle(), c.get_io_handle()) {
                (Ok(w), Ok(r)) => {
                    writers.push(w);
                    readers.push(r);
                }
                _ => {
                    c.set_faulted_state(FaultReason::CantOpen);
                    error!("{self:?}: failed to get I/O handle for {c:?}");
                }
            });

        // then add write-only children
        if !self.readers.is_empty() {
            self.nexus()
                .children_iter()
                .filter(|c| c.is_rebuilding())
                .for_each(|c| match c.get_io_handle() {
                    Ok(hdl) => {
                        debug!(
                            "{self:?}: connecting child device \
                                in write-only mode: {c:?}"
                        );
                        writers.push(hdl);
                    }
                    Err(e) => {
                        c.set_faulted_state(FaultReason::CantOpen);
                        error!(
                            "{self:?}: failed to get I/O handle \
                                for {c:?}: {e}"
                        );
                    }
                });
        }

        // add all active rebuild logs.
        self.nexus()
            .children_iter()
            .filter(|c| !c.is_rebuilding())
            .for_each(|c| {
                if let Some(log) = c.rebuild_log() {
                    rebuild_logs.push(log);
                }
            });

        self.writers.clear();
        self.readers.clear();
        self.rebuild_logs.clear();

        self.writers = writers;
        self.readers = readers;
        self.rebuild_logs = rebuild_logs;

        debug!("{self:?}: child devices reconnected");
    }

    /// Faults the child by its device, with the given fault reason.
    /// The faulted device is scheduled to be retired.
    pub(super) fn fault_device(
        &mut self,
        child_device: &str,
        reason: FaultReason,
    ) -> Option<RebuildLogHandle> {
        self.nexus_mut()
            .retire_child_device(child_device, reason, true)
    }

    /// Attaches a new rebuild log to write to.
    pub(super) fn attach_rebuild_log(&mut self, log: RebuildLogHandle) {
        if self
            .rebuild_logs
            .iter()
            .all(|i| i.device_name() != log.device_name())
        {
            debug!(
                "{self:?}: attaching rebuild log for '{dev}'",
                dev = log.device_name()
            );

            self.rebuild_logs.push(log);
        }
    }

    /// Returns core on which channel was created.
    pub fn core(&self) -> u32 {
        self.core
    }
}
