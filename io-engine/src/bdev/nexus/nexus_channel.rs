//!
//! IO is driven by means of so called channels.
use std::{
    cell::UnsafeCell,
    fmt::{Debug, Display, Formatter},
    pin::Pin,
    sync::atomic::Ordering,
};

use super::{FaultReason, IOLogChannel, Nexus, NexusBio};

use crate::core::{BlockDeviceHandle, CoreError, Cores};
use spdk_rs::Thread;

/// I/O channel, per core.
#[repr(C)]
pub struct NexusChannel<'n> {
    writers: Vec<Box<dyn BlockDeviceHandle>>,
    readers: Vec<Box<dyn BlockDeviceHandle>>,
    detached: Vec<Box<dyn BlockDeviceHandle>>,
    io_logs: Vec<IOLogChannel>,
    previous_reader: UnsafeCell<usize>,
    fail_fast: u32,
    io_mode: IoMode,
    frozen_ios: Vec<NexusBio<'n>>,
    nexus: Pin<&'n mut Nexus<'n>>,
    core: u32,
    is_io_chan: bool,
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
            l = self.io_logs.len(),
            c = self.nexus.child_count(),
        )
    }
}

/// Channel I/O disposition.
#[derive(Debug, Copy, Clone)]
pub enum IoMode {
    /// I/Os are running normally.
    Normal,
    /// I/O submissions are frozen.
    Freeze,
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

        let b_init_thrd_hdls =
            super::ENABLE_IO_ALL_THRD_NX_CHAN.load(Ordering::SeqCst);
        let is_io_chan =
            Thread::current().unwrap() != Thread::primary() || b_init_thrd_hdls;

        let mut writers = Vec::new();
        let mut readers = Vec::new();

        if is_io_chan {
            nexus.children_iter().filter(|c| c.is_healthy()).for_each(
                |c| match (c.get_io_handle(), c.get_io_handle()) {
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
                },
            );
        } else {
            // If we are here, this means the nexus channel being created is not
            // the one to be used for normal IOs. Such a channel is
            // created in rebuild path today, and it's on the init
            // thread. The channels that we use for normal nexus IO
            // are not on init thread however, those are on spdk threads
            // created for nvmf target during poll group init. Those are the
            // spdk threads named mayastor_nvmf_tcp_pg_core_*. The
            // channel on init thread is only used for rebuild IOs.
            // And the rebuild IOs are dispatched by
            // directly calling write API without going via writers abstraction.
            // Refer GTM-1075 for the race condition details.
            debug!("{nexus:?}: skip nexus channel setup({t:?}). is_io_channel: {is_io_chan}", t = Thread::current().unwrap());
        }

        Self {
            writers,
            readers,
            detached: Vec::new(),
            io_logs: nexus.io_log_channels(),
            previous_reader: UnsafeCell::new(0),
            nexus: unsafe { nexus.pinned_mut() },
            fail_fast: 0,
            io_mode: IoMode::Normal,
            frozen_ios: Vec::new(),
            core: Cores::current(),
            is_io_chan,
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
        self.io_logs.clear();
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

    // Returns a bool indicating whether this channel is setup for normal IOs.
    pub(crate) fn is_io_channel(&self) -> bool {
        self.is_io_chan
    }

    /// Calls the given callback for each active writer.
    #[inline(always)]
    pub(super) fn for_each_writer<F>(&self, mut f: F) -> Result<(), CoreError>
    where
        F: FnMut(&dyn BlockDeviceHandle) -> Result<(), CoreError>,
    {
        self.writers.iter().try_for_each(|h| f(h.as_ref()))
    }

    /// Calls the given callback for each active I/O log.
    #[inline(always)]
    pub(super) fn for_each_io_log<F>(&self, f: F)
    where
        F: FnMut(&IOLogChannel),
    {
        self.io_logs.iter().for_each(f)
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

    /// Detaches a child device from this I/O channel, moving the device's
    /// handles to the list of detached devices to disconnect later.
    ///
    /// The detached handles must be disconnected and dropped by a
    /// `disconnect_detached_devices()` call.
    pub(super) fn detach_device(&mut self, device_name: &str) {
        self.previous_reader = UnsafeCell::new(0);

        if let Some(d) = self
            .readers
            .iter()
            .position(|c| c.get_device().device_name() == device_name)
        {
            let t = self.readers.remove(d);
            self.detached.push(t);
        }

        if let Some(d) = self
            .writers
            .iter()
            .position(|c| c.get_device().device_name() == device_name)
        {
            let t = self.writers.remove(d);
            self.detached.push(t);
        }

        // Since we've removed the device from the IO path, make sure we
        // reconnect the io logs in case we haven't done so yet.
        // Otherwise, a given channel might never see an error for this device
        // and will therefore not log the IOs until a reconnect_io_logs.
        self.reconnect_io_logs();

        debug!("{self:?}: device '{device_name}' detached");
    }

    /// Disconnects previously detached device handles by dropping them.
    /// Devices to drop are filtered by the given predicate: true to drop
    /// a device, false to keep it.
    pub(super) fn disconnect_detached_devices<F>(&mut self, mut drop_pred: F)
    where
        F: FnMut(&dyn BlockDeviceHandle) -> bool,
    {
        let n = self.detached.len();
        info!("{self:?}: disconnecting {n} detached device handles...");

        self.detached.retain(|h| !drop_pred(h.as_ref()));

        let m = self.detached.len();
        if m == 0 {
            info!("{self:?}: all detached device handles disconnected");
        } else {
            let d = n - m;
            info!(
                "{self:?}: {d} detached device handle(s) disconnected, \
                {m} remain(s)"
            );
        }
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
        if !readers.is_empty() {
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

        self.writers = writers;
        self.readers = readers;

        self.reconnect_io_logs();

        debug!("{self:?}: child devices reconnected");
    }

    /// Reconnects all active I/O logs.
    pub(super) fn reconnect_io_logs(&mut self) {
        self.io_logs = self.nexus().io_log_channels();
    }

    /// Faults the child by its device, with the given fault reason.
    /// The faulted device is scheduled to be retired.
    pub(super) fn fault_device(
        &mut self,
        child_device: &str,
        reason: FaultReason,
    ) -> Option<IOLogChannel> {
        self.nexus_mut()
            .retire_child_device(child_device, reason, true)
    }

    /// Returns core on which channel was created.
    pub fn core(&self) -> u32 {
        self.core
    }

    /// Sets the current I/O mode for this channel.
    pub(super) fn set_io_mode(&mut self, io_mode: IoMode) {
        self.io_mode = io_mode;
        debug!("{self:?}: setting I/O mode to {io_mode:?}");
        if matches!(self.io_mode, IoMode::Normal) {
            self.resubmit_frozen();
        }
    }

    /// Determines if I/Os are frozen.
    pub(super) fn is_frozen(&self) -> bool {
        matches!(self.io_mode, IoMode::Freeze)
    }

    /// Resubmits all frozen I/Os.
    fn resubmit_frozen(&mut self) {
        debug!(
            "{self:?}: resubmitting {n} frozen I/Os ...",
            n = self.frozen_ios.len()
        );

        self.frozen_ios.drain(..).for_each(|io| {
            trace!("{io:?}: resubmitting a frozen I/O");
            io.submit_request();
        });
    }

    /// Aborts all frozen I/Os.
    pub(super) fn abort_frozen(&mut self) {
        debug!(
            "{self:?}: aborting {n} frozen I/Os ...",
            n = self.frozen_ios.len()
        );

        self.frozen_ios.drain(..).for_each(|io| {
            trace!("{io:?}: aborting a frozen I/O");
            io.fail();
        });
    }

    /// Freezes submission of the given Nexus I/O.
    pub(super) fn freeze_io_submission(&mut self, io: NexusBio<'n>) {
        trace!("{io:?}: freezing I/O");
        self.frozen_ios.push(io)
    }
}
