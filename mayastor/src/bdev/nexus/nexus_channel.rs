//!
//! IO is driven by means of so called channels.
use std::{ffi::c_void, fmt::Debug, ptr::NonNull};

use futures::channel::oneshot;

use spdk_sys::{
    spdk_for_each_channel,
    spdk_for_each_channel_continue,
    spdk_io_channel,
    spdk_io_channel_iter,
    spdk_io_channel_iter_get_channel,
    spdk_io_channel_iter_get_ctx,
    spdk_io_channel_iter_get_io_device,
};

use crate::{
    bdev::{nexus::nexus_child::ChildState, Nexus, Reason},
    core::{BlockDeviceHandle, Mthread},
};

/// io channel, per core
#[repr(C)]
#[derive(Debug)]
pub(crate) struct NexusChannel {
    inner: *mut NexusChannelInner,
}

#[repr(C)]
pub(crate) struct NexusChannelInner {
    pub(crate) writers: Vec<Box<dyn BlockDeviceHandle>>,
    pub(crate) readers: Vec<Box<dyn BlockDeviceHandle>>,
    pub(crate) previous: usize,
    device: *mut c_void,
}

impl Debug for NexusChannelInner {
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
/// reconfigure context holding among others
/// the completion channel.
pub struct ReconfigureCtx {
    /// channel to send completion on.
    sender: oneshot::Sender<i32>,
    device: NonNull<c_void>,
}

impl ReconfigureCtx {
    pub(crate) fn new(
        sender: oneshot::Sender<i32>,
        device: NonNull<c_void>,
    ) -> Self {
        Self {
            sender,
            device,
        }
    }
}

#[derive(Debug)]
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
    /// Child status information is being applied
    ChildStatusSync,
}

impl NexusChannelInner {
    /// very simplistic routine to rotate between children for read operations
    /// note that the channels can be None during a reconfigure; this is usually
    /// not the case but a side effect of using the async. As we poll
    /// threads more often depending on what core we are on etc, we might be
    /// "awaiting' while the thread is already trying to submit IO.
    pub(crate) fn child_select(&mut self) -> Option<usize> {
        if self.readers.is_empty() {
            None
        } else {
            if self.previous < self.readers.len() - 1 {
                self.previous += 1;
            } else {
                self.previous = 0;
            }
            Some(self.previous)
        }
    }

    /// refreshing our channels simply means that we either have a child going
    /// online or offline. We don't know which child has gone, or was added, so
    /// we simply put back all the channels, and reopen the bdevs that are in
    /// the online state.

    pub(crate) fn refresh(&mut self) {
        let nexus = unsafe { Nexus::from_raw(self.device) };
        info!(
            "{}(thread:{:?}), refreshing IO channels",
            nexus.name,
            Mthread::current().unwrap().name(),
        );

        trace!(
            "{}: Current number of IO channels write: {} read: {}",
            nexus.name,
            self.writers.len(),
            self.readers.len(),
        );

        // clear the vector of channels and reset other internal values,
        // clearing the values will drop any existing handles in the
        // channel
        self.writers.clear();
        self.readers.clear();
        self.previous = 0;

        // iterate over all our children which are in the open state
        nexus
            .children
            .iter_mut()
            .filter(|c| c.state() == ChildState::Open)
            .for_each(|c| match (c.get_io_handle(), c.get_io_handle()) {
                (Ok(w), Ok(r)) => {
                    self.writers.push(w);
                    self.readers.push(r);
                }
                _ => {
                    c.set_state(ChildState::Faulted(Reason::CantOpen));
                    error!("failed to get I/O handle for {}", c.get_name());
                }
            });

        // then add write-only children
        if !self.readers.is_empty() {
            nexus
                .children
                .iter_mut()
                .filter(|c| c.rebuilding())
                .for_each(|c| {
                    if let Ok(hdl) = c.get_io_handle() {
                        self.writers.push(hdl);
                    } else {
                        c.set_state(ChildState::Faulted(Reason::CantOpen));
                        error!("failed to get I/O handle for {}", c.get_name());
                    }
                });
        }

        trace!(
            "{}: New number of IO channels write:{} read:{} out of {} children",
            nexus.name,
            self.writers.len(),
            self.readers.len(),
            nexus.children.len()
        );

        //trace!("{:?}", nexus.children);
    }
}

impl NexusChannel {
    /// allocates an io channel per child
    pub(crate) extern "C" fn create(
        device: *mut c_void,
        ctx: *mut c_void,
    ) -> i32 {
        let nexus = unsafe { Nexus::from_raw(device) };
        debug!("{}: Creating IO channels at {:p}", nexus.bdev.name(), ctx);

        let ch = NexusChannel::from_raw(ctx);
        let mut channels = Box::new(NexusChannelInner {
            writers: Vec::new(),
            readers: Vec::new(),
            previous: 0,
            device,
        });

        nexus
            .children
            .iter_mut()
            .filter(|c| c.state() == ChildState::Open)
            .for_each(|c| match (c.get_io_handle(), c.get_io_handle()) {
                (Ok(w), Ok(r)) => {
                    channels.writers.push(w);
                    channels.readers.push(r);
                }
                _ => {
                    c.set_state(ChildState::Faulted(Reason::CantOpen));
                    error!("Failed to get I/O handle for {}, skipping block device", c.get_name())
                }
            });
        ch.inner = Box::into_raw(channels);
        0
    }

    /// function called on io channel destruction
    pub(crate) extern "C" fn destroy(device: *mut c_void, ctx: *mut c_void) {
        let nexus = unsafe { Nexus::from_raw(device) };
        debug!("{} Destroying IO channels", nexus.bdev.name());
        let inner = NexusChannel::from_raw(ctx).inner_mut();
        inner.writers.clear();
        inner.readers.clear();
    }

    /// function called when we receive a Dynamic Reconfigure event (DR)
    pub extern "C" fn reconfigure(
        device: *mut c_void,
        ctx: Box<ReconfigureCtx>,
        event: &DrEvent,
    ) {
        match event {
            DrEvent::ChildOffline
            | DrEvent::ChildRemove
            | DrEvent::ChildFault
            | DrEvent::ChildRebuild
            | DrEvent::ChildStatusSync => unsafe {
                spdk_for_each_channel(
                    device,
                    Some(NexusChannel::refresh_io_channels),
                    Box::into_raw(ctx).cast(),
                    Some(Self::reconfigure_completed),
                );
            },
        }
    }

    /// a generic callback for signaling that all cores have reconfigured
    pub extern "C" fn reconfigure_completed(
        ch_iter: *mut spdk_io_channel_iter,
        status: i32,
    ) {
        let nexus = unsafe {
            Nexus::from_raw(spdk_io_channel_iter_get_io_device(ch_iter))
        };

        let ctx: Box<ReconfigureCtx> = unsafe {
            Box::from_raw(
                spdk_io_channel_iter_get_ctx(ch_iter) as *mut ReconfigureCtx
            )
        };

        info!("{}: Reconfigure completed", nexus.name);
        ctx.sender.send(status).expect("reconfigure channel gone");
    }

    /// Refresh the IO channels of the underlying children. Typically, this is
    /// called when a device is either added or removed. IO that has already
    /// been issued may or may not complete. In case of remove that is fine.

    pub extern "C" fn refresh_io_channels(ch_iter: *mut spdk_io_channel_iter) {
        let channel = unsafe { spdk_io_channel_iter_get_channel(ch_iter) };
        let inner = Self::inner_from_channel(channel);
        inner.refresh();
        unsafe { spdk_for_each_channel_continue(ch_iter, 0) };
    }

    /// Converts a raw pointer to a nexusChannel. Note that the memory is not
    /// allocated by us.
    pub(crate) fn from_raw<'a>(n: *mut c_void) -> &'a mut Self {
        unsafe { &mut *(n as *mut NexusChannel) }
    }

    /// helper function to get a mutable reference to the inner channel
    /// FIXME; we can have several types of inner channels and so
    /// it would be nice to have that abstracted properly
    pub(crate) fn inner_from_channel<'a>(
        channel: *mut spdk_io_channel,
    ) -> &'a mut NexusChannelInner {
        NexusChannel::from_raw(Self::io_channel_ctx(channel)).inner_mut()
    }

    /// get the offset to our ctx from the channel
    fn io_channel_ctx(ch: *mut spdk_io_channel) -> *mut c_void {
        unsafe {
            use std::mem::size_of;
            (ch as *mut u8).add(size_of::<spdk_io_channel>()) as *mut c_void
        }
    }

    pub(crate) fn inner_mut(&mut self) -> &mut NexusChannelInner {
        unsafe { &mut *self.inner }
    }
}
