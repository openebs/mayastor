//!
//! IO is driven by means of so called channels.
use std::{convert::TryFrom, ffi::c_void};

use spdk_sys::{
    spdk_for_each_channel,
    spdk_for_each_channel_continue,
    spdk_io_channel,
    spdk_io_channel_iter,
    spdk_io_channel_iter_get_channel,
    spdk_io_channel_iter_get_io_device,
};

use crate::{
    bdev::nexus::{nexus_child::ChildState, Nexus},
    core::BdevHandle,
};

/// io channel, per core
#[repr(C)]
#[derive(Debug)]
pub(crate) struct NexusChannel {
    inner: *mut NexusChannelInner,
}

#[repr(C)]
#[derive(Debug)]
pub(crate) struct NexusChannelInner {
    pub(crate) ch: Vec<BdevHandle>,
    pub(crate) previous: usize,
    device: *mut c_void,
}

#[derive(Debug)]
/// Dynamic Reconfiguration Events occur when a child is added or removed
pub enum DREvent {
    /// Child offline reconfiguration event
    ChildOffline,
    /// Child online reconfiguration event
    ChildOnline,
    /// mark the child as faulted
    ChildFault,
    /// Child remove reconfiguration event
    ChildRemove,
}

impl NexusChannelInner {
    /// very simplistic routine to rotate between children for read operations
    pub(crate) fn child_select(&mut self) -> usize {
        if self.previous != self.ch.len() - 1 {
            self.previous += 1;
        } else {
            self.previous = 0;
        }
        self.previous
    }

    /// refreshing our channels simply means that we either have a child going
    /// online or offline. We don't know which child has gone, or was added, so
    /// we simply put back all the channels, and reopen the bdevs that are in
    /// the online state.

    pub(crate) fn refresh(&mut self) {
        let nexus = unsafe { Nexus::from_raw(self.device) };
        info!(
            "{}(tid:{:?}), refreshing IO channels",
            nexus.name,
            std::thread::current().name().unwrap()
        );

        trace!(
            "{}: Current number of IO channels {}",
            nexus.name,
            self.ch.len()
        );

        // clear the vector of channels and reset other internal values,
        // clearing the values will drop any existing handles in the
        // channel
        self.ch.clear();
        self.previous = 0;

        // iterate to over all our children which are in the open state
        nexus
            .children
            .iter_mut()
            .filter(|c| c.state == ChildState::Open)
            .map(|c| {
                self.ch.push(
                    BdevHandle::try_from(c.get_descriptor().unwrap()).unwrap(),
                )
            })
            .for_each(drop);

        trace!(
            "{}: New number of IO channels {} out of {} children",
            nexus.name,
            self.ch.len(),
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
            ch: Vec::new(),
            previous: 0,
            device,
        });

        nexus
            .children
            .iter_mut()
            .filter(|c| c.state == ChildState::Open)
            .map(|c| {
                channels.ch.push(
                    BdevHandle::try_from(c.get_descriptor().unwrap()).unwrap(),
                )
            })
            .for_each(drop);
        ch.inner = Box::into_raw(channels);
        0
    }

    /// function called on io channel destruction
    pub(crate) extern "C" fn destroy(device: *mut c_void, ctx: *mut c_void) {
        let nexus = unsafe { Nexus::from_raw(device) };
        debug!("{} Destroying IO channels", nexus.bdev.name());
        let inner = NexusChannel::from_raw(ctx).inner_mut();
        inner.ch.clear();
    }

    /// function called when we receive a Dynamic Reconfigure event (DR)
    pub extern "C" fn reconfigure(device: *mut c_void, event: &DREvent) {
        match event {
            DREvent::ChildOffline
            | DREvent::ChildOnline
            | DREvent::ChildRemove
            | DREvent::ChildFault => unsafe {
                spdk_for_each_channel(
                    device,
                    Some(NexusChannel::refresh_io_channels),
                    std::ptr::null_mut(),
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

        trace!("{}: Reconfigure completed", nexus.name);
        let sender = nexus.dr_complete_notify.take().unwrap();
        sender.send(status).expect("reconfigure channel gone");
    }

    /// Refresh the IO channels of the underlying children. Typically, this is
    /// called when a device is either added or removed. IO that has already
    /// may or may not complete. In case of remove that is fine.

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
    pub(crate) fn inner_from_channel<'a>(
        channel: *mut spdk_io_channel,
    ) -> &'a mut NexusChannelInner {
        NexusChannel::from_raw(Self::io_channel_ctx(channel)).inner_mut()
    }

    /// get the offset to our ctx tchannel
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
