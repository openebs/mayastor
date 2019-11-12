//!
//! IO is driven by means of so called channels.
use crate::bdev::nexus::{nexus_child::ChildState, Nexus};
use spdk_sys::{
    spdk_bdev_desc,
    spdk_for_each_channel,
    spdk_for_each_channel_continue,
    spdk_io_channel,
    spdk_io_channel_iter,
    spdk_io_channel_iter_get_channel,
    spdk_io_channel_iter_get_io_device,
    spdk_put_io_channel,
};
use std::ffi::c_void;

/// io channel, per core
#[repr(C)]
#[derive(Debug)]
pub(crate) struct NexusChannel {
    inner: *mut NexusChannelInner,
}

#[derive(Debug)]
pub(crate) struct NexusChannelInner {
    pub(crate) ch: Vec<(*mut spdk_bdev_desc, *mut spdk_io_channel)>,
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
    /// online or offline. We dont know which child has gone, or was added so
    /// we simply put back all the channels, and reopen them bdevs that are in
    /// the online state.
    ///
    /// Every core has its own copy of NexusChannelInner, so we this must be
    /// executed for each core.

    pub(crate) fn refresh(&mut self) {
        let nexus = unsafe { Nexus::from_raw(self.device) };
        info!(
            "{}(tid:{:?}), refreshing IO channels",
            nexus.name(),
            std::thread::current().name().unwrap()
        );

        trace!(
            "{}: Current number of IO channels {}",
            nexus.name(),
            self.ch.len()
        );

        // put the IO channels back, if a device is removed the resources will
        // not be reclaimed as long as there is a reference to the the channel
        self.ch
            .iter_mut()
            .map(|c| unsafe { spdk_put_io_channel(c.1) })
            .for_each(drop);

        // clear the vector of channels and reset other internal values
        self.ch.clear();
        self.previous = 0;

        // iterate to over all our children which are in the open state
        nexus
            .children
            .iter_mut()
            .filter(|c| c.state == ChildState::Open)
            .map(|c| {
                info!(
                    "{}: Getting new channel for child {} desc {:p}",
                    c.parent, c.name, c.desc
                );
                self.ch.push((c.desc, c.get_io_channel()))
            })
            .for_each(drop);

        trace!(
            "{}: New number of IO channels {}",
            nexus.name(),
            self.ch.len()
        );
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
            .map(|c| channels.ch.push((c.desc, c.get_io_channel())))
            .for_each(drop);
        ch.inner = Box::into_raw(channels);
        0
    }

    /// function called on io channel destruction
    pub(crate) extern "C" fn destroy(device: *mut c_void, ctx: *mut c_void) {
        let nexus = unsafe { Nexus::from_raw(device) };
        debug!("{} Destroying IO channels", nexus.bdev.name());
        let inner = NexusChannel::from_raw(ctx).inner_mut();
        inner
            .ch
            .iter()
            .map(|e| unsafe { spdk_put_io_channel(e.1) })
            .for_each(drop);
    }

    /// function called when we receive a Dynamic Reconfigure event (DR)
    pub extern "C" fn reconfigure(device: *mut c_void, event: &DREvent) {
        match event {
            DREvent::ChildOffline | DREvent::ChildOnline => unsafe {
                spdk_for_each_channel(
                    device,
                    Some(NexusChannel::refresh_io_channels),
                    std::ptr::null_mut(),
                    Some(Self::reconfigure_completed),
                );
            },
        }
    }

    /// generic callback for signaling that all cores have reconfigured
    pub extern "C" fn reconfigure_completed(
        ch_iter: *mut spdk_io_channel_iter,
        status: i32,
    ) {
        let nexus = unsafe {
            Nexus::from_raw(spdk_io_channel_iter_get_io_device(ch_iter))
        };

        trace!("{}: Reconfigure completed", nexus.name());
        let sender = nexus.dr_complete_notify.take().unwrap();
        sender.send(status).expect("reconfigure channel gone");
    }

    /// Refresh the IO channels of the underlying children. Typically this is
    /// called when a device is either added or removed. IO that has already
    /// may or may not complete. In case of remove that is fine.

    pub extern "C" fn refresh_io_channels(ch_iter: *mut spdk_io_channel_iter) {
        let channel = unsafe { spdk_io_channel_iter_get_channel(ch_iter) };
        let inner = Self::inner_from_channel(channel);
        inner.refresh();
        unsafe { spdk_for_each_channel_continue(ch_iter, 0) };
    }

    /// converts a raw pointer to a nexusChannel
    pub(crate) fn from_raw<'a>(n: *mut c_void) -> &'a mut Self {
        unsafe { &mut *(n as *mut NexusChannel) }
    }

    /// helper function to get a mutable reference to the inner channel
    pub(crate) fn inner_from_channel<'a>(
        channel: *mut spdk_io_channel,
    ) -> &'a mut NexusChannelInner {
        NexusChannel::from_raw(Self::io_channel_ctx(channel)).inner_mut()
    }

    /// get the offset to the our channel
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
