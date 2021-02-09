/* I/O channel for NVMe controller, one per core. */

use crate::{
    bdev::dev::nvmx::{NvmeControllerState, NVME_CONTROLLERS},
    core::poller,
    subsys::NvmeBdevOpts,
};
use std::{cmp::max, mem::size_of, os::raw::c_void, ptr::NonNull};

use spdk_sys::{
    nvme_qpair_abort_reqs,
    spdk_io_channel,
    spdk_nvme_ctrlr,
    spdk_nvme_ctrlr_alloc_io_qpair,
    spdk_nvme_ctrlr_connect_io_qpair,
    spdk_nvme_ctrlr_disconnect_io_qpair,
    spdk_nvme_ctrlr_free_io_qpair,
    spdk_nvme_ctrlr_get_default_io_qpair_opts,
    spdk_nvme_ctrlr_reconnect_io_qpair,
    spdk_nvme_io_qpair_opts,
    spdk_nvme_poll_group,
    spdk_nvme_poll_group_add,
    spdk_nvme_poll_group_create,
    spdk_nvme_poll_group_destroy,
    spdk_nvme_poll_group_process_completions,
    spdk_nvme_poll_group_remove,
    spdk_nvme_qpair,
    spdk_put_io_channel,
};

#[repr(C)]
pub struct NvmeIoChannel<'a> {
    inner: *mut NvmeIoChannelInner<'a>,
}

impl<'a> NvmeIoChannel<'a> {
    #[inline]
    fn from_raw(p: *mut c_void) -> &'a mut NvmeIoChannel<'a> {
        unsafe { &mut *(p as *mut NvmeIoChannel) }
    }

    #[inline]
    fn inner_mut(&mut self) -> &'a mut NvmeIoChannelInner<'a> {
        unsafe { &mut *self.inner }
    }

    #[inline]
    pub fn inner_from_channel(
        io_channel: *mut spdk_io_channel,
    ) -> &'a mut NvmeIoChannelInner<'a> {
        NvmeIoChannel::from_raw(Self::io_channel_ctx(io_channel)).inner_mut()
    }

    #[inline]
    fn io_channel_ctx(ch: *mut spdk_io_channel) -> *mut c_void {
        unsafe {
            (ch as *mut u8).add(size_of::<spdk_io_channel>()) as *mut c_void
        }
    }
}

pub struct NvmeIoChannelInner<'a> {
    // qpair and poller needs to be a raw pointer since it's gonna be NULL'ed
    // upon unregistration.
    pub qpair: *mut spdk_nvme_qpair,
    poll_group: NonNull<spdk_nvme_poll_group>,
    poller: poller::Poller<'a>,
    // Flag to indicate the shutdown state of the channel.
    // We need such a flag to differentiate between channel reset and shutdown.
    // Channel reset is a reversible operation, which is followed by
    // reinitialize(), which 'resurrects' channel (i.e. recreates all its
    // I/O resources): such behaviour is observed during controller reset.
    // Shutdown, in contrary, means 'one-way' ticket for the channel, which
    // doesn't assume any further resurrections: such behaviour is seen
    // upon controller shutdown. Being able to differentiate between these
    // 2 states allows controller reset to behave properly in parallel with
    // shutdown (if case reset is initiated before shutdown), and
    // not to reinitialize channels already processed by shutdown logic.
    is_shutdown: bool,
}

impl NvmeIoChannelInner<'_> {
    /// Reset channel, making it unusable till reinitialize() is called.
    pub fn reset(&mut self) -> i32 {
        if self.qpair.is_null() {
            return 0;
        }

        self._abort_queue();

        let q = self.qpair;
        self.qpair = std::ptr::null_mut();

        unsafe {
            spdk_nvme_ctrlr_disconnect_io_qpair(q);
            spdk_nvme_ctrlr_free_io_qpair(q)
        }
    }

    /// Checks whether the I/O channel is shutdown.
    pub fn is_shutdown(&self) -> bool {
        self.is_shutdown
    }

    /// Shutdown I/O channel and make it completely unusable for I/O.
    pub fn shutdown(&mut self) -> i32 {
        if self.is_shutdown {
            return 0;
        }

        let rc = self.reset();
        if rc == 0 {
            self.is_shutdown = true;
        }
        rc
    }

    /// Aborts all the requests in the qpair.
    fn _abort_queue(&self) {
        unsafe { nvme_qpair_abort_reqs(self.qpair, 1) };
    }

    /// Reinitializes channel after reset unless the channel is shutdown.
    pub fn reinitialize(
        &mut self,
        ctrlr_name: &str,
        ctrlr_handle: *mut spdk_nvme_ctrlr,
    ) -> i32 {
        if self.is_shutdown {
            error!(
                "{} I/O channel is shutdown, channel reinitialization not possible",
                ctrlr_name
            );
            return -libc::ENODEV;
        }

        // Create qpair for target controller.
        let mut opts = spdk_nvme_io_qpair_opts::default();
        let default_opts = NvmeBdevOpts::default();

        unsafe {
            spdk_nvme_ctrlr_get_default_io_qpair_opts(
                ctrlr_handle,
                &mut opts,
                size_of::<spdk_nvme_io_qpair_opts>() as u64,
            );

            opts.io_queue_requests =
                max(opts.io_queue_requests, default_opts.io_queue_requests);
            opts.create_only = true;

            let qpair: *mut spdk_nvme_qpair = spdk_nvme_ctrlr_alloc_io_qpair(
                ctrlr_handle,
                &opts,
                size_of::<spdk_nvme_io_qpair_opts>() as u64,
            );

            if qpair.is_null() {
                error!("{} Failed to allocate qpair", ctrlr_name);
                return -libc::ENOMEM;
            }

            let mut rc =
                spdk_nvme_poll_group_add(self.poll_group.as_ptr(), qpair);

            if rc != 0 {
                error!("{} failed to add qpair to poll group", ctrlr_name);
                spdk_nvme_ctrlr_free_io_qpair(qpair);
                return rc;
            }

            rc = spdk_nvme_ctrlr_connect_io_qpair(ctrlr_handle, qpair);

            if rc != 0 {
                error!("{} failed to connect qpair (errno={})", ctrlr_name, rc);
                spdk_nvme_poll_group_remove(self.poll_group.as_ptr(), qpair);
                spdk_nvme_ctrlr_free_io_qpair(qpair);
                return rc;
            }

            debug!("{} I/O channel successfully reinitialized", ctrlr_name);
            self.qpair = qpair;
            0
        }
    }
}

pub struct NvmeControllerIoChannel(NonNull<spdk_io_channel>);

extern "C" fn disconnected_qpair_cb(
    qpair: *mut spdk_nvme_qpair,
    _ctx: *mut c_void,
) {
    warn!(?qpair, "NVMe qpair disconnected");
    /*
     * Currently, just try to reconnect indefinitely. If we are doing a
     * reset, the reset will reconnect a qpair and we will stop getting a
     * callback for this one.
     */
    unsafe {
        spdk_nvme_ctrlr_reconnect_io_qpair(qpair);
    }
}

extern "C" fn nvme_poll(ctx: *mut c_void) -> i32 {
    let inner = NvmeIoChannel::from_raw(ctx).inner_mut();

    let num_completions = unsafe {
        spdk_nvme_poll_group_process_completions(
            inner.poll_group.as_ptr(),
            0,
            Some(disconnected_qpair_cb),
        )
    };

    if num_completions > 0 {
        1
    } else {
        0
    }
}

impl NvmeControllerIoChannel {
    pub extern "C" fn create(device: *mut c_void, ctx: *mut c_void) -> i32 {
        let id = device as u64;

        debug!("Creating IO channel for controller ID 0x{:X}", id);

        let carc = match NVME_CONTROLLERS.lookup_by_name(id.to_string()) {
            None => {
                error!("No NVMe controller found for ID 0x{:X}", id);
                return 1;
            }
            Some(c) => c,
        };

        let (cname, spdk_handle) = {
            let controller = carc.lock().expect("lock error");
            // Make sure controller is available.
            if controller.get_state() != NvmeControllerState::Running {
                error!(
                    "{} controller is in {:?} state, I/O channel creation not possible",
                    controller.get_name(),
                    controller.get_state()
                );
                return 1;
            }
            // Release controller's lock before proceeding to avoid deadlocks,
            // as qpair-related operations might hang in case of
            // network connection failures. Note that we still hold
            // the reference to the controller instance (carc) which
            // guarantees that the controller exists during I/O channel
            // creation.
            (controller.get_name(), controller.ctrlr_as_ptr())
        };

        let nvme_channel = NvmeIoChannel::from_raw(ctx);
        let mut opts = spdk_nvme_io_qpair_opts::default();
        let default_opts = NvmeBdevOpts::default();

        unsafe {
            spdk_nvme_ctrlr_get_default_io_qpair_opts(
                spdk_handle,
                &mut opts,
                size_of::<spdk_nvme_io_qpair_opts>() as u64,
            )
        }

        opts.io_queue_requests =
            max(opts.io_queue_requests, default_opts.io_queue_requests);
        opts.create_only = true;

        debug!("{} allocating I/O qpair", cname);
        let qpair: *mut spdk_nvme_qpair = unsafe {
            spdk_nvme_ctrlr_alloc_io_qpair(
                spdk_handle,
                &opts,
                size_of::<spdk_nvme_io_qpair_opts>() as u64,
            )
        };

        if qpair.is_null() {
            error!("{} Failed to allocate qpair", cname);
            return 1;
        }
        debug!("{} I/O qpair successfully allocated", cname);

        // Create poll group.
        let poll_group: *mut spdk_nvme_poll_group =
            unsafe { spdk_nvme_poll_group_create(ctx) };
        if poll_group.is_null() {
            error!("{} Failed to create a poll group for the qpair", cname);
            return 1;
        }

        // Create poller.
        let poller = poller::Builder::new()
            .with_interval(default_opts.nvme_ioq_poll_period_us)
            .with_poll_fn(move || nvme_poll(ctx))
            .build();

        let inner = Box::new(NvmeIoChannelInner {
            qpair,
            poll_group: NonNull::new(poll_group).unwrap(),
            poller,
            is_shutdown: false,
        });

        nvme_channel.inner = Box::into_raw(inner);

        let mut rc = unsafe { spdk_nvme_poll_group_add(poll_group, qpair) };
        if rc != 0 {
            error!("{} failed to add qpair to poll group", cname);
            return 1;
        }

        // Connect qpair.
        debug!("{} connecting I/O qpair", cname);
        rc = unsafe { spdk_nvme_ctrlr_connect_io_qpair(spdk_handle, qpair) };

        if rc != 0 {
            error!("{} failed to connect qpair (errno={})", cname, rc);
            return 1;
        }

        info!("{} I/O channel {:?} successfully initialized", cname, ctx);
        0
    }

    /// Callback function to be invoked by SPDK to deinitialize I/O channel for
    /// NVMe controller.
    pub extern "C" fn destroy(device: *mut c_void, ctx: *mut c_void) {
        debug!(
            "Destroying IO channel for controller ID 0x{:X}",
            device as u64
        );

        let ch = NvmeIoChannel::from_raw(ctx);
        let inner = unsafe { Box::from_raw(ch.inner) };

        // Release resources associated with this particular channel.
        unsafe {
            if !inner.qpair.is_null() {
                spdk_nvme_poll_group_remove(
                    inner.poll_group.as_ptr(),
                    inner.qpair,
                );
            }
            inner.poller.stop();
            spdk_nvme_poll_group_destroy(inner.poll_group.as_ptr());

            if !inner.qpair.is_null() {
                spdk_nvme_ctrlr_free_io_qpair(inner.qpair);
            }
        };

        debug!(
            "IO channel for controller ID 0x{:X} successfully destroyed",
            device as u64
        );
    }
}

/// Wrapper around SPDK I/O channel.
impl NvmeControllerIoChannel {
    pub fn from_null_checked(
        ch: *mut spdk_io_channel,
    ) -> Option<NvmeControllerIoChannel> {
        if ch.is_null() {
            None
        } else {
            Some(NvmeControllerIoChannel(NonNull::new(ch).unwrap()))
        }
    }

    pub fn as_ptr(&self) -> *mut spdk_io_channel {
        self.0.as_ptr()
    }
}

impl Drop for NvmeControllerIoChannel {
    fn drop(&mut self) {
        debug!("I/O channel {:p} dropped", self.0.as_ptr());
        unsafe { spdk_put_io_channel(self.0.as_ptr()) }
    }
}
