/* I/O channel for NVMe controller, one per core. */

use std::{mem::size_of, os::raw::c_void, ptr::NonNull, time::Duration};

use spdk_rs::{
    libspdk::{
        nvme_qpair_abort_all_queued_reqs,
        nvme_transport_qpair_abort_reqs,
        spdk_io_channel,
        spdk_nvme_poll_group_process_completions,
        spdk_nvme_qpair,
        spdk_put_io_channel,
    },
    Poller,
    PollerBuilder,
};

use crate::{
    bdev::device_lookup,
    core::{BlockDevice, BlockDeviceIoStats, IoType},
};

use super::{
    nvme_bdev_running_config,
    NvmeControllerState,
    PollGroup,
    QPair,
    SpdkNvmeController,
    NVME_CONTROLLERS,
};

/// TODO
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

impl std::fmt::Debug for NvmeIoChannelInner<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NvmeIoChannelInner")
            .field("qpair", &self.qpair)
            .field("pending IO", &self.num_pending_ios)
            .finish()
    }
}

pub struct NvmeIoChannelInner<'a> {
    qpair: Option<QPair>,
    poll_group: PollGroup,
    poller: Poller<'a>,
    io_stats_controller: IoStatsController,
    pub device: Box<dyn BlockDevice>,
    /// to prevent the controller from being destroyed before the channel
    ctrl: Option<
        std::sync::Arc<parking_lot::Mutex<crate::bdev::NvmeController<'a>>>,
    >,
    num_pending_ios: u64,

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
    /// Returns SPDK pointer for the QPair. The QPair must exist.
    #[inline(always)]
    pub(crate) unsafe fn qpair_ptr(&mut self) -> *mut spdk_nvme_qpair {
        self.qpair.as_mut().expect("QPair must exist").as_ptr()
    }

    #[inline(always)]
    pub(crate) fn qpair(&self) -> &Option<QPair> {
        &self.qpair
    }

    #[inline(always)]
    pub(crate) fn qpair_mut(&mut self) -> &mut Option<QPair> {
        &mut self.qpair
    }

    fn remove_qpair(&mut self) -> Option<QPair> {
        if let Some(q) = &self.qpair {
            trace!(qpair = ?q.as_ptr(), "removing qpair");
        }
        self.qpair.take()
    }

    /// Reset channel, making it unusable till reinitialize() is called.
    pub fn reset(&mut self) -> i32 {
        // Remove qpair and trigger its deallocation via drop().
        match self.remove_qpair() {
            Some(qpair) => {
                trace!(
                    "reset: dropping qpair {:p} ({}) I/O requests pending)",
                    qpair.as_ptr(),
                    self.num_pending_ios
                );
            }
            None => {
                trace!(
                    "reset: no qpair ({}) I/O requests pending)",
                    self.num_pending_ios
                );
            }
        }

        0
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
            self.ctrl.take();
        }
        rc
    }

    /// Account active I/O for channel.
    #[inline]
    pub fn account_io(&mut self) {
        self.num_pending_ios += 1;
    }

    /// Discard active I/O operation for channel.
    #[inline]
    pub fn discard_io(&mut self) {
        if self.num_pending_ios == 0 {
            warn!("Discarding I/O operation without any active I/O operations")
        } else {
            self.num_pending_ios -= 1;
        }
    }

    /// Reinitialize channel after reset unless the channel is shutdown.
    pub fn reinitialize(
        &mut self,
        ctrlr_name: &str,
        ctrlr_handle: SpdkNvmeController,
    ) -> i32 {
        if self.is_shutdown {
            error!(
                "{} I/O channel is shutdown, channel reinitialization not possible",
                ctrlr_name
            );
            return -libc::ENODEV;
        }

        // We assume that channel is reinitialized after being reset, so we
        // expect to see no I/O qpair.
        if self.remove_qpair().is_some() {
            warn!(
                ?ctrlr_name,
                "I/O channel has active I/O qpair while being reinitialized, clearing"
            );
        }

        // Create qpair for target controller.
        let qpair = match QPair::create(ctrlr_handle, ctrlr_name) {
            Ok(qpair) => qpair,
            Err(e) => {
                error!(?ctrlr_name, ?e, "Failed to allocate qpair,");
                return -libc::ENOMEM;
            }
        };

        // Add qpair to the poll group.
        let mut rc = self.poll_group.add_qpair(&qpair);
        if rc != 0 {
            error!(?ctrlr_name, "failed to add qpair to poll group");
            return rc;
        }

        // Connect qpair.
        rc = qpair.connect();
        if rc != 0 {
            error!("{} failed to connect qpair (errno={})", ctrlr_name, rc);
            self.poll_group.remove_qpair(&qpair);
            return rc;
        }

        trace!("{} I/O channel successfully reinitialized", ctrlr_name);
        self.qpair = Some(qpair);
        0
    }

    /// Get I/O statistics for channel.
    #[inline]
    pub fn get_io_stats_controller(&mut self) -> &mut IoStatsController {
        &mut self.io_stats_controller
    }
}
pub struct IoStatsController {
    // Note that for the sake of optimization, all bytes-related I/O stats
    // (bytes_read, bytes_written and bytes_unmapped) are accounted in
    // sectors. Translation into bytes occurs only when providing the full
    // I/O stats to the caller, inside get_io_stats().
    io_stats: BlockDeviceIoStats,
    block_size: u64,
}

/// Top-level wrapper around device I/O statistics.
impl IoStatsController {
    fn new(block_size: u64) -> Self {
        Self {
            io_stats: BlockDeviceIoStats::default(),
            block_size,
        }
    }

    #[inline]
    /// Account amount of blocks and I/O operations.
    pub fn account_block_io(
        &mut self,
        op: IoType,
        num_ops: u64,
        num_blocks: u64,
    ) {
        match op {
            IoType::Read => {
                self.io_stats.num_read_ops += num_ops;
                self.io_stats.bytes_read += num_blocks;
            }
            IoType::Write => {
                self.io_stats.num_write_ops += num_ops;
                self.io_stats.bytes_written += num_blocks;
            }
            IoType::Unmap => {
                self.io_stats.num_unmap_ops += num_ops;
                self.io_stats.bytes_unmapped += num_blocks;
            }
            IoType::Compare | IoType::WriteZeros | IoType::Flush => {}
            _ => {
                warn!("Unsupported I/O type for I/O statistics: {:?}", op);
            }
        }
    }

    /// Get I/O statistics for channel.
    #[inline]
    pub fn get_io_stats(&self) -> BlockDeviceIoStats {
        let mut stats = self.io_stats;

        // Translate sectors into bytes before returning the stats.
        stats.bytes_read *= self.block_size;
        stats.bytes_written *= self.block_size;
        stats.bytes_unmapped *= self.block_size;

        stats
    }
}

pub struct NvmeControllerIoChannel(NonNull<spdk_io_channel>);

extern "C" fn disconnected_qpair_cb(
    _qpair: *mut spdk_nvme_qpair,
    ctx: *mut c_void,
) {
    let inner = NvmeIoChannel::from_raw(ctx).inner_mut();

    if let Some(qpair) = inner.qpair() {
        unsafe {
            nvme_qpair_abort_all_queued_reqs(qpair.as_ptr(), 1);
            nvme_transport_qpair_abort_reqs(qpair.as_ptr(), 1);
        }
    }

    //warn!(?qpair, "NVMe qpair disconnected");
    // shutdown the channel such that pending IO if any, gets aborted.
    //inner.shutdown();
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

        trace!("Creating IO channel for controller ID 0x{:X}", id);

        let carc = match NVME_CONTROLLERS.lookup_by_name(id.to_string()) {
            None => {
                error!("No NVMe controller found for ID 0x{:X}", id);
                return 1;
            }
            Some(c) => c,
        };

        let (cname, controller, block_size) = {
            let controller = carc.lock();
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
            let block_size = controller
                .namespace()
                .expect("No namespaces in active controller")
                .block_len();
            (
                controller.get_name(),
                controller.controller().unwrap(),
                block_size,
            )
        };

        let nvme_channel = NvmeIoChannel::from_raw(ctx);

        // Get a block device that corresponds to the controller.
        let device = match device_lookup(&cname) {
            Some(device) => device,
            None => {
                error!(
                    "{} no block device exists for controller, I/O channel creation not possible",
                    cname,
                );
                return 1;
            }
        };

        // Allocate qpair.
        let qpair = match QPair::create(controller, &cname) {
            Ok(qpair) => qpair,
            Err(e) => {
                error!(?cname, ?e, "Failed to allocate qpair");
                return 1;
            }
        };
        trace!(?cname, "I/O qpair successfully created");

        // Create poll group.
        let mut poll_group = match PollGroup::create(ctx, &cname) {
            Ok(poll_group) => poll_group,
            Err(e) => {
                error!(?cname, ?e, "Failed to create a poll group");
                return 1;
            }
        };

        // Add qpair to poll group.
        let rc = poll_group.add_qpair(&qpair);
        if rc != 0 {
            error!(?cname, ?rc, "failed to add qpair to poll group");
            return 1;
        }

        // Create poller.
        let poller = PollerBuilder::new()
            .with_interval(Duration::from_micros(
                nvme_bdev_running_config().nvme_ioq_poll_period_us,
            ))
            .with_poll_fn(move |_| nvme_poll(ctx))
            .build();

        let inner = Box::new(NvmeIoChannelInner {
            qpair: Some(qpair),
            poll_group,
            poller,
            io_stats_controller: IoStatsController::new(block_size),
            is_shutdown: false,
            device,
            ctrl: Some(carc),
            num_pending_ios: 0,
        });

        nvme_channel.inner = Box::into_raw(inner);
        trace!(?cname, ?ctx, "I/O channel successfully initialized");
        0
    }

    /// Callback function to be invoked by SPDK to deinitialize I/O channel for
    /// NVMe controller.
    pub extern "C" fn destroy(device: *mut c_void, ctx: *mut c_void) {
        info!(
            "Destroying IO channel for controller ID 0x{:X}",
            device as u64
        );

        {
            let ch = NvmeIoChannel::from_raw(ctx);
            let mut inner = unsafe { Box::from_raw(ch.inner) };

            let qpair = inner.remove_qpair();

            // Stop the poller and do extra handling for I/O qpair, as it needs
            // to be detached from the poller prior poller
            // destruction.
            inner.poller.stop();

            if let Some(qpair) = qpair {
                inner.poll_group.remove_qpair(&qpair);
            }
        }

        trace!(
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
        trace!("I/O channel {:p} dropped", self.0.as_ptr());
        unsafe { spdk_put_io_channel(self.0.as_ptr()) }
    }
}
