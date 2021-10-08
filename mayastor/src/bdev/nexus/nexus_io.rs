use std::{
    fmt::Debug,
    ops::{Deref, DerefMut},
};

use libc::c_void;
use nix::errno::Errno;

use spdk_rs::{
    libspdk::{spdk_bdev_io, spdk_io_channel},
    BdevIo,
};

use super::{
    nexus_lookup_mut,
    Nexus,
    NexusChannel,
    NexusChannelInner,
    NexusStatus,
    NEXUS_PRODUCT_ID,
};

use crate::core::{
    BlockDevice,
    BlockDeviceHandle,
    CoreError,
    Cores,
    GenericStatusCode,
    IoCompletionStatus,
    IoStatus,
    IoType,
    Mthread,
    NvmeCommandStatus,
    Reactors,
};

#[repr(transparent)]
#[derive(Debug, Clone)]
pub(crate) struct NexusBio(BdevIo<Nexus>);

impl DerefMut for NexusBio {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Deref for NexusBio {
    type Target = BdevIo<Nexus>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<*mut spdk_bdev_io> for NexusBio {
    fn from(ptr: *mut spdk_bdev_io) -> Self {
        Self(BdevIo::<Nexus>::legacy_from_ptr(ptr))
    }
}

impl From<BdevIo<Nexus>> for NexusBio {
    fn from(io: BdevIo<Nexus>) -> Self {
        Self(io)
    }
}

impl NexusBio {
    fn as_ptr(&self) -> *mut spdk_bdev_io {
        self.0.legacy_as_ptr()
    }
}

#[derive(Debug)]
#[repr(C)]
pub struct NioCtx {
    /// number of IO's submitted. Nexus IO's may never be freed until this
    /// counter drops to zero.
    in_flight: u8,
    /// intermediate status of the IO
    status: IoStatus,
    /// a reference to  our channel
    channel: spdk_rs::IoChannel<NexusChannel>,
    /// the IO must fail regardless of when it completes
    must_fail: bool,
}

pub(crate) fn nexus_submit_io(mut io: NexusBio) {
    if let Err(_e) = match io.cmd() {
        IoType::Read => io.readv(),
        // these IOs are submitted to all the underlying children
        IoType::Write | IoType::WriteZeros | IoType::Reset | IoType::Unmap => {
            io.submit_all()
        }
        IoType::Flush => {
            io.ok();
            Ok(())
        }
        IoType::NvmeAdmin => {
            io.fail();
            Err(CoreError::NotSupported {
                source: Errno::EINVAL,
            })
        }
        _ => {
            trace!(?io, "not supported");
            io.fail();
            Err(CoreError::NotSupported {
                source: Errno::EOPNOTSUPP,
            })
        }
    } {
        //trace!(?e, ?io, "Error during IO submission");
    }
}

impl NexusBio {
    /// helper function to wrap the raw pointers into new types. From here we
    /// should not be dealing with any raw pointers.
    pub fn nexus_bio_setup(
        channel: spdk_rs::IoChannel<NexusChannel>,
        io: spdk_rs::BdevIo<Nexus>,
    ) -> Self {
        // NexusBio::nexus_bio_setup(channel.legacy_as_ptr(),
        // io.legacy_as_ptr())
        let mut bio = NexusBio::from(io);
        let ctx = bio.ctx_mut();
        ctx.channel = channel;
        ctx.status = IoStatus::Pending;
        ctx.in_flight = 0;
        ctx.must_fail = false;
        bio
    }

    /// assess the IO if we need to mark it failed or ok.
    /// obtain the Nexus struct embedded within the bdev
    pub(crate) fn nexus_as_ref(&self) -> &Nexus {
        self.bdev_checked(NEXUS_PRODUCT_ID).data()
    }

    /// invoked when a nexus IO completes
    fn child_completion(
        device: &dyn BlockDevice,
        status: IoCompletionStatus,
        ctx: *mut c_void,
    ) {
        let mut nexus_io = NexusBio::from(ctx as *mut spdk_bdev_io);
        nexus_io.complete(device, status);
    }

    #[inline(always)]
    /// a mutable reference to the IO context
    pub fn ctx_mut(&mut self) -> &mut NioCtx {
        self.driver_ctx_mut::<NioCtx>()
    }

    #[inline(always)]
    /// immutable reference to the IO context
    pub fn ctx(&self) -> &NioCtx {
        self.driver_ctx::<NioCtx>()
    }

    /// Returns the type of command for this IO.
    #[inline(always)]
    fn cmd(&self) -> IoType {
        self.io_type()
    }

    /// completion handler for the nexus when a child IO completes
    pub fn complete(
        &mut self,
        child: &dyn BlockDevice,
        status: IoCompletionStatus,
    ) {
        let success = status == IoCompletionStatus::Success;

        self.ctx_mut().in_flight -= 1;

        if success {
            self.ok_checked();
        } else {
            // IO failure, mark the IO failed and take the child out
            error!(
                ?self,
                "{} IO completion failed: {:?}",
                child.device_name(),
                self.ctx()
            );
            self.ctx_mut().status = IoStatus::Failed;
            self.ctx_mut().must_fail = true;
            self.handle_failure(child, status);
        }
    }

    /// Complete the IO marking at as successfully when all child IO's have been
    /// accounted for. Failing to account for all child IO's will result in
    /// a lockup.
    #[inline]
    fn ok_checked(&mut self) {
        if self.ctx().in_flight == 0 {
            if self.ctx().must_fail {
                //warn!(?self, "resubmitted due to must_fail");
                self.retry_checked();
                //self.fail();
            } else {
                self.ok();
            }
        }
    }

    /// Complete the IO marking it as failed.
    #[inline]
    pub fn fail_checked(&mut self) {
        if self.ctx().in_flight == 0 {
            self.fail();
        }
    }

    /// retry this IO when all other IOs have completed
    #[inline]
    pub fn retry_checked(&mut self) {
        if self.ctx().in_flight == 0 {
            let bio = Self::nexus_bio_setup(
                self.ctx().channel.clone(),
                self.0.clone(),
            );
            debug!(?self, "resubmitting IO");
            nexus_submit_io(bio);
        }
    }

    /// reference to the inner channels. The inner channel contains the specific
    /// per-core data structures.
    fn inner_channel(&self) -> &NexusChannelInner {
        self.ctx().channel.channel_data().inner()
    }

    /// mutable reference to the inner channels. The inner channel contains the
    /// specific per-core data structures.
    fn inner_channel_mut(&mut self) -> &mut NexusChannelInner {
        self.ctx_mut().channel.channel_data_mut().inner_mut()
    }

    // TODO make const
    fn data_ent_offset(&self) -> u64 {
        self.nexus_as_ref().data_ent_offset
    }

    /// helper routine to get a channel to read from
    #[inline]
    fn read_channel_at_index(&self, i: usize) -> &dyn BlockDeviceHandle {
        &*self.inner_channel().readers[i]
    }

    /// submit a read operation to one of the children of this nexus
    #[inline]
    fn submit_read(
        &self,
        hdl: &dyn BlockDeviceHandle,
    ) -> Result<(), CoreError> {
        hdl.readv_blocks(
            self.iovs(),
            self.iov_count(),
            self.offset() + self.data_ent_offset(),
            self.num_blocks(),
            Self::child_completion,
            self.as_ptr().cast(),
        )
    }

    /// submit a read operation
    fn do_readv(&mut self) -> Result<(), CoreError> {
        if let Some(i) = self.inner_channel_mut().child_select() {
            let hdl = self.read_channel_at_index(i);
            let r = self.submit_read(hdl);

            if r.is_err() {
                // Such a situation can happen when there is no active I/O in
                // the queues, but error on qpair is observed
                // due to network timeout, which initiates
                // controller reset. During controller reset all
                // I/O channels are de-initialized, so no I/O
                // submission is possible (spdk returns -6/ENXIO), so we have to
                // start device retire.
                // TODO: ENOMEM and ENXIO should be handled differently and
                // device should not be retired in case of ENOMEM.

                let device = hdl.get_device().device_name();
                trace!(
                    "(core: {} thread: {}): read IO to {} submission failed with error {:?}",
                    Cores::current(), Mthread::current().unwrap().name(), device, r);

                let inner = self.inner_channel_mut();
                let must_retire = inner.fault_child(&device);
                if must_retire {
                    self.do_retire(device);
                }

                self.fail();
            } else {
                self.ctx_mut().in_flight = 1;
            }
            r
        } else {
            trace!(
                "(core: {} thread: {}): read IO submission failed no children available",
                Cores::current(), Mthread::current().unwrap().name());
            self.fail();
            Err(CoreError::NoDevicesAvailable {})
        }
    }

    extern "C" fn nexus_get_buf_cb(
        _ch: *mut spdk_io_channel,
        io: *mut spdk_bdev_io,
        success: bool,
    ) {
        let mut bio = NexusBio::from(io);

        if !success {
            trace!(
                "(core: {} thread: {}): get_buf() failed",
                Cores::current(),
                Mthread::current().unwrap().name()
            );
            bio.no_mem();
        }

        let _ = bio.do_readv();
    }

    /// submit read IO to some child
    fn readv(&mut self) -> Result<(), CoreError> {
        if self.need_buf() {
            unsafe {
                self.alloc_buffer(Self::nexus_get_buf_cb);
            }
            Ok(())
        } else {
            self.do_readv()
        }
    }

    #[inline]
    fn submit_write(
        &self,
        hdl: &dyn BlockDeviceHandle,
    ) -> Result<(), CoreError> {
        hdl.writev_blocks(
            self.iovs(),
            self.iov_count(),
            self.offset() + self.data_ent_offset(),
            self.num_blocks(),
            Self::child_completion,
            self.as_ptr().cast(),
        )
    }

    #[inline]
    fn submit_unmap(
        &self,
        hdl: &dyn BlockDeviceHandle,
    ) -> Result<(), CoreError> {
        hdl.unmap_blocks(
            self.offset() + self.data_ent_offset(),
            self.num_blocks(),
            Self::child_completion,
            self.as_ptr().cast(),
        )
    }

    #[inline]
    fn submit_write_zeroes(
        &self,
        hdl: &dyn BlockDeviceHandle,
    ) -> Result<(), CoreError> {
        hdl.write_zeroes(
            self.offset() + self.data_ent_offset(),
            self.num_blocks(),
            Self::child_completion,
            self.as_ptr().cast(),
        )
    }

    #[inline]
    fn submit_reset(
        &self,
        hdl: &dyn BlockDeviceHandle,
    ) -> Result<(), CoreError> {
        hdl.reset(Self::child_completion, self.as_ptr().cast())
    }

    /// Submit the IO to all underlying children, failing on the first error we
    /// find. When an IO is partially submitted -- we must wait until all
    /// the child IOs have completed before we mark the whole IO failed to
    /// avoid double frees. This function handles IO for a subset that must
    /// be submitted to all the underlying children.
    fn submit_all(&mut self) -> Result<(), CoreError> {
        let mut inflight = 0;
        // Name of the device which experiences I/O submission failures.
        let mut failed_device = None;

        let result = self.inner_channel().writers.iter().try_for_each(|h| {
            match self.cmd() {
                IoType::Write => self.submit_write(h.as_ref()),
                IoType::Unmap => self.submit_unmap(h.as_ref()),
                IoType::WriteZeros => self.submit_write_zeroes(h.as_ref()),
                IoType::Reset => self.submit_reset(h.as_ref()),
                // we should never reach here, if we do it is a bug.
                _ => unreachable!(),
            }
                .map(|_| {
                    inflight += 1;
                })
                .map_err(|se| {
                    error!(
                        "(core: {} thread: {}): IO submission failed with error {:?}, I/Os submitted: {}",
                        Cores::current(), Mthread::current().unwrap().name(), se, inflight
                    );

                    // Record the name of the device for immediate retire.
                    failed_device = Some(h.get_device().device_name());
                    se
                })
        });

        // Submission errors can also trigger device retire.
        // Such a situation can happen when there is no active I/O in the
        // queues, but error on qpair is observed due to network
        // timeout, which initiates controller reset. During controller
        // reset all I/O channels are de-initialized, so no I/O
        // submission is possible (spdk returns -6/ENXIO), so we have to
        // start device retire.
        // TODO: ENOMEM and ENXIO should be handled differently and
        // device should not be retired in case of ENOMEM.
        if result.is_err() {
            let device = failed_device.unwrap();
            // set the IO as failed in the submission stage.
            self.ctx_mut().must_fail = true;
            if self.inner_channel_mut().remove_child(&device) {
                self.do_retire(device);
            }
        }

        // partial submission
        if inflight != 0 {
            // An error was experienced during submission. Some IO however, has
            // been submitted successfully prior to the error condition.
            self.ctx_mut().in_flight = inflight;
            self.ctx_mut().status = IoStatus::Success;
            self.ok_checked();
            return result;
        }

        self.fail_checked();

        result
    }

    fn do_retire(&self, child: String) {
        Reactors::master().send_future(Self::child_retire(
            self.nexus_as_ref().name.clone(),
            child,
        ));
    }

    fn handle_failure(
        &mut self,
        child: &dyn BlockDevice,
        status: IoCompletionStatus,
    ) {
        // We have experienced a failure on one of the child devices. We need to
        // ensure we do not submit more IOs to this child. We do not
        // need to tell other cores about this because
        // they will experience the same errors on their own channels, and
        // handle it on their own.
        //
        // We differentiate between errors in the submission and completion.
        // When we have a completion error, it typically means that the
        // child has lost the connection to the nexus. In order for
        // outstanding IO to complete, the IO's to that child must be aborted.
        // The abortion is implicit when removing the device.

        if matches!(
            status,
            IoCompletionStatus::NvmeError(
                NvmeCommandStatus::GenericCommandStatus(
                    GenericStatusCode::InvalidOpcode
                )
            )
        ) {
            debug!(
                "Device {} experienced invalid opcode error: retiring skipped",
                child.device_name()
            );
            return;
        }

        let retry = matches!(
            status,
            IoCompletionStatus::NvmeError(
                NvmeCommandStatus::GenericCommandStatus(
                    GenericStatusCode::AbortedSubmissionQueueDeleted
                )
            )
        );

        let child = child.device_name();
        // check if this child needs to be retired
        let needs_retire = self.inner_channel_mut().fault_child(&child);
        // The child state was not faulted yet, so this is the first IO
        // to this child for which we encountered an error.
        if needs_retire {
            self.do_retire(child);
        }

        // if the IO was failed because of retire, resubmit the IO
        if retry {
            return self.ok_checked();
        }

        self.fail_checked();
    }

    /// Retire a child for this nexus.
    async fn child_retire(nexus_name: String, device: String) {
        if let Some(nexus) = nexus_lookup_mut(&nexus_name) {
            warn!(?nexus, ?device, "retiring child");

            if let Err(e) = nexus.child_retire(device.clone()).await {
                error!(?e, "double pause which we cant sneak in...");
                Reactors::current()
                    .send_future(Self::child_retire(nexus_name, device));
                return;
            }

            if matches!(nexus.status(), NexusStatus::Faulted) {
                warn!(?nexus, "no children left");
            }
        }
    }
}
