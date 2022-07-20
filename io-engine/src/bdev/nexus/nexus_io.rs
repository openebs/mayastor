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

use super::{Nexus, NexusChannel, NEXUS_PRODUCT_ID};

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
};

/// TODO
#[repr(C)]
#[derive(Debug)]
pub(super) struct NioCtx<'n> {
    /// number of IO's submitted. Nexus IO's may never be freed until this
    /// counter drops to zero.
    in_flight: u8,
    /// intermediate status of the IO
    status: IoStatus,
    /// a reference to  our channel
    channel: spdk_rs::IoChannel<NexusChannel<'n>>,
    /// the IO must fail regardless of when it completes
    must_fail: bool,
}

/// TODO
#[repr(transparent)]
#[derive(Debug)]
pub(super) struct NexusBio<'n>(BdevIo<Nexus<'n>>);

impl<'n> Deref for NexusBio<'n> {
    type Target = BdevIo<Nexus<'n>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for NexusBio<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<'n> From<*mut spdk_bdev_io> for NexusBio<'n> {
    fn from(ptr: *mut spdk_bdev_io) -> Self {
        Self(BdevIo::<Nexus<'n>>::legacy_from_ptr(ptr))
    }
}

impl Clone for NexusBio<'_> {
    fn clone(&self) -> Self {
        Self::new(self.ctx().channel.clone(), self.0.clone())
    }
}

impl<'n> NexusBio<'n> {
    fn as_ptr(&self) -> *mut spdk_bdev_io {
        self.0.legacy_as_ptr()
    }

    /// Makes a new instance of `NexusBio` from a channel and `BdevIo`.
    pub(super) fn new(
        channel: spdk_rs::IoChannel<NexusChannel<'n>>,
        io: BdevIo<Nexus<'n>>,
    ) -> Self {
        let mut bio = NexusBio(io);
        let ctx = bio.ctx_mut();
        ctx.channel = channel;
        ctx.status = IoStatus::Pending;
        ctx.in_flight = 0;
        ctx.must_fail = false;
        bio
    }

    /// TODO
    pub(super) fn submit_request(mut self) {
        if let Err(_e) = match self.io_type() {
            IoType::Read => self.readv(),
            // these IOs are submitted to all the underlying children
            IoType::Write
            | IoType::WriteZeros
            | IoType::Reset
            | IoType::Unmap => self.submit_all(),
            IoType::Flush => {
                self.ok();
                Ok(())
            }
            IoType::NvmeAdmin => {
                self.fail();
                Err(CoreError::NotSupported {
                    source: Errno::EINVAL,
                })
            }
            _ => {
                trace!(?self, "not supported");
                self.fail();
                Err(CoreError::NotSupported {
                    source: Errno::EOPNOTSUPP,
                })
            }
        } {
            //trace!(?e, ?io, "Error during IO submission");
        }
    }

    /// assess the IO if we need to mark it failed or ok.
    /// obtain the Nexus struct embedded within the bdev
    pub(crate) fn nexus(&self) -> &Nexus<'n> {
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

    /// immutable reference to the IO context
    #[inline(always)]
    fn ctx(&self) -> &NioCtx<'n> {
        self.driver_ctx::<NioCtx>()
    }

    /// a mutable reference to the IO context
    #[inline(always)]
    fn ctx_mut(&mut self) -> &mut NioCtx<'n> {
        self.driver_ctx_mut::<NioCtx>()
    }

    /// completion handler for the nexus when a child IO completes
    fn complete(
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
    fn fail_checked(&mut self) {
        if self.ctx().in_flight == 0 {
            self.fail();
        }
    }

    /// retry this IO when all other IOs have completed
    #[inline]
    fn retry_checked(&mut self) {
        if self.ctx().in_flight == 0 {
            debug!(?self, "resubmitting IO");
            self.clone().submit_request();
        }
    }

    /// reference to the channel. The channel contains the specific
    /// per-core data structures.
    #[inline(always)]
    fn channel(&self) -> &NexusChannel<'n> {
        self.ctx().channel.channel_data()
    }

    /// mutable reference to the channels. The channel contains the
    /// specific per-core data structures.
    #[inline(always)]
    fn channel_mut(&mut self) -> &mut NexusChannel<'n> {
        self.ctx_mut().channel.channel_data_mut()
    }

    /// Returns the offset in num blocks where the data partition starts.
    #[inline]
    fn data_ent_offset(&self) -> u64 {
        self.nexus().data_ent_offset
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
        if let Some(hdl) = self.channel().select_reader() {
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

                self.retire_device(&device);

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

        let result = self.channel().for_each_writer(|h| {
            match self.io_type() {
                IoType::Write => self.submit_write(h),
                IoType::Unmap => self.submit_unmap(h),
                IoType::WriteZeros => self.submit_write_zeroes(h),
                IoType::Reset => self.submit_reset(h),
                // we should never reach here, if we do it is a bug.
                _ => unreachable!(),
            }
                .map(|_| {
                    inflight += 1;
                })
                .map_err(|err| {
                    error!(
                        "(core: {} thread: {}): IO submission failed with error {:?}, I/Os submitted: {}",
                        Cores::current(), Mthread::current().unwrap().name(), err, inflight
                    );

                    // Record the name of the device for immediate retire.
                    failed_device = Some(h.get_device().device_name());
                    err
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

            self.channel_mut().disconnect_device(&device);

            self.retire_device(&device);
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

    /// TODO
    fn retire_device(&mut self, child_device: &str) {
        self.channel_mut()
            .nexus_mut()
            .retire_child(child_device, true);
    }

    /// TODO
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

        self.retire_device(&child.device_name());

        // if the IO was failed because of retire, resubmit the IO
        if retry {
            return self.ok_checked();
        }

        self.fail_checked();
    }
}
