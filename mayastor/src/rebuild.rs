use std::{convert::TryInto, os::raw::c_void, time::SystemTime};

use futures::channel::oneshot;

use spdk_sys::{
    spdk_bdev_io,
    spdk_bdev_read_blocks,
    spdk_bdev_write_blocks,
    spdk_env_get_current_core,
    spdk_poller,
    SPDK_BDEV_LARGE_BUF_MAX_SIZE,
};

use crate::{
    bdev::nexus::{nexus_io::Bio, Error},
    descriptor::{Descriptor, DmaBuf},
    event::Event,
    poller::{register_poller, PollTask, SetPoller},
};

/// struct that holds the state of a copy task. This struct
/// is used during rebuild.
#[derive(Debug)]
pub struct RebuildTask {
    /// the source where to copy from
    pub source: Descriptor,
    /// the target where to copy to
    pub target: Descriptor,
    /// the last LBA for which an io copy has been submitted
    current_lba: u64,
    /// used to provide progress indication
    previous_lba: u64,
    /// the IO we are rebuilding
    source_io: Option<*mut spdk_bdev_io>,
    /// used to signal completion to the callee
    sender: Option<oneshot::Sender<bool>>,
    /// progress reported to logs
    progress: Option<PollTask>,
    /// the number of segments we need to rebuild. The segment is derived from
    /// the max IO size and by the actual block length. The MAX io size is
    /// currently 64K and is not dynamically configurable.
    num_segments: u64,
    /// the last partial segment
    partial_segment: u32,
    /// DMA buffer during rebuild
    buf: DmaBuf,
    /// How many blocks per segments we have
    blocks_per_segment: u32,
    /// start time of the rebuild task
    start_time: Option<SystemTime>,
}

impl SetPoller for RebuildTask {
    fn set_inner_poller(&mut self, poller: *mut spdk_poller) {
        self.progress = Some(PollTask {
            poller,
        });
    }
}

impl RebuildTask {
    /// return a new rebuild task
    pub fn new(
        source: Descriptor,
        target: Descriptor,
    ) -> Result<Box<Self>, Error> {
        // if the target is to small, we bail out. A future extension is to see,
        // if we can grow; the target to match the size of the source.

        if target.get_bdev().num_blocks() < source.get_bdev().num_blocks() {
            let error = format!(
                "source {} is larger than the target {}",
                source.get_bdev().name(),
                target.get_bdev().name()
            );

            error!("{}", &error);
            return Err(Error::Invalid(error));
        }

        let num_blocks = target.get_bdev().num_blocks();
        let block_len = target.get_bdev().block_len();
        let blocks_per_segment =
            u64::from(SPDK_BDEV_LARGE_BUF_MAX_SIZE / block_len);

        let num_segments = num_blocks / blocks_per_segment as u64;
        let remainder = num_blocks % blocks_per_segment;

        let buf = source.dma_malloc(
            (blocks_per_segment * source.get_bdev().block_len() as u64)
                as usize,
        )?;

        Ok(Box::new(Self {
            blocks_per_segment: blocks_per_segment as u32,
            buf,
            current_lba: 0,
            num_segments,
            previous_lba: 0,
            progress: None,
            partial_segment: remainder.try_into().unwrap(),
            sender: None,
            source,
            source_io: None,
            target,
            start_time: None,
        }))
    }

    /// start the rebuild task specified in the RebuildTask struct this is the
    /// only public function visible outside of the module.

    pub fn start_rebuild(
        mut task: Box<RebuildTask>,
    ) -> Result<oneshot::Receiver<bool>, Error> {
        //TODO: make dynamic
        let current_core = unsafe { spdk_env_get_current_core() };
        trace!("Will start rebuild task on core {}", current_core);
        trace!("rebuild started at: {:?}", std::time::SystemTime::now());

        let (s, r) = oneshot::channel::<bool>();
        task.sender = Some(s);
        Event::new(current_core, Self::rebuild_init, task)?.call();
        Ok(r)
    }

    /// callback when the read of the rebuild progress has completed
    extern "C" fn read_complete(
        io: *mut spdk_bdev_io,
        success: bool,
        ctx: *mut c_void,
    ) {
        let mut task = RebuildTask::get_rebuild_ctx(ctx);
        if success {
            let _r = task.target_write_blocks(io);
        } else {
            task.shutdown(false);
        }

        std::mem::forget(task);
        Bio::io_free(io);
    }

    /// callback function when write IO of the rebuild phase has completed
    extern "C" fn write_complete(
        io: *mut spdk_bdev_io,
        success: bool,
        ctx: *mut c_void,
    ) {
        let mut task = RebuildTask::get_rebuild_ctx(ctx);
        Bio::io_free(io);

        if !success {
            error!("rebuilding to target failed");
            task.shutdown(false);
            return;
        }

        match task.dispatch_next_segment() {
            Ok(next) => {
                if next {
                    task.rebuild_completed();
                } else {
                    // we are not done yet, forget the task to avoid dropping
                    std::mem::forget(task);
                }
            }

            Err(e) => {
                dbg!(e);
                // task will be dropped
                panic!("error during rebuild");
            }
        }
    }

    /// function called when the rebuild has completed. We record something in
    /// the logs that provides some information about the time in seconds
    /// and average throughput mbs.
    fn rebuild_completed(&mut self) {
        let elapsed = self.start_time.unwrap().elapsed().unwrap();
        let mb = (self.source.get_bdev().block_len() as u64
            * self.source.get_bdev().num_blocks())
            >> 20;

        let mbs = if 0 < elapsed.as_secs() {
            mb / elapsed.as_secs()
        } else {
            mb
        };
        info!(
            "Rebuild completed after {:.} seconds total of {} ({}MBs) from {} to {}",
            elapsed.as_secs(),
            mb,
            mbs,
            self.source.get_bdev().name(),
            self.target.get_bdev().name());

        self.shutdown(true)
    }

    /// helper function to cast the void pointer to a rebuildtask
    #[inline]
    fn get_rebuild_ctx(arg: *mut c_void) -> Box<RebuildTask> {
        unsafe { Box::from_raw(arg as *mut RebuildTask) }
    }

    /// function used shutdown the rebuild task whenever it is successful or
    /// not.
    fn shutdown(&mut self, success: bool) {
        self.send_completion(success);
    }

    /// determine the next segment for which we will issue a rebuild
    #[inline]
    fn num_blocks(&mut self) -> u32 {
        if self.num_segments > 0 {
            self.blocks_per_segment
        } else {
            self.num_segments += 1;
            self.partial_segment
        }
    }
    /// Copy blocks from source to target with increments of segment size.
    /// When the task has been completed, this function returns
    /// Ok(true). When a new IO has been successfully dispatched in returns
    /// Ok(false)
    ///
    /// When memory allocation fails, it shall return an error no attempts will
    /// be made to restart a build automatically, ideally we want this to be
    /// done from the control plane and not internally, but we will implement
    /// some form of implicit retries.
    pub(crate) fn dispatch_next_segment(&mut self) -> Result<bool, Error> {
        let num_blocks = self.num_blocks();

        // if we are a multiple of the max segment size this will be 0 and thus
        // we have completed the job
        if num_blocks == 0 {
            self.shutdown(true);
            return Ok(true);
        }

        if self.current_lba < self.source.get_bdev().num_blocks() {
            self.source_read_blocks(num_blocks)
        } else {
            assert_eq!(self.current_lba, self.source.get_bdev().num_blocks());
            trace!("Rebuild task completed! \\o/");
            Ok(true)
        }
    }

    // wrapper around read_blocks that handles error processing implicitly and
    // updates the internal data structures
    fn source_read_blocks(&mut self, num_blocks: u32) -> Result<bool, Error> {
        let ret = unsafe {
            spdk_bdev_read_blocks(
                self.source.desc,
                self.source.ch,
                self.buf.buf,
                self.current_lba,
                num_blocks as u64,
                Some(Self::read_complete),
                &*self as *const _ as *mut _,
            )
        };

        if ret == 0 {
            self.current_lba += num_blocks as u64;
            self.num_segments -= 1;
            Ok(false)
        } else {
            // we should be able to retry later for now fail on all errors;
            // typically, with ENOMEM we should retry
            // however, we want to delay this so likely use a
            // (one time) poller?
            Err(Error::Internal("failed to dispatch IO".into()))
        }
    }

    /// wrapper function around write_blocks
    pub(crate) fn target_write_blocks(
        &self,
        io: *mut spdk_bdev_io,
    ) -> Result<(), Error> {
        let bio = Bio::from(io);
        let rc = unsafe {
            spdk_bdev_write_blocks(
                self.target.desc,
                self.target.ch,
                self.buf.buf,
                bio.offset(),
                bio.num_blocks(),
                Some(Self::write_complete),
                &*self as *const _ as *mut _,
            )
        };

        if rc != 0 {
            panic!("failed IO");
        }

        Ok(())
    }

    /// send the callee that we completed successfully
    fn send_completion(&mut self, success: bool) {
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(success);
        }
    }

    /// progress function that prints to the log; this will be removed in the
    /// future and will be exposed via an API call
    extern "C" fn progress(ctx: *mut c_void) -> i32 {
        let mut task = RebuildTask::get_rebuild_ctx(ctx);
        info!(
            "Rebuild from {} to {} MiBs: {}",
            task.source.get_bdev().name(),
            task.target.get_bdev().name(),
            (((task.current_lba - task.previous_lba)
                * task.source.get_bdev().block_len() as u64)
                >> 20)
                * 2 // times two here as we to account for the read/write cycle
        );

        task.previous_lba = task.current_lba;
        std::mem::forget(task);
        0
    }

    /// the actual start rebuild task in FFI context that is called by the
    /// reactor
    extern "C" fn rebuild_init(ctx: *mut c_void, _arg2: *mut c_void) {
        let mut task = RebuildTask::get_rebuild_ctx(ctx);
        task.start_time = Some(std::time::SystemTime::now());
        match task.dispatch_next_segment() {
            Err(next) => {
                error!("{:?}", next);
                let _ = task.sender.unwrap().send(false);
            }
            Ok(..) => {
                register_poller(Self::progress, task, 1_000_000).unwrap();
            }
        }
    }
}
