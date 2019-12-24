use futures::channel::oneshot;
use nix::errno::Errno;
use snafu::{ResultExt, Snafu};
use spdk_sys::{
    spdk_bdev_io,
    spdk_bdev_read_blocks,
    spdk_bdev_write_blocks,
    SPDK_BDEV_LARGE_BUF_MAX_SIZE,
};
use std::{convert::TryInto, os::raw::c_void, rc::Rc, time::SystemTime};

use crate::{
    bdev::nexus::{
        nexus_bdev::{nexus_lookup, NexusState},
        nexus_io::Bio,
    },
    descriptor::Descriptor,
    dma::{DmaBuf, DmaError},
    event::MayaCtx,
    executor::errno_result_from_i32,
    poller::{register_poller, PollTask},
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Rebuild source {} is larger than the target {}",
        src,
        tgt
    ))]
    SourceBigger { src: String, tgt: String },
    #[snafu(display("Cannot suspend an already completed rebuild task"))]
    SuspendCompleted {},
    #[snafu(display(
        "Cannot resume a rebuild task which has not been suspended"
    ))]
    ResumeNotSuspended {},
    #[snafu(display("Failed to dispatch rebuild IO"))]
    DispatchIo { source: Errno },
    #[snafu(display("Failed to allocate buffer for rebuild IO"))]
    BufferAlloc { source: DmaError },
}

#[derive(Debug, Copy, Clone, PartialOrd, PartialEq)]
pub enum RebuildState {
    /// Initialized properly, but not running
    Initialized,
    /// Running
    Running,
    /// Completed
    Completed,
    /// the rebuild task has failed
    Failed,
    /// a request has been made to cancel the task
    Cancelled,
    /// Suspended
    Suspended,
}

impl MayaCtx for RebuildTask {
    type Item = RebuildTask;
    #[inline]
    fn into_ctx<'a>(arg: *mut c_void) -> &'a mut Self::Item {
        unsafe { &mut *(arg as *const _ as *mut RebuildTask) }
    }
}

/// struct that holds the state of a copy task. This struct
/// is used during rebuild.
#[derive(Debug)]
pub struct RebuildTask {
    pub state: RebuildState,
    /// the source where to copy from
    pub source: Rc<Descriptor>,
    /// the target where to copy to
    pub target: Rc<Descriptor>,
    /// the last LBA for which an io copy has been submitted
    current_lba: u64,
    /// used to provide progress indication
    previous_lba: u64,
    /// used to signal completion to the callee
    sender: Option<oneshot::Sender<RebuildState>>,
    pub completed: Option<oneshot::Receiver<RebuildState>>,
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
    pub start_time: Option<SystemTime>,
    /// the name of the nexus we refer are rebuilding
    pub(crate) nexus: Option<String>,
}

impl RebuildTask {
    /// return a new rebuild task
    pub fn new(
        source: Rc<Descriptor>,
        target: Rc<Descriptor>,
    ) -> Result<Box<Self>, Error> {
        // if the target is to small, we bail out. A future extension is to see,
        // if we can grow; the target to match the size of the source.

        if target.get_bdev().num_blocks() < source.get_bdev().num_blocks() {
            return Err(Error::SourceBigger {
                src: source.get_bdev().name(),
                tgt: target.get_bdev().name(),
            });
        }

        let num_blocks = target.get_bdev().num_blocks();
        let block_len = target.get_bdev().block_len();
        let blocks_per_segment =
            u64::from(SPDK_BDEV_LARGE_BUF_MAX_SIZE / block_len);

        let num_segments = num_blocks / blocks_per_segment as u64;
        let remainder = num_blocks % blocks_per_segment;

        let buf = source
            .dma_malloc(
                (blocks_per_segment * source.get_bdev().block_len() as u64)
                    as usize,
            )
            .context(BufferAlloc {})?;

        let (s, r) = oneshot::channel::<RebuildState>();
        let task = Box::new(Self {
            state: RebuildState::Initialized,
            blocks_per_segment: blocks_per_segment as u32,
            buf,
            current_lba: 0,
            num_segments,
            previous_lba: 0,
            progress: None,
            partial_segment: remainder.try_into().unwrap(),
            sender: Some(s),
            completed: Some(r),
            source,
            target,
            start_time: None,
            nexus: None,
        });

        Ok(task)
    }

    pub fn suspend(&mut self) -> Result<RebuildState, Error> {
        info!(
            "{}: suspending rebuild task",
            self.nexus.as_ref().unwrap_or(&String::from("unknown"))
        );

        if self.state == RebuildState::Completed {
            Err(Error::SuspendCompleted {})
        } else {
            self.state = RebuildState::Suspended;
            Ok(self.state)
        }
    }

    pub fn resume(&mut self) -> Result<RebuildState, Error> {
        info!(
            "{}: resuming rebuild task",
            self.nexus.as_ref().unwrap_or(&String::from("unknown"))
        );

        if self.state == RebuildState::Suspended {
            self.state = RebuildState::Running;
            self.next_segment()
        } else {
            Err(Error::ResumeNotSuspended {})
        }
    }

    pub async fn completed(
        &mut self,
    ) -> Result<RebuildState, oneshot::Canceled> {
        self.completed.as_mut().unwrap().await
    }

    /// callback when the read of the rebuild progress has completed
    extern "C" fn read_complete(
        io: *mut spdk_bdev_io,
        success: bool,
        ctx: *mut c_void,
    ) {
        let task = RebuildTask::into_ctx(ctx);
        trace!("rebuild read complete {:?}", Bio(io));
        if success {
            let _r = task.target_write_blocks(io);
        } else {
            task.shutdown(false);
        }

        Bio::io_free(io);
    }

    /// callback function when write IO of the rebuild phase has completed
    extern "C" fn write_complete(
        io: *mut spdk_bdev_io,
        success: bool,
        ctx: *mut c_void,
    ) {
        let task = RebuildTask::into_ctx(ctx);

        trace!("rebuild write complete {:?}", Bio(io));
        Bio::io_free(io);

        if !success {
            error!("rebuilding to target failed");
            task.shutdown(false);
            return;
        }

        if task.state == RebuildState::Suspended {
            info!("{}: rebuild suspended", task.nexus.as_ref().unwrap());
            return;
        }

        match task.next_segment() {
            Ok(next) => match next {
                RebuildState::Completed => task.rebuild_completed(),
                RebuildState::Initialized => {}
                RebuildState::Running => {}
                RebuildState::Failed => {}
                RebuildState::Cancelled => {}
                RebuildState::Suspended => {
                    info!("suspended rebuild!");
                    dbg!(task);
                }
            },
            Err(e) => {
                dbg!(e);
                panic!("error during rebuild");
            } // fallthrough
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

    /// function used shutdown the rebuild task whenever it is successful or
    /// not.
    pub fn shutdown(&mut self, success: bool) {
        if success {
            self.state = RebuildState::Completed;
        } else {
            self.state = RebuildState::Failed;
        }

        let _ = self.progress.take();
        self.send_completion(self.state);
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

    pub fn current(&self) -> u64 {
        self.current_lba
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
    fn next_segment(&mut self) -> Result<RebuildState, Error> {
        let num_blocks = self.num_blocks();

        // if we are a multiple of the max segment size this will be 0 and thus
        // we have completed the job
        if num_blocks == 0 {
            self.shutdown(true);
            return Ok(RebuildState::Completed);
        }

        if self.current_lba < self.source.get_bdev().num_blocks() {
            self.source_read_blocks(num_blocks)
        } else {
            assert_eq!(self.current_lba, self.source.get_bdev().num_blocks());
            trace!("Rebuild task completed! \\o/");
            Ok(RebuildState::Completed)
        }
    }

    // wrapper around read_blocks that handles error processing implicitly and
    // updates the internal data structures
    fn source_read_blocks(
        &mut self,
        num_blocks: u32,
    ) -> Result<RebuildState, Error> {
        let errno = unsafe {
            spdk_bdev_read_blocks(
                self.source.as_ptr(),
                self.source.channel(),
                *self.buf,
                self.current_lba,
                num_blocks as u64,
                Some(Self::read_complete),
                &*self as *const _ as *mut _,
            )
        };

        match errno_result_from_i32((), errno) {
            Ok(_) => {
                self.current_lba += num_blocks as u64;
                self.num_segments -= 1;
                Ok(self.state)
            }
            Err(err) => {
                // we should be able to retry later for now fail on all errors;
                // typically, with ENOMEM we should retry
                // however, we want to delay this so likely use a
                // (one time) poller?
                self.state = RebuildState::Failed;
                Err(err).context(DispatchIo {})
            }
        }
    }

    /// wrapper function around write_blocks
    pub(crate) fn target_write_blocks(
        &mut self,
        io: *mut spdk_bdev_io,
    ) -> Result<(), Error> {
        let bio = Bio(io);
        let errno = unsafe {
            spdk_bdev_write_blocks(
                self.target.as_ptr(),
                self.target.channel(),
                *self.buf,
                bio.offset(),
                bio.num_blocks(),
                Some(Self::write_complete),
                &*self as *const _ as *mut _,
            )
        };

        // XXX what do we need to set/clear when the write IO fails?
        errno_result_from_i32((), errno).context(DispatchIo {})
    }

    /// send the callee that we completed successfully
    fn send_completion(&mut self, state: RebuildState) {
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(state);
        }
    }

    /// progress function that prints to the log; this will be removed in the
    /// future and will be exposed via an API call
    extern "C" fn progress(ctx: *mut c_void) -> i32 {
        let mut task = RebuildTask::into_ctx(ctx);
        info!(
            "Rebuild {:?} from {} to {} MiBs: {}",
            task.state,
            task.source.get_bdev().name(),
            task.target.get_bdev().name(),
            (((task.current_lba - task.previous_lba)
                * task.source.get_bdev().block_len() as u64)
                >> 20)
                * 2 // times two here as we to account for the read/write cycle
        );

        task.previous_lba = task.current_lba;
        0
    }

    fn start_progress_poller(&mut self) {
        self.progress =
            Some(register_poller(Self::progress, &*self, 1_000_000).unwrap());
    }

    pub fn run(&mut self) {
        self.start_time = Some(std::time::SystemTime::now());

        if let Some(name) = self.nexus.as_ref() {
            if let Some(nexus) = nexus_lookup(name) {
                nexus.set_state(NexusState::Remuling);
            } else {
                error!("nexus {} gone, aborting rebuild", name);
                self.send_completion(RebuildState::Cancelled);
            }
        }

        match self.next_segment() {
            Err(next) => {
                error!("{:?}", next);
                self.shutdown(false);
            }
            Ok(..) => {
                self.state = RebuildState::Running;
                self.start_progress_poller();
            }
        }
    }
}
