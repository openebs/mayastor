#![allow(dead_code)]

use std::{os::raw::c_void, time::SystemTime};

use futures::channel::oneshot;
use nix::errno::Errno;
use snafu::Snafu;

use spdk_sys::spdk_bdev_io;

use crate::{
    bdev::nexus::nexus_bdev::{nexus_lookup, NexusState},
    core::{Descriptor, DmaBuf, DmaError},
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

/// struct that holds the state of a copy task. This struct
/// is used during rebuild.
#[derive(Debug)]
pub struct RebuildTask {
    pub state: RebuildState,
    /// the source where to copy from
    pub source: Descriptor,
    /// the target where to copy to
    pub target: Descriptor,
    /// the last LBA for which an io copy has been submitted
    current_lba: u64,
    /// used to provide progress indication
    previous_lba: u64,
    /// used to signal completion to the callee
    sender: Option<oneshot::Sender<RebuildState>>,
    pub completed: Option<oneshot::Receiver<RebuildState>>,
    /// progress reported to logs
    progress: Option<u32>,
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
    pub fn new(_source: String, _target: String) -> Result<Box<Self>, Error> {
        // if the target is to small, we bail out. A future extension is to see,
        // if we can grow; the target to match the size of the source.
        unimplemented!();
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
        _io: *mut spdk_bdev_io,
        _success: bool,
        _ctx: *mut c_void,
    ) {
        unimplemented!()
    }

    /// callback function when write IO of the rebuild phase has completed
    extern "C" fn write_complete(
        _io: *mut spdk_bdev_io,
        _success: bool,
        _ctx: *mut c_void,
    ) {
        unimplemented!();
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
        _num_blocks: u32,
    ) -> Result<RebuildState, Error> {
        unimplemented!();
    }

    /// wrapper function around write_blocks
    pub(crate) fn target_write_blocks(
        &mut self,
        _io: *mut spdk_bdev_io,
    ) -> Result<(), Error> {
        unimplemented!();
    }

    /// send the callee that we completed successfully
    fn send_completion(&mut self, state: RebuildState) {
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(state);
        }
    }

    /// progress function that prints to the log; this will be removed in the
    /// future and will be exposed via an API call
    extern "C" fn progress(_ctx: *mut c_void) -> i32 {
        unimplemented!()
    }

    fn start_progress_poller(&mut self) {
        self.progress = None;
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
