#![warn(missing_docs)]

use crate::core::{Bdev, BdevHandle, DmaBuf, Reactors};
use crossbeam::channel::{unbounded, Receiver};
use once_cell::sync::OnceCell;
use snafu::ResultExt;
use spdk_sys::spdk_get_thread;
use std::{cell::UnsafeCell, collections::HashMap};

use futures::{channel::mpsc, StreamExt};

use super::rebuild_api::*;

/// Global list of rebuild jobs using a static OnceCell
pub(super) struct RebuildInstances {
    inner: UnsafeCell<HashMap<String, RebuildJob>>,
}

unsafe impl Sync for RebuildInstances {}
unsafe impl Send for RebuildInstances {}

/// Result returned by each segment task worker
/// used to communicate with the management task indicating that the
/// segment task worker is ready to copy another segment
struct TaskResult {
    /// block that was being rebuilt
    blk: u64,
    /// id of the task
    id: u64,
    /// encountered error, if any
    error: Option<RebuildError>,
}

/// Number of concurrent copy tasks per rebuild job
const SEGMENT_TASKS: u64 = 4;
/// Size of each segment used by the copy task
const SEGMENT_SIZE: u64 = 10 * 1024; // 10KiB

/// Each rebuild task needs a unique buffer to read/write from source to target
/// a mpsc channel is used to communicate with the management task and each
/// task used a clone of the sender allowing the management to poll a single
/// receiver
pub(super) struct RebuildTasks {
    buffers: Vec<DmaBuf>,
    senders: Vec<mpsc::Sender<TaskResult>>,

    channel: (mpsc::Sender<TaskResult>, mpsc::Receiver<TaskResult>),
    active: u64,
    total: u64,
}

impl RebuildJob {
    /// Stores a rebuild job in the rebuild job list
    pub(super) fn store(self: Self) -> Result<(), RebuildError> {
        let rebuild_list = Self::get_instances();

        if rebuild_list.contains_key(&self.destination) {
            Err(RebuildError::JobAlreadyExists {
                job: self.destination,
            })
        } else {
            let _ = rebuild_list.insert(self.destination.clone(), self);
            Ok(())
        }
    }

    /// Returns a new rebuild job based on the parameters
    pub(super) fn new(
        nexus: &str,
        source: &str,
        destination: &str,
        start: u64,
        end: u64,
        complete_fn: fn(String, String) -> (),
    ) -> Result<Self, RebuildError> {
        let source_hdl =
            BdevHandle::open(source, false, false).context(NoBdevHandle {
                bdev: source,
            })?;
        let destination_hdl = BdevHandle::open(destination, true, false)
            .context(NoBdevHandle {
                bdev: destination,
            })?;

        if !Self::validate(&source_hdl.get_bdev(), &destination_hdl.get_bdev())
        {
            return Err(RebuildError::InvalidParameters {});
        };

        // validation passed, block size is the same for both
        let block_size = destination_hdl.get_bdev().block_len() as u64;
        let segment_size_blks = (SEGMENT_SIZE / block_size) as u64;

        let mut tasks = RebuildTasks {
            buffers: Vec::new(),
            senders: Vec::new(),
            // only sending one message per channel at a time so we don't need
            // the extra buffer
            channel: mpsc::channel(0),
            active: 0,
            total: SEGMENT_TASKS,
        };

        for _ in 0 .. tasks.total {
            let copy_buffer = source_hdl
                .dma_malloc((segment_size_blks * block_size) as usize)
                .context(NoCopyBuffer {})?;
            tasks.buffers.push(copy_buffer);
            tasks.senders.push(tasks.channel.0.clone());
        }

        let (source, destination, nexus) = (
            source.to_string(),
            destination.to_string(),
            nexus.to_string(),
        );

        Ok(Self {
            nexus,
            source,
            source_hdl,
            destination,
            destination_hdl,
            start,
            end,
            next: start,
            block_size,
            segment_size_blks,
            tasks,
            complete_fn,
            complete_chan: unbounded::<RebuildState>(),
            state: RebuildState::Pending,
        })
    }

    // Runs the management async task that kicks off N rebuild copy tasks and
    // awaits each completion. When any task completes it kicks off another
    // until the bdev is fully rebuilt
    pub(super) async fn run(&mut self) {
        self.change_state(RebuildState::Running);
        self.next = self.start;
        self.stats();

        self.start_all_tasks();
        while self.tasks.active > 0 {
            match self.await_one_task().await {
                Some(r) => match r.error {
                    None => {
                        if self.state == RebuildState::Stopped
                            || self.state == RebuildState::Paused
                        {
                            // await all active tasks as we might still have
                            // ongoing IO do we need
                            // a timeout?
                            self.await_all_tasks().await;
                            break;
                        }

                        self.start_task_by_id(r.id);
                    }
                    Some(e) => {
                        error!("Failed to rebuild segment id {} block {} with error: {}", r.id, r.blk, e);
                        self.change_state(RebuildState::Failed);
                        self.await_all_tasks().await;
                        break;
                    }
                },
                None => {
                    // all senders have disconnected, out of place termination?
                    self.change_state(RebuildState::Failed);

                    if self.tasks.active != 0 {
                        error!(
                            "Completing rebuild with potentially {} active tasks",
                            self.tasks.active
                        );
                    }
                    break;
                }
            }
        }

        self.complete();
    }

    /// Copies one segment worth of data from source into destination
    async fn copy_one(
        &mut self,
        id: u64,
        blk: u64,
    ) -> Result<(), RebuildError> {
        let mut copy_buffer: DmaBuf;

        let mut copy_buffer = if (blk + self.segment_size_blks) > self.end {
            let segment_size_blks = self.end - blk;

            trace!(
                    "Adjusting last segment size from {} to {}. offset: {}, start: {}, end: {}",
                    self.segment_size_blks, segment_size_blks, blk, self.start, self.end,
                );

            copy_buffer = self
                .source_hdl
                .dma_malloc((segment_size_blks * self.block_size) as usize)
                .context(NoCopyBuffer {})?;

            &mut copy_buffer
        } else {
            &mut self.tasks.buffers[id as usize]
        };

        self.source_hdl
            .read_at(blk * self.block_size, &mut copy_buffer)
            .await
            .context(IoError {
                bdev: &self.source,
            })?;

        self.destination_hdl
            .write_at(blk * self.block_size, &copy_buffer)
            .await
            .context(IoError {
                bdev: &self.destination,
            })?;

        Ok(())
    }

    fn complete(&mut self) {
        self.stats();
        self.send_complete();
    }

    /// Calls the job's registered complete fn callback and complete sender
    /// channel
    fn send_complete(&mut self) {
        // should this return a status before we complete the sender channel?
        (self.complete_fn)(self.nexus.clone(), self.destination.clone());
        if let Err(e) = self.complete_chan.0.send(self.state) {
            error!("Rebuild Job {} of nexus {} failed to send complete via the unbound channel with err {}", self.destination, self.nexus, e);
        }
    }

    /// Check if the source and destination block devices are compatible for
    /// rebuild
    fn validate(source: &Bdev, destination: &Bdev) -> bool {
        !(source.size_in_bytes() != destination.size_in_bytes()
            || source.block_len() != destination.block_len())
    }

    /// Changing the state should be performed on the same
    /// reactor as the rebuild job
    fn change_state(&mut self, new_state: RebuildState) {
        info!(
            "Rebuild job {}: changing state from {:?} to {:?}",
            self.destination, self.state, new_state
        );
        self.state = new_state;
    }

    /// Get the rebuild job instances container, we ensure that this can only
    /// ever be called on a properly allocated thread
    pub(super) fn get_instances() -> &'static mut HashMap<String, Self> {
        let thread = unsafe { spdk_get_thread() };
        if thread.is_null() {
            panic!("not called from SPDK thread")
        }

        static REBUILD_INSTANCES: OnceCell<RebuildInstances> = OnceCell::new();

        let global_instances =
            REBUILD_INSTANCES.get_or_init(|| RebuildInstances {
                inner: UnsafeCell::new(HashMap::new()),
            });

        unsafe { &mut *global_instances.inner.get() }
    }
}

impl RebuildOperations for RebuildJob {
    fn stats(&self) -> Option<RebuildStats> {
        info!(
            "State: {:?}, Src: {}, Dst: {}, start: {}, end: {}, next: {}, block: {}",
            self.state, self.source, self.destination,
            self.start, self.end, self.next, self.block_size
        );

        None
    }

    fn start(&mut self) -> Receiver<RebuildState> {
        let destination = self.destination.clone();
        let complete_receiver = self.complete_chan.clone().1;

        Reactors::get_by_core(0).unwrap().send_future(async move {
            let job = match RebuildJob::lookup(&destination) {
                Ok(job) => job,
                Err(_) => {
                    return error!(
                        "Failed to find and start the rebuild job {}",
                        destination
                    );
                }
            };

            // todo: WA until cas-194 is addressed...
            if job.state == RebuildState::Pending {
                job.run().await;
            }
        });
        complete_receiver
    }

    fn stop(&mut self) -> Result<(), RebuildError> {
        match self.state {
            RebuildState::Pending | RebuildState::Paused => {
                self.change_state(RebuildState::Stopped);
                // The rebuild is paused or pending so call complete here
                // because the run function is inactive
                self.complete();
            }
            _ => self.change_state(RebuildState::Stopped),
        }

        Ok(())
    }

    fn pause(&mut self) -> Result<(), RebuildError> {
        match self.state {
            RebuildState::Running | RebuildState::Pending => {
                self.change_state(RebuildState::Paused);
                Ok(())
            }
            _ => Err(RebuildError::OpError {
                operation: "Pause".to_string(),
                state: self.state.to_string(),
            }),
        }
    }

    fn resume(&mut self) -> Result<(), RebuildError> {
        match self.state {
            RebuildState::Paused => {
                // Kick off the rebuild job again
                self.change_state(RebuildState::Pending);
                self.start();
                Ok(())
            }
            _ => Err(RebuildError::OpError {
                operation: "Resume".to_string(),
                state: self.state.to_string(),
            }),
        }
    }
}

impl RebuildJob {
    fn start_all_tasks(&mut self) {
        assert_eq!(self.tasks.active, 0, "{} active tasks", self.tasks.active);

        for n in 0 .. self.tasks.total {
            self.next = match self.send_segment_task(n) {
                Some(next) => {
                    self.tasks.active += 1;
                    next
                }
                None => break, /* we've already got enough tasks to rebuild
                                * the bdev */
            };
        }
    }

    fn start_task_by_id(&mut self, id: u64) {
        match self.send_segment_task(id) {
            Some(next) => {
                self.tasks.active += 1;
                self.next = next;
            }
            None => {
                if self.tasks.active == 0 {
                    self.state = RebuildState::Completed;
                }
            }
        };
    }

    async fn await_one_task(&mut self) -> Option<TaskResult> {
        self.tasks.channel.1.next().await.map(|f| {
            self.tasks.active -= 1;
            f
        })
    }

    async fn await_all_tasks(&mut self) {
        while self.await_one_task().await.is_some() {
            if self.tasks.active == 0 {
                break;
            }
        }
    }

    /// Sends one segment worth of data in a reactor future and notifies the
    /// management channel. Returns the next segment offset to rebuild, if any
    fn send_segment_task(&self, id: u64) -> Option<u64> {
        if self.next >= self.end {
            None
        } else {
            let blk = self.next;
            let next =
                std::cmp::min(self.next + self.segment_size_blks, self.end);
            let name = self.destination.clone();

            Reactors::current().send_future(async move {
                let job = Self::lookup(&name).unwrap();

                let r = TaskResult {
                    blk,
                    id,
                    error: job.copy_one(id, blk).await.err(),
                };

                if let Err(e) = job.tasks.senders[id as usize].start_send(r) {
                    error!("Failed to notify job of segment id: {} blk: {} completion, err: {}", id, blk, e);
                }
            });

            Some(next)
        }
    }
}
