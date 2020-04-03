use crate::core::{Bdev, BdevHandle, CoreError, DmaBuf, DmaError, Reactors};
use crossbeam::channel::{unbounded, Receiver, Sender};
use once_cell::sync::OnceCell;
use snafu::{ResultExt, Snafu};
use spdk_sys::spdk_get_thread;
use std::{cell::UnsafeCell, collections::HashMap, fmt};

pub struct RebuildInstances {
    inner: UnsafeCell<HashMap<String, RebuildJob>>,
}

unsafe impl Sync for RebuildInstances {}
unsafe impl Send for RebuildInstances {}

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum RebuildError {
    #[snafu(display("Failed to allocate buffer for the rebuild copy"))]
    NoCopyBuffer { source: DmaError },
    #[snafu(display("Failed to validate rebuild job creation parameters"))]
    InvalidParameters {},
    #[snafu(display("Failed to get a handle for bdev {}", bdev))]
    NoBdevHandle { source: CoreError, bdev: String },
    #[snafu(display("IO failed for bdev {}", bdev))]
    IoError { source: CoreError, bdev: String },
    #[snafu(display("Failed to find rebuild job {}", job))]
    JobNotFound { job: String },
    #[snafu(display("Job {} already exists", job))]
    JobAlreadyExists { job: String },
    #[snafu(display("Missing rebuild destination {}", job))]
    MissingDestination { job: String },
    #[snafu(display(
        "{} operation failed because current rebuild state is {}.",
        operation,
        state,
    ))]
    OpError { operation: String, state: String },
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum RebuildState {
    Pending,
    Running,
    Stopped,
    Paused,
    Failed,
    Completed,
}

impl fmt::Display for RebuildState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            RebuildState::Pending => write!(f, "pending"),
            RebuildState::Running => write!(f, "running"),
            RebuildState::Stopped => write!(f, "stopped"),
            RebuildState::Paused => write!(f, "paused"),
            RebuildState::Failed => write!(f, "failed"),
            RebuildState::Completed => write!(f, "completed"),
        }
    }
}

#[derive(Debug)]
pub struct RebuildJob {
    pub nexus: String,
    source: String,
    source_hdl: BdevHandle,
    pub destination: String,
    destination_hdl: BdevHandle,
    block_size: u64,
    start: u64,
    end: u64,
    current: u64,
    segment_size_blks: u64,
    copy_buffer: DmaBuf,
    complete_fn: fn(String, String) -> (),
    pub complete_chan: (Sender<RebuildState>, Receiver<RebuildState>),
    pub state: RebuildState,
}

pub struct RebuildStats {}

pub trait RebuildOperations {
    fn stats(&self) -> Option<RebuildStats>;
    fn start(&mut self) -> Receiver<RebuildState>;
    fn stop(&mut self) -> Result<(), RebuildError>;
    fn pause(&mut self) -> Result<(), RebuildError>;
    fn resume(&mut self) -> Result<(), RebuildError>;
}

impl RebuildJob {
    /// Returns a newly created RebuildJob which is already stored in the
    /// rebuild list
    pub fn create<'a>(
        nexus: &str,
        source: &str,
        destination: &'a str,
        start: u64,
        end: u64,
        complete_fn: fn(String, String) -> (),
    ) -> Result<&'a mut Self, RebuildError> {
        Self::new(nexus, source, destination, start, end, complete_fn)?
            .store()?;

        Ok(Self::lookup(destination)?)
    }

    /// Lookup a rebuild job by its destination uri and return it
    pub fn lookup(name: &str) -> Result<&mut Self, RebuildError> {
        if let Some(job) = Self::get_instances().get_mut(name) {
            Ok(job)
        } else {
            Err(RebuildError::JobNotFound {
                job: name.to_owned(),
            })
        }
    }

    /// Lookup a rebuild job by its destination uri then remove and return it
    pub fn remove(name: &str) -> Result<Self, RebuildError> {
        match Self::get_instances().remove(name) {
            Some(job) => Ok(job),
            None => Err(RebuildError::JobNotFound {
                job: name.to_owned(),
            }),
        }
    }

    /// Number of rebuild job instances
    pub fn count() -> usize {
        Self::get_instances().len()
    }

    /// Stores a rebuild job in the rebuild job list
    fn store(self: Self) -> Result<(), RebuildError> {
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
    fn new(
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

        let segment_size = 10 * 1024;
        // validation passed, block size is the same for both
        let block_size = destination_hdl.get_bdev().block_len() as u64;
        let segment_size_blks = (segment_size / block_size) as u64;

        let copy_buffer = source_hdl
            .dma_malloc((segment_size_blks * block_size) as usize)
            .context(NoCopyBuffer {})?;

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
            current: start,
            block_size,
            segment_size_blks,
            copy_buffer,
            complete_fn,
            complete_chan: unbounded::<RebuildState>(),
            state: RebuildState::Pending,
        })
    }

    /// Rebuilds a non-healthy child from a healthy child from start to end
    async fn run(&mut self) {
        self.change_state(RebuildState::Running);
        self.current = self.start;
        self.stats();

        while self.current < self.end {
            if let Err(e) = self.copy_one().await {
                error!("Failed to copy segment {}", e);
                self.change_state(RebuildState::Failed);
                self.send_complete();
                return;
            }

            if self.state == RebuildState::Stopped
                || self.state == RebuildState::Paused
            {
                return self.send_complete();
            }
        }

        self.change_state(RebuildState::Completed);
        self.send_complete();
    }

    /// Copies one segment worth of data from source into destination
    async fn copy_one(&mut self) -> Result<(), RebuildError> {
        // Adjust size of the last segment
        if (self.current + self.segment_size_blks) >= self.start + self.end {
            self.segment_size_blks = self.end - self.current;

            self.copy_buffer = self
                .source_hdl
                .dma_malloc((self.segment_size_blks * self.block_size) as usize)
                .context(NoCopyBuffer {})?;

            info!(
                "Adjusting segment size to {}. offset: {}, start: {}, end: {}",
                self.segment_size_blks, self.current, self.start, self.end
            );
        }

        self.source_hdl
            .read_at(self.current * self.block_size, &mut self.copy_buffer)
            .await
            .context(IoError {
                bdev: &self.source,
            })?;

        self.destination_hdl
            .write_at(self.current * self.block_size, &self.copy_buffer)
            .await
            .context(IoError {
                bdev: &self.destination,
            })?;

        self.current += self.segment_size_blks;
        Ok(())
    }

    /// Calls the job's registered complete fn callback and complete sender
    /// channel
    fn send_complete(&mut self) {
        self.stats();
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
    fn get_instances() -> &'static mut HashMap<String, Self> {
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
            "State: {:?}, Src: {}, Dst: {}, start: {}, end: {}, current: {}, block: {}",
            self.state, self.source, self.destination,
            self.start, self.end, self.current, self.block_size
        );

        None
    }

    // todo: ideally we'd want the nexus out of here but sadly rust does not yet
    // support async trait's
    // the course of action might just be not using traits
    fn start(&mut self) -> Receiver<RebuildState> {
        let destination = self.destination.clone();
        let complete_receiver = self.complete_chan.clone().1;

        Reactors::current().send_future(async move {
            let job = match RebuildJob::lookup(&destination) {
                Ok(job) => job,
                Err(_) => {
                    return error!(
                        "Failed to find the rebuild job {}",
                        destination
                    );
                }
            };

            job.run().await;
        });
        complete_receiver
    }

    fn stop(&mut self) -> Result<(), RebuildError> {
        match self.state {
            RebuildState::Paused => {
                self.change_state(RebuildState::Stopped);
                // The rebuild is paused so call complete here
                // because the run function is inactive
                self.send_complete();
            }
            _ => self.change_state(RebuildState::Stopped),
        }

        Ok(())
    }

    fn pause(&mut self) -> Result<(), RebuildError> {
        match self.state {
            RebuildState::Running => {
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
                // Kick off the rebuild job again.
                // The rebuild state doesn't need to be changed because
                // this is done by the run function
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
