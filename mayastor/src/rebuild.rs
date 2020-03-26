use crate::{
    bdev::nexus::nexus_bdev::nexus_lookup,
    core::{Bdev, BdevHandle, CoreError, DmaBuf, DmaError, Reactors},
};
use crossbeam::channel::{unbounded, Receiver, Sender};
use snafu::{ResultExt, Snafu};

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum RebuildError {
    #[snafu(display("Failed to allocate buffer for the rebuild copy"))]
    NoCopyBuffer { source: DmaError },
    #[snafu(display("Failed to validate rebuild task creation parameters"))]
    InvalidParameters {},
    #[snafu(display("Failed to get a handle for bdev {}", bdev))]
    NoBdevHandle { source: CoreError, bdev: String },
    #[snafu(display("IO failed for bdev {}", bdev))]
    IoError { source: CoreError, bdev: String },
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum RebuildState {
    Pending,
    Running,
    Failed,
    Completed,
}

#[derive(Debug)]
pub struct RebuildTask {
    nexus_name: String,
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

pub trait RebuildActions {
    fn stats(&self) -> Option<RebuildStats>;
    fn start(&mut self) -> Receiver<RebuildState>;
    fn stop(&mut self);
    fn pause(&mut self);
    fn resume(&mut self);
}

impl RebuildTask {
    pub fn new(
        nexus_name: String,
        source: String,
        destination: String,
        start: u64,
        end: u64,
        complete_fn: fn(String, String) -> (),
    ) -> Result<RebuildTask, RebuildError> {
        let source_hdl = BdevHandle::open(&source, false, false)
            .context(NoBdevHandle { bdev: &source })?;
        let destination_hdl = BdevHandle::open(&destination, true, false)
            .context(NoBdevHandle { bdev: &destination })?;

        if !RebuildTask::validate(
            &source_hdl.get_bdev(),
            &destination_hdl.get_bdev(),
        ) {
            return Err(RebuildError::InvalidParameters {});
        };

        let segment_size = 10 * 1024;
        // validation passed, block size is the same for both
        let block_size = destination_hdl.get_bdev().block_len() as u64;
        let segment_size_blks = (segment_size / block_size) as u64;

        let copy_buffer = source_hdl
            .dma_malloc((segment_size_blks * block_size) as usize)
            .context(NoCopyBuffer {})?;

        Ok(RebuildTask {
            nexus_name,
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

    /// rebuild a non-healthy child from a healthy child from start to end
    async fn run(&mut self) {
        self.state = RebuildState::Running;
        self.current = self.start;
        self.stats();

        while self.current < self.end {
            if let Err(e) = self.copy_one().await {
                error!("Failed to copy segment {}", e);
                self.state = RebuildState::Failed;
                self.send_complete();
            }
            // TODO: check if the task received a "pause/stop" request, eg child
            // is being removed
        }

        self.state = RebuildState::Completed;
        self.send_complete();
    }

    /// copy one segment worth of data from source into destination
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
            .context(IoError { bdev: &self.source })?;

        self.destination_hdl
            .write_at(self.current * self.block_size, &self.copy_buffer)
            .await
            .context(IoError {
                bdev: &self.destination,
            })?;

        self.current += self.segment_size_blks;
        Ok(())
    }

    fn send_complete(&self) {
        self.stats();
        (self.complete_fn)(self.nexus_name.clone(), self.destination.clone());
        if let Err(e) = self.complete_chan.0.send(self.state) {
            error!("Rebuild Task {} of nexus {} failed to send complete via the unbound channel with err {}", self.destination, self.nexus_name, e);
        }
    }

    fn validate(source: &Bdev, destination: &Bdev) -> bool {
        !(source.size_in_bytes() != destination.size_in_bytes()
            || source.block_len() != destination.block_len())
    }
}

impl RebuildActions for RebuildTask {
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
        let nexus = self.nexus_name.clone();
        let destination = self.destination.clone();
        let complete_receiver = self.complete_chan.clone().1;

        Reactors::current().send_future(async move {
            let nexus = match nexus_lookup(&nexus) {
                Some(nexus) => nexus,
                None => {
                    return error!("Failed to find the nexus {}", nexus);
                }
            };

            let task = match nexus
                .rebuilds
                .iter_mut()
                .find(|t| t.destination == destination)
            {
                Some(task) => task,
                None => {
                    return error!(
                        "Failed to find the rebuild task {} for nexus {}",
                        destination, nexus.name
                    );
                }
            };

            task.run().await;
        });
        complete_receiver
    }
    fn stop(&mut self) {
        todo!("stop the rebuild task");
    }
    fn pause(&mut self) {
        todo!("pause the rebuild task");
    }
    fn resume(&mut self) {
        todo!("resume the rebuild task");
    }
}
