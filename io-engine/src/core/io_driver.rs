//! helper routines to drive IO to the nexus for testing purposes
use futures::channel::oneshot;
use rand::Rng;
use std::{ptr::NonNull, sync::Mutex};

use spdk_rs::libspdk::{
    spdk_bdev_free_io,
    spdk_bdev_io,
    spdk_bdev_read,
    spdk_bdev_reset,
    spdk_bdev_write,
};

use crate::{
    core::{Cores, Descriptor, IoChannel, Mthread, UntypedBdev},
    ffihelper::pair,
    nexus_uri::bdev_create,
};

use spdk_rs::DmaBuf;

#[derive(Debug, Copy, Clone)]
pub enum IoType {
    /// perform random read operations
    Read,
    /// perform random write operations
    Write,
}

impl Default for IoType {
    fn default() -> Self {
        Self::Read
    }
}

#[derive(Debug)]
#[allow(dead_code)]
struct Io {
    /// buffer we read/write from/to
    buf: DmaBuf,
    /// type of IO we are supposed to issue
    iot: IoType,
    /// current offset where we are reading or writing
    offset: u64,
    /// pointer to our the job we belong too
    job: NonNull<Job>,
}

impl Io {
    /// start submitting
    fn run(&mut self, job: *mut Job) {
        self.job = NonNull::new(job).unwrap();
        match self.iot {
            IoType::Read => self.read(0),
            IoType::Write => self.write(0),
        };
    }

    /// obtain a reference to the inner job
    fn job(&mut self) -> &mut Job {
        unsafe { self.job.as_mut() }
    }

    /// dispatch the next IO, this is called from within the completion callback
    pub fn next(&mut self, offset: u64) {
        if self.job().request_reset {
            self.job().request_reset = false;
            self.reset();
            return;
        }

        match self.iot {
            IoType::Read => self.read(offset),
            IoType::Write => self.write(offset),
        }
    }

    /// dispatch the read IO at given offset
    fn read(&mut self, offset: u64) {
        unsafe {
            if spdk_bdev_read(
                self.job.as_ref().desc.as_ptr(),
                self.job.as_ref().ch.as_ref().unwrap().as_ptr(),
                *self.buf,
                offset,
                self.buf.len(),
                Some(Job::io_completion),
                self as *const _ as *mut _,
            ) == 0
            {
                self.job.as_mut().n_inflight += 1;
            } else {
                eprintln!(
                    "failed to submit read IO to {}",
                    self.job.as_ref().bdev.name()
                );
            }
        };
    }

    /// dispatch write IO at given offset
    fn write(&mut self, offset: u64) {
        unsafe {
            if spdk_bdev_write(
                self.job.as_ref().desc.as_ptr(),
                self.job.as_ref().ch.as_ref().unwrap().as_ptr(),
                *self.buf,
                offset,
                self.buf.len(),
                Some(Job::io_completion),
                self as *const _ as *mut _,
            ) == 0
            {
                self.job.as_mut().n_inflight += 1;
            } else {
                eprintln!(
                    "failed to submit write IO to {}",
                    self.job.as_ref().bdev.name()
                );
            }
        };
    }

    /// reset the bdev under test
    pub fn reset(&mut self) {
        unsafe {
            if spdk_bdev_reset(
                self.job.as_ref().desc.as_ptr(),
                self.job.as_ref().ch.as_ref().unwrap().as_ptr(),
                Some(Job::io_completion),
                self as *const _ as *mut _,
            ) == 0
            {
                self.job.as_mut().n_inflight += 1;
            } else {
                eprintln!(
                    "failed to submit reset IO to {}",
                    self.job.as_ref().bdev.name()
                );
            }
        }
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct Job {
    /// that drives IO to a bdev using its own channel.
    bdev: UntypedBdev,
    /// descriptor to the bdev
    desc: Descriptor,
    /// io channel used to submit IO
    ch: Option<IoChannel>,
    /// queue depth configured for this job
    qd: u64,
    /// io_size the io_size is the number of blocks submit per IO
    io_size: u64,
    /// blk_size of the underlying device
    blk_size: u32,
    /// num_blocks the device has
    num_blocks: u64,
    /// aligned set of IOs we can do
    io_blocks: u64,
    /// io queue
    queue: Vec<Io>,
    /// number of IO's completed
    n_io: u64,
    /// number of IO's currently inflight
    n_inflight: u32,
    ///generate random number between 0 and num_block
    rng: rand::rngs::ThreadRng,
    /// drain the job which means that we wait for all pending IO to complete
    /// and stop the run
    drain: bool,
    /// channels used to signal completion
    s: Option<oneshot::Sender<bool>>,
    r: Option<oneshot::Receiver<bool>>,
    /// issue a reset to the bdev
    request_reset: bool,
    /// core to run this job on
    core: u32,
    /// thread this job is run on
    thread: Option<Mthread>,
}

impl Job {
    extern "C" fn io_completion(
        bdev_io: *mut spdk_bdev_io,
        success: bool,
        arg: *mut std::ffi::c_void,
    ) {
        let ioq: &mut Io = unsafe { &mut *arg.cast() };
        let job = unsafe { ioq.job.as_mut() };

        if !success {
            // trace!(
            //     "core: {} mthread: {:?}{}: {:#?}",
            //     Cores::current(),
            //     Mthread::current().unwrap(),
            //     job.thread.as_ref().unwrap().name(),
            //     bdev_io
            // );

            // let bio = Bio::from(bdev_io);
            // dbg!(&bio);
            //
            // dbg!(NvmeStatus::from(bio));
        }

        assert_eq!(Cores::current(), job.core);
        job.n_io += 1;
        job.n_inflight -= 1;

        unsafe { spdk_bdev_free_io(bdev_io) }

        if job.n_inflight == 0 {
            trace!("{} fully drained", job.thread.as_ref().unwrap().name());
            job.s.take().unwrap().send(true).unwrap();
            return;
        }

        if job.drain {
            return;
        }

        let offset = (job.rng.gen::<u64>() % job.io_size) * job.io_blocks;
        ioq.next(offset);
    }

    pub fn stop(&mut self) -> oneshot::Receiver<bool> {
        self.drain = true;
        self.r.take().expect("double shut down for job")
    }

    fn as_ptr(&self) -> *mut Job {
        self as *const _ as *mut _
    }
    /// start the job that will dispatch an IO up to the provided queue depth
    fn start(mut self) -> Box<Job> {
        let thread =
            Mthread::new(format!("job_{}", self.bdev.name()), self.core)
                .unwrap();
        thread.with(|| {
            self.ch = self.desc.get_channel();
            let mut boxed = Box::new(self);
            let ptr = boxed.as_ptr();
            boxed.queue.iter_mut().for_each(|q| q.run(ptr));
            boxed.thread = Mthread::current();
            boxed
        })
    }
}

#[derive(Default)]
pub struct Builder {
    /// bdev URI to create
    uri: String,
    /// queue depth
    qd: u64,
    /// size of each IO
    io_size: u64,
    /// type of workload to generate
    iot: IoType,
    /// existing bdev to use instead of creating one
    bdev: Option<UntypedBdev>,
    /// core to start the job on, the command will crash if the core is invalid
    core: u32,
}

impl Builder {
    pub fn new() -> Self {
        Self::default()
    }

    /// create a bdev using the given URI
    pub fn uri(mut self, uri: &str) -> Self {
        self.uri = String::from(uri);
        self
    }

    /// set the queue depth of the job
    pub fn qd(mut self, qd: u64) -> Self {
        self.qd = qd;
        self
    }

    /// io size per IO for the job
    pub fn io_size(mut self, io_size: u64) -> Self {
        self.io_size = io_size;
        self
    }

    /// issue read or write requests
    pub fn rw(mut self, iot: IoType) -> Self {
        self.iot = iot;
        self
    }

    /// use the given bdev instead of the URI to create the job
    pub fn bdev(mut self, bdev: UntypedBdev) -> Self {
        self.bdev = Some(bdev);
        self
    }
    /// set the core to run on
    pub fn core(mut self, core: u32) -> Self {
        self.core = core;
        self
    }

    pub async fn build(mut self) -> Job {
        let bdev = if self.bdev.is_some() {
            self.bdev.take().unwrap()
        } else {
            let name = bdev_create(&self.uri).await.unwrap();
            UntypedBdev::lookup_by_name(&name).unwrap()
        };

        let desc = bdev.open(true).unwrap();

        let blk_size = bdev.block_len();
        let num_blocks = bdev.num_blocks();

        let io_size = self.io_size / blk_size as u64;
        let io_blocks = num_blocks / io_size;

        let mut queue = Vec::new();

        (0 .. self.qd).for_each(|offset| {
            queue.push(Io {
                buf: DmaBuf::new(self.io_size as u64, bdev.alignment())
                    .unwrap(),
                iot: self.iot,
                offset,
                job: NonNull::dangling(),
            });
        });
        let (s, r) = pair::<bool>();
        Job {
            core: self.core,
            bdev,
            desc,
            ch: None,
            qd: self.qd,
            io_size,
            blk_size,
            num_blocks,
            queue,
            io_blocks,
            n_io: 0,
            n_inflight: 0,
            rng: Default::default(),
            drain: false,
            s: Some(s),
            r: Some(r),
            request_reset: false,
            thread: None,
        }
    }
}

pub struct JobQueue {
    #[allow(clippy::vec_box)]
    inner: Mutex<Vec<Box<Job>>>,
}

impl Default for JobQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl JobQueue {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Vec::new()),
        }
    }

    /// look up the job by bdev name
    fn lookup(&self, name: &str) -> Option<Box<Job>> {
        let mut inner = self.inner.lock().unwrap();
        inner
            .iter()
            .position(|job| job.bdev.name() == name)
            .map(|index| inner.remove(index))
    }

    /// start the job
    pub fn start(&self, job: Job) {
        self.inner.lock().unwrap().push(job.start());
    }

    /// stop the job by bdev name
    pub async fn stop(&self, bdevname: &str) {
        if let Some(mut job) = self.lookup(bdevname) {
            job.stop().await.unwrap();
            job.thread.unwrap().with(|| drop(job));
        }
    }

    /// stop all jobs we allow holding the lock during await as its fine here
    /// because we are shutting down and can only ever shut down if all jobs
    /// stop
    #[allow(clippy::await_holding_lock)]
    pub async fn stop_all(&self) {
        let mut inner = self.inner.lock().unwrap();
        while let Some(mut job) = inner.pop() {
            job.stop().await.unwrap();
            job.thread.unwrap().with(|| drop(job));
        }
    }

    /// reset all jobs
    pub fn send_reset(&self) {
        self.inner.lock().unwrap().iter_mut().for_each(|j| {
            j.request_reset = true;
        });
    }
}
