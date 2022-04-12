use std::{cell::RefCell, os::raw::c_void, ptr::NonNull};

use clap::{value_t, App, AppSettings, Arg};
use rand::Rng;

use io_engine::{
    core::{
        mayastor_env_stop,
        Cores,
        Descriptor,
        IoChannel,
        MayastorCliArgs,
        MayastorEnvironment,
        Mthread,
        Reactors,
        UntypedBdev,
    },
    logger,
    nexus_uri::bdev_create,
    subsys::Config,
};
use spdk_rs::{
    libspdk::{
        spdk_bdev_free_io,
        spdk_bdev_io,
        spdk_bdev_read,
        spdk_bdev_write,
        spdk_poller,
        spdk_poller_register,
        spdk_poller_unregister,
    },
    DmaBuf,
};
use version_info::version_info_str;

#[derive(Debug)]
enum IoType {
    /// perform random read operations
    Read,
    /// perform random write operations
    #[allow(dead_code)]
    Write,
}

/// default queue depth
const QD: u64 = 64;
/// default io_size
const IO_SIZE: u64 = 512;

/// a Job refers to a set of work typically defined by either time or size
/// that drives IO to a bdev using its own channel.
#[derive(Debug)]
#[allow(dead_code)]
struct Job {
    bdev: UntypedBdev,
    /// descriptor to the bdev
    desc: Descriptor,
    /// io channel being used to submit IO
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
    /// generate random number between 0 and num_block
    rng: rand::rngs::ThreadRng,
    /// drain the job which means that we wait for all pending IO to complete
    /// and stop the run
    drain: bool,
    /// number of seconds we are running
    period: u64,
}
thread_local! {
    #[allow(clippy::vec_box)]
    static JOBLIST: RefCell<Vec<Box<Job>>> = RefCell::new(Vec::new());
    static PERF_TICK: RefCell<Option<NonNull<spdk_poller>>> = RefCell::new(None);
}

impl Job {
    /// io completion callback
    extern "C" fn io_completion(
        bdev_io: *mut spdk_bdev_io,
        success: bool,
        arg: *mut std::ffi::c_void,
    ) {
        let ioq: &mut Io = unsafe { &mut *arg.cast() };
        let job = unsafe { ioq.job.as_mut() };

        if !success {
            eprintln!(
                "IO error for bdev {}, LBA {}",
                job.bdev.name(),
                ioq.offset
            );
        }

        job.n_io += 1;
        job.n_inflight -= 1;

        unsafe { spdk_bdev_free_io(bdev_io) }

        if job.drain && job.n_inflight == 0 {
            JOBLIST.with(|l| {
                let mut list = l.borrow_mut();
                list.retain(|this| job.bdev.name() != this.bdev.name());
                if list.is_empty() {
                    Reactors::master().send_future(async {
                        mayastor_env_stop(0);
                    });
                }
            });
        }

        if job.drain {
            return;
        }

        let offset = (job.rng.gen::<u64>() % job.io_size) * job.io_blocks;
        ioq.next(offset);
    }

    /// construct a new job
    async fn new(bdev: &str, size: u64, qd: u64) -> Box<Self> {
        let bdev = bdev_create(bdev)
            .await
            .map_err(|e| {
                eprintln!("Failed to open URI {}: {}", bdev, e);
                std::process::exit(1);
            })
            .map(|name| UntypedBdev::lookup_by_name(&name).unwrap())
            .unwrap();

        let desc = bdev.open(true).unwrap();

        let blk_size = bdev.block_len();
        let num_blocks = bdev.num_blocks();

        let io_size = size / blk_size as u64;
        let io_blocks = num_blocks / io_size;

        let mut queue = Vec::new();

        (0 ..= qd).for_each(|offset| {
            queue.push(Io {
                buf: DmaBuf::new(size, bdev.alignment()).unwrap(),
                iot: IoType::Read,
                offset,
                job: NonNull::dangling(),
            });
        });

        Box::new(Self {
            bdev,
            desc,
            ch: None,
            qd,
            io_size: size,
            blk_size,
            num_blocks,
            queue,
            io_blocks,
            n_io: 0,
            n_inflight: 0,
            rng: Default::default(),
            drain: false,
            period: 0,
        })
    }

    fn as_ptr(&self) -> *mut Job {
        self as *const _ as *mut _
    }

    /// start the job that will dispatch an IO up to the provided queue depth
    fn run(mut self: Box<Self>) {
        self.ch = self.desc.get_channel();
        let ptr = self.as_ptr();
        self.queue.iter_mut().for_each(|q| q.run(ptr));
        JOBLIST.with(|l| l.borrow_mut().push(self));
    }
}

#[derive(Debug)]
struct Io {
    /// buffer we read/write from/to
    buf: DmaBuf,
    /// type of IO we are supposed to issue
    iot: IoType,
    /// current offset where we are reading from
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

    /// dispatch the next IO, this is called from within the completion callback
    pub fn next(&mut self, offset: u64) {
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
}

/// override the default signal handler as we need to stop the jobs first
/// before we can shut down
fn sig_override() {
    let handler = || {
        Mthread::get_init().msg((), |_| {
            PERF_TICK.with(|t| {
                let ticker = t.borrow_mut().take().unwrap();
                unsafe { spdk_poller_unregister(&mut ticker.as_ptr()) }
            });

            println!("Draining jobs....");
            JOBLIST.with(|l| {
                l.borrow_mut().iter_mut().for_each(|j| j.drain = true);
            });
        });
    };

    unsafe {
        signal_hook::low_level::register(signal_hook::consts::SIGTERM, handler)
            .expect("failed to set SIGTERM");
        signal_hook::low_level::register(signal_hook::consts::SIGINT, handler)
            .expect("failed to set SIGINT");
    };
}

/// prints the performance statistics to stdout on every tick (1s)
extern "C" fn perf_tick(_: *mut c_void) -> i32 {
    let mut total_io_per_second = 0;
    let mut total_mb_per_second = 0;
    JOBLIST.with(|l| {
        for j in l.borrow_mut().iter_mut() {
            j.period += 1;
            let io_per_second = j.n_io / j.period;
            let mb_per_second = io_per_second * j.io_size / (1024 * 1024);
            println!(
                "\r {:20}: {:10} IO/s {:10}: MB/s",
                j.bdev.name(),
                io_per_second,
                mb_per_second
            );
            total_io_per_second += io_per_second;
            total_mb_per_second += mb_per_second;
        }

        println!("\r ==================================================== +");
        println!(
            "\r {:20}: {:10} IO/s {:10}: MB/s\n",
            "Total", total_io_per_second, total_mb_per_second
        );
    });
    0
}

fn main() {
    logger::init("INFO");

    // do not start the target(s)
    Config::get_or_init(|| {
        let mut cfg = Config::default();
        cfg.nexus_opts.nvmf_enable = false;
        cfg
    });

    let matches = App::new("Mayastor performance tool")
        .version(version_info_str!())
        .settings(&[AppSettings::ColoredHelp, AppSettings::ColorAlways])
        .about("Perform IO to storage URIs")
        .arg(
            Arg::with_name("io_size")
                .value_name("io_size")
                .short("b")
                .help("block size in bytes")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("queue_depth")
                .value_name("queue_depth")
                .short("q")
                .help("queue depth")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("URI")
                .value_name("URI")
                .help("storage URI's")
                .index(1)
                .multiple(true)
                .takes_value(true),
        )
        .get_matches();

    let mut uris = matches
        .values_of("URI")
        .unwrap()
        .map(|u| u.to_string())
        .collect::<Vec<_>>();

    let io_size = value_t!(matches.value_of("io_size"), u64).unwrap_or(IO_SIZE);
    let qd = value_t!(matches.value_of("queue_depth"), u64).unwrap_or(QD);
    let args = MayastorCliArgs {
        reactor_mask: "0x2".to_string(),
        ..Default::default()
    };

    MayastorEnvironment::new(args).init();
    sig_override();
    Reactors::master().send_future(async move {
        let jobs = uris
            .iter_mut()
            .map(|u| Job::new(u, io_size, qd))
            .collect::<Vec<_>>();

        for j in jobs {
            let job = j.await;
            let thread =
                Mthread::new(job.bdev.name().to_string(), Cores::current())
                    .unwrap();
            thread.msg(job, |job| {
                job.run();
            });
        }

        unsafe {
            PERF_TICK.with(|p| {
                *p.borrow_mut() = NonNull::new(spdk_poller_register(
                    Some(perf_tick),
                    std::ptr::null_mut(),
                    1_000_000,
                ))
            });
        }
    });

    Reactors::master().running();
    Reactors::master().poll_reactor();
}
