use super::file_io::DataSize;
use std::sync::atomic::{AtomicU32, Ordering};

/// TODO
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct FioJob {
    /// Job counter.
    counter: u32,
    /// Job name.
    name: String,
    /// I/O engine to use. Default: spdk.
    ioengine: String,
    /// Filename.
    filename: String,
    /// Type of I/O pattern.
    rw: String,
    /// If true, use non-buffered I/O (usually O_DIRECT). Default: true.
    direct: bool,
    /// Block size for I/O units. Default: 4k.
    blocksize: Option<u32>,
    /// Offset in the file to start I/O. Data before the offset will not be
    /// touched.
    offset: Option<DataSize>,
    /// Number of I/O units to keep in flight against the file.
    iodepth: Option<u32>,
    /// Number of clones (processes/threads performing the same workload) of
    /// this job. Default: 1.
    numjobs: u32,
    /// Terminate processing after the specified number of seconds.
    runtime: Option<u32>,
    /// Total size of I/O for this job.
    size: Option<DataSize>,
}

impl Default for FioJob {
    fn default() -> Self {
        Self::new()
    }
}

impl FioJob {
    pub fn new() -> Self {
        static JOB_COUNTER: AtomicU32 = AtomicU32::new(0);

        let counter = JOB_COUNTER.fetch_add(1, Ordering::SeqCst);

        Self {
            counter,
            name: format!("fio-{counter}"),
            ioengine: "spdk".to_string(),
            filename: String::new(),
            rw: "write".to_string(),
            direct: true,
            blocksize: None,
            offset: None,
            iodepth: None,
            numjobs: 1,
            runtime: None,
            size: None,
        }
    }

    pub fn as_fio_args(&self) -> Vec<String> {
        assert!(!self.filename.is_empty(), "Filename must be defined");

        let mut r = vec![
            format!("--name={}", self.name),
            format!("--ioengine={}", self.ioengine),
            format!("--filename='{}'", self.filename),
            format!("--thread=1"),
            format!("--direct={}", if self.direct { "1" } else { "0" }),
            format!("--norandommap=1"),
            format!("--rw={}", self.rw),
            format!("--numjobs={}", self.numjobs),
            format!("--random_generator=tausworthe64"),
        ];

        if let Some(v) = self.blocksize {
            r.push(format!("--bs={v}"));
        }

        if let Some(ref v) = self.offset {
            r.push(format!("--offset={v}"));
        }

        if let Some(v) = self.iodepth {
            r.push(format!("--iodepth={v}"));
        }

        if let Some(v) = self.runtime {
            r.push("--time_based=1".to_string());
            r.push(format!("--runtime={v}s"));
        }

        if let Some(ref v) = self.size {
            r.push(format!("--size={v}"));
        }

        r
    }

    /// I/O engine to use. Default: spdk.
    pub fn with_ioengine(mut self, v: &str) -> Self {
        self.ioengine = v.to_string();
        self
    }

    /// Filename.
    pub fn with_filename(mut self, v: &str) -> Self {
        self.filename = v.to_string();
        self
    }

    /// If true, use non-buffered I/O (usually O_DIRECT). Default: true.
    pub fn with_direct(mut self, v: bool) -> Self {
        self.direct = v;
        self
    }

    /// Block size for I/O units. Default: 4k.
    pub fn with_bs(mut self, v: u32) -> Self {
        self.blocksize = Some(v);
        self
    }

    /// Offset in the file to start I/O. Data before the offset will not be
    /// touched.
    pub fn with_offset(mut self, v: DataSize) -> Self {
        self.offset = Some(v);
        self
    }

    /// Number of I/O units to keep in flight against the file.
    pub fn with_iodepth(mut self, v: u32) -> Self {
        self.iodepth = Some(v);
        self
    }

    /// Number of clones (processes/threads performing the same workload) of
    /// this job. Default: 1.
    pub fn with_numjobs(mut self, v: u32) -> Self {
        self.numjobs = v;
        self
    }

    /// Terminate processing after the specified number of seconds.
    pub fn with_runtime(mut self, v: u32) -> Self {
        self.runtime = Some(v);
        self
    }

    /// Total size of I/O for this job.
    pub fn with_size(mut self, v: DataSize) -> Self {
        self.size = Some(v);
        self
    }
}

/// TODO
#[derive(Default, Debug, Clone)]
#[allow(dead_code)]
pub struct Fio {
    pub jobs: Vec<FioJob>,
    pub verbose: bool,
    pub verbose_err: bool,
}

impl Fio {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn with_jobs(mut self, jobs: Vec<FioJob>) -> Self {
        self.jobs = jobs;
        self
    }

    pub fn with_job(mut self, job: FioJob) -> Self {
        self.jobs.push(job);
        self
    }

    pub fn with_verbose(mut self, v: bool) -> Self {
        self.verbose = v;
        self
    }

    pub fn with_verbose_err(mut self, v: bool) -> Self {
        self.verbose_err = v;
        self
    }

    pub fn run(&self) -> std::io::Result<()> {
        let cmd = "sudo -E LD_PRELOAD=$FIO_SPDK fio";

        let args = self
            .jobs
            .iter()
            .map(|j| j.as_fio_args().join(" "))
            .collect::<Vec<_>>()
            .join(" ");

        if self.verbose {
            println!("{cmd} {args}");
        }

        let script = format!("{cmd} {args}");

        let (exit, stdout, stderr) = run_script::run(
            &script,
            &Vec::new(),
            &run_script::ScriptOptions::new(),
        )
        .unwrap();

        if exit == 0 {
            Ok(())
        } else {
            if self.verbose_err {
                println!("Error running FIO:");
                println!("{cmd} {args}");
                println!("Exit code: {exit}");
                println!("Output:");
                println!("{stdout}");
                println!("Error output:");
                println!("{stderr}");
            }

            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("SPDK FIO error: {stderr}"),
            ))
        }
    }
}

/// TODO
pub async fn run_fio_jobs(fio: &Fio) -> std::io::Result<()> {
    let fio = fio.clone();
    tokio::spawn(async move { fio.run() }).await.unwrap()
}
