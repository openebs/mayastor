use super::file_io::DataSize;
use nix::errno::Errno;
use std::{
    path::Path,
    sync::atomic::{AtomicU32, Ordering},
    time::{Duration, Instant},
};

/// TODO
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FioJobResult {
    NotRun,
    Ok,
    Error(Errno),
}

/// TODO
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct FioJob {
    /// Job counter.
    pub counter: u32,
    /// Job name.
    pub name: String,
    /// I/O engine to use. Default: spdk.
    pub ioengine: String,
    /// Filename.
    pub filename: String,
    /// Type of I/O pattern.
    pub rw: String,
    /// If true, use non-buffered I/O (usually O_DIRECT). Default: true.
    pub direct: bool,
    /// Block size for I/O units. Default: 4k.
    pub blocksize: Option<u32>,
    /// Offset in the file to start I/O. Data before the offset will not be
    /// touched.
    pub offset: Option<DataSize>,
    /// Number of I/O units to keep in flight against the file.
    pub iodepth: Option<u32>,
    /// Number of clones (processes/threads performing the same workload) of
    /// this job. Default: 1.
    pub numjobs: u32,
    /// Terminate processing after the specified number of seconds.
    pub runtime: Option<u32>,
    /// Total size of I/O for this job.
    pub size: Option<DataSize>,
    /// Run result.
    pub result: FioJobResult,
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
            result: FioJobResult::NotRun,
        }
    }

    pub fn as_fio_args(&self) -> Vec<String> {
        assert!(!self.filename.is_empty(), "Filename must be defined");

        let mut r = vec![
            format!("--name={}", self.name),
            format!("--ioengine={}", self.ioengine),
            format!("--filename={}", self.filename),
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

    /// Sets job name.
    pub fn with_name(mut self, v: &str) -> Self {
        self.name = v.to_string();
        self
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

    /// Filename.
    pub fn with_filename_path(mut self, v: impl AsRef<Path>) -> Self {
        self.filename = v.as_ref().to_str().unwrap().to_string();
        self
    }

    /// Read-write FIO mode.
    pub fn with_rw(mut self, rw: &str) -> Self {
        self.rw = rw.to_string();
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
    pub script: String,
    pub total_time: Duration,
    pub exit: i32,
    pub err_messages: Vec<String>,
}

impl Fio {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn with_jobs(mut self, jobs: impl Iterator<Item = FioJob>) -> Self {
        jobs.for_each(|j| self.jobs.push(j));
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

    pub fn run(mut self) -> Self {
        let cmd = "sudo -E LD_PRELOAD=$FIO_SPDK fio";

        let args = self
            .jobs
            .iter()
            .map(|j| j.as_fio_args().join(" "))
            .collect::<Vec<_>>()
            .join(" ");

        self.script = format!("{cmd} --output-format=json {args}");

        if self.verbose || self.verbose_err {
            println!("{}", self.script);
        }

        let start_time = Instant::now();
        let (exit, stdout, stderr) = run_script::run(
            &self.script,
            &Vec::new(),
            &run_script::ScriptOptions::new(),
        )
        .unwrap();

        self.total_time = start_time.elapsed();
        self.push_err(&stderr);
        self.exit = exit;

        if let Err(e) = self.update_result(&stdout) {
            self.push_err(&e);
        }

        if self.verbose_err {
            println!(
                "Error(s) running FIO: {s}",
                s = self.err_messages.join("\n")
            );
        }

        self
    }

    /// TODO
    fn push_err(&mut self, msg: &str) {
        let s = msg.trim_end_matches('\n');
        if !s.is_empty() {
            self.err_messages.push(s.to_string());
        }
    }

    /// TODO
    fn update_result(&mut self, out: &str) -> Result<(), String> {
        // Filter out lines error messages, those starting with "fio: ".
        let out = out
            .split('\n')
            .filter(|s| !s.starts_with("fio: "))
            .collect::<Vec<_>>()
            .join("\n");

        serde_json::from_str::<serde_json::Value>(&out)
            .map_err(|e| e.to_string())?
            .get("jobs")
            .ok_or_else(|| "No 'jobs' item in output".to_string())?
            .as_array()
            .ok_or_else(|| "'jobs' item in output is not an array".to_string())?
            .iter()
            .for_each(|j| {
                let name =
                    j.get("jobname").unwrap().as_str().unwrap().to_string();
                let err = j.get("error").unwrap().as_i64().unwrap() as i32;

                if let Some(j) = self.find_job_mut(&name) {
                    if err == 0 {
                        j.result = FioJobResult::Ok;
                    } else {
                        j.result = FioJobResult::Error(Errno::from_i32(err));
                    }
                }
            });

        Ok(())
    }

    /// TODO
    pub fn find_job(&self, name: &str) -> Option<&FioJob> {
        self.jobs.iter().find(|j| j.name == name)
    }

    /// TODO
    pub fn find_job_mut(&mut self, name: &str) -> Option<&mut FioJob> {
        self.jobs.iter_mut().find(|j| j.name == name)
    }
}

/// Spawns a tokio task and runs the given FIO on it. Any FIO error is converted
/// into an `std::io::Result`.
pub async fn spawn_fio_task(fio: &Fio) -> std::io::Result<()> {
    let fio = tokio::spawn({
        let fio = fio.clone();
        async move { fio.run() }
    })
    .await
    .unwrap();

    if fio.exit == 0 {
        Ok(())
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!(
                "SPDK FIO error: {exit} {err_msg}",
                exit = fio.exit,
                err_msg = fio.err_messages.join("\n")
            ),
        ))
    }
}
