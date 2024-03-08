use derive_builder::Builder;
use nix::errno::Errno;
use serde::Serialize;
use std::{
    path::Path,
    sync::atomic::{AtomicU32, Ordering},
    time::{Duration, Instant},
};

use super::file_io::DataSize;

/// TODO
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FioJobResult {
    NotRun,
    Ok,
    Error(Errno),
}

/// FIO job.
/// Non-optional fields are always passed as FIO CLI arguments.
/// Optionals fields are passed to FIO only if they are defined.
#[derive(Debug, Clone, Builder, Serialize)]
#[builder(setter(prefix = "with"))]
#[builder(build_fn(name = "try_build"))]
#[builder(default)]
#[allow(dead_code)]
pub struct FioJob {
    /// Job name.
    #[builder(setter(into))]
    pub name: String,
    /// I/O engine to use. Default: spdk.
    #[builder(setter(into))]
    pub ioengine: String,
    /// Filename.
    #[builder(setter(custom))]
    pub filename: String,
    /// Type of I/O pattern.
    #[builder(setter(into))]
    pub rw: String,
    /// If true, use non-buffered I/O (usually O_DIRECT). Default: true.
    pub direct: bool,
    /// Block size for I/O units. Default: 4k.
    #[builder(setter(strip_option, into))]
    pub bs: Option<DataSize>,
    /// Offset in the file to start I/O. Data before the offset will not be
    /// touched.
    #[builder(setter(strip_option, into))]
    pub offset: Option<DataSize>,
    /// Number of I/O units to keep in flight against the file.
    #[builder(setter(strip_option))]
    pub iodepth: Option<u32>,
    /// Number of clones (processes/threads performing the same workload) of
    /// this job. Default: 1.
    pub numjobs: u32,
    /// TODO
    pub thread: u32,
    /// Terminate processing after the specified number of seconds.
    /// If this field is defined, --timebased=1 is set as well.
    #[builder(setter(strip_option))]
    pub runtime: Option<u32>,
    /// Total size of I/O for this job.
    #[builder(setter(strip_option, into))]
    pub size: Option<DataSize>,
    /// TODO
    pub norandommap: bool,
    /// TODO
    #[builder(setter(into))]
    pub random_generator: Option<String>,
    /// TODO
    #[builder(setter(strip_option))]
    pub do_verify: Option<bool>,
    /// TODO
    #[builder(setter(strip_option, into))]
    pub verify: Option<String>,
    /// TODO
    #[builder(setter(strip_option))]
    pub verify_async: Option<u32>,
    /// TODO
    #[builder(setter(strip_option))]
    pub verify_fatal: Option<bool>,
    /// Run result.
    #[builder(setter(skip))]
    #[serde(skip_serializing)]
    pub result: FioJobResult,
    /// Job counter.
    #[builder(setter(skip))]
    #[serde(skip_serializing)]
    counter: u32,
}

impl FioJobBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn build(&self) -> FioJob {
        self.try_build()
            .expect("FIO job builder is expected to succeed")
    }

    pub fn with_filename(&mut self, v: impl AsRef<Path>) -> &mut Self {
        self.filename = Some(v.as_ref().to_str().unwrap().to_string());
        self
    }
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
            name: format!("fio-{counter}"),
            ioengine: "spdk".to_string(),
            filename: String::new(),
            rw: "write".to_string(),
            direct: true,
            bs: None,
            offset: None,
            iodepth: None,
            numjobs: 1,
            thread: 1,
            runtime: None,
            size: None,
            norandommap: true,
            random_generator: Some("tausworthe64".to_string()),
            do_verify: None,
            verify: None,
            verify_async: None,
            verify_fatal: None,
            result: FioJobResult::NotRun,
            counter,
        }
    }

    pub fn as_fio_args(&self) -> Vec<String> {
        assert!(!self.filename.is_empty(), "Filename must be defined");

        let mut r: Vec<String> = serde_json::to_value(self)
            .unwrap()
            .as_object()
            .unwrap()
            .into_iter()
            .filter_map(|(k, v)| {
                if v.is_null() {
                    None
                } else if v.is_string() {
                    // Serde adds quotes around strings, we don't want them.
                    Some(format!("--{k}={v}", v = v.as_str().unwrap()))
                } else if v.is_boolean() {
                    // Map booleans to 1 or 0.
                    Some(format!(
                        "--{k}={v}",
                        v = if v.as_bool().unwrap() { 1 } else { 0 }
                    ))
                } else {
                    Some(format!("--{k}={v}"))
                }
            })
            .collect();

        if self.runtime.is_some() {
            r.push("--time_based=1".to_string());
        }

        r
    }
}

/// TODO
#[derive(Default, Debug, Clone, Builder)]
#[builder(setter(prefix = "with", into))]
#[builder(build_fn(name = "try_build"))]
#[builder(default)]
#[allow(dead_code)]
pub struct Fio {
    /// TODO
    #[builder(setter(custom))]
    pub jobs: Vec<FioJob>,
    /// TODO
    pub verbose: bool,
    /// TODO
    pub verbose_err: bool,
    /// TODO
    #[builder(setter(skip))]
    pub script: String,
    /// TODO
    #[builder(setter(skip))]
    pub total_time: Duration,
    /// TODO
    #[builder(setter(skip))]
    pub exit: i32,
    /// TODO
    #[builder(setter(skip))]
    pub err_messages: Vec<String>,
}

impl FioBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn build(&self) -> Fio {
        self.try_build()
            .expect("FIO builder is expected to succeed")
    }

    pub fn with_jobs(
        &mut self,
        jobs: impl Iterator<Item = FioJob>,
    ) -> &mut Self {
        jobs.for_each(|j| {
            self.with_job(j);
        });
        self
    }

    pub fn with_job(&mut self, job: FioJob) -> &mut Self {
        if self.jobs.is_none() {
            self.jobs = Some(Vec::new());
        }
        self.jobs.as_mut().unwrap().push(job);
        self
    }
}

impl Fio {
    pub fn new() -> Self {
        Default::default()
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
            if self.err_messages.is_empty() {
                println!("FIO is okay");
            } else {
                println!(
                    "Error(s) running FIO: {s}",
                    s = self.err_messages.join("\n")
                );
            }
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
    let frun = fio.clone();
    let fio = tokio::task::spawn_blocking(|| frun.run()).await.unwrap();

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
