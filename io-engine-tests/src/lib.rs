//! Test utility functions.
//!
//! TODO: All functions here should return errors instead of using assert and
//! panic macros. The caller can decide how to handle the error appropriately.
//! Panics and asserts in this file are still ok for usage & programming errors.

use std::{io, io::Write, process::Command, time::Duration};

use crossbeam::channel::{after, select, unbounded};
use once_cell::sync::OnceCell;
use run_script::{self, ScriptOptions};
use url::{ParseError, Url};

use tracing::{error, info, trace};

use io_engine::{
    core::{MayastorEnvironment, Mthread},
    logger,
    logger::LogFormat,
    rebuild::{NexusRebuildJob, RebuildState},
};

pub mod bdev;
pub mod bdev_io;
pub mod cli_tools;
pub mod compose;
pub mod error_bdev;
pub mod file_io;
pub mod fio;
pub mod nexus;
pub mod nvme;
pub mod nvmf;
pub mod pool;
pub mod replica;
pub mod snapshot;
pub mod test;
pub mod test_task;

pub use compose::MayastorTest;

/// call F cnt times, and sleep for a duration between each invocation
pub fn retry<F, T, E>(mut cnt: u32, timeout: Duration, mut f: F) -> T
where
    F: FnMut() -> Result<T, E>,
    E: std::fmt::Debug,
{
    loop {
        cnt -= 1;
        if let Ok(result) = f() {
            return result;
        }

        if cnt == 0 {
            break;
        }
        std::thread::sleep(timeout);
    }

    panic!("failed operation with retries");
}

pub static MSTEST: OnceCell<MayastorEnvironment> = OnceCell::new();

#[macro_export]
macro_rules! reactor_poll {
    ($ch:ident, $name:ident) => {
        loop {
            io_engine::core::Reactors::current().poll_once();
            if let Ok(r) = $ch.try_recv() {
                $name = r;
                break;
            }
        }
    };
    ($ch:ident) => {
        loop {
            io_engine::core::Reactors::current().poll_once();
            if $ch.try_recv().is_ok() {
                break;
            }
        }
    };
    ($n:expr) => {
        for _ in 0 .. $n {
            io_engine::core::Reactors::current().poll_once();
        }
        io_engine::core::Reactors::current();
    };
}

/// The same as reactor_poll above but it asserts that the result received
/// from the channel is as expected.
#[macro_export]
macro_rules! assert_reactor_poll {
    ($ch:ident, $val:expr) => {
        loop {
            io_engine::core::Reactors::current().poll_once();
            if let Ok(r) = $ch.try_recv() {
                assert_eq!(r, $val);
                break;
            }
        }
    };
}

#[macro_export]
macro_rules! test_init {
    () => {
        common::MSTEST.get_or_init(|| {
            common::mayastor_test_init();
            MayastorEnvironment::new(MayastorCliArgs {
                reactor_mask: "0x1".to_string(),
                ..Default::default()
            })
            .init()
        });
        io_engine::core::Mthread::primary().set_current();
    };
    ($yaml_config:expr) => {
        common::MSTEST.get_or_init(|| {
            common::mayastor_test_init();
            MayastorEnvironment::new(MayastorCliArgs {
                reactor_mask: "0x1".to_string(),
                mayastor_config: Some($yaml_config.to_string()),
                ..Default::default()
            })
            .init()
        });
    };
}

pub fn mayastor_test_init() {
    mayastor_test_init_ex(LogFormat::default(), None);
}

pub fn mayastor_test_init_ex(log_format: LogFormat, log_level: Option<&str>) {
    fn binary_present(name: &str) -> Result<bool, std::env::VarError> {
        std::env::var("PATH").map(|paths| {
            paths
                .split(':')
                .map(|p| format!("{p}/{name}"))
                .any(|p| std::fs::metadata(p).is_ok())
        })
    }

    ["dd", "mkfs.xfs", "mkfs.ext4", "cmp", "fsck", "truncate"]
        .iter()
        .for_each(|binary| {
            if binary_present(binary).is_err() {
                panic!("binary: {} not present in path", binary);
            }
        });

    logger::init_ex(
        log_level.unwrap_or("info,io_engine=DEBUG"),
        log_format,
        None,
    );

    io_engine::CPS_INIT!();
}

pub fn dd_random_file(path: &str, bs: u32, size: u64) {
    let count = size * 1024 / bs as u64;
    let output = Command::new("dd")
        .args([
            "if=/dev/urandom",
            &format!("of={path}"),
            &format!("bs={bs}"),
            &format!("count={count}"),
        ])
        .output()
        .expect("failed exec dd");

    assert!(output.status.success());
}

pub fn truncate_file(path: &str, size: u64) {
    let output = Command::new("truncate")
        .args(["-s", &format!("{}m", size / 1024), path])
        .output()
        .expect("failed exec truncate");

    assert!(output.status.success());
}

pub fn truncate_file_bytes(path: &str, size: u64) {
    let output = Command::new("truncate")
        .args(["-s", &format!("{size}"), path])
        .output()
        .expect("failed exec truncate");
    assert!(output.status.success());
}

/// Automatically assign a loopdev to path
pub fn setup_loopdev_file(path: &str, sector_size: Option<u64>) -> String {
    let log_sec = sector_size.unwrap_or(512);

    let output = Command::new("losetup")
        .args(["-f", "--show", "-b", &format!("{log_sec}"), path])
        .output()
        .expect("failed exec losetup");
    assert!(output.status.success());
    // return the assigned loop device
    String::from_utf8(output.stdout).unwrap().trim().to_string()
}

/// Detach the provided loop device.
pub fn detach_loopdev(dev: &str) {
    let output = Command::new("losetup")
        .args(["-d", dev])
        .output()
        .expect("failed exec losetup");
    assert!(output.status.success());
}

pub fn fscheck(device: &str) {
    let output = Command::new("fsck")
        .args([device, "-n"])
        .output()
        .expect("fsck exec failed");

    io::stdout().write_all(&output.stderr).unwrap();
    io::stdout().write_all(&output.stdout).unwrap();

    assert!(output.status.success());
}

pub fn mkfs(path: &str, fstype: &str) -> bool {
    let (fs, args) = match fstype {
        "xfs" => ("mkfs.xfs", ["-f", path]),
        "ext4" => ("mkfs.ext4", ["-F", path]),
        _ => {
            panic!("unsupported fstype");
        }
    };

    let output = Command::new(fs)
        .args(args)
        .output()
        .expect("mkfs exec error");

    io::stdout().write_all(&output.stderr).unwrap();
    io::stdout().write_all(&output.stdout).unwrap();
    output.status.success()
}

pub fn delete_file(disks: &[String]) {
    let output = Command::new("rm")
        .args(["-rf"])
        .args(disks)
        .output()
        .expect("failed to execute rm");

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr),
    );
}

pub fn compare_files(a: &str, b: &str) {
    let output = Command::new("cmp")
        .args([a, b])
        .output()
        .expect("failed to execute \"cmp\"");

    io::stdout().write_all(&output.stderr).unwrap();
    io::stdout().write_all(&output.stdout).unwrap();
    assert!(output.status.success());
}

pub fn mount_umount(device: &str) -> Result<String, String> {
    let (exit, stdout, stderr) = run_script::run(
        r#"
        mkdir -p /tmp/__test
        mount $1 /tmp/__test
        umount /tmp/__test
        exit 0
    "#,
        &vec![device.into()],
        &run_script::ScriptOptions::new(),
    )
    .unwrap();
    if exit == 0 {
        Ok(stdout)
    } else {
        Err(stderr)
    }
}

pub fn mount_and_write_file(device: &str) -> Result<String, String> {
    let mut options = ScriptOptions::new();
    options.exit_on_error = true;
    options.print_commands = false;

    let (exit, stdout, stderr) = run_script::run(
        r#"
        mkdir -p /tmp/__test
        mount $1 /tmp/__test
        echo test > /tmp/__test/test
        md5sum /tmp/__test/test
        umount /tmp/__test
        rm -rf /tmp/__test
        exit 0
    "#,
        &vec![device.into()],
        &options,
    )
    .unwrap();
    if exit != 0 {
        Err(stderr)
    } else {
        Ok(stdout)
    }
}

pub fn mount_and_get_md5(device: &str) -> Result<String, String> {
    let (exit, stdout, stderr) = run_script::run(
        r#"
        mkdir -p /tmp/__test
        mount $1 /tmp/__test
        md5sum /tmp/__test/test
        umount /tmp/__test
        rm -rf /tmp/__test
        exit 0
    "#,
        &vec![device.into()],
        &run_script::ScriptOptions::new(),
    )
    .unwrap();
    if exit != 0 {
        Err(stderr)
    } else {
        Ok(stdout)
    }
}

pub fn fio_run_verify(device: &str) -> Result<String, String> {
    let (exit, stdout, stderr) = run_script::run(
        r"
        fio --name=randrw --rw=randrw --ioengine=libaio --direct=1 --time_based=1 \
        --runtime=5 --bs=4k --verify=crc32 --group_reporting=1 --output-format=terse \
        --verify_fatal=1 --verify_async=2 --filename=$1
    ",
    &vec![device.into()],
    &run_script::ScriptOptions::new(),
    )
        .unwrap();
    if exit == 0 {
        Ok(stdout)
    } else {
        Err(stderr)
    }
}

pub fn clean_up_temp() {
    let (_exit, _stdout, _stderr) = run_script::run(
        r#" rm -rf $1 "#,
        &vec!["/tmp/__test".into()],
        &run_script::ScriptOptions::new(),
    )
    .unwrap();
}

pub fn thread() -> Mthread {
    Mthread::primary()
}

pub fn dd_urandom_blkdev_test(device: &str) -> i32 {
    let (exit, stdout, stderr) = run_script::run(
        r#"
        dd if=/dev/urandom of=$1 oflag=direct bs=512 count=1 seek=6144
    "#,
        &vec![device.into()],
        &run_script::ScriptOptions::new(),
    )
    .unwrap();
    tracing::debug!(
        "dd_urandom_blkdev:\nstdout: {}\nstderr: {}",
        stdout,
        stderr
    );
    exit
}
pub fn dd_urandom_blkdev(device: &str) -> i32 {
    let (exit, stdout, stderr) = run_script::run(
        r#"
        dd if=/dev/urandom of=$1 conv=fsync,nocreat,notrunc iflag=count_bytes count=`blockdev --getsize64 $1`
    "#,
    &vec![device.into()],
    &run_script::ScriptOptions::new(),
    )
    .unwrap();
    trace!("dd_urandom_blkdev:\nstdout: {}\nstderr: {}", stdout, stderr);
    exit
}
pub fn dd_urandom_file_size(device: &str, size: u64) -> String {
    let (exit, stdout, _stderr) = run_script::run(
        r#"
        dd if=/dev/urandom of=$1 conv=fsync,nocreat,notrunc iflag=count_bytes count=$2
    "#,
        &vec![device.into(), size.to_string()],
        &run_script::ScriptOptions::new(),
    )
        .unwrap();
    assert_eq!(exit, 0);
    stdout
}

pub fn compare_nexus_device(
    nexus_device: &str,
    device: &str,
    expected_pass: bool,
) -> String {
    let (exit, stdout, _stderr) = run_script::run(
        r#"
        cmp -n `blockdev --getsize64 $1` $1 $2 0 5M
        test $? -eq $3
    "#,
        &vec![
            nexus_device.into(),
            device.into(),
            (!expected_pass as i32).to_string(),
        ],
        &run_script::ScriptOptions::new(),
    )
    .unwrap();
    assert_eq!(exit, 0);
    stdout
}

pub fn compare_devices(
    first_device: &str,
    second_device: &str,
    size: u64,
    expected_pass: bool,
) {
    let (exit, stdout, stderr) = run_script::run(
        r#"
        cmp -b $1 $2 -n $3
        test $? -eq $4
    "#,
        &vec![
            first_device.into(),
            second_device.into(),
            size.to_string(),
            (!expected_pass as i32).to_string(),
        ],
        &run_script::ScriptOptions::new(),
    )
    .unwrap();
    assert_eq!(exit, 0, "stdout: {stdout}\nstderr: {stderr}");
}

pub fn device_path_from_uri(device_uri: &str) -> String {
    assert_ne!(
        Url::parse(device_uri),
        Err(ParseError::RelativeUrlWithoutBase)
    );
    let url = Url::parse(device_uri).unwrap();
    String::from(url.path())
}

pub fn get_device_size(nexus_device: &str) -> u64 {
    let output = Command::new("blockdev")
        .args(["--getsize64", nexus_device])
        .output()
        .expect("failed to get block device size");

    assert!(output.status.success());
    String::from_utf8(output.stdout)
        .unwrap()
        .trim_end()
        .parse::<u64>()
        .unwrap()
}

/// Waits for the rebuild to reach `state`, up to `timeout`
pub async fn wait_for_rebuild(
    dst_uri: String,
    state: RebuildState,
    timeout: Duration,
) {
    let (s, r) = unbounded::<()>();
    let job = match NexusRebuildJob::lookup(&dst_uri) {
        Ok(job) => job,
        Err(_) => return,
    };
    job.stats().await;

    let mut curr_state = job.state();
    let ch = job.notify_chan();
    let cname = dst_uri.clone();
    let t = Mthread::spawn_unaffinitized(move || {
        let now = std::time::Instant::now();
        let mut error = Ok(());
        while curr_state != state && error.is_ok() {
            select! {
                recv(ch) -> state => {
                    trace!("rebuild of child {} signalled with state {:?}", cname, state);
                    curr_state = state.unwrap_or_else(|e| {
                        error!("failed to wait for the rebuild with error: {}", e);
                        error = Err(());
                        curr_state
                    })
                },
                recv(after(timeout - now.elapsed())) -> _ => {
                    error!("timed out waiting for the rebuild after {:?}", timeout);
                    error = Err(())
                }
            }
        }

        s.send(()).ok();
        error
    });
    reactor_poll!(r);
    if let Ok(job) = NexusRebuildJob::lookup(&dst_uri) {
        job.stats().await;
    }
    t.join().unwrap().unwrap();
}

pub fn fio_verify_size(device: &str, size: u64) -> i32 {
    let (exit, stdout, stderr) = run_script::run(
        r"
        fio --thread=1 --numjobs=1 --iodepth=16 --bs=512 \
        --direct=1 --ioengine=libaio --rw=randwrite --verify=crc32 \
        --verify_fatal=1 --name=write_verify --filename=$1 --size=$2

        fio --thread=1 --numjobs=1 --iodepth=16 --bs=512 \
        --direct=1 --ioengine=libaio --verify=crc32 --verify_only \
        --verify_fatal=1 --name=verify --filename=$1
    ",
        &vec![device.into(), size.to_string()],
        &run_script::ScriptOptions::new(),
    )
    .unwrap();
    info!("stdout: {}\nstderr: {}", stdout, stderr);
    exit
}

pub fn reactor_run_millis(milliseconds: u64) {
    let (s, r) = unbounded::<()>();
    Mthread::spawn_unaffinitized(move || {
        std::thread::sleep(Duration::from_millis(milliseconds));
        s.send(())
    });
    reactor_poll!(r);
}

pub fn composer_init() {
    std::fs::create_dir_all("/var/run/dpdk").ok();
    let path = std::path::PathBuf::from(std::env!("CARGO_MANIFEST_DIR"));
    let srcdir = path.parent().unwrap();
    composer::initialize(srcdir);
}

/// Formats a serializable object as a JSON text.
#[allow(dead_code)]
pub fn nice_json<T>(obj: &T) -> String
where
    T: ?Sized + serde::Serialize,
{
    use colored_json::ToColoredJson;
    serde_json::to_string_pretty(obj)
        .unwrap()
        .to_colored_json_auto()
        .unwrap()
}

/// Generates a UUID and returns its string representation.
pub fn generate_uuid() -> String {
    spdk_rs::Uuid::generate().to_string()
}

/// Diagnostics println! that prints timestamp and thread ID.
#[macro_export]
macro_rules! test_diag {
    ($($arg:tt)*) => {{
        const PREFIX: &str = "ThreadId(";
        let ts = format!("{:?}", std::thread::current().id());
        let ts = if ts.len() > PREFIX.len() && ts.starts_with(PREFIX) {
            &ts[PREFIX.len()..ts.len() - 1]
        } else {
            &ts
        };
        print!(
            "[{ts}] {n} :: ",
            n = chrono::Utc::now().format("%T%.6f")
        );
        println!($($arg)*);
    }}
}

pub use io_engine_tests_macros::spdk_test;
