use crossbeam::channel::{after, select, unbounded};
use log::info;
use std::{env, io, io::Write, process::Command, time::Duration};

use once_cell::sync::OnceCell;
use run_script::{self, ScriptOptions};

use mayastor::{
    core::{MayastorEnvironment, Mthread},
    logger,
    rebuild::RebuildJob,
};
use spdk_sys::spdk_get_thread;
use url::{ParseError, Url};

pub mod ms_exec;
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
            mayastor::core::Reactors::current().poll_once();
            if let Ok(r) = $ch.try_recv() {
                $name = r;
                break;
            }
        }

        mayastor::core::Reactors::current().thread_enter();
    };
    ($ch:ident) => {
        loop {
            mayastor::core::Reactors::current().poll_once();
            if $ch.try_recv().is_ok() {
                break;
            }
        }
        mayastor::core::Reactors::current().thread_enter();
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
    };
}

pub fn mayastor_test_init() {
    fn binary_present(name: &str) -> Result<bool, std::env::VarError> {
        std::env::var("PATH").and_then(|paths| {
            Ok(paths
                .split(':')
                .map(|p| format!("{}/{}", p, name))
                .any(|p| std::fs::metadata(&p).is_ok()))
        })
    }

    ["dd", "mkfs.xfs", "mkfs.ext4", "cmp", "fsck", "truncate"]
        .iter()
        .for_each(|binary| {
            if binary_present(binary).is_err() {
                panic!("binary: {} not present in path", binary);
            }
        });

    logger::init("TRACE");
    env::set_var("MAYASTOR_LOGLEVEL", "4");
    mayastor::CPS_INIT!();
}

pub fn dd_random_file(path: &str, bs: u32, size: u64) {
    let count = size * 1024 / bs as u64;
    let output = Command::new("dd")
        .args(&[
            "if=/dev/urandom",
            &format!("of={}", path),
            &format!("bs={}", bs),
            &format!("count={}", count),
        ])
        .output()
        .expect("failed exec dd");

    assert_eq!(output.status.success(), true);
}

pub fn truncate_file(path: &str, size: u64) {
    let output = Command::new("truncate")
        .args(&["-s", &format!("{}m", size / 1024), path])
        .output()
        .expect("failed exec truncate");

    assert_eq!(output.status.success(), true);
}

pub fn truncate_file_bytes(path: &str, size: u64) {
    let output = Command::new("truncate")
        .args(&["-s", &format!("{}", size), path])
        .output()
        .expect("failed exec truncate");

    assert_eq!(output.status.success(), true);
}

pub fn fscheck(device: &str) {
    let output = Command::new("fsck")
        .args(&[device, "-n"])
        .output()
        .expect("fsck exec failed");

    io::stdout().write_all(&output.stderr).unwrap();
    io::stdout().write_all(&output.stdout).unwrap();
    assert_eq!(output.status.success(), true);
}

pub fn mkfs(path: &str, fstype: &str) {
    let (fs, args) = match fstype {
        "xfs" => ("mkfs.xfs", ["-f", path]),
        "ext4" => ("mkfs.ext4", ["-F", path]),
        _ => {
            panic!("unsupported fstype");
        }
    };

    let output = Command::new(fs)
        .args(&args)
        .output()
        .expect("mkfs exec error");

    io::stdout().write_all(&output.stderr).unwrap();
    io::stdout().write_all(&output.stdout).unwrap();
    assert_eq!(output.status.success(), true);
}

pub fn delete_file(disks: &[String]) {
    let output = Command::new("rm")
        .args(&["-rf"])
        .args(disks)
        .output()
        .expect("failed delete test file");

    assert_eq!(output.status.success(), true);
}

pub fn compare_files(a: &str, b: &str) {
    let output = Command::new("cmp")
        .args(&[a, b])
        .output()
        .expect("failed to execute \"cmp\"");

    io::stdout().write_all(&output.stderr).unwrap();
    io::stdout().write_all(&output.stdout).unwrap();
    assert_eq!(output.status.success(), true);
}

pub fn mount_umount(device: &str) -> String {
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
    if exit != 0 {
        panic!("Script failed with error: {}", stderr);
    }
    stdout
}

pub fn mount_and_write_file(device: &str) -> String {
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
        panic!("Script failed with error: {}", stderr);
    }
    stdout.trim_end().to_string()
}

pub fn mount_and_get_md5(device: &str) -> String {
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
        panic!("Script failed with error: {}", stderr);
    }
    stdout
}

pub fn fio_run_verify(device: &str) -> String {
    let (exit, stdout, _stderr) = run_script::run(
        r#"
        fio --name=randrw --rw=randrw --ioengine=libaio --direct=1 --time_based=1 \
        --runtime=5 --bs=4k --verify=crc32 --group_reporting=1 --output-format=terse \
        --verify_fatal=1 --verify_async=2 --filename=$1
    "#,
    &vec![device.into()],
    &run_script::ScriptOptions::new(),
    )
        .unwrap();
    assert_eq!(exit, 0);
    stdout
}

pub fn clean_up_temp() {
    let (_exit, _stdout, _stderr) = run_script::run(
        r#" rm -rf $1 "#,
        &vec!["/tmp/__test".into()],
        &run_script::ScriptOptions::new(),
    )
    .unwrap();
}

pub fn thread() -> Option<Mthread> {
    Mthread::from_null_checked(unsafe { spdk_get_thread() })
}

pub fn dd_urandom_blkdev(device: &str) -> String {
    let (exit, stdout, _stderr) = run_script::run(
        r#"
        dd if=/dev/urandom of=$1 conv=fsync,nocreat,notrunc iflag=count_bytes count=`blockdev --getsize64 $1`
    "#,
    &vec![device.into()],
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
    expected_pass: bool,
) -> String {
    let (exit, stdout, stderr) = run_script::run(
        r#"
        cmp -b $1 $2 5M 5M
        test $? -eq $3
    "#,
        &vec![
            first_device.into(),
            second_device.into(),
            (!expected_pass as i32).to_string(),
        ],
        &run_script::ScriptOptions::new(),
    )
    .unwrap();
    assert_eq!(exit, 0, "stdout: {}\nstderr: {}", stdout, stderr);
    stdout
}

pub fn device_path_from_uri(device_uri: String) -> String {
    assert_ne!(
        Url::parse(device_uri.as_str()),
        Err(ParseError::RelativeUrlWithoutBase)
    );
    let url = Url::parse(device_uri.as_str()).unwrap();
    String::from(url.path())
}

pub fn wait_for_rebuild(name: String, timeout: Duration) {
    let (s, r) = unbounded::<()>();
    let job = match RebuildJob::lookup(&name) {
        Ok(job) => job,
        Err(_) => return,
    };

    let ch = job.complete_chan.1.clone();
    std::thread::spawn(move || {
        select! {
            recv(ch) -> state => info!("rebuild of child {} finished with state {:?}", name, state),
            recv(after(timeout)) -> _ => panic!("timed out waiting for the rebuild to complete"),
        }
        s.send(())
    });
    reactor_poll!(r);
}
