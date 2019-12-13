use mayastor::mayastor_logger_init;
use run_script::{self, ScriptOptions};
use std::{env, io, io::Write, process::Command};

pub fn mayastor_test_init() {
    mayastor_logger_init("TRACE");
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
        .expect("mkfs exec truncate");

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
    let (exit, stdout, _stderr) = run_script::run(
        r#"
        mkdir /tmp/__test
        mount $1 /tmp/__test
        umount /tmp/__test
        exit 0
    "#,
        &vec![device.into()],
        &run_script::ScriptOptions::new(),
    )
    .unwrap();
    assert_eq!(exit, 0);
    stdout
}

pub fn mount_and_write_file(device: &str) -> String {
    let mut options = ScriptOptions::new();
    options.capture_output = true;
    options.exit_on_error = true;
    options.print_commands = false;

    let (exit, stdout, _stderr) = run_script::run(
        r#"
        mkdir /tmp/__test
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
    assert_eq!(exit, 0);
    stdout.trim_end().to_string()
}

pub fn mount_and_get_md5(device: &str) -> String {
    let (exit, stdout, _stderr) = run_script::run(
        r#"
        mkdir /tmp/__test
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
    assert_eq!(exit, 0);
    stdout
}

pub fn fio_run_verify(device: &str) -> String {
    let (exit, stdout, _stderr) = run_script::run(
        r#"
        fio --name=randrw --rw=randrw --ioengine=libaio --direct=1 --time_based=1 \
        --runtime=60 --bs=4k --verify=crc32 --group_reporting=1 --output-format=terse \
        --verify_fatal=1 --verify_async=2 --filename=$1
    "#,
        &vec![device.into()],
        &run_script::ScriptOptions::new(),
    )
    .unwrap();
    assert_eq!(exit, 0);
    stdout
}
