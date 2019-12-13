use mayastor::mayastor_logger_init;
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

    assert_eq!(output.status.success(), true);
}
