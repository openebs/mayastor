use std::{env, process::Command};

pub fn mayastor_test_init() {
    let log = mayastor::spdklog::SpdkLog::new();
    let _ = log.init();
    env::set_var("MAYASTOR_LOGLEVEL", "4");
    mayastor::CPS_INIT!();
}

pub fn dd_random_file(path: &str, bs: &str, count: &str) {
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

pub fn truncate_file(path: &str, size: &str) {
    let output = Command::new("truncate")
        .args(&["-s", size, path])
        .output()
        .expect("failed exec truncate");

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
