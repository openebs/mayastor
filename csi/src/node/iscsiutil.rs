use once_cell::sync::Lazy;
use std::{env, path::Path, process::Command, thread, time};

// The iscsiadm executable invoked is dependent on the environment.
// For the container we set it using and environment variable,
// typically this is the "/bin/mayastor-iscsiadm" script,
// created by the mayastor image build scripts.
// For development hosts just setting it to iscsiadm works.
static ISCSIADM: Lazy<String> = Lazy::new(|| {
    if env::var("ISCSIADM").is_err() {
        debug!("defaulting to using iscsiadm");
        "iscsiadm".to_string()
    } else {
        debug!("using {}", env::var("ISCSIADM").unwrap());
        env::var("ISCSIADM").unwrap()
    }
});

pub fn wait_for_path_to_exist(devpath: &str, max_retries: i32) -> bool {
    let second = time::Duration::from_millis(1000);
    let device_path = Path::new(devpath);
    let mut retries: i32 = 0;
    let now = time::Instant::now();
    while !device_path.exists() && retries < max_retries {
        thread::sleep(second);
        retries += 1;
    }
    trace!(
        "wait_for_path_to_exist for elapsed time is {:?}",
        now.elapsed()
    );
    device_path.exists()
}

fn attach_disk(
    ip_addr: &str,
    port: &str,
    iqn: &str,
    lun: &str,
) -> Result<String, String> {
    let tp = format!("{}:{}", ip_addr, port);
    let device_path =
        format!("/dev/disk/by-path/ip-{}-iscsi-{}-lun-{}", tp, iqn, lun);

    // Rescan sessions to discover newly mapped LUNs
    // Do not specify the interface when rescanning
    // to avoid establishing additional sessions to the same target.
    let args_rescan = ["-m", "node", "-p", &tp, "-T", &iqn, "-R"];
    trace!("iscsiadm {:?}", args_rescan);
    let _ = Command::new(ISCSIADM.as_str())
        .args(&args_rescan)
        .output()
        .expect("Failed iscsiadm rescan");

    // If the device path exists then a previous invocation of this
    // method has succeeded.
    if wait_for_path_to_exist(device_path.as_str(), 1) {
        trace!("path already exists!");
        return Ok(device_path);
    }

    let args_discoverydb_new = [
        "-m",
        "discoverydb",
        "-t",
        "sendtargets",
        "-p",
        "{}:{}",
        "-I",
        "default",
        "-o",
        "new",
    ];
    trace!("iscsiadm {:?}", &args_discoverydb_new);
    let output = Command::new(ISCSIADM.as_str())
        .args(&args_discoverydb_new)
        .output()
        .expect("Failed iscsiadm discovery");
    if !output.status.success() {
        return Err(String::from_utf8(output.stderr).unwrap());
    }

    let args_discover = [
        "-m",
        "discoverydb",
        "-t",
        "sendtargets",
        "-p",
        &tp,
        "-I",
        "default",
        "--discover",
    ];
    trace!("iscsiadm {:?}", args_discover);
    // build discoverydb and discover iscsi target
    let output = Command::new(ISCSIADM.as_str())
        .args(&args_discover)
        .output()
        .expect("Failed iscsiadm discovery");
    if !output.status.success() {
        let args_discover_del = [
            "-m",
            "discoverydb",
            "-t",
            "sendtargets",
            "-p",
            &tp,
            "-I",
            "default",
            "-o",
            "delete",
        ];
        // delete discoverydb record
        Command::new(ISCSIADM.as_str()).args(&args_discover_del);
        return Err(String::from_utf8(output.stderr).unwrap());
    }

    let args_login = [
        "-m", "node", "-p", &tp, "-T", &iqn, "-I", "default", "--login",
    ];
    trace!("iscsiadm {:?}", args_login);
    // login to iscsi target
    let output = Command::new(ISCSIADM.as_str())
        .args(&args_login)
        .output()
        .expect("Failed iscsiadm login");
    if !output.status.success() {
        let args_login_del = [
            "-m", "node", "-p", &tp, "-T", &iqn, "-I", "default", "-o",
            "delete",
        ];
        // delete the node record from the database.
        Command::new(ISCSIADM.as_str()).args(&args_login_del);
        return Err(String::from_utf8(output.stderr).unwrap());
    }

    if wait_for_path_to_exist(device_path.as_str(), 10) {
        trace!("{} path exists!", device_path)
    } else {
        trace!("{} path does not exist after 10s!", device_path);
        return Err("Could not attach disk: Timeout after 10s".to_string());
    }
    Ok(device_path)
}

pub fn iscsi_attach_disk(iscsi_uri: &str) -> Result<String, String> {
    trace!("iscsi_attach_disk {}", iscsi_uri);

    static RE_ISCSI_URI: Lazy<regex::Regex> = Lazy::new(|| {
        regex::Regex::new(
            r"(?x)
            iscsi://(?P<ip>\d+.\d+.\d+.\d+):(?P<port>\d+)/(?P<iqn>.*)/(?P<lun>\d+)
            ",
        )
        .unwrap()
    });

    let caps = RE_ISCSI_URI.captures(iscsi_uri);
    match caps {
        Some(details) => attach_disk(
            &details["ip"],
            &details["port"],
            &details["iqn"],
            &details["lun"],
        ),
        None => Err(format!("Invalid iscsi URI {}", iscsi_uri)),
    }
}

pub fn detach_disk(ip_addr: &str, port: &str, iqn: &str) -> Result<(), String> {
    let tp = format!("{}:{}", ip_addr, port);

    let args_logout = ["-m", "node", "-T", &iqn, "-p", &tp, "-u"];
    trace!("iscsiadm {:?}", args_logout);
    let output = Command::new(ISCSIADM.as_str())
        .args(&args_logout)
        .output()
        .expect("Failed iscsiadm logout");
    if !output.status.success() {
        return Err(String::from_utf8(output.stderr).unwrap());
    }

    let args_delete = ["-m", "node", "-o", "delete", "-T", &iqn];
    trace!("iscsiadm {:?}", args_delete);
    let output = Command::new(ISCSIADM.as_str())
        .args(&args_delete)
        .output()
        .expect("Failed iscsiadm login");
    if !output.status.success() {
        return Err(String::from_utf8(output.stderr).unwrap());
    }

    let args_discoverydb_del = [
        "-m",
        "discoverydb",
        "-t",
        "sendtargets",
        "-p",
        &tp,
        "-o",
        "delete",
    ];
    trace!("iscsiadm {:?}", args_discoverydb_del);
    let output = Command::new(ISCSIADM.as_str())
        .args(&args_discoverydb_del)
        .output()
        .expect("Failed iscsiadm login");
    if !output.status.success() {
        return Err(String::from_utf8(output.stderr).unwrap());
    }
    Ok(())
}

pub fn iscsi_detach_disk(device_path: &str) -> Result<(), String> {
    trace!("iscsi_detach_disk {}", device_path);
    static RE_DEVICE_PATH: Lazy<regex::Regex> = Lazy::new(|| {
        regex::Regex::new(
            r"(?x)
            ip-(?P<ip>\d+.\d+.\d+.\d+):(?P<port>\d+)-iscsi-(?P<iqn>.*)-lun-(?P<lun>\d+)
            ",
        )
        .unwrap()
    });

    let caps = RE_DEVICE_PATH.captures(device_path);
    match caps {
        Some(details) => {
            trace!("{:?}", details);
            detach_disk(&details["ip"], &details["port"], &details["iqn"])
        }
        None => {
            trace!("Doh!");
            Err(format!("Invalid device path: {}", device_path))
        }
    }
}

pub fn iscsi_find(uuid: &str) -> Option<String> {
    if which::which(ISCSIADM.as_str()).is_err() {
        trace!("Cannot find {}", ISCSIADM.as_str());
        return None;
    }
    let output = Command::new(ISCSIADM.as_str())
        .args(&["-m", "node"])
        .output()
        .expect("Failed iscsiadm");
    if !output.status.success() {
        debug!(
            "iscsiadm failed: {}",
            String::from_utf8(output.stderr).unwrap()
        );
        return None;
    }
    let op = String::from_utf8(output.stdout).unwrap();

    static RE_TARGET: Lazy<regex::Regex> = Lazy::new(|| {
        regex::Regex::new(
            r"(?x)
            (?P<ip>\d+.\d+.\d+.\d+):(?P<port>\d+),(?P<lun>\d+)\s+(?P<iqn>.*:\w+)-(?P<uuid>.*)
            ",
        )
        .unwrap()
    });

    for cap in RE_TARGET.captures_iter(op.as_str()) {
        trace!("unstage: searching for {} got {}", uuid, &cap["uuid"]);
        if uuid == &cap["uuid"] {
            return Some(format!(
                "/dev/disk/by-path/ip-{}:{}-iscsi-{}-{}-lun-{}",
                &cap["ip"],
                &cap["port"],
                &cap["iqn"],
                &cap["uuid"],
                &cap["lun"],
            ));
        }
    }
    None
}
