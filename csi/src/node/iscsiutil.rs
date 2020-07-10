use once_cell::sync::Lazy;
use std::{env, path::Path, process::Command, thread, time};

use crate::CSIError;
use failure::Error;

// The iscsiadm executable invoked is dependent on the environment.
// For the container we set it using an environment variable,
// typically this is the "/bin/mayastor-iscsiadm" script,
// created by the mayastor image build scripts.
// For development && test environments setting it to iscsiadm works
// as long as iscsiadm exists and mayastor has the right permissions
// to execute iscsiadm.
static ISCSIADM: Lazy<String> = Lazy::new(|| {
    let mayastor_iscsiadm = "/bin/mayastor-iscsiadm";
    if env::var("ISCSIADM").is_ok()
        && which::which(env::var("ISCSIADM").unwrap().as_str()).is_ok()
    {
        debug!("Using {} for iscsiadm", env::var("ISCSIADM").unwrap());
        env::var("ISCSIADM").unwrap()
    } else if which::which(mayastor_iscsiadm).is_ok() {
        debug!("Using {} for iscsiadm", mayastor_iscsiadm);
        mayastor_iscsiadm.to_string()
    } else if which::which("iscsiadm").is_ok() {
        debug!("Using iscsiadm in PATH");
        "iscsiadm".to_string()
    } else {
        debug!("No isciadm found");
        "".to_string()
    }
});

fn get_iscsiadm() -> Result<&'static str, Error> {
    match ISCSIADM.len() {
        0 => Err(Error::from(CSIError::ExecutableNotFound {
            execname: "iscsiadm".to_string(),
        })),
        _ => Ok(ISCSIADM.as_str()),
    }
}

fn wait_for_path_to_exist(
    devpath: &str,
    timeout: time::Duration,
    max_retries: u32,
) -> bool {
    let device_path = Path::new(devpath);
    let mut retries: u32 = 0;
    let now = time::Instant::now();
    while !device_path.exists() && retries < max_retries {
        thread::sleep(timeout);
        retries += 1;
    }
    trace!(
        "wait_for_path_to_exist for elapsed time is {:?}",
        now.elapsed()
    );
    device_path.exists()
}

fn iscsi_realpath(path: String) -> String {
    match std::fs::read_link(path.as_str()) {
        Ok(linkpath) => {
            // For iscsi the root path is /dev/disk/by-path
            let mut devpath = std::path::PathBuf::from("/dev/disk/by-path");
            devpath.push(linkpath);
            let absdevpath = std::fs::canonicalize(devpath).unwrap();
            absdevpath.into_os_string().into_string().unwrap()
        }
        _ => path,
    }
}

fn attach_disk(
    ip_addr: &str,
    port: u16,
    iqn: &str,
    lun: &str,
) -> Result<String, Error> {
    let tp = format!("{}:{}", ip_addr, port);
    let device_path =
        format!("/dev/disk/by-path/ip-{}-iscsi-{}-lun-{}", tp, iqn, lun);
    let iscsiadm = get_iscsiadm()?;

    static RE_SESSION: Lazy<regex::Regex> = Lazy::new(|| {
        regex::Regex::new(
            r"\s*(?P<tp>\d+.\d+.\d+.\d+:\d+),0\s*(?P<iqn>iqn.2019-05.io.openebs:nexus-.*)\s"
    )
    .unwrap()
    });

    let args_sessions = ["-m", "session"];
    let output = Command::new(&iscsiadm)
        .args(&args_sessions)
        .output()
        .expect("Failed iscsiadm session");

    let mut have_session = false;
    if output.status.success() {
        let op = String::from_utf8(output.stdout).unwrap();
        let haystack: Vec<&str> = op.split('\n').collect();
        for session in haystack {
            if let Some(details) = RE_SESSION.captures(session) {
                if tp == &details["tp"] && iqn == &details["iqn"] {
                    debug!("Found session for {} {}", tp, iqn);
                    have_session = true;
                }
            }
        }
    }

    // Do not attempt to create a session if one exists.
    if !have_session {
        let target = format!("{},{} {}", tp, lun, iqn);

        let args_discovery =
            ["-m", "discovery", "-t", "st", "-p", &tp, "-I", "default"];
        trace!("iscsiadm {:?}", &args_discovery);
        let output = Command::new(&iscsiadm)
            .args(&args_discovery)
            .output()
            .expect("Failed iscsiadm discovery");
        if !output.status.success() {
            return Err(Error::from(CSIError::Iscsiadm {
                error: String::from_utf8(output.stderr).unwrap(),
            }));
        }

        // Check that the output from the iscsiadm discover command lists
        // the iscsi target we need to login.
        // If not fail.
        let op = String::from_utf8(output.stdout).unwrap();
        let haystack: Vec<&str> = op.split('\n').collect();
        if !haystack.iter().any(|&s| s == target.as_str()) {
            trace!("After discovery no record for {}", target);
            return Err(Error::from(CSIError::Iscsiadm {
                error: format!("No record for {}", target),
            }));
        }

        let args_login = [
            "-m", "node", "-p", &tp, "-T", &iqn, "-I", "default", "--login",
        ];
        trace!("iscsiadm {:?}", args_login);
        // login to iscsi target
        let output = Command::new(&iscsiadm)
            .args(&args_login)
            .output()
            .expect("Failed iscsiadm login");
        if !output.status.success() {
            let args_login_del = [
                "-m", "node", "-p", &tp, "-T", &iqn, "-I", "default", "-o",
                "delete",
            ];
            // delete the node record from the database.
            Command::new(&iscsiadm).args(&args_login_del);
            return Err(Error::from(CSIError::Iscsiadm {
                error: String::from_utf8(output.stderr).unwrap(),
            }));
        }
    }

    // 10 retries at 100ms intervals = 1000ms = 1 second.
    let timeout = time::Duration::from_millis(100);
    const RETRIES: u32 = 10;
    if wait_for_path_to_exist(device_path.as_str(), timeout, RETRIES) {
        trace!("{} path exists!", device_path)
    } else {
        trace!(
            "{} path does not exist after {:?}!",
            device_path,
            timeout * RETRIES
        );
        return Err(Error::from(CSIError::AttachTimeout {
            value: (timeout * RETRIES),
        }));
    }
    Ok(iscsi_realpath(device_path))
}

/// Attaches a nexus iscsi target matching the uri specfied.
/// Returns path to the device on which the nexus iscsi target
/// has been mounted succesfully or error
pub fn iscsi_attach_disk(iscsi_uri: &str) -> Result<String, Error> {
    trace!("iscsi_attach_disk {}", iscsi_uri);

    if let Ok(url) = url::Url::parse(iscsi_uri) {
        if url.scheme() == "iscsi" {
            let tokens =
                url.path_segments().map(|c| c.collect::<Vec<_>>()).unwrap();
            return attach_disk(
                url.host_str().unwrap(),
                url.port().unwrap(),
                tokens[0],
                tokens[1],
            );
        }
    }

    Err(Error::from(CSIError::InvalidURI {
        uristr: iscsi_uri.to_string(),
    }))
}

fn detach_disk(ip_addr: &str, port: &str, iqn: &str) -> Result<(), Error> {
    let iscsiadm = get_iscsiadm()?;

    let tp = format!("{}:{}", ip_addr, port);

    let args_logout = ["-m", "node", "-T", &iqn, "-p", &tp, "-u"];
    trace!("iscsiadm {:?}", args_logout);
    let output = Command::new(&iscsiadm)
        .args(&args_logout)
        .output()
        .expect("Failed iscsiadm logout");
    if !output.status.success() {
        return Err(Error::from(CSIError::Iscsiadm {
            error: String::from_utf8(output.stderr).unwrap(),
        }));
    }

    let args_delete = ["-m", "node", "-o", "delete", "-T", &iqn];
    trace!("iscsiadm {:?}", args_delete);
    let output = Command::new(&iscsiadm)
        .args(&args_delete)
        .output()
        .expect("Failed iscsiadm login");
    if !output.status.success() {
        return Err(Error::from(CSIError::Iscsiadm {
            error: String::from_utf8(output.stderr).unwrap(),
        }));
    }

    Ok(())
}

/// Detaches nexus iscsi target matching the volume id if has
/// been mounted.
/// Returns error is the nexus iscsi target was not mounted.
pub fn iscsi_detach_disk(uuid: &str) -> Result<(), Error> {
    trace!("iscsi_detach_disk {}", uuid);
    let device_path = get_iscsi_device_path(uuid)?;

    static RE_DEVICE_PATH: Lazy<regex::Regex> = Lazy::new(|| {
        regex::Regex::new(
            r"(?x)
            ip-(?P<ip>\d+.\d+.\d+.\d+):(?P<port>\d+)-iscsi-(?P<iqn>.*)-lun-(?P<lun>\d+)
            ",
        )
        .unwrap()
    });

    let caps = RE_DEVICE_PATH.captures(device_path.as_str());
    match caps {
        Some(details) => {
            trace!("{:?}", details);
            detach_disk(&details["ip"], &details["port"], &details["iqn"])
        }
        None => Err(Error::from(CSIError::InvalidDevicePath {
            devpath: device_path.to_string(),
        })),
    }
}

fn get_iscsi_device_path(uuid: &str) -> Result<String, Error> {
    let iscsiadm = get_iscsiadm()?;

    let output = Command::new(&iscsiadm)
        .args(&["-m", "node"])
        .output()
        .expect("Failed iscsiadm");
    if !output.status.success() {
        return Err(Error::from(CSIError::Iscsiadm {
            error: String::from_utf8(output.stderr).unwrap(),
        }));
    }
    let op = String::from_utf8(output.stdout).unwrap();

    static RE_TARGET: Lazy<regex::Regex> = Lazy::new(|| {
        regex::Regex::new(
            r"(?x)
            (?P<ip>\d+.\d+.\d+.\d+):(?P<port>\d+),(?P<lun>\d+)\s+(?P<iqn>iqn\.\d+-\d+\.io\.openebs:nexus)-(?P<uuid>.*)
            ",
        )
        .unwrap()
    });

    for cap in RE_TARGET.captures_iter(op.as_str()) {
        trace!("iscsiutil: searching for {} got {}", uuid, &cap["uuid"]);
        if uuid == &cap["uuid"] {
            return Ok(format!(
                "/dev/disk/by-path/ip-{}:{}-iscsi-{}-{}-lun-{}",
                &cap["ip"],
                &cap["port"],
                &cap["iqn"],
                &cap["uuid"],
                &cap["lun"],
            ));
        }
    }
    Err(Error::from(CSIError::NotFound {
        value: format!("iscsi device for {}", uuid),
    }))
}

/// Search for and return path to the device on which a nexus iscsi
/// target matching the volume id has been mounted or None.
pub fn iscsi_find(uuid: &str) -> Option<String> {
    if let Ok(path) = get_iscsi_device_path(uuid) {
        if Path::new(path.as_str()).exists() {
            return Some(iscsi_realpath(path));
        }
    }
    None
}
