use std::{env, path::Path, process::Command, thread, time};

lazy_static! {
    // Regex to parse the nexus URI, thish matches and returns the
    // required components of the expected nexus URI.
    // In particular using a regex allows us to
    //  * extract the uuid of the nexus share easily
    //  * enforce that the URI is an exact natch if required
    pub static ref RE_NEXUS_ISCSI_URI: regex::Regex = regex::Regex::new(
        r"(?x)(?P<scheme>\w+)://
            (?P<ip>\d+.\d+.\d+.\d+):
            (?P<port>\d+)/
            (?P<iqn>.*?nexus)-
            (?P<uuid>.*)/(?P<lun>\d+)
        ",
    )
    .unwrap();

    // The iscsiadm executable invoked is dependent on the environment.
    // For the container we set it using and environment variable,
    // typically this is the "/bin/mayastor-iscsiadm" script,
    // created by the mayastor image build scripts.
    // For development hosts just setting it to iscsiadm works.
    static ref ISCSIADM: String = if env::var("ISCSIADM").is_err() {
            debug!("defaulting to using iscsiadm");
            "iscsiadm".to_string()
        } else {
            debug!("using {}", env::var("ISCSIADM").unwrap());
            env::var("ISCSIADM").unwrap()
    };
}

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
    trace!("iscsiadm -m node -p {}:{} -T {} -R", ip_addr, port, iqn);
    let _ = Command::new(ISCSIADM.as_str())
        .arg("-m")
        .arg("node")
        .arg("-p")
        .arg(&tp)
        .arg("-T")
        .arg(&iqn)
        .arg("-R")
        .output()
        .expect("Failed iscsiadm rescan");

    // If the device path exists then a previous invocation of this
    // method has succeeded.
    if wait_for_path_to_exist(device_path.as_str(), 1) {
        trace!("path already exists!");
        return Ok(device_path);
    }

    trace!(
        "iscsiadm -m discoverydb -t sendtargets -p {}:{} -I default -o new",
        ip_addr,
        port
    );
    let output = Command::new(ISCSIADM.as_str())
        .arg("-m")
        .arg("discoverydb")
        .arg("-t")
        .arg("sendtargets")
        .arg("-p")
        .arg(&tp)
        .arg("-I")
        .arg("default")
        .arg("-o")
        .arg("new")
        .output()
        .expect("Failed iscsiadm discovery");
    if !output.status.success() {
        return Err(format!(
            "iscsi: failed to update discoverydb to portal {}, Error: {}",
            &tp,
            String::from_utf8(output.stderr).unwrap()
        ));
    }

    trace!(
        "iscsiadm -m discoverydb -t sendtargets -p {}:{} -I default --discover",
        ip_addr,
        port
    );
    // build discoverydb and discover iscsi target
    let output = Command::new(ISCSIADM.as_str())
        .arg("-m")
        .arg("discoverydb")
        .arg("-t")
        .arg("sendtargets")
        .arg("-p")
        .arg(&tp)
        .arg("-I")
        .arg("default")
        .arg("--discover")
        .output()
        .expect("Failed iscsiadm discovery");
    if !output.status.success() {
        // delete discoverydb record
        let _ = Command::new(ISCSIADM.as_str())
            .arg("-m")
            .arg("discoverydb")
            .arg("-t")
            .arg("sendtargets")
            .arg("-p")
            .arg(&tp)
            .arg("-I")
            .arg("default")
            .arg("-o")
            .arg("delete")
            .output()
            .expect("Failed iscsiadm discovery");
        return Err(format!(
            "iscsi: failed to sendtargets to portal {}, Error: {}",
            &tp,
            String::from_utf8(output.stderr).unwrap()
        ));
    }

    trace!(
        "iscsiadm -m node -p {}:{} -T {} -I default --login",
        ip_addr,
        port,
        iqn
    );
    // login to iscsi target
    let output = Command::new(ISCSIADM.as_str())
        .arg("-m")
        .arg("node")
        .arg("-p")
        .arg(&tp)
        .arg("-T")
        .arg(&iqn)
        .arg("-I")
        .arg("default")
        .arg("--login")
        .output()
        .expect("Failed iscsiadm login");
    if !output.status.success() {
        // delete the node record from the database.
        let _ = Command::new(ISCSIADM.as_str())
            .arg("-m")
            .arg("node")
            .arg("-p")
            .arg(&tp)
            .arg("-T")
            .arg(&iqn)
            .arg("-I")
            .arg("default")
            .arg("-o")
            .arg("delete")
            .output()
            .expect("Failed iscsiadm login");
        return Err(format!(
            "iscsi: failed to attach disk: Error: {}",
            String::from_utf8(output.stderr).unwrap()
        ));
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
    lazy_static! {
        static ref RE_ISCSI_URI: regex::Regex = regex::Regex::new(
            r"(?x)
            iscsi://(?P<ip>\d+.\d+.\d+.\d+):(?P<port>\d+)/(?P<iqn>.*)/(?P<lun>\d+)
            ",
        )
        .unwrap();
    }

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
    /*
        let ip_addr = &details["ip"];
        let port = &details["port"];
        let iqn = &details["iqn"];
    */
    let tp = format!("{}:{}", ip_addr, port);

    trace!("iscsiadm -m node -T {} -p {}:{} -u", iqn, ip_addr, port);
    let output = Command::new(ISCSIADM.as_str())
        .arg("-m")
        .arg("node")
        .arg("-T")
        .arg(&iqn)
        .arg("-p")
        .arg(&tp)
        .arg("-u")
        .output()
        .expect("Failed iscsiadm logout");
    if !output.status.success() {
        return Err(format!(
            "iscsiadm failed: {}",
            String::from_utf8(output.stderr).unwrap()
        ));
    }

    trace!("iscsiadm -m node -o delete -T {}", iqn);
    let output = Command::new(ISCSIADM.as_str())
        .arg("-m")
        .arg("node")
        .arg("-o")
        .arg("delete")
        .arg("-T")
        .arg(iqn)
        .output()
        .expect("Failed iscsiadm login");
    if !output.status.success() {
        return Err(format!(
            "iscsiadm failed: {}",
            String::from_utf8(output.stderr).unwrap()
        ));
    }

    trace!(
        "iscsiadm -m discoverydb -t sendtargets -p {}:{} -o delete",
        ip_addr,
        port
    );
    let output = Command::new(ISCSIADM.as_str())
        .arg("-m")
        .arg("discoverydb")
        .arg("-t")
        .arg("sendtargets")
        .arg("-p")
        .arg(tp)
        .arg("-o")
        .arg("delete")
        .output()
        .expect("Failed iscsiadm login");
    if !output.status.success() {
        return Err(format!(
            "iscsiadm failed: {}",
            String::from_utf8(output.stderr).unwrap()
        ));
    }
    Ok(())
}

pub fn iscsi_detach_disk(device_path: &str) -> Result<(), String> {
    trace!("iscsi_detach_disk {}", device_path);
    lazy_static! {
        static ref RE_DEVICE_PATH: regex::Regex = regex::Regex::new(
            r"(?x)
            ip-(?P<ip>\d+.\d+.\d+.\d+):(?P<port>\d+)-iscsi-(?P<iqn>.*)-lun-(?P<lun>\d+)
            ",
        )
        .unwrap();
    }

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

pub fn iscsi_find(uuid: &str) -> Result<String, String> {
    trace!("unstage: iscsi_find for {}", uuid);
    let output = Command::new(ISCSIADM.as_str())
        .arg("-m")
        .arg("node")
        .output()
        .expect("Failed iscsiadm");
    if !output.status.success() {
        return Err(format!(
            "iscsiadm failed: {}",
            String::from_utf8(output.stderr).unwrap()
        ));
    }
    let op = String::from_utf8(output.stdout).unwrap();
    trace!("unstage: iscsi_find op {}", op);

    lazy_static! {
        static ref RE_TARGET: regex::Regex = regex::Regex::new(
            r"(?x)
        (?P<ip>\d+.\d+.\d+.\d+):(?P<port>\d+),(?P<lun>\d+)\s+(?P<iqn>.*:\w+)-(?P<uuid>.*)
        ",
        )
        .unwrap();
    }
    for cap in RE_TARGET.captures_iter(op.as_str()) {
        trace!("unstage: searching for {} got {}", uuid, &cap["uuid"]);
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
    Err("Not found".to_string())
}
