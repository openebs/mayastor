use crate::cli_tools::run_command_args;
use io_engine::constants::NVME_CONTROLLER_MODEL_ID;
use regex::Regex;
use std::{
    collections::BTreeMap,
    net::SocketAddr,
    path::PathBuf,
    process::{Command, ExitStatus},
};

/// Connects an NVMe device upon creation and disconnects when dropped.
pub struct NmveConnectGuard {
    nqn: String,
}

impl NmveConnectGuard {
    pub fn connect(target_addr: &str, nqn: &str) -> Self {
        nvme_connect(target_addr, nqn, "tcp", true);

        Self {
            nqn: nqn.to_string(),
        }
    }

    pub fn connect_addr(addr: &SocketAddr, nqn: &str) -> Self {
        Self::connect(&addr.ip().to_string(), nqn)
    }
}

impl Drop for NmveConnectGuard {
    fn drop(&mut self) {
        assert!(!self.nqn.is_empty());

        nvme_disconnect_nqn(&self.nqn);
        self.nqn.clear();
    }
}

pub fn nvme_discover(target_addr: &str) -> Vec<BTreeMap<String, String>> {
    let re = Regex::new(r"^(\w+):\s+(.+)$").unwrap();

    match run_command_args(
        "nvme",
        vec!["discover", "-t", "tcp", "-a", target_addr, "-s", "8420"],
        None,
    ) {
        Ok((_, lines)) => {
            let mut res = vec![];
            let mut obj = None;

            for line in lines.iter() {
                let s = line.to_str().unwrap();
                if s.starts_with("=====Discovery Log Entry") {
                    if let Some(o) = obj {
                        res.push(o);
                    }
                    obj = Some(BTreeMap::new());
                    continue;
                }

                if obj.is_none() {
                    continue;
                }

                if let Some(cap) = re.captures(s) {
                    let k = cap.get(1).unwrap().as_str().to_string();
                    let v = cap.get(2).unwrap().as_str().to_string();
                    obj.as_mut().unwrap().insert(k, v);
                }
            }

            if let Some(o) = obj {
                res.push(o);
            }

            res
        }
        Err(e) => {
            println!("Failed to discover NVMEs: {e}");
            vec![]
        }
    }
}

pub fn nvme_connect(
    target_addr: &str,
    nqn: &str,
    transport: &str,
    must_succeed: bool,
) -> ExitStatus {
    let status = Command::new("nvme")
        .args(["connect"])
        .args(["-t", transport])
        .args(["-a", target_addr])
        .args(["-s", "8420"])
        .args(["-c", "1"])
        .args(["-n", nqn])
        .status()
        .unwrap();

    if !status.success() {
        let msg = format!(
            "failed to connect to {target_addr}, nqn '{nqn}': {status}"
        );
        if must_succeed {
            panic!("{}", msg);
        } else {
            eprintln!("{msg}");
        }
    } else {
        std::thread::sleep(std::time::Duration::from_secs(1));
    }

    status
}

pub fn nvme_disconnect_all() {
    let output_dis = Command::new("nvme")
        .args(["disconnect-all"])
        .output()
        .unwrap();
    assert!(
        output_dis.status.success(),
        "failed to disconnect all existing nvme target ",
    );
}

pub fn nvme_disconnect_nqn(nqn: &str) {
    let output_dis = Command::new("nvme")
        .args(["disconnect"])
        .args(["-n", nqn])
        .output()
        .unwrap();
    assert!(
        output_dis.status.success(),
        "failed to disconnect from {}: {}",
        nqn,
        output_dis.status
    );
}

pub fn list_mayastor_nvme_devices() -> Vec<libnvme_rs::NvmeDevice> {
    libnvme_rs::NvmeTarget::list()
        .into_iter()
        .filter(|dev| dev.model.contains(NVME_CONTROLLER_MODEL_ID))
        .collect()
}

pub fn find_mayastor_nvme_device(
    serial: &str,
) -> Option<libnvme_rs::NvmeDevice> {
    list_mayastor_nvme_devices()
        .into_iter()
        .find(|d| d.serial == serial)
}

/// Returns /dev/ file path for the given NVMe serial.
pub fn find_mayastor_nvme_device_path(
    serial: &str,
) -> std::io::Result<PathBuf> {
    list_mayastor_nvme_devices()
        .into_iter()
        .find(|d| d.serial == serial)
        .map(|d| PathBuf::from(format!("/dev/{}", d.device)))
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("NVMe device with serial '{serial}' not found"),
            )
        })
}

pub fn get_nvme_resv_report(nvme_dev: &str) -> serde_json::Value {
    let output_resv = Command::new("nvme")
        .args(["resv-report"])
        .args([nvme_dev])
        .args(["-c", "1"])
        .args(["-o", "json"])
        .output()
        .unwrap();
    assert!(
        output_resv.status.success(),
        "failed to get reservation report from {}: {}",
        nvme_dev,
        output_resv.status
    );
    let resv_rep = String::from_utf8(output_resv.stdout).unwrap();
    let v: serde_json::Value =
        serde_json::from_str(&resv_rep).expect("JSON was not well-formatted");
    v
}
