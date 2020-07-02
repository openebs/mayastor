use std::{io, thread, time};
use udev::Enumerator;

fn find_nvmf_device_by_uuid(str_uuid: &str) -> Result<String, String> {
    let prop = "ID_WWN";
    let value = format!("uuid.{}", str_uuid);

    trace!("find_nvmf_device_by_uuid uuid={}", str_uuid);
    let mut enumerator = Enumerator::new().unwrap();
    enumerator.match_subsystem("block").unwrap();
    enumerator
        .match_property("ID_MODEL", "MayaStor NVMF controller")
        .unwrap();
    for dev in enumerator.scan_devices().unwrap() {
        if let Some(udev_value) = dev.property_value(prop) {
            if udev_value.to_str().unwrap().contains(&value) {
                trace!(
                    "find_nvmf_device_by_uuid {} got {:?}",
                    str_uuid,
                    dev.property_value("DEVNAME")
                );

                return Ok(dev
                    .property_value("DEVNAME")
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_string());
            }
        }
    }

    // fall through
    Err(format!("device not found for {}", str_uuid))
}

fn uuid_from_str(s: &str) -> String {
    //TODO something more sane.
    //hack { convert something like
    //0)    nvmf://192.168.122.98:8420/nqn.2019-
    //1)        05.io.openebs:nexus-
    //2)        04089481-
    //3)        cc69-
    //4)        45c1-
    //5)        8009-
    //6)        8aa7167a633a
    // to uuid string value.
    // will return a garbage string if passed in garbage,
    // but that is okay for now since this is only used
    // to find devices with a matching uuid and nothing
    // will match the garbage.
    let mut frags = s.split('-').collect::<Vec<_>>();
    while frags.len() > 5 {
        frags.remove(0);
    }
    itertools::join(&frags, "-")
    // } hack
}

fn wait_for_path_to_exist(uuid: String, max_retries: i32) -> Option<String> {
    let second = time::Duration::from_secs(1);
    let mut retries: i32 = 0;
    let now = time::Instant::now();
    while retries < max_retries {
        thread::sleep(second);

        if let Ok(path) = find_nvmf_device_by_uuid(&uuid) {
            trace!(
                "wait_for_path_to_exist {} {} success for elapsed time is {:?}",
                uuid,
                path,
                now.elapsed()
            );
            return Some(path);
        }
        retries += 1;
    }
    debug!("wait_for_path_to_exist timed out after {:?}", now.elapsed());
    None
}

fn nvmeadm_attach_disk(
    ip_addr: &str,
    port: u32,
    nqn: &str,
    uri: &str,
) -> Result<String, String> {
    trace!(
        "nvmeadm_attach_disk ip_addr={} port={} nqn={}",
        ip_addr,
        port,
        nqn
    );

    let result = nvmeadm::nvmf_discovery::connect(ip_addr, port, nqn);
    if let Err(e) = result {
        let mut in_progress = false;
        if let Some(ioerr) = e.downcast_ref::<io::Error>() {
            if let Some(errcode) = ioerr.raw_os_error() {
                if errcode == 114 {
                    in_progress = true;
                    debug!("nvmeadm operation already in progress for {}", uri);
                }
            }
        }
        if !in_progress {
            debug!("nvmeadm connect failed error {} {:?}", uri, e);
            return Err(format!("{}", e));
        }
    }

    match wait_for_path_to_exist(uuid_from_str(uri), 10) {
        Some(path) => Ok(path),
        _ => {
            debug!("nvmeadm nvmf device path not found.");
            Err("No path for nvme device".to_string())
        }
    }
}

pub fn nvmeadm_detach_disk(nqn: &str) -> Result<(), String> {
    match nvmeadm::nvmf_discovery::disconnect(&nqn) {
        Ok(_) => {
            trace!("nvmf disconnected {}", nqn);
            Ok(())
        }
        Err(e) => {
            debug!("nvmf disconnect {} FAILED.", nqn);
            Err(format!("{}", e))
        }
    }
}

pub fn nvmf_attach_disk(nvmf_uri: &str) -> Result<String, String> {
    trace!("nvmf_attach_disk {}", nvmf_uri);

    if let Some(path) = wait_for_path_to_exist(uuid_from_str(nvmf_uri), 1) {
        return Ok(path);
    }

    if let Ok(url) = url::Url::parse(nvmf_uri) {
        if url.scheme() == "nvmf" {
            let tokens: Vec<&str> = url.path_segments().unwrap().collect();
            return nvmeadm_attach_disk(
                url.host_str().unwrap(),
                u32::from(url.port().unwrap()),
                tokens[0],
                nvmf_uri,
            );
        }
    }

    Err(format!("Invalid nvmf URI {}", nvmf_uri))
}

/// Search for and return path to the device on which a nexus nvmf
/// target matching the volume id has been mounted or None.
pub fn nvmf_find(uuid: &str) -> Option<String> {
    trace!("nvmf_find {}", uuid);
    match find_nvmf_device_by_uuid(uuid) {
        Ok(path) => {
            trace!("nvmf_find for {} got {}", uuid, path);
            Some(path)
        }
        _ => {
            debug!("nvmf_find for {} FAILED", uuid);
            None
        }
    }
}

pub fn nvmf_detach_disk(uuid: &str) -> Result<(), String> {
    // Ugh! hardcoded nqn, bad, bad, bad
    let nqn = format!("nqn.2019-05.io.openebs:{}", uuid);
    trace!("nvmf_detach_disk for {} nqn is {}", uuid, nqn);
    nvmeadm_detach_disk(&nqn)
}
