use crate::{error, parse_value};
use error::NvmeError;
use glob::glob;
use std::{os::unix::fs::FileTypeExt, path::Path};

/// NvmeDevices are devices that are already connected to the kernel
/// they have not interaction with the fabric itself. Notice that a
/// nvme device, for the post part is a subsystem + nsid.

#[derive(Debug, Default)]
pub struct NvmeDevice {
    /// device path of the device
    pub path: String,
    /// the device model defined by the manufacturer
    model: String,
    /// serial number of the device
    serial: String,
    /// the size in bytes
    size: u64,
    /// the UUID for the device
    uuid: String,
    /// the world wide name of the device typically wwn.uuid
    wwid: String,
    /// the namespace id
    nsid: u64,
    /// firmware revision
    fw_rev: String,
    /// the nqn of the subsystem this device instance is connected to
    pub subsysnqn: String,
}

impl NvmeDevice {
    /// Construct a new NVMe device from a given path. The [struct.NvmeDevice]
    /// will fill in all the details defined within the structure or return an
    /// error if the value for the structure could not be found.
    fn new(p: &Path) -> Result<Self, NvmeError> {
        let name = p.file_name().unwrap().to_str().unwrap();
        let devpath = format!("/sys/block/{}", name);
        let subsyspath = format!("/sys/block/{}/device", name);
        let source = Path::new(devpath.as_str());
        let subsys = Path::new(subsyspath.as_str());

        Ok(NvmeDevice {
            path: p.display().to_string(),
            fw_rev: parse_value(&subsys, "firmware_rev")?,
            subsysnqn: parse_value(&subsys, "subsysnqn")?,
            model: parse_value(&subsys, "model")?,
            serial: parse_value(&subsys, "serial")?,
            size: parse_value(&source, "size")?,
            // /* NOTE: during my testing, it seems that NON fabric devices
            //  * do not have a UUID, this means that local PCIe devices will
            //  * be filtered out automatically. We should not depend on this
            //  * feature or, bug until we gather more data
            uuid: parse_value(&source, "uuid")
                .unwrap_or_else(|_| String::from("N/A")),
            wwid: parse_value(&source, "wwid")?,
            nsid: parse_value(&source, "nsid")?,
        })
    }
}
/// The DeviceList of all NVMe devices found that provide all properties as
/// defined in the [struct.NvmeDevice]
#[derive(Debug, Default)]
pub struct NvmeDeviceList {
    devices: Vec<String>,
}

impl Iterator for NvmeDeviceList {
    type Item = Result<NvmeDevice, NvmeError>;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(e) = self.devices.pop() {
            return Some(NvmeDevice::new(Path::new(&e)));
        }
        None
    }
}

impl NvmeDeviceList {
    /// glob sysfs and filter out all devices that start with /dev/nvme
    pub fn new() -> Self {
        let mut list = NvmeDeviceList::default();
        let path_entries = glob("/dev/nvme*").unwrap();
        for path in path_entries.flatten() {
            if let Ok(meta) = std::fs::metadata(&path) {
                if meta.file_type().is_block_device() {
                    list.devices.push(path.display().to_string());
                }
            }
        }
        list
    }
}
