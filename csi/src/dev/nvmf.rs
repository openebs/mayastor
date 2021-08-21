use std::{
    collections::HashMap,
    convert::{From, TryFrom},
    path::Path,
};

use nvmeadm::{
    error::NvmeError,
    nvmf_discovery::{disconnect, ConnectArgsBuilder},
};

use glob::glob;
use regex::Regex;
use udev::{Device, Enumerator};
use url::Url;
use uuid::Uuid;

use crate::{dev::util::extract_uuid, match_dev::match_nvmf_device};

use super::{Attach, Detach, DeviceError, DeviceName};

lazy_static! {
    static ref DEVICE_REGEX: Regex = Regex::new(r"nvme(\d{1,3})n1").unwrap();
}

pub(super) struct NvmfAttach {
    host: String,
    port: u16,
    uuid: Uuid,
    nqn: String,
    io_timeout: Option<u32>,
}

impl NvmfAttach {
    fn new(host: String, port: u16, uuid: Uuid, nqn: String) -> NvmfAttach {
        NvmfAttach {
            host,
            port,
            uuid,
            nqn,
            io_timeout: None,
        }
    }

    fn get_device(&self) -> Result<Option<Device>, DeviceError> {
        let key: String = format!("uuid.{}", self.uuid.to_string());
        let mut enumerator = Enumerator::new()?;

        enumerator.match_subsystem("block")?;
        enumerator.match_property("DEVTYPE", "disk")?;

        for device in enumerator.scan_devices()? {
            if match_nvmf_device(&device, &key).is_some() {
                return Ok(Some(device));
            }
        }

        Ok(None)
    }
}

impl TryFrom<&Url> for NvmfAttach {
    type Error = DeviceError;

    fn try_from(url: &Url) -> Result<Self, Self::Error> {
        let host = url
            .host_str()
            .ok_or_else(|| DeviceError::new("missing host"))?;

        let segments: Vec<&str> = url
            .path_segments()
            .ok_or_else(|| DeviceError::new("no path segment"))?
            .collect();

        if segments.is_empty()
            || (segments.len() == 1 && segments[0].is_empty())
        {
            return Err(DeviceError::new("no path segment"));
        }

        if segments.len() > 1 {
            return Err(DeviceError::new("too many path segments"));
        }

        let components: Vec<&str> = segments[0].split(':').collect();

        if components.len() != 2 {
            return Err(DeviceError::new("invalid NQN"));
        }

        let uuid = extract_uuid(components[1]).map_err(|error| {
            DeviceError::from(format!("invalid UUID: {}", error))
        })?;

        let port = url.port().unwrap_or(4420);

        Ok(NvmfAttach::new(
            host.to_string(),
            port,
            uuid,
            segments[0].to_string(),
        ))
    }
}

#[tonic::async_trait]
impl Attach for NvmfAttach {
    async fn parse_parameters(
        &mut self,
        context: &HashMap<String, String>,
    ) -> Result<(), DeviceError> {
        if let Some(val) = context.get("ioTimeout") {
            self.io_timeout = Some(val.parse::<u32>().map_err(|_| {
                DeviceError::new(&format!(
                    "Invalid io_timeout value: \"{}\"",
                    val
                ))
            })?);
        };
        Ok(())
    }

    async fn attach(&self) -> Result<(), DeviceError> {
        // The default reconnect delay in linux kernel is set to 10s. Use the
        // same default value unless the timeout is less or equal to 10.
        let reconnect_delay = match self.io_timeout {
            Some(io_timeout) => {
                if io_timeout <= 10 {
                    Some(1)
                } else {
                    Some(10)
                }
            }
            None => None,
        };
        let ca = ConnectArgsBuilder::default()
            .traddr(&self.host)
            .trsvcid(self.port.to_string())
            .nqn(&self.nqn)
            .ctrl_loss_tmo(self.io_timeout)
            .reconnect_delay(reconnect_delay)
            .build()?;
        match ca.connect() {
            Err(NvmeError::ConnectInProgress) => Ok(()),
            Err(err) => Err(format!("connect failed: {}", err).into()),
            Ok(_) => Ok(()),
        }
    }

    async fn find(&self) -> Result<Option<DeviceName>, DeviceError> {
        self.get_device().map(|device_maybe| match device_maybe {
            Some(device) => device
                .property_value("DEVNAME")
                .map(|path| path.to_str().unwrap().into()),
            None => None,
        })
    }

    async fn fixup(&self) -> Result<(), DeviceError> {
        if let Some(io_timeout) = self.io_timeout {
            let device = self
                .get_device()?
                .ok_or_else(|| DeviceError::new("NVMe device not found"))?;
            let dev_name = device.sysname().to_str().unwrap();
            let major = DEVICE_REGEX
                .captures(dev_name)
                .ok_or_else(|| {
                    DeviceError::new(&format!(
                        "NVMe device \"{}\" does not match \"{}\"",
                        dev_name, *DEVICE_REGEX,
                    ))
                })?
                .get(1)
                .unwrap()
                .as_str();
            let pattern =
                format!("/sys/class/nvme/nvme{}/nvme*n1/queue", major);
            let path = glob(&pattern)
                .unwrap()
                .next()
                .ok_or_else(|| {
                    DeviceError::new(&format!(
                        "failed to look up sysfs device directory \"{}\"",
                        pattern,
                    ))
                })?
                .map_err(|_| {
                    DeviceError::new(&format!(
                        "IO error when reading device directory \"{}\"",
                        pattern
                    ))
                })?;
            // If the timeout was higher than nexus's timeout then IOs could
            // error out earlier than they should. Therefore we should make sure
            // that timeouts in the nexus are set to a very high value.
            debug!(
                "Setting IO timeout on \"{}\" to {}s",
                path.to_string_lossy(),
                io_timeout
            );
            sysfs::write_value(&path, "io_timeout", 1000 * io_timeout)?;
        }
        Ok(())
    }
}

pub(super) struct NvmfDetach {
    name: DeviceName,
    nqn: String,
}

impl NvmfDetach {
    pub(super) fn new(name: DeviceName, nqn: String) -> NvmfDetach {
        NvmfDetach { name, nqn }
    }
}

#[tonic::async_trait]
impl Detach for NvmfDetach {
    async fn detach(&self) -> Result<(), DeviceError> {
        if disconnect(&self.nqn)? == 0 {
            return Err(DeviceError::from(format!(
                "nvmf disconnect {} failed: no device found",
                self.nqn
            )));
        }

        Ok(())
    }

    fn devname(&self) -> DeviceName {
        self.name.clone()
    }
}

/// Set the nvme_core module IO timeout
/// (note, this is a system-wide parameter)
pub(crate) fn set_nvmecore_iotimeout(
    io_timeout_secs: u32,
) -> Result<(), std::io::Error> {
    let path = Path::new("/sys/module/nvme_core/parameters");
    debug!(
        "Setting nvme_core IO timeout on \"{}\" to {}s",
        path.to_string_lossy(),
        io_timeout_secs
    );
    sysfs::write_value(path, "io_timeout", io_timeout_secs)?;
    Ok(())
}
