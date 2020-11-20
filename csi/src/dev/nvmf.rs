use std::convert::TryFrom;

use udev::Enumerator;
use url::Url;
use uuid::Uuid;

use crate::{dev::util::extract_uuid, match_dev::match_nvmf_device};

use super::{Attach, Detach, DeviceError, DeviceName};

pub(super) struct NvmfAttach {
    host: String,
    port: u16,
    uuid: Uuid,
    nqn: String,
}

impl NvmfAttach {
    fn new(host: String, port: u16, uuid: Uuid, nqn: String) -> NvmfAttach {
        NvmfAttach {
            host,
            port,
            uuid,
            nqn,
        }
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
    async fn attach(&self) -> Result<(), DeviceError> {
        if let Err(error) =
            nvmeadm::nvmf_discovery::connect(&self.host, self.port, &self.nqn)
        {
            return match error {
                nvmeadm::error::NvmeError::ConnectInProgress => Ok(()),
                _ => {
                    Err(DeviceError::from(format!("connect failed: {}", error)))
                }
            };
        }

        Ok(())
    }

    async fn find(&self) -> Result<Option<DeviceName>, DeviceError> {
        let key: String = format!("uuid.{}", self.uuid.to_string());

        let mut enumerator = Enumerator::new()?;

        enumerator.match_subsystem("block")?;
        enumerator.match_property("DEVTYPE", "disk")?;

        for device in enumerator.scan_devices()? {
            if let Some(devname) = match_nvmf_device(&device, &key) {
                return Ok(Some(devname.to_string()));
            }
        }

        Ok(None)
    }
}

pub(super) struct NvmfDetach {
    name: DeviceName,
    nqn: String,
}

impl NvmfDetach {
    pub(super) fn new(name: DeviceName, nqn: String) -> NvmfDetach {
        NvmfDetach {
            name,
            nqn,
        }
    }
}

#[tonic::async_trait]
impl Detach for NvmfDetach {
    async fn detach(&self) -> Result<(), DeviceError> {
        if nvmeadm::nvmf_discovery::disconnect(&self.nqn)? == 0 {
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
