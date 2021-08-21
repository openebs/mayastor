use std::{collections::HashMap, convert::TryFrom};

use regex::Regex;
use udev::Enumerator;
use url::Url;
use uuid::Uuid;

use crate::{dev::util::extract_uuid, match_dev::match_iscsi_device};

use super::{Attach, Detach, DeviceError, DeviceName};

mod iscsiadm;
use iscsiadm::IscsiAdmin;

pub(super) struct IscsiDevice {
    portal: String,
    iqn: String,
    uuid: Uuid,
    lun: u16,
}

impl IscsiDevice {
    fn new(portal: String, iqn: String, uuid: Uuid, lun: u16) -> IscsiDevice {
        IscsiDevice {
            portal,
            iqn,
            uuid,
            lun,
        }
    }

    fn to_path(&self) -> String {
        format!("ip-{}-iscsi-{}-lun-{}", self.portal, self.iqn, self.lun)
    }

    fn from_path(path: &str) -> Result<IscsiDevice, DeviceError> {
        lazy_static! {
            static ref PATTERN: Regex = Regex::new(r"^ip-(?P<host>[^:]+):(?P<port>[[:digit:]]+)-(?P<scheme>[[:alpha:]]+)-(?P<iqn>[[:alpha:]]+\.[[:digit:]]{4}-[[:digit:]]{2}\.[^:]+:(?P<suffix>[[:alnum:]]+(?:-[[:xdigit:]]+)+))-lun-(?P<lun>[[:digit:]]+)$").unwrap();
        }

        if let Some(captures) = PATTERN.captures(path) {
            let host = captures.name("host").unwrap().as_str();
            let port =
                captures.name("port").unwrap().as_str().parse::<u16>()?;
            let scheme = captures.name("scheme").unwrap().as_str();
            let iqn = captures.name("iqn").unwrap().as_str();
            let suffix = captures.name("suffix").unwrap().as_str();
            let lun = captures.name("lun").unwrap().as_str().parse::<u16>()?;

            let uuid = extract_uuid(suffix)?;

            if scheme == "iscsi" {
                return Ok(IscsiDevice::new(
                    format!("{}:{}", host, port),
                    iqn.to_string(),
                    uuid,
                    lun,
                ));
            }
        }

        Err(DeviceError::from(format!(
            "invalid target specification: {}",
            path
        )))
    }
}

pub(super) type IscsiAttach = IscsiDevice;

impl TryFrom<&Url> for IscsiDevice {
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

        if segments.len() > 2 {
            return Err(DeviceError::new("too many path segments"));
        }

        let lun: u16 = if segments.len() > 1 {
            segments[1].parse()?
        } else {
            0
        };

        let components: Vec<&str> = segments[0].split(':').collect();

        if components.len() != 2 {
            return Err(DeviceError::new("invalid IQN"));
        }

        let uuid = extract_uuid(components[1]).map_err(|error| {
            DeviceError::from(format!("invalid UUID: {}", error))
        })?;

        let portal =
            format!("{}:{}", host.to_string(), url.port().unwrap_or(3260));

        Ok(IscsiDevice::new(portal, segments[0].to_string(), uuid, lun))
    }
}

#[tonic::async_trait]
impl Attach for IscsiAttach {
    async fn parse_parameters(
        &mut self,
        _context: &HashMap<String, String>,
    ) -> Result<(), DeviceError> {
        Ok(())
    }

    async fn attach(&self) -> Result<(), DeviceError> {
        match IscsiAdmin::find_session(&self.portal, &self.iqn) {
            Ok(found) => {
                if found {
                    // session already exists - nothing to do
                    return Ok(());
                }
            }
            Err(error) => {
                return Err(DeviceError::from(format!(
                    "iscsiadm command (session) failed: {}",
                    error
                )));
            }
        }

        if let Err(error) = IscsiAdmin::discover(&self.portal, &self.iqn) {
            return Err(DeviceError::from(format!(
                "iscsiadm command (discovery) failed: {}",
                error
            )));
        }

        if let Err(error) = IscsiAdmin::login(&self.portal, &self.iqn) {
            let _ = IscsiAdmin::delete(&self.portal, &self.iqn);
            return Err(DeviceError::from(format!(
                "iscsiadm command (login) failed: {}",
                error
            )));
        }

        Ok(())
    }

    async fn find(&self) -> Result<Option<DeviceName>, DeviceError> {
        let key: String = self.to_path();

        let mut enumerator = Enumerator::new()?;

        enumerator.match_subsystem("block")?;
        enumerator.match_property("DEVTYPE", "disk")?;

        for device in enumerator.scan_devices()? {
            if let Some((devname, path)) = match_iscsi_device(&device) {
                if path == key {
                    return Ok(Some(devname.to_string()));
                }
            }
        }

        Ok(None)
    }

    async fn fixup(&self) -> Result<(), DeviceError> {
        Ok(())
    }
}

pub(super) struct IscsiDetach {
    name: DeviceName,
    device: IscsiDevice,
}

impl IscsiDetach {
    pub(super) fn from_path(
        name: DeviceName,
        path: &str,
    ) -> Result<IscsiDetach, DeviceError> {
        let device = IscsiDevice::from_path(path)?;
        Ok(IscsiDetach { name, device })
    }

    pub(super) fn uuid(&self) -> &Uuid {
        &self.device.uuid
    }
}

#[tonic::async_trait]
impl Detach for IscsiDetach {
    fn devname(&self) -> DeviceName {
        self.name.clone()
    }

    async fn detach(&self) -> Result<(), DeviceError> {
        if let Err(error) =
            IscsiAdmin::logout(&self.device.portal, &self.device.iqn)
        {
            return Err(DeviceError::from(format!(
                "iscsiadm command (logout) failed: {}",
                error
            )));
        }

        if let Err(error) =
            IscsiAdmin::delete(&self.device.portal, &self.device.iqn)
        {
            return Err(DeviceError::from(format!(
                "iscsiadm command (delete) failed: {}",
                error
            )));
        }

        Ok(())
    }
}
