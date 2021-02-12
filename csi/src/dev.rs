//! Definition of traits required for attaching and detaching devices.
//! Note that (unfortunately) the attach and detach operations are not
//! quite symmetric. This is because it is not possible to discover all
//! the information contained in a device URI from the entries in udev.
//! Hence there are separate traits for attach and detach that each
//! device type must implement.
//!
//! Attaching a device is performed as follows:
//! ```ignore
//!     let uri = "iscsi://192.168.0.20:3260/iqn.2019-05.io.openebs:nexus-11111111-0000-0000-0000-000000000000/0";
//!     let device = Device::parse(uri)?;
//!     if let Some(path) = device.find().await? {
//!         // device already attached
//!     } else {
//!         // attach the device
//!         device.attach().await?;
//!         // wait for it to show up in udev and obtain the path
//!         let path = Device::wait_for_device(device, timeout, 10).await?;
//!     }
//! ```
//!
//! Detaching a device is performed via:
//! ```ignore
//!     let uuid = Uuid::parse_str(&volume_id)?;
//!     if let Some(device) = Device::lookup(&uuid).await? {
//!         device.detach().await?;
//!     }
//! ```

use std::{collections::HashMap, convert::TryFrom, time::Duration};

use tokio::time::sleep;
use udev::Enumerator;
use url::Url;
use uuid::Uuid;

mod iscsi;
mod nbd;
mod nvmf;
mod util;

const NVME_NQN_PREFIX: &str = "nqn.2019-05.io.openebs";

pub use crate::error::DeviceError;
use crate::match_dev;

pub type DeviceName = String;

#[tonic::async_trait]
pub trait Attach: Sync + Send {
    async fn attach(&self) -> Result<(), DeviceError>;
    async fn find(&self) -> Result<Option<DeviceName>, DeviceError>;
    async fn fixup(
        &self,
        context: &HashMap<String, String>,
    ) -> Result<(), DeviceError>;
}

#[tonic::async_trait]
pub trait Detach: Sync + Send {
    async fn detach(&self) -> Result<(), DeviceError>;
    fn devname(&self) -> DeviceName;
}

pub struct Device;

impl Device {
    /// Main dispatch function for parsing URIs in order
    /// to obtain a device implementing the Attach trait.
    pub fn parse(uri: &str) -> Result<Box<dyn Attach>, DeviceError> {
        let url = Url::parse(uri).map_err(|error| error.to_string())?;
        match url.scheme() {
            "file" => Ok(Box::new(nbd::Nbd::try_from(&url)?)),
            "iscsi" => Ok(Box::new(iscsi::IscsiAttach::try_from(&url)?)),
            "nvmf" => Ok(Box::new(nvmf::NvmfAttach::try_from(&url)?)),
            "nbd" => Ok(Box::new(nbd::Nbd::try_from(&url)?)),
            scheme => Err(DeviceError::from(format!(
                "unsupported device scheme: {}",
                scheme
            ))),
        }
    }

    /// Lookup an existing device in udev matching the given UUID
    /// to obtain a device implementing the Detach trait.
    pub async fn lookup(
        uuid: &Uuid,
    ) -> Result<Option<Box<dyn Detach>>, DeviceError> {
        let nvmf_key: String = format!("uuid.{}", uuid.to_string());

        let mut enumerator = Enumerator::new()?;

        enumerator.match_subsystem("block")?;
        enumerator.match_property("DEVTYPE", "disk")?;

        for device in enumerator.scan_devices()? {
            if let Some((devname, path)) =
                match_dev::match_iscsi_device(&device)
            {
                let value =
                    iscsi::IscsiDetach::from_path(devname.to_string(), path)?;

                if value.uuid() == uuid {
                    return Ok(Some(Box::new(value)));
                }

                continue;
            }

            if let Some(devname) =
                match_dev::match_nvmf_device(&device, &nvmf_key)
            {
                return Ok(Some(Box::new(nvmf::NvmfDetach::new(
                    devname.to_string(),
                    format!("{}:nexus-{}", NVME_NQN_PREFIX, uuid.to_string()),
                ))));
            }
        }

        Ok(None)
    }

    /// Wait for a device to show up in udev
    /// once attach() has been called.
    pub async fn wait_for_device(
        device: &dyn Attach,
        timeout: Duration,
        retries: u32,
    ) -> Result<DeviceName, DeviceError> {
        for _ in 0 ..= retries {
            if let Some(devname) = device.find().await? {
                return Ok(devname);
            }
            sleep(timeout).await;
        }
        Err(DeviceError::new("device attach timeout"))
    }
}
