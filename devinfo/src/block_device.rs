use std::{convert::TryFrom, iter::Iterator};
use udev::Enumerator;
use url::Url;
use uuid::Uuid;

use crate::{
    DevInfoError,
    DevInfoError::{NqnInvalid, Udev},
};

pub(crate) type Failable<T, E = DevInfoError> = std::result::Result<T, E>;

#[derive(Debug)]
pub enum BlkDev {
    Scsi(Uuid),
    Nvmf(Uuid),
    File(String),
}

impl TryFrom<&str> for BlkDev {
    type Error = DevInfoError;
    /// convert a URI in the form of
    ///
    /// scheme:://hostname/first_segment/second/segment
    ///
    /// When parsing the scheme iscsi:// or nvmf:// on the first segment is
    /// considered when dealing with the file:// scheme, we dont do much but concatenate the two segments
    fn try_from(uri: &str) -> Failable<Self> {
        let value = Url::parse(uri).map_err(|e| DevInfoError::ParseError {
            value: e.to_string(),
        })?;

        let mut nq = value.path_segments().ok_or_else(|| {
            DevInfoError::NotSupported {
                value: "The uri contains contains no path segments".to_string(),
            }
        })?;

        // for files we dont have to do much, simply merge the first two
        // elements of the path segment
        if value.scheme() == "file" {
            return Ok(BlkDev::File(format!(
                "{}/{}",
                nq.next().unwrap(),
                nq.next().unwrap()
            )));
        }

        // this is not a file scheme so we should have a nqn:uuid type layout
        // here
        let nq = nq.next().unwrap().split(':').collect::<Vec<_>>();

        if nq.len() != 2 {
            return Err(DevInfoError::NqnInvalid {
                value: "the NQN does not contain the expected separator \':\'"
                    .to_string(),
            });
        }

        let uuid = Uuid::parse_str(nq[1]).map_err(|e| NqnInvalid {
            value: format!("the UUID is invalid {}", e.to_string()),
        })?;

        match value.scheme() {
            "iscsi" => Ok(BlkDev::Scsi(uuid)),
            "nvmf" => Ok(BlkDev::Nvmf(uuid)),
            scheme => Err(DevInfoError::NotSupported {
                value: scheme.to_string(),
            }),
        }
    }
}

impl BlkDev {
    /// lookup the device path for this BlkDev. The approach is rather simply,
    /// based on the URI's path segment, look for properties that will match
    /// the UUID. Right now we try to match only one property, but an array
    /// of properties could be matched on as well.
    pub fn lookup(&self) -> Failable<String> {
        let mut enumerator = Enumerator::new().map_err(|value| Udev {
            value: value.to_string(),
        })?;

        // we are only interested in disks not partitions
        enumerator.match_subsystem("block").unwrap();
        enumerator.match_property("DEVTYPE", "disk").unwrap();

        // look for a specific propertie(s) given the device type
        let (prop, value) = match self {
            BlkDev::Scsi(uuid) => ("SCSI_IDENT_SERIAL", uuid.to_string()),
            BlkDev::Nvmf(uuid) => ("ID_WWN", uuid.to_string()),
            BlkDev::File(uuid) => ("DEVNAME", uuid.to_string()),
        };

        // traverse the device tree and match the value, we stop at first match
        for dev in enumerator.scan_devices().unwrap() {
            if let Some(udev_value) = dev.property_value(prop) {
                if udev_value.to_str().unwrap().contains(&value) {
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
        Err(DevInfoError::NotFound {
            path: value,
        })
    }
}
