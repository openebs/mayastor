//! Utility functions for matching a udev record against a known device type.

use udev::Device;

macro_rules! require {
    (let $name:ident = $attribute:expr) => {
        let $name = match $attribute {
            Some(outer) => match outer.to_str() {
                Some(inner) => inner,
                None => {
                    return None;
                }
            },
            None => {
                return None;
            }
        };
    };
    ($value:ident == $attribute:expr) => {
        match $attribute {
            Some(outer) => match outer.to_str() {
                Some(inner) => {
                    if $value != inner {
                        return None;
                    }
                }
                None => {
                    return None;
                }
            },
            None => {
                return None;
            }
        }
    };
    ($value:literal == $attribute:expr) => {
        match $attribute {
            Some(outer) => match outer.to_str() {
                Some(inner) => {
                    if $value != inner {
                        return None;
                    }
                }
                None => {
                    return None;
                }
            },
            None => {
                return None;
            }
        }
    };
}

pub(super) fn match_iscsi_device(device: &Device) -> Option<(&str, &str)> {
    require!("Nexus_CAS_Driver" == device.property_value("ID_MODEL"));
    require!("scsi" == device.property_value("ID_BUS"));

    require!(let devname = device.property_value("DEVNAME"));
    require!(let path = device.property_value("ID_PATH"));

    Some((devname, path))
}

pub(super) fn match_nvmf_device<'a>(
    device: &'a Device,
    key: &str,
) -> Option<&'a str> {
    require!("Mayastor NVMe controller" == device.property_value("ID_MODEL"));
    require!(key == device.property_value("ID_WWN"));

    require!(let devname = device.property_value("DEVNAME"));

    Some(devname)
}
