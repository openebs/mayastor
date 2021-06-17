use crate::error::DeviceError;
use serde_json::Value;
use std::{collections::HashMap, process::Command, string::String, vec::Vec};

// Keys of interest we expect to find in the JSON output generated
/// by findmnt.
const TARGET_KEY: &str = "target";
const SOURCE_KEY: &str = "source";
const FSTYPE_KEY: &str = "fstype";

#[derive(Debug)]
pub struct DeviceMount {
    pub mount_path: String,
    pub fstype: String,
}

#[derive(Debug)]
struct Filter<'a> {
    key: &'a str,
    value: &'a str,
}

/// Convert a json value of a key-value pair, to a string,
/// adjusted if required, on the key.
///
/// The source field returned from findmnt can be different for
/// the same source on different systems, for example
///   dev[/nvme0n1], udev[/nvme0n1], tmpfs[/nvme0n1], devtmpfs[/nvme0n1]
/// this function converts those values to the expected /dev/nvme0n1
fn key_adjusted_value(key: &str, value: &Value) -> String {
    lazy_static! {
        static ref RE_UDEVPATH: regex::Regex = regex::Regex::new(
            r"(?x).*\[(?P<device>/.*)\]
        ",
        )
        .unwrap();
    }

    // Do NOT do
    //    let strvalue = value.to_string();
    // that will return a string delimited with quote characters.
    let strvalue: String = match value {
        Value::String(str) => str.to_string(),
        _ => value.to_string(),
    };
    if key == SOURCE_KEY {
        if let Some(caps) = RE_UDEVPATH.captures(&strvalue) {
            return format!("/dev{}", &caps["device"]);
        };
    }
    strvalue
}

const KEYS: &[&str] = &[TARGET_KEY, SOURCE_KEY, FSTYPE_KEY];

/// Convert the json map entry to a hashmap of strings
/// with source, target and fstype key-value pairs only.
/// The source field returned from findmnt is converted
/// to the /dev/xxx form if required.
fn jsonmap_to_hashmap(
    json_map: &serde_json::Map<String, Value>,
) -> HashMap<String, String> {
    let mut hmap: HashMap<String, String> = HashMap::new();
    for (key, value) in json_map {
        if KEYS.contains(&key.as_str()) {
            hmap.insert(key.clone(), key_adjusted_value(key, value));
        }
    }
    hmap
}

/// This function recurses over the de-serialised JSON returned by findmnt,
/// finding entries which have key-value pair's matching the
/// filter key-value pair, and populates a vector with those
/// entries as hashmaps of strings.
///
/// For Mayastor usage the assumptions made on the structure are:
///  1. An object has keys named "target" and "source" for a mount point.
///  2. An object may contain nested arrays of objects.
///
/// The search is deliberately generic (and hence slower) in an attempt to
/// be more robust to future changes in findmnt.
fn filter_findmnt(
    json_val: &serde_json::value::Value,
    filter: &Filter,
    results: &mut Vec<HashMap<String, String>>,
) {
    match json_val {
        Value::Array(json_array) => {
            for jsonvalue in json_array {
                filter_findmnt(jsonvalue, filter, results);
            }
        }
        Value::Object(json_map) => {
            if let Some(value) = json_map.get(filter.key) {
                if filter.value == value
                    || filter.value == key_adjusted_value(filter.key, value)
                {
                    results.push(jsonmap_to_hashmap(json_map));
                }
            }
            // If the object has arrays, then the assumption is that they are
            // arrays of objects.
            for (_, jsonvalue) in json_map {
                if jsonvalue.is_array() {
                    filter_findmnt(jsonvalue, filter, results);
                }
            }
        }
        jvalue => {
            warn!("Unexpected json type {}", jvalue);
        }
    };
}

/// findmnt executable name.
const FIND_MNT: &str = "findmnt";
/// findmnt arguments, we only want source, target and filesystem type fields.
const FIND_MNT_ARGS: [&str; 3] = ["-J", "-o", "SOURCE,TARGET,FSTYPE"];

/// Execute the Linux utility findmnt, collect the json output,
/// invoke the filter function and return the filtered results.
fn findmnt(
    params: Filter,
) -> Result<Vec<HashMap<String, String>>, DeviceError> {
    let output = Command::new(FIND_MNT).args(&FIND_MNT_ARGS).output()?;
    if output.status.success() {
        let json_str = String::from_utf8(output.stdout)?;
        let json: Value = serde_json::from_str(&json_str)?;
        let mut results: Vec<HashMap<String, String>> = Vec::new();
        filter_findmnt(&json, &params, &mut results);
        Ok(results)
    } else {
        Err(DeviceError {
            message: String::from_utf8(output.stderr)?,
        })
    }
}

/// Use the Linux utility findmnt to find the name of the device mounted at a
/// directory or block special file, if any.
/// mount_path is the path a device is mounted on.
pub(crate) fn get_devicepath(
    mount_path: &str,
) -> Result<Option<String>, DeviceError> {
    let tgt_filter = Filter {
        key: TARGET_KEY,
        value: mount_path,
    };
    let sources = findmnt(tgt_filter)?;
    {
        match sources.len() {
            0 => Ok(None),
            1 => {
                if let Some(devicepath) = sources[0].get(SOURCE_KEY) {
                    Ok(Some(devicepath.to_string()))
                } else {
                    Err(DeviceError {
                        message: "missing source field".to_string(),
                    })
                }
            }
            _ => {
                // should be impossible ...
                warn!(
                    "multiple sources mounted on target {:?}->{}",
                    sources, mount_path
                );
                Err(DeviceError {
                    message: format!(
                        "multiple devices mounted at {}",
                        mount_path
                    ),
                })
            }
        }
    }
}

/// Use the Linux utility findmnt to find the mount paths for a block device,
/// if any.
/// device_path is the path to the device for example "/dev/sda1"
pub(crate) fn get_mountpaths(
    device_path: &str,
) -> Result<Vec<DeviceMount>, DeviceError> {
    let dev_filter = Filter {
        key: SOURCE_KEY,
        value: device_path,
    };
    match findmnt(dev_filter) {
        Ok(results) => {
            let mut mountpaths: Vec<DeviceMount> = Vec::new();
            for entry in results {
                if let Some(mountpath) = entry.get(TARGET_KEY) {
                    if let Some(fstype) = entry.get(FSTYPE_KEY) {
                        mountpaths.push(DeviceMount {
                            mount_path: mountpath.to_string(),
                            fstype: fstype.to_string(),
                        })
                    } else {
                        error!("Missing fstype for {}", mountpath);
                        mountpaths.push(DeviceMount {
                            mount_path: mountpath.to_string(),
                            fstype: "unspecified".to_string(),
                        })
                    }
                } else {
                    warn!("missing target field {:?}", entry);
                }
            }
            Ok(mountpaths)
        }
        Err(e) => Err(e),
    }
}
