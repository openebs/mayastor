//! Pool-level options for the LVM backend.
//!
//! Options are query parameters on the pool's disks entries, written the same
//! way as bdev URIs, for example `<disk>?thinpool=512m&thinpoolchunk=64k`.
//! The query string is stored verbatim as a volume group tag, so listings echo
//! back exactly the disks the pool was created with.

use super::Error;

/// Name of the thin pool logical volume owned by the backend within a volume
/// group. Replica names are uuids, so this cannot collide.
pub(super) const THIN_POOL_LV: &str = "mayastor-thinpool";

/// Parsed pool options for the LVM backend.
#[derive(Debug, Default, Clone, PartialEq)]
pub(crate) struct LvmPoolOpts {
    /// The device paths with any query parameters stripped.
    devices: Vec<String>,
    /// The verbatim query string ("" when no options were given).
    query: String,
    /// Size of the thin pool to create within the volume group.
    thinpool: Option<u64>,
    /// Chunk size for the thin pool.
    chunk: Option<u64>,
}

impl LvmPoolOpts {
    /// Parse the disks entries into device paths and pool options.
    /// Options may only be specified on the first entry.
    pub(crate) fn try_from_disks(disks: &[String]) -> Result<Self, Error> {
        if disks.is_empty() {
            return Err(Error::InvalidOption {
                error: "at least one disk device is required".to_string(),
            });
        }

        let mut opts = Self::default();
        for (index, disk) in disks.iter().enumerate() {
            let (device, query) = match disk.split_once('?') {
                None => (disk.as_str(), ""),
                Some((device, query)) => (device, query),
            };
            if device.is_empty() {
                return Err(Error::InvalidOption {
                    error: format!("'{disk}' has an empty device path"),
                });
            }
            if !query.is_empty() && index != 0 {
                return Err(Error::InvalidOption {
                    error: "pool options are only accepted on the first disks entry".to_string(),
                });
            }
            opts.devices.push(device.to_string());
            if index == 0 {
                opts.query = query.to_string();
            }
        }

        opts.parse_query()?;
        Ok(opts)
    }

    fn parse_query(&mut self) -> Result<(), Error> {
        let query = self.query.clone();
        for pair in query.split('&').filter(|p| !p.is_empty()) {
            let Some((key, value)) = pair.split_once('=') else {
                return Err(Error::InvalidOption {
                    error: format!("'{pair}' is not a key=value pair"),
                });
            };
            match key {
                "thinpool" => {
                    self.thinpool = Some(parse_size(value)?);
                }
                "thinpoolchunk" => {
                    self.chunk = Some(parse_size(value)?);
                }
                key => {
                    return Err(Error::InvalidOption {
                        error: format!("'{key}' is not a supported LVM pool option"),
                    });
                }
            }
        }
        if self.thinpool.is_none() && self.chunk.is_some() {
            return Err(Error::InvalidOption {
                error: "'thinpoolchunk' requires 'thinpool'".to_string(),
            });
        }
        Ok(())
    }

    /// Rebuild the options from the query string in the volume group tag.
    pub(super) fn try_from_query(devices: Vec<String>, query: &str) -> Result<Self, Error> {
        let mut opts = Self {
            devices,
            query: query.to_string(),
            ..Default::default()
        };
        opts.parse_query()?;
        Ok(opts)
    }

    /// The device paths without query parameters.
    pub(super) fn devices(&self) -> &Vec<String> {
        &self.devices
    }
    /// The verbatim query string.
    pub(super) fn query(&self) -> &str {
        &self.query
    }
    /// The thin pool size, if a thin pool was requested.
    pub(super) fn thinpool(&self) -> Option<u64> {
        self.thinpool
    }
    /// The thin pool chunk size.
    pub(super) fn chunk(&self) -> Option<u64> {
        self.chunk
    }
    /// The disks entries as the user gave them, with the query string back on
    /// the first entry.
    pub(super) fn disks(&self) -> Vec<String> {
        let mut disks = self.devices.clone();
        if let Some(first) = disks.first_mut() {
            if !self.query.is_empty() {
                *first = format!("{first}?{}", self.query);
            }
        }
        disks
    }
}

/// Parse a size with an optional binary suffix, for example 512, 64k or 2GiB.
pub(super) fn parse_size(value: &str) -> Result<u64, Error> {
    let split = value
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(value.len());
    let (digits, suffix) = value.split_at(split);
    if digits.is_empty() {
        return Err(Error::InvalidOption {
            error: format!("'{value}' is not a size"),
        });
    }
    let number = digits.parse::<u64>().map_err(|_| Error::InvalidOption {
        error: format!("'{value}' is not a size"),
    })?;
    let suffix = suffix.to_ascii_lowercase();
    let suffix = suffix.trim_end_matches("ib").trim_end_matches('b');
    let multiplier: u64 = match suffix {
        "" => 1,
        "k" => 1 << 10,
        "m" => 1 << 20,
        "g" => 1 << 30,
        "t" => 1 << 40,
        _ => {
            return Err(Error::InvalidOption {
                error: format!("'{value}' has an unknown size suffix"),
            })
        }
    };
    number
        .checked_mul(multiplier)
        .ok_or_else(|| Error::InvalidOption {
            error: format!("'{value}' overflows"),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn disks(entries: &[&str]) -> Vec<String> {
        entries.iter().map(|e| e.to_string()).collect()
    }

    #[test]
    fn pool_opts_parse_matrix() {
        let opts = LvmPoolOpts::try_from_disks(&disks(&["/dev/sda"])).unwrap();
        assert_eq!(opts.devices(), &vec!["/dev/sda".to_string()]);
        assert_eq!(opts.thinpool(), None);
        assert_eq!(opts.disks(), vec!["/dev/sda".to_string()]);

        let opts =
            LvmPoolOpts::try_from_disks(&disks(&["/dev/sda?thinpool=512m&thinpoolchunk=64k"]))
                .unwrap();
        assert_eq!(opts.thinpool(), Some(512 * 1024 * 1024));
        assert_eq!(opts.chunk(), Some(64 * 1024));
        assert_eq!(opts.query(), "thinpool=512m&thinpoolchunk=64k");
        assert_eq!(
            opts.disks(),
            vec!["/dev/sda?thinpool=512m&thinpoolchunk=64k".to_string()]
        );

        let opts =
            LvmPoolOpts::try_from_disks(&disks(&["/dev/sda?thinpool=1g", "/dev/sdb"])).unwrap();
        assert_eq!(opts.devices().len(), 2);
        assert_eq!(opts.thinpool(), Some(1 << 30));

        assert!(LvmPoolOpts::try_from_disks(&disks(&[])).is_err());
        assert!(LvmPoolOpts::try_from_disks(&disks(&["?thinpool=1g"])).is_err());
        assert!(LvmPoolOpts::try_from_disks(&disks(&["/dev/sda?bogus=1"])).is_err());
        assert!(LvmPoolOpts::try_from_disks(&disks(&["/dev/sda?thinpool"])).is_err());
        assert!(LvmPoolOpts::try_from_disks(&disks(&["/dev/sda?thinpoolchunk=64k"])).is_err());
        assert!(
            LvmPoolOpts::try_from_disks(&disks(&["/dev/sda", "/dev/sdb?thinpool=1g"])).is_err()
        );
    }

    #[test]
    fn query_round_trip() {
        let entries = disks(&["/dev/sda?thinpool=512m", "/dev/sdb"]);
        let opts = LvmPoolOpts::try_from_disks(&entries).unwrap();
        let rebuilt = LvmPoolOpts::try_from_query(opts.devices().clone(), opts.query()).unwrap();
        assert_eq!(opts, rebuilt);
        assert_eq!(rebuilt.disks(), entries);
    }

    #[test]
    fn size_parsing() {
        assert_eq!(parse_size("512").unwrap(), 512);
        assert_eq!(parse_size("64k").unwrap(), 64 * 1024);
        assert_eq!(parse_size("64K").unwrap(), 64 * 1024);
        assert_eq!(parse_size("64KiB").unwrap(), 64 * 1024);
        assert_eq!(parse_size("512MB").unwrap(), 512 * 1024 * 1024);
        assert_eq!(parse_size("2g").unwrap(), 2 << 30);
        assert!(parse_size("").is_err());
        assert!(parse_size("k").is_err());
        assert!(parse_size("1.5g").is_err());
        assert!(parse_size("16x").is_err());
        assert!(parse_size("99999999999t").is_err());
    }
}
