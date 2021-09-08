//! Logical Volume Manager (LVM) is a device mapper framework that provides
//! logical volume management for the Linux kernel.
//!  - PV (Physical Volume) is any block device that is configured to be used by
//!    lvm i.e. formatted withthe lvm2_member filesystem. Commands available
//!       - pvcreate -> to create a physical volume out of any block device
//!       - pvchange -> to make any change like adding tags
//!       - pvs -> to list the physical volumes with their attributes
//!       - pvremove -> to delete a PV which removes the lvm specific filesystem
//!         from the block device
//!  - VG (Volume Group) is a collection of PVs that is used as a store to
//!    provision volumes. Commands available
//!       - vgcreate -> to create a volume group with a specific name and
//!         mentioned physical volumes
//!       - vgchange -> to make any change like adding tags, activate/deactivate
//!         volume group
//!       - vgs -> to list the VGs with their attributes
//!       - vgremove -> removes the volume group
//!  - LV (Logical Volume) is a block device carved out of VG. Commands
//!    available
//!       - lvcreate -> to create a logical volume with a specific name on
//!         mentioned volume group
//!       - lvchange -> to make any change like adding tags, activate/deactivate
//!         logical volume
//!       - lvs -> to list the logical volumes with their attributes
//!       - lvremove -> removes the logical volume
use crate::lvm::error::Error;
use serde::de::{self, Deserialize, Deserializer};
use std::{
    fmt::Display,
    io::{Error as ioError, ErrorKind},
    str::FromStr,
};
use tokio::process::Command;

use rpc::mayastor::CreatePoolRequest;

const PVS_COMMAND: &str = "pvs";
const VGCHANGE_COMMAND: &str = "vgchange";
const VGS_COMMAND: &str = "vgs";
pub const MAYASTOR_TAG: &str = "mayastor";
pub const MAYASTOR_LABEL: &str = "@mayastor";

pub fn deserialize_number_from_string<'de, T, D>(
    deserializer: D,
) -> Result<T, D::Error>
where
    T: FromStr,
    T::Err: Display,
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    T::from_str(&s).map_err(de::Error::custom)
}

/// used to decode the json output for vgs command to get
/// the capacity and free size of a given vol group
/// sudo vgs --options=vg_size,vg_free --units=b --nosuffix --reportformat=json
///   {
///       "report": [
///           {
///               "vg": [
///                   {"vg_name": "pool", "vg_size":"15372124160",
/// "vg_free":"15372124160"}                ]
///           }
///       ]
///   }
#[derive(Debug, Serialize, Deserialize)]
struct VolGroupList {
    report: Vec<VolGroups>,
}
#[derive(Debug, Serialize, Deserialize)]
struct VolGroups {
    /// corresponds to the vg field in json output
    vg: Vec<VolGroup>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolGroup {
    /// corresponds to the vg_name field in json output, the name of the
    /// vol group
    vg_name: String,
    /// corresponds to the vg_size field in json output, the total capacity of
    /// vol group in bytes
    #[serde(deserialize_with = "deserialize_number_from_string")]
    vg_size: u64,
    /// corresponds to the vg_free field in json output, the free space on
    /// vol group in bytes
    #[serde(deserialize_with = "deserialize_number_from_string")]
    vg_free: u64,
    /// the physical vol disks used by the vol group
    #[serde(skip_deserializing)]
    disks: Vec<String>,
}

/// used to decode the json output for pvs command to get
/// the all the physical vols and its corresponding vol group
/// sudo pvs --options=vg_name,pv_name  --reportformat=json
///   {
///       "report": [
///           {
///               "pv": [
///                   {"vg_name":"pool", "pv_name":"/dev/sdb"}
///               ]
///           }
///       ]
///   }
#[derive(Debug, Serialize, Deserialize)]
struct PhysicalVolsReport {
    /// corresponds to the report field in json output
    report: Vec<PhysicalVol>,
}
#[derive(Debug, Serialize, Deserialize)]
struct PhysicalVol {
    /// corresponds to the pv field in json output
    pv: Vec<VolGroupPhysicalVolMap>,
}
#[derive(Debug, Serialize, Deserialize)]
struct VolGroupPhysicalVolMap {
    /// corresponds to the vg_name field in json output
    vg_name: String,
    /// corresponds to the pv_name field in json output
    pv_name: String,
}

impl VolGroup {
    /// lookup a vol group by its name
    pub async fn lookup_by_name(name: &str, label: &str) -> Option<Self> {
        Self::list(label)
            .await
            .ok()?
            .iter()
            .find(|p| p.vg_name == name)
            .cloned()
    }

    /// check if the given disk is already in use by some other vol group
    /// and returns the vol group which is using it
    pub async fn lookup_by_disk(name: &str) -> Option<VolGroup> {
        Self::list("")
            .await
            .ok()?
            .iter()
            .find(|p| p.disks.iter().any(|disk| disk.as_str() == name))
            .cloned()
    }

    /// list all the vol group having the specified label tag
    pub async fn list(label: &str) -> Result<Vec<VolGroup>, Error> {
        let mut args = vec![
            "--units=b",
            "--nosuffix",
            "--options=vg_name,vg_size,vg_free",
            "--reportformat=json",
        ];
        if !label.is_empty() {
            args.push(label);
        }

        let output = Command::new(VGS_COMMAND)
            .args(args.as_slice())
            .output()
            .await?;

        if !output.status.success() {
            let msg = String::from_utf8(output.stderr).map_or_else(
                |e: std::string::FromUtf8Error| {
                    format!("failed to parse stderr for vgs: {}", e.to_string())
                },
                |s| s,
            );
            return Err(Error::FailedExec {
                err: msg,
            });
        }

        let json_result: VolGroupList =
            serde_json::from_slice(output.stdout.as_slice()).map_err(|e| {
                Error::FailedParsing {
                    err: e.to_string(),
                }
            })?;

        let mut pools = json_result.report[0].vg.clone();
        for p in &mut pools {
            p.disks = p.clone().get_disks().await?
        }
        Ok(pools)
    }

    /// import a vol group with the name provided or create one with the name
    /// and disks provided currently only import is supported
    pub async fn import_or_create(
        req: CreatePoolRequest,
    ) -> Result<VolGroup, Error> {
        let pool = Self::import(req.name.as_str()).await?;
        info!("The lvm pool '{}' has been created.", pool.name(),);
        Ok(pool)
    }

    /// import a vol group by its name, match the disks on the vol group
    /// and if true add the tag mayastor to the vol group to make it available
    /// as a Pool.
    pub async fn import(name: &str) -> Result<VolGroup, Error> {
        if let Some(pool) = Self::lookup_by_name(name, "").await {
            let output = Command::new(VGCHANGE_COMMAND)
                .arg(name)
                .arg("--addtag=mayastor")
                .output()
                .await?;
            if !output.status.success() {
                let msg = String::from_utf8(output.stderr).map_or_else(
                    |e: std::string::FromUtf8Error| {
                        format!(
                            "failed to parse stderr for vg_change: {}",
                            e.to_string()
                        )
                    },
                    |s| s,
                );
                return Err(Error::FailedExec {
                    err: msg,
                });
            }

            Ok(pool)
        } else {
            Err(Error::Io {
                err: ioError::new(
                    ErrorKind::NotFound,
                    format!("vol group {} not found", name),
                ),
            })
        }
    }

    /// fetch the physical vols for the vol group
    pub async fn get_disks(self) -> Result<Vec<String>, Error> {
        let output = Command::new(PVS_COMMAND)
            .args(&["--options=vg_name,pv_name", "--reportformat=json"])
            .output()
            .await?;
        if !output.status.success() {
            let msg = String::from_utf8(output.stderr).map_or_else(
                |e: std::string::FromUtf8Error| {
                    format!("failed to parse stderr for pvs: {}", e.to_string())
                },
                |s| s,
            );
            return Err(Error::FailedExec {
                err: msg,
            });
        }

        let json_output: PhysicalVolsReport =
            serde_json::from_slice(output.stdout.as_slice()).map_err(|e| {
                Error::FailedParsing {
                    err: e.to_string(),
                }
            })?;

        let mut disks: Vec<String> = vec![];

        for p in json_output
            .report
            .get(0)
            .ok_or(Error::FailedParsing {
                err: "failed to get pvs report".to_string(),
            })?
            .pv
            .as_slice()
        {
            if p.vg_name == self.vg_name {
                disks.push(p.pv_name.as_str().to_string())
            }
        }
        Ok(disks)
    }

    /// return the name of the current vol group
    pub fn name(&self) -> &str {
        self.vg_name.as_str()
    }

    /// return the disks of the current vol group
    pub fn disks(&self) -> Vec<String> {
        self.disks.clone()
    }

    /// returns the total capacity of the vol group
    pub fn capacity(&self) -> u64 {
        self.vg_size
    }

    /// returns the available capacity
    pub fn available(&self) -> u64 {
        self.vg_free
    }

    /// returns the used capacity
    pub fn used(&self) -> u64 {
        self.capacity() - self.available()
    }

    /// delete a given vol group and its corresponding physical vols
    pub async fn destroy(self) -> Result<(), Error> {
        // As currently only import of vol group is supported
        // exporting the vol group on destroy.
        self.clone().export().await?;
        info!("pool '{}' has been destroyed successfully", self.name());
        Ok(())
    }

    /// exports a given vol group by removing the mayastor tag
    pub async fn export(self) -> Result<(), Error> {
        let output = Command::new(VGCHANGE_COMMAND)
            .arg(self.name())
            .arg("--deltag=mayastor")
            .output()
            .await?;
        if !output.status.success() {
            let msg = String::from_utf8(output.stderr).map_or_else(
                |e: std::string::FromUtf8Error| {
                    format!(
                        "failed to parse stderr for vg_change: {}",
                        e.to_string()
                    )
                },
                |s| s,
            );
            return Err(Error::FailedExec {
                err: msg,
            });
        }
        Ok(())
    }
}
