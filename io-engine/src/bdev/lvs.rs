//! Allows creation of an lvs lvol through a URI specification rather than gRPC
//! or direct function call. This is not intended for the usual product
//! operation but for testing and benchmarking.
//!
//! # Uri
//! lvol:///$name?size=$size&lvs=$lvs
//!
//! # Parameters
//! name: A name for the lvol, example: "lvol-1"
//! size: A size specified using units, example: 100GiB
//! lvs: lvs:///$name?mode=$mode&disk=$disk
//!      name: A name for the lvs, example: "lvs-1"
//!      mode:
//!         create: create new lvs pool
//!         import: import existing lvs pool
//!         create_import: import existing or create new lvs pool
//!         purge: destroy and create new lvs pool
//!      disk: The disk uri for the lvs, example: "aio:///dev/sda"

use std::{
    collections::HashMap,
    convert::TryFrom,
    fmt::{Debug, Formatter},
};

use async_trait::async_trait;
use url::Url;

use crate::{
    bdev::{dev::reject_unknown_parameters, util::uri, CreateDestroy, GetName},
    bdev_api::BdevError,
    core::LogicalVolume,
    lvs::LvsLvol,
    pool_backend::{PoolArgs, PoolBackend},
};

/// An lvol specified via URI.
pub(super) struct Lvol {
    /// Name of the lvol.
    name: String,
    /// The size of the lvol in bytes.
    size: u64,
    /// The lvs specification.
    lvs: Lvs,
}
struct Lvs {
    /// Name of the lvs.
    name: String,
    /// The backing bdev disk uri.
    disk: String,
    /// The lvs creation mode.
    mode: LvsMode,
}

impl Debug for Lvol {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Lvol '{}' {} B <== {:?}", self.name, self.size, self.lvs)
    }
}
impl Debug for Lvs {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Lvs '{}' <== {}", self.name, self.disk)
    }
}

impl TryFrom<&Url> for Lvol {
    type Error = BdevError;

    fn try_from(uri: &Url) -> Result<Self, Self::Error> {
        let segments = uri::segments(uri);
        if segments.is_empty() {
            return Err(BdevError::InvalidUri {
                uri: uri.to_string(),
                message: "empty path".to_string(),
            });
        }

        let mut parameters: HashMap<String, String> =
            uri.query_pairs().into_owned().collect();

        let size = parameters
            .remove("size")
            .ok_or(BdevError::InvalidUri {
                uri: uri.to_string(),
                message: "'size' is not specified".to_string(),
            })
            .and_then(|size| {
                byte_unit::Byte::from_str(size).map_err(|error| {
                    BdevError::InvalidUri {
                        uri: uri.to_string(),
                        message: format!("'size' is invalid: {error}"),
                    }
                })
            })?
            .get_bytes() as u64;

        let lvs = parameters
            .remove("lvs")
            .ok_or(BdevError::InvalidUri {
                uri: uri.to_string(),
                message: "'lvs' must be specified".to_string(),
            })
            .and_then(|lvs| {
                let disk = parameters.remove("disk").unwrap_or_default();
                Lvs::try_from(format!("{lvs}&disk={disk}"))
            })?;

        reject_unknown_parameters(uri, parameters)?;

        Ok(Self {
            name: uri.path()[1 ..].into(),
            size,
            lvs,
        })
    }
}

impl TryFrom<String> for Lvs {
    type Error = BdevError;

    fn try_from(uri: String) -> Result<Self, Self::Error> {
        let uri =
            Url::parse(&uri).map_err(|source| BdevError::UriParseFailed {
                uri,
                source,
            })?;
        let segments = uri::segments(&uri);
        if segments.is_empty() {
            return Err(BdevError::InvalidUri {
                uri: uri.to_string(),
                message: "empty path".to_string(),
            });
        }

        let mut parameters: HashMap<String, String> =
            uri.query_pairs().into_owned().collect();

        let disk = parameters.remove("disk").ok_or(BdevError::InvalidUri {
            uri: uri.to_string(),
            message: "'disk' must be specified".to_string(),
        })?;

        let mode = parameters
            .remove("mode")
            .ok_or(BdevError::InvalidUri {
                uri: uri.to_string(),
                message: "'mode' must be specified".to_string(),
            })
            .map(LvsMode::from)?;

        Ok(Lvs {
            name: uri.path()[1 ..].into(),
            disk,
            mode,
        })
    }
}

/// Lvs Creation Mode.
enum LvsMode {
    /// Create fresh pool on blank bdev.
    Create,
    /// Import pool from existing bdev.
    Import,
    /// Create or Import.
    CreateOrImport,
    /// Destroy any existing pool from bdev and Create new.
    Purge,
}
impl From<String> for LvsMode {
    fn from(value: String) -> Self {
        match value.as_str() {
            "create" => Self::Create,
            "import" => Self::Import,
            "create_import" => Self::CreateOrImport,
            "purge" => Self::Purge,
            _ => Self::Import,
        }
    }
}

impl GetName for Lvol {
    fn get_name(&self) -> String {
        self.name.clone()
    }
}

#[async_trait(?Send)]
impl CreateDestroy for Lvol {
    type Error = BdevError;

    async fn create(&self) -> Result<String, Self::Error> {
        let lvs = self.lvs.create().await?;
        self.lvs.destroy_lvol(&self.name).await.ok();
        lvs.create_lvol(&self.name, self.size, None, false, None)
            .await
            .map_err(|error| BdevError::CreateBdevFailedStr {
                error: error.to_string(),
                name: self.name.to_owned(),
            })?;
        Ok(self.name.to_owned())
    }

    async fn destroy(self: Box<Self>) -> Result<(), Self::Error> {
        debug!("{:?}: deleting", self);
        self.lvs.destroy_lvol(&self.name).await?;
        self.lvs.destroy().await
    }
}

impl Lvs {
    async fn create(&self) -> Result<crate::lvs::Lvs, BdevError> {
        let args = PoolArgs {
            name: self.name.to_owned(),
            disks: vec![self.disk.to_owned()],
            uuid: None,
            cluster_size: None,
            md_args: None,
            backend: PoolBackend::Lvs,
        };
        match &self.mode {
            LvsMode::Create => {
                match crate::lvs::Lvs::import_from_args(args.clone()).await {
                    Err(crate::lvs::LvsError::Import {
                        ..
                    }) => crate::lvs::Lvs::create_or_import(args).await,
                    _ => {
                        return Err(BdevError::BdevExists {
                            name: self.name.to_owned(),
                        })
                    }
                }
            }
            LvsMode::Import => crate::lvs::Lvs::import_from_args(args).await,
            LvsMode::CreateOrImport => {
                crate::lvs::Lvs::create_or_import(args).await
            }
            LvsMode::Purge => {
                Self::wipe_super(args.clone()).await?;
                crate::lvs::Lvs::create_or_import(args).await
            }
        }
        .map_err(|error| BdevError::CreateBdevFailedStr {
            error: error.to_string(),
            name: self.name.to_owned(),
        })
    }

    async fn wipe_super(args: PoolArgs) -> Result<(), BdevError> {
        let disk =
            crate::lvs::Lvs::parse_disk(args.disks.clone()).map_err(|_| {
                BdevError::InvalidUri {
                    uri: String::new(),
                    message: String::new(),
                }
            })?;

        let parsed = super::uri::parse(&disk)?;
        let bdev_str = parsed.create().await?;
        {
            let bdev =
                crate::core::Bdev::get_by_name(&bdev_str).map_err(|_| {
                    BdevError::BdevNotFound {
                        name: bdev_str,
                    }
                })?;

            let hdl = crate::core::Bdev::open(&bdev, true)
                .and_then(|desc| desc.into_handle())
                .map_err(|_| BdevError::BdevNotFound {
                    name: bdev.name().into(),
                })?;

            let mut wiper = crate::core::wiper::Wiper::new(
                hdl,
                crate::core::wiper::WipeMethod::WriteZeroes,
            )
            .map_err(|_| BdevError::WipeFailed {})?;
            wiper
                .wipe(0, 8 * 1024 * 1024)
                .await
                .map_err(|_| BdevError::WipeFailed {})?;
        }
        // We can't destroy the device here as this causes issues with the next
        // section. Seems the deletion of nvme device is not sync as
        // bdev_nvme_delete implies..
        // todo: ensure nvme delete does what it says..
        // parsed.destroy().await.unwrap();
        Ok(())
    }

    async fn destroy(&self) -> Result<(), BdevError> {
        debug!("{self:?}: deleting");
        let Some(lvs) = crate::lvs::Lvs::lookup(&self.name) else {
            return Err(BdevError::BdevNotFound {
                name: self.name.to_owned(),
            });
        };
        lvs.destroy()
            .await
            .map_err(|error| BdevError::DestroyBdevFailedStr {
                error: error.to_string(),
                name: self.name.to_owned(),
            })
    }

    async fn destroy_lvol(&self, name: &str) -> Result<(), BdevError> {
        let Some(lvs) = crate::lvs::Lvs::lookup(&self.name) else {
            return Err(BdevError::BdevNotFound {
                name: self.name.to_owned(),
            });
        };
        let Some(lvols) = lvs.lvols() else {
            return Ok(());
        };
        let Some(lvol) = lvols.into_iter().find(|l| l.name() == name) else {
            return Ok(());
        };
        lvol.destroy().await.map(|_| ()).map_err(|error| {
            BdevError::DestroyBdevFailedStr {
                error: error.to_string(),
                name: self.name.to_owned(),
            }
        })
    }
}
