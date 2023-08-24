//! Allows creation of a nexus through a URI specification rather than gRPC or
//! direct function call. This is not intended for the usual product operation
//! but for testing and benchmarking.
//!
//! # Uri
//! nexus:///$name?size=$size&children=$children
//!
//! # Parameters
//! name: A name for the nexus, example: "nexus-1"
//! size: A size specified using units, example: 100GiB
//! children: A comma-separated list of children URI's, example: aio:///dev/sda
//!
//! # Examples
//! Single child:
//! nexus:///nx1?size=240GiB&children=aio:///dev/sda
//! Multiple children:
//! nexus:///nx1?size=240GiB&children=aio:///dev/sda,aio:///dev/sdc

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
};

/// A nexus specified via URI.
pub struct Nexus {
    /// Name of the nexus we created, this is equal to the URI path minus
    /// the leading '/'.
    name: String,
    /// The size of the nexus in bytes.
    size: u64,
    /// The children of the nexus.
    children: Vec<String>,
}

impl Debug for Nexus {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Nexus '{}' {} B <= {:?}",
            self.name, self.size, self.children
        )
    }
}

impl TryFrom<&Url> for Nexus {
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

        let size: u64 = if let Some(value) = parameters.remove("size") {
            byte_unit::Byte::from_str(value)
                .map_err(|error| BdevError::InvalidUri {
                    uri: uri.to_string(),
                    message: format!("'size' is invalid: {error}"),
                })?
                .get_bytes() as u64
        } else {
            return Err(BdevError::InvalidUri {
                uri: uri.to_string(),
                message: "'size' is not specified".to_string(),
            });
        };

        let children: Vec<String> =
            if let Some(value) = parameters.remove("children") {
                value.split(',').map(|s| s.to_string()).collect::<Vec<_>>()
            } else {
                return Err(BdevError::InvalidUri {
                    uri: uri.to_string(),
                    message: "'children' must be specified".to_string(),
                });
            };

        reject_unknown_parameters(uri, parameters)?;

        Ok(Self {
            name: uri.path()[1 ..].into(),
            size,
            children,
        })
    }
}

impl GetName for Nexus {
    fn get_name(&self) -> String {
        self.name.clone()
    }
}

#[async_trait(?Send)]
impl CreateDestroy for Nexus {
    type Error = BdevError;

    async fn create(&self) -> Result<String, Self::Error> {
        crate::bdev::nexus::nexus_create(
            &self.name,
            self.size,
            None,
            &self.children,
        )
        .await
        .map_err(|error| BdevError::CreateBdevFailedStr {
            error: error.to_string(),
            name: self.name.to_owned(),
        })?;

        Ok(self.name.to_owned())
    }

    async fn destroy(self: Box<Self>) -> Result<(), Self::Error> {
        debug!("{:?}: deleting", self);
        let Some(nexus) = crate::bdev::nexus::nexus_lookup_mut(&self.name) else {
            return Err(BdevError::BdevNotFound { name: self.name.to_owned() });
        };
        nexus
            .destroy()
            .await
            .map_err(|error| BdevError::DestroyBdevFailedStr {
                error: error.to_string(),
                name: self.name.to_owned(),
            })
    }
}
