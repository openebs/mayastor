use std::{collections::HashMap, convert::TryFrom};

use async_trait::async_trait;
use snafu::ResultExt;
use url::Url;

use crate::{
    bdev::{
        dev::reject_unknown_parameters,
        nexus::lookup_nexus_child,
        util::uri,
        CreateDestroy,
        GetName,
    },
    core::UntypedBdev,
    nexus_uri::{self, NexusBdevError},
};

#[derive(Debug)]
pub(super) struct Loopback {
    name: String,
    alias: String,
    uuid: Option<uuid::Uuid>,
}

impl TryFrom<&Url> for Loopback {
    type Error = NexusBdevError;

    fn try_from(url: &Url) -> Result<Self, Self::Error> {
        let segments = uri::segments(url);

        if segments.is_empty() {
            return Err(NexusBdevError::UriInvalid {
                uri: url.to_string(),
                message: String::from("no path segments"),
            });
        }

        let mut parameters: HashMap<String, String> =
            url.query_pairs().into_owned().collect();

        let uuid = uri::uuid(parameters.remove("uuid")).context(
            nexus_uri::UuidParamParseError {
                uri: url.to_string(),
            },
        )?;

        reject_unknown_parameters(url, parameters)?;

        Ok(Loopback {
            name: segments.join("/"),
            alias: url.to_string(),
            uuid,
        })
    }
}

impl GetName for Loopback {
    fn get_name(&self) -> String {
        self.name.clone()
    }
}

#[async_trait(?Send)]
impl CreateDestroy for Loopback {
    type Error = NexusBdevError;

    async fn create(&self) -> Result<String, Self::Error> {
        if let Some(mut bdev) = UntypedBdev::lookup_by_name(&self.name) {
            if self.uuid.is_some() && Some(bdev.uuid()) != self.uuid {
                return Err(NexusBdevError::BdevWrongUuid {
                    name: self.get_name(),
                    uuid: bdev.uuid_as_string(),
                });
            }

            if !bdev.add_alias(&self.alias) {
                error!(
                    "failed to add alias {} to device {}",
                    self.alias,
                    self.get_name()
                );
            }

            return Ok(self.get_name());
        }

        Err(NexusBdevError::BdevNotFound {
            name: self.get_name(),
        })
    }

    async fn destroy(self: Box<Self>) -> Result<(), Self::Error> {
        if let Some(child) = lookup_nexus_child(&self.name) {
            child.remove();
        }
        if let Some(mut bdev) = UntypedBdev::lookup_by_name(&self.name) {
            bdev.remove_alias(&self.alias);
        }
        Ok(())
    }
}
