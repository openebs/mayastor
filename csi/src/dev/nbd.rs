use std::{collections::HashMap, convert::TryFrom};

use url::Url;

use super::{Attach, DeviceError, DeviceName};

pub(super) struct Nbd {
    path: String,
}

impl Nbd {
    fn new(path: String) -> Nbd {
        Nbd { path }
    }
}

impl TryFrom<&Url> for Nbd {
    type Error = DeviceError;

    fn try_from(url: &Url) -> Result<Self, Self::Error> {
        let segments: Vec<&str> = url
            .path_segments()
            .ok_or_else(|| DeviceError::new("no path segments"))?
            .collect();

        if segments.is_empty()
            || (segments.len() == 1 && segments[0].is_empty())
        {
            return Err(DeviceError::new("no path segments"));
        }

        Ok(Nbd::new(url.path().to_string()))
    }
}

#[tonic::async_trait]
impl Attach for Nbd {
    async fn parse_parameters(
        &mut self,
        _context: &HashMap<String, String>,
    ) -> Result<(), DeviceError> {
        Ok(())
    }

    async fn attach(&self) -> Result<(), DeviceError> {
        Ok(())
    }

    async fn find(&self) -> Result<Option<DeviceName>, DeviceError> {
        Ok(Some(DeviceName::from(&self.path)))
    }

    async fn fixup(&self) -> Result<(), DeviceError> {
        Ok(())
    }
}
