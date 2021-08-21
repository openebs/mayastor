use std::{convert::TryFrom, time::Duration};

use url::{ParseError, Url};

use crate::{
    error::NvmeError,
    nvme_namespaces::{NvmeDevice, NvmeDeviceList},
    nvmf_discovery::{disconnect, ConnectArgsBuilder},
};

pub struct NvmeTarget {
    host: String,
    port: u16,
    subsysnqn: String,
    trtype: String,
}

impl TryFrom<String> for NvmeTarget {
    type Error = NvmeError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        NvmeTarget::try_from(value.as_str())
    }
}

impl TryFrom<&str> for NvmeTarget {
    type Error = NvmeError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let url = Url::parse(value)
            .map_err(|source| NvmeError::UrlError { source })?;

        let trtype = match url.scheme() {
            "nvmf" | "nvmf+tcp" => Ok("tcp"),
            _ => Err(NvmeError::UrlError {
                source: ParseError::IdnaError,
            }),
        }?
        .into();

        let host = url
            .host_str()
            .ok_or(NvmeError::UrlError {
                source: ParseError::EmptyHost,
            })?
            .into();

        let subnqn = match url.path_segments() {
            None => Err(NvmeError::UrlError {
                source: ParseError::RelativeUrlWithCannotBeABaseBase,
            }),
            Some(s) => {
                let segments = s.collect::<Vec<&str>>();
                if segments[0].is_empty() {
                    Err(NvmeError::UrlError {
                        source: ParseError::RelativeUrlWithCannotBeABaseBase,
                    })
                } else {
                    Ok(segments[0].to_string())
                }
            }
        }?;

        Ok(Self {
            trtype,
            host,
            port: url.port().unwrap_or(4420),
            subsysnqn: subnqn,
        })
    }
}

impl NvmeTarget {
    pub fn connect(&self) -> Result<Vec<NvmeDevice>, NvmeError> {
        if self.trtype != "tcp" {
            return Err(NvmeError::TransportError {
                trtype: self.trtype.clone(),
            });
        }

        ConnectArgsBuilder::default()
            .traddr(&self.host)
            .trsvcid(self.port.to_string())
            .nqn(&self.subsysnqn)
            .build()
            .map_err(|_| NvmeError::ParseError {})?
            .connect()?;

        let mut retries = 10;
        let mut all_nvme_devices;
        loop {
            std::thread::sleep(Duration::from_millis(1000));

            all_nvme_devices = NvmeDeviceList::new()
                .filter_map(Result::ok)
                .filter(|b| b.subsysnqn == self.subsysnqn)
                .collect::<Vec<_>>();

            retries -= 1;
            if retries == 0 || !all_nvme_devices.is_empty() {
                break;
            }
        }

        Ok(all_nvme_devices)
    }

    pub fn disconnect(&self) -> Result<usize, NvmeError> {
        disconnect(&self.subsysnqn)
    }
}

#[test]
fn nvme_parse_uri() {
    let target =
        NvmeTarget::try_from("nvmf://1.2.3.4:1234/testnqn.what-ever.foo")
            .unwrap();

    assert_eq!(target.port, 1234);
    assert_eq!(target.host, "1.2.3.4");
    assert_eq!(target.trtype, "tcp");
    assert_eq!(target.subsysnqn, "testnqn.what-ever.foo");

    let target =
        NvmeTarget::try_from("nvmf+tcp://1.2.3.4:1234/testnqn.what-ever.foo")
            .unwrap();

    assert_eq!(target.port, 1234);
    assert_eq!(target.host, "1.2.3.4");
    assert_eq!(target.trtype, "tcp");
    assert_eq!(target.subsysnqn, "testnqn.what-ever.foo");
}
