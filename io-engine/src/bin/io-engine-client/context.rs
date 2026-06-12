use crate::{BdevClient, JsonClient, MayaClient};
use byte_unit::Byte;
use bytes::Bytes;
use http::uri::{Authority, PathAndQuery, Scheme, Uri};
use snafu::{Backtrace, ResultExt, Snafu};
use std::{cmp::max, str::FromStr};
use tonic::transport::Endpoint;

#[derive(Debug, Snafu)]
#[snafu(context(suffix(false)))]
pub enum Error {
    #[snafu(display("Invalid URI"))]
    InvalidUriBytes {
        source: http::uri::InvalidUri,
        backtrace: Backtrace,
    },
    #[snafu(display("Invalid URI parts"))]
    InvalidUriParts {
        source: http::uri::InvalidUriParts,
        backtrace: Backtrace,
    },
    #[snafu(display("Invalid URI"))]
    TonicInvalidUri {
        source: tonic::codegen::http::uri::InvalidUri,
        backtrace: Backtrace,
    },
    #[snafu(display("Invalid URI"))]
    InvalidUri {
        source: http::uri::InvalidUri,
        backtrace: Backtrace,
    },
}

/// Output format for CLI commands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum OutputFormat {
    /// Human-readable default output
    #[value(name = "default")]
    Default,
    /// JSON output
    #[value(name = "json")]
    Json,
}

/// Unit base for displaying byte sizes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum Units {
    /// Raw bytes (e.g. 1073741824)
    #[value(name = "b")]
    Bytes,
    /// Binary units (e.g. 1.00 GiB)
    #[value(name = "i")]
    Binary,
    /// Decimal units (e.g. 1.07 GB)
    #[value(name = "d")]
    Decimal,
}

mod v1 {
    use super::Error;
    use io_engine_api::v1::*;
    use tonic::transport::{Channel, Endpoint};

    pub type BdevRpcClient = bdev::BdevRpcClient<Channel>;
    pub type JsonRpcClient = json::JsonRpcClient<Channel>;
    pub type PoolRpcClient = pool::PoolRpcClient<Channel>;
    pub type ReplicaRpcClient = replica::ReplicaRpcClient<Channel>;
    pub type HostRpcClient = host::HostRpcClient<Channel>;
    pub type NexusRpcClient = nexus::NexusRpcClient<Channel>;
    pub type SnapshotRpcClient = snapshot::SnapshotRpcClient<Channel>;
    pub type SnapshotRebuildRpcClient = snapshot_rebuild::SnapshotRebuildRpcClient<Channel>;
    pub type TestRpcClient = test::TestRpcClient<Channel>;
    pub type StatsRpcClient = stats::StatsRpcClient<Channel>;

    pub struct Context {
        pub bdev: BdevRpcClient,
        pub json: JsonRpcClient,
        pub pool: PoolRpcClient,
        pub replica: ReplicaRpcClient,
        pub host: HostRpcClient,
        pub nexus: NexusRpcClient,
        pub snapshot: SnapshotRpcClient,
        pub snapshot_rebuild: SnapshotRebuildRpcClient,
        pub test: TestRpcClient,
        pub stats: StatsRpcClient,
    }

    impl Context {
        pub async fn new(h: Endpoint) -> Result<Self, Error> {
            let bdev = BdevRpcClient::connect(h.clone()).await.unwrap();
            let json = JsonRpcClient::connect(h.clone()).await.unwrap();
            let pool = PoolRpcClient::connect(h.clone()).await.unwrap();
            let replica = ReplicaRpcClient::connect(h.clone()).await.unwrap();
            let host = HostRpcClient::connect(h.clone()).await.unwrap();
            let nexus = NexusRpcClient::connect(h.clone()).await.unwrap();
            let snapshot = SnapshotRpcClient::connect(h.clone()).await.unwrap();
            let snapshot_rebuild = SnapshotRebuildRpcClient::connect(h.clone()).await.unwrap();
            let test = TestRpcClient::connect(h.clone()).await.unwrap();
            let stats = StatsRpcClient::connect(h).await.unwrap();

            Ok(Self {
                bdev,
                json,
                pool,
                replica,
                host,
                nexus,
                snapshot,
                snapshot_rebuild,
                test,
                stats,
            })
        }
    }
}

pub struct Context {
    pub(crate) client: MayaClient,
    pub(crate) bdev: BdevClient,
    pub(crate) json: JsonClient,
    pub(crate) v1: v1::Context,
    verbosity: u8,
    units: Units,
    pub(crate) output: OutputFormat,
}

impl Context {
    pub(crate) async fn new(
        bind: &str,
        quiet: bool,
        verbose: u8,
        units: Units,
        output: OutputFormat,
    ) -> Result<Self, Error> {
        let verbosity = if quiet { 0 } else { verbose + 1 };
        let host = {
            let uri = match bind.parse::<Uri>().context(InvalidUri) {
                Ok(uri) => Ok(uri),
                Err(error) => format!("[{bind}]").parse::<Uri>().map_err(|_| error),
            }?;
            let mut parts = uri.into_parts();
            if parts.scheme.is_none() {
                parts.scheme = Scheme::from_str("http").ok();
            }
            if let Some(ref mut authority) = parts.authority {
                if authority.port().is_none() {
                    parts.authority = Authority::from_maybe_shared(Bytes::from(format!(
                        "{}:{}",
                        authority.host(),
                        10124
                    )))
                    .ok()
                }
            }
            if parts.path_and_query.is_none() {
                parts.path_and_query = PathAndQuery::from_str("/").ok();
            }
            let uri = Uri::from_parts(parts).context(InvalidUriParts)?;
            Endpoint::from(uri)
        };
        if verbosity > 1 {
            println!("Connecting to {:?}", host.uri());
        }
        let client = MayaClient::connect(host.clone()).await.unwrap();
        let bdev = BdevClient::connect(host.clone()).await.unwrap();
        let json = JsonClient::connect(host.clone()).await.unwrap();
        let v1 = v1::Context::new(host).await.unwrap();
        Ok(Context {
            client,
            bdev,
            json,
            v1,
            verbosity,
            units,
            output,
        })
    }

    pub(crate) fn v1(&self, s: &str) {
        if self.verbosity > 0 {
            println!("{s}")
        }
    }

    pub(crate) fn v2(&self, s: &str) {
        if self.verbosity > 1 {
            println!("{s}")
        }
    }

    pub(crate) fn units(&self, n: Byte) -> String {
        match self.units {
            Units::Binary => format!("{:.2}", n.get_appropriate_unit(byte_unit::UnitType::Binary)),
            Units::Decimal => format!(
                "{:.2}",
                n.get_appropriate_unit(byte_unit::UnitType::Decimal)
            ),
            Units::Bytes => n.as_u64().to_string(),
        }
    }

    pub(crate) fn units_with(&self, n: Byte, unit: byte_unit::UnitType) -> String {
        match self.units {
            Units::Bytes => n.as_u64().to_string(),
            _ => format!("{:.2}", n.get_appropriate_unit(unit)),
        }
    }

    pub(crate) fn print_list(&self, headers: Vec<&str>, mut data: Vec<Vec<String>>) {
        assert_ne!(data.len(), 0);
        let ncols = data.first().unwrap().len();
        assert_eq!(headers.len(), ncols);

        let columns = if self.verbosity > 0 {
            data.insert(
                0,
                headers
                    .iter()
                    .map(|h| {
                        if let Some(stripped) = h.strip_prefix('>') {
                            stripped.to_string()
                        } else {
                            h.to_string()
                        }
                    })
                    .collect(),
            );

            data.iter().fold(
                headers
                    .iter()
                    .map(|h| (h.starts_with('>'), 0usize))
                    .collect(),
                |thus_far: Vec<(bool, usize)>, elem| {
                    thus_far
                        .iter()
                        .zip(elem)
                        .map(|((a, l), s)| (*a, max(*l, s.len())))
                        .collect()
                },
            )
        } else {
            vec![(false, 0usize); ncols]
        };

        for row in data {
            let vals = row.iter().enumerate().map(|(idx, s)| {
                if columns[idx].0 {
                    format!("{:>1$}", s, columns[idx].1)
                } else {
                    format!("{:<1$}", s, columns[idx].1)
                }
            });

            let line = vals.collect::<Vec<String>>().join(" ");
            println!("{line}");
        }
    }

    pub(crate) async fn print_streamed_list(
        &self,
        headers: Vec<&str>,
        mut recv: tokio::sync::mpsc::Receiver<Result<Vec<String>, tonic::Status>>,
    ) -> Result<(), tonic::Status> {
        let Some(data) = recv.recv().await else {
            return Ok(());
        };
        let mut data = vec![data?];
        let ncols = data.first().unwrap().len();
        assert_eq!(headers.len(), ncols);

        let columns = if self.verbosity > 0 {
            data.insert(
                0,
                headers
                    .iter()
                    .map(|h| {
                        if let Some(stripped) = h.strip_prefix('>') {
                            stripped.to_string()
                        } else {
                            h.to_string()
                        }
                    })
                    .collect(),
            );

            data.iter().fold(
                headers
                    .iter()
                    .map(|h| (h.starts_with('>'), 0usize))
                    .collect(),
                |thus_far: Vec<(bool, usize)>, elem| {
                    thus_far
                        .iter()
                        .zip(elem)
                        .map(|((a, l), s)| (*a, max(*l, s.len())))
                        .collect()
                },
            )
        } else {
            vec![(false, 0usize); ncols]
        };

        data.reverse();
        while let Some(row) = {
            if let Some(data) = data.pop() {
                Some(Ok(data))
            } else {
                recv.recv().await
            }
        } {
            let vals = row?.into_iter().enumerate().map(|(idx, s)| {
                if columns[idx].0 {
                    format!("{:>1$}", s, columns[idx].1)
                } else {
                    format!("{:<1$}", s, columns[idx].1)
                }
            });

            let line = vals.collect::<Vec<String>>().join("  ");
            println!("{line}");
        }

        Ok(())
    }
}
