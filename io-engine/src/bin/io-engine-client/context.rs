use crate::{BdevClient, JsonClient, MayaClient};
use byte_unit::Byte;
use bytes::Bytes;
use http::uri::{Authority, PathAndQuery, Scheme, Uri};
use io_engine::grpc::tls::{GrpcServerTls, TlsConfig};
use snafu::{Backtrace, ResultExt, Snafu};
use std::{cmp::max, path::PathBuf, str::FromStr};
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
    #[snafu(display("Invalid TLS configuration: {message}"))]
    InvalidTls { message: String },
}

/// TLS connection options for the gRPC client.
#[derive(clap::Args, Debug, Clone)]
pub(crate) struct TlsArgs {
    /// Connect over TLS, defaulting to an ephemeral, unverified server {n}
    /// certificate (auto-TLS). Supplying the certificate files below overrides
    /// this with file-backed (optionally mutual) TLS.
    #[arg(
        long = "grpc-tls",
        env = "GRPC_TLS",
        global = true,
        help_heading = "TLS Options"
    )]
    tls: bool,
    /// Connect over TLS using an ephemeral, unverified server certificate {n}
    /// (auto-TLS). Matches a server started with `--grpc-auto-tls`.
    #[arg(
        long = "grpc-auto-tls",
        env = "GRPC_AUTO_TLS",
        global = true,
        conflicts_with_all = ["tls_cert", "tls_key", "tls_ca"],
        help_heading = "TLS Options"
    )]
    auto_tls: bool,
    /// Path to the client TLS certificate chain for mutual TLS. {n}
    /// Must be provided together with the private key.
    #[arg(
        long = "grpc-tls-cert-file",
        env = "GRPC_TLS_CERT_FILE",
        value_name = "FILE",
        requires = "tls_key",
        global = true,
        help_heading = "TLS Options"
    )]
    tls_cert: Option<PathBuf>,
    /// Path to the client TLS private key for mutual TLS. {n}
    /// Must be provided together with the certificate.
    #[arg(
        long = "grpc-tls-key-file",
        env = "GRPC_TLS_KEY_FILE",
        value_name = "FILE",
        requires = "tls_cert",
        global = true,
        help_heading = "TLS Options"
    )]
    tls_key: Option<PathBuf>,
    /// Path to the CA bundle used to verify the gRPC server certificate.
    #[arg(
        long = "grpc-tls-ca-file",
        env = "GRPC_TLS_CA_FILE",
        value_name = "FILE",
        global = true,
        help_heading = "TLS Options"
    )]
    tls_ca: Option<PathBuf>,
}

impl TlsArgs {
    /// Resolve the TLS configuration selected on the command line, if any.
    fn resolve(&self) -> Result<Option<GrpcServerTls>, Error> {
        if self.auto_tls {
            return Ok(Some(GrpcServerTls::Auto));
        }
        let tls = TlsConfig::new(
            self.tls_ca.clone(),
            self.tls_cert.clone(),
            self.tls_key.clone(),
        )
        .map_err(|message| Error::InvalidTls { message })?;
        if tls.enabled() {
            return Ok(Some(GrpcServerTls::Files(tls)));
        }
        // `--tls` enables TLS without any certificate files, defaulting to an
        // ephemeral self-signed certificate (auto-TLS).
        if self.tls {
            return Ok(Some(GrpcServerTls::Auto));
        }
        Ok(None)
    }
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
    use tonic::transport::Channel;

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
        pub async fn new(h: Channel) -> Result<Self, Error> {
            let bdev = BdevRpcClient::new(h.clone());
            let json = JsonRpcClient::new(h.clone());
            let pool = PoolRpcClient::new(h.clone());
            let replica = ReplicaRpcClient::new(h.clone());
            let host = HostRpcClient::new(h.clone());
            let nexus = NexusRpcClient::new(h.clone());
            let snapshot = SnapshotRpcClient::new(h.clone());
            let snapshot_rebuild = SnapshotRebuildRpcClient::new(h.clone());
            let test = TestRpcClient::new(h.clone());
            let stats = StatsRpcClient::new(h);

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
        tls: &TlsArgs,
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
        // Build the channel for the selected transport: plaintext, auto-TLS
        // (ephemeral, unverified server certificate) or file-backed TLS
        // (verifying the server against the configured CA and presenting the
        // client certificate for mutual TLS when provided).
        let channel = io_engine::grpc::tls::registration_channel(&host, tls.resolve()?.as_ref())
            .map_err(|message| Error::InvalidTls { message })?;
        let client = MayaClient::new(channel.clone());
        let bdev = BdevClient::new(channel.clone());
        let json = JsonClient::new(channel.clone());
        let v1 = v1::Context::new(channel).await.unwrap();
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
