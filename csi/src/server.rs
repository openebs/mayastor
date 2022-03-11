//! Mayastor CSI plugin.
//!
//! Implementation of gRPC methods from the CSI spec. This includes mounting
//! of mayastor volumes using iscsi/nvmf protocols on the node.

extern crate clap;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate tracing;

use std::{
    fs,
    io::{ErrorKind, Write},
    sync::Arc,
};

use crate::{identity::Identity, mount::probe_filesystems, node::Node};
use chrono::Local;
use clap::{App, Arg};
use csi::{identity_server::IdentityServer, node_server::NodeServer};
use env_logger::{Builder, Env};
use futures::TryFutureExt;
use nodeplugin_grpc::MayastorNodePluginGrpcServer;
use std::{
    path::Path,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::UnixListener,
};
use tonic::transport::{server::Connected, Server};

#[allow(dead_code)]
#[allow(clippy::type_complexity)]
#[allow(clippy::unit_arg)]
#[allow(clippy::redundant_closure)]
#[allow(clippy::enum_variant_names)]
#[allow(clippy::upper_case_acronyms)]
pub mod csi {
    tonic::include_proto!("csi.v1");
}

mod block_vol;
pub(crate) mod config;
mod dev;
mod error;
mod filesystem_vol;
mod findmnt;
mod format;
mod identity;
mod match_dev;
mod mount;
mod node;
mod nodeplugin_grpc;
mod nodeplugin_svc;
pub(crate) mod shutdown_event;

#[derive(Clone, Debug)]
pub struct UdsConnectInfo {
    pub peer_addr: Option<Arc<tokio::net::unix::SocketAddr>>,
    pub peer_cred: Option<tokio::net::unix::UCred>,
}

#[derive(Debug)]
struct UnixStream(tokio::net::UnixStream);

impl Connected for UnixStream {
    type ConnectInfo = UdsConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        UdsConnectInfo {
            peer_addr: self.0.peer_addr().ok().map(Arc::new),
            peer_cred: self.0.peer_cred().ok(),
        }
    }
}

impl AsyncRead for UnixStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl AsyncWrite for UnixStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

const GRPC_PORT: u16 = 10199;

// Returns only base hostname, stripping all (sub)domain parts.
fn normalize_hostname(hostname: &str) -> &str {
    match hostname.find('.') {
        Some(idx) => &hostname[0 .. idx],
        None => hostname,
    }
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let matches = App::new("Mayastor CSI plugin")
        .about("k8s sidecar for Mayastor implementing CSI among others")
        .arg(
            Arg::with_name("csi-socket")
                .short("c")
                .long("csi-socket")
                .value_name("PATH")
                .help("CSI gRPC listen socket (default /var/tmp/csi.sock)")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("log-debug")
                .short("l")
                .help("Log extra info - file name and line number"),
        )
        .arg(
            Arg::with_name("node-name")
                .short("n")
                .long("node-name")
                .value_name("NAME")
                .help("Unique node name where this instance runs")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("grpc-endpoint")
                .short("g")
                .long("grpc-endpoint")
                .value_name("NAME")
                .help("ip address where this instance runs, and optionally the gRPC port")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("v")
                .short("v")
                .multiple(true)
                .help("Sets the verbosity level"),
        )
        .arg(
            Arg::with_name("nvme-core-io-timeout")
                .long("nvme-core-io-timeout")
                .value_name("TIMEOUT")
                .takes_value(true)
                .required(false)
                .help("Sets the global nvme_core module io_timeout, in seconds"),
        )
        .arg(
            Arg::with_name("nvme-nr-io-queues")
                .long("nvme-nr-io-queues")
                .value_name("NUMBER")
                .takes_value(true)
                .required(false)
                .help("Sets the nvme-nr-io-queues parameter when connecting to a volume target"),
        )
        .get_matches();

    let node_name = normalize_hostname(matches.value_of("node-name").unwrap());
    let endpoint = matches.value_of("grpc-endpoint").unwrap();
    let csi_socket = matches
        .value_of("csi-socket")
        .unwrap_or("/var/tmp/csi.sock");
    let level = match matches.occurrences_of("v") as usize {
        0 => "info",
        1 => "debug",
        _ => "trace",
    };

    // configure logger: env var takes precedence over cmd line options
    let filter_expr = format!("{}={}", module_path!(), level);
    let mut builder =
        Builder::from_env(Env::default().default_filter_or(filter_expr));
    if matches.is_present("log-debug") {
        builder.format(|buf, record| {
            let mut level_style = buf.default_level_style(record.level());
            level_style.set_intense(true);
            writeln!(
                buf,
                "[{} {} {}:{}] {}",
                Local::now().format("%Y-%m-%dT%H:%M:%SZ"),
                level_style.value(record.level()),
                Path::new(record.file().unwrap())
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap(),
                record.line().unwrap(),
                record.args()
            )
        });
    }
    builder.init();

    if let Some(nvme_io_timeout_secs) = matches.value_of("nvme_core io_timeout")
    {
        let io_timeout_secs: u32 = nvme_io_timeout_secs
            .parse()
            .expect("nvme_core io_timeout should be an integer number, representing the timeout in seconds");

        if let Err(error) = dev::nvmf::set_nvmecore_iotimeout(io_timeout_secs) {
            panic!("Failed to set nvme_core io_timeout: {}", error.to_string());
        }
    }

    // Remove stale CSI socket from previous instance if there is any
    match fs::remove_file(csi_socket) {
        Ok(_) => info!("Removed stale CSI socket {}", csi_socket),
        Err(err) => {
            if err.kind() != ErrorKind::NotFound {
                return Err(format!(
                    "Error removing stale CSI socket {}: {}",
                    csi_socket, err
                ));
            }
        }
    }

    let sock_addr = if endpoint.contains(':') {
        endpoint.to_string()
    } else {
        format!("{}:{}", endpoint, GRPC_PORT)
    };

    *config::config().nvme_as_mut() =
        std::convert::TryFrom::try_from(&matches)?;

    let _ = tokio::join!(
        CsiServer::run(csi_socket, node_name),
        MayastorNodePluginGrpcServer::run(
            sock_addr.parse().expect("Invalid gRPC endpoint")
        ),
    );

    Ok(())
}

struct CsiServer {}

impl CsiServer {
    pub async fn run(csi_socket: &str, node_name: &str) -> Result<(), ()> {
        let incoming = {
            let uds = UnixListener::bind(csi_socket).unwrap();
            info!("CSI plugin bound to {}", csi_socket);

            async_stream::stream! {
                while let item = uds.accept().map_ok(|(st, _)| UnixStream(st)).await {
                    yield item;
                }
            }
        };

        if let Err(e) = Server::builder()
            .add_service(NodeServer::new(Node {
                node_name: node_name.into(),
                filesystems: probe_filesystems(),
            }))
            .add_service(IdentityServer::new(Identity {}))
            .serve_with_incoming_shutdown(incoming, shutdown_event::wait())
            .await
        {
            error!("CSI server failed with error: {}", e);
            return Err(());
        }

        Ok(())
    }
}
