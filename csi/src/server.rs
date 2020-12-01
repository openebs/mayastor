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
};

use chrono::Local;
use clap::{App, Arg};
use csi::{identity_server::IdentityServer, node_server::NodeServer};
use env_logger::{Builder, Env};
use futures::stream::TryStreamExt;
use nodeplugin_grpc::MayastorNodePluginGrpcServer;
use std::{
    path::Path,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::{net::UnixListener, prelude::*};
use tonic::transport::{server::Connected, Server};

use crate::{identity::Identity, mount::probe_filesystems, node::Node};

#[allow(dead_code)]
#[allow(clippy::type_complexity)]
#[allow(clippy::unit_arg)]
#[allow(clippy::redundant_closure)]
#[allow(clippy::enum_variant_names)]
pub mod csi {
    tonic::include_proto!("csi.v1");
}

mod block_vol;
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

#[derive(Debug)]
struct UnixStream(tokio::net::UnixStream);

impl Connected for UnixStream {}

impl AsyncRead for UnixStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
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
        .get_matches();

    let node_name = matches.value_of("node-name").unwrap();
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

    let _ = tokio::join!(
        CSIServer::run(csi_socket, node_name),
        MayastorNodePluginGrpcServer::run(
            sock_addr.parse().expect("Invalid gRPC endpoint")
        ),
    );

    Ok(())
}

struct CSIServer {}

impl CSIServer {
    pub async fn run(csi_socket: &str, node_name: &str) -> Result<(), ()> {
        let mut uds_sock = UnixListener::bind(csi_socket).unwrap();
        info!("CSI plugin bound to {}", csi_socket);

        if let Err(e) = Server::builder()
            .add_service(NodeServer::new(Node {
                node_name: node_name.into(),
                filesystems: probe_filesystems(),
            }))
            .add_service(IdentityServer::new(Identity {}))
            .serve_with_incoming(uds_sock.incoming().map_ok(UnixStream))
            .await
        {
            error!("CSI server failed with error: {}", e);
            return Err(());
        }

        Ok(())
    }
}
