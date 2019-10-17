//! gRPC mayastor proxy implementation
//!
//! It is a kind of proxy. Input request is gRPC, which is translated to
//! JSON-RPC understood by mayastor (SPDK). The return value goes through the
//! same transformation in opposite direction. The only exception is mounting
//! of volumes, which is actually done here in the proxy rather than in
//! mayastor. We aim for 1:1 mapping between the two RPCs.

#[macro_use]
extern crate clap;
#[macro_use]
extern crate run_script;
#[macro_use]
extern crate log;
use std::{io::Write, path::Path};

use crate::mount::probe_filesystems;
use chrono::Local;
use clap::{App, Arg};
use env_logger::{Builder, Env};
use git_version::git_version;
use tokio;
use tonic::transport::Server;
// These libs are needed for gRPC generated code
use rpc;

use crate::{identity::Identity, mayastor_svc::MayastorService, node::Node};

#[allow(dead_code)]
#[allow(clippy::type_complexity)]
#[allow(clippy::unit_arg)]
#[allow(clippy::redundant_closure)]
#[allow(clippy::enum_variant_names)]
pub mod csi {
    tonic::include_proto!("csi.v1");
}

mod format;
mod identity;
mod mayastor_svc;
mod mount;
mod node;
mod router;

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let matches = App::new("Mayastor grpc server")
        .version(git_version!())
        .about("gRPC mayastor server with CSI and egress services")
        .arg(
            Arg::with_name("address")
                .short("a")
                .long("address")
                .value_name("IP")
                .help("IP address of the k8s pod where this app is running")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("port")
                .short("p")
                .long("port")
                .value_name("NUMBER")
                .help("Port number to listen on for egress svc (default 10124)")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("mayastor-socket")
                .short("s")
                .long("mayastor-socket")
                .value_name("PATH")
                .help("Socket path to mayastor backend (default /var/tmp/mayastor.sock)")
                .takes_value(true),
        )
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
            Arg::with_name("v")
                .short("v")
                .multiple(true)
                .help("Sets the verbosity level"),
        )
        .get_matches();

    let node_name = matches.value_of("node-name").unwrap();
    let port = value_t!(matches.value_of("port"), u16).unwrap_or(10124);
    let addr = matches.value_of("address").unwrap();
    let ms_socket = matches
        .value_of("mayastor-socket")
        .unwrap_or("/var/tmp/mayastor.sock");
    let _csi_socket = matches
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

    let saddr = format!("{}:{}", addr, port).parse().unwrap();
    let csi_builder = Server::builder();
    // TODO need to listen on unix socket as well
    csi_builder
        .serve(
            saddr,
            router::Router {
                node_service: std::sync::Arc::new(Node {
                    node_name: node_name.into(),
                    addr: addr.to_string(),
                    port,
                    socket: ms_socket.into(),
                    filesystems: probe_filesystems().unwrap(),
                }),
                identity_service: std::sync::Arc::new(Identity {
                    socket: ms_socket.into(),
                }),
                mayastor_service: std::sync::Arc::new(MayastorService {
                    socket: ms_socket.into(),
                }),
            },
        )
        .await?;

    Ok(())
}
