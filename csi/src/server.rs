//! gRPC mayastor proxy implementation
//!
//! It is a kind of proxy. Input request is gRPC, which is translated to
//! JSON-RPC understood by mayastor (SPDK). The return value goes through the
//! same transformation in opposite direction. The only exception is mounting
//! of volumes, which is actually done here in the proxy rather than in
//! mayastor. We aim for 1:1 mapping between the two RPCs.

#![warn(unused_extern_crates)]
#[macro_use]
extern crate clap;
#[macro_use]
extern crate log;
#[macro_use]
extern crate run_script;
use tokio;

mod format;
mod identity;
mod mayastor_svc;
mod mount;
#[macro_use]
mod node;
// These libs are needed for gRPC generated code
use rpc;

#[allow(dead_code)]
#[allow(clippy::type_complexity)]
#[allow(clippy::unit_arg)]
#[allow(clippy::redundant_closure)]
#[allow(clippy::enum_variant_names)]
mod csi {
    include!(concat!(env!("OUT_DIR"), "/csi.v1.rs"));
}

use crate::{
    identity::Identity,
    mayastor_svc::MayastorService,
    mount::probe_filesystems,
    node::Node,
};
use chrono::Local;
use clap::{App, Arg};
use env_logger::{Builder, Env};
use futures::{Future, Stream};
use git_version::git_version;
use grpc_router::Router2;
use std::{
    fs,
    io::{Error as IoError, Write},
    path::Path,
};
use tokio::net::{TcpListener, UnixListener};
use tower_hyper::server::{Http, Server};

pub fn main() {
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

    let csi_svc = Router2::new(
        "/csi.v1.Identity/",
        csi::server::IdentityServer::new(Identity {
            socket: ms_socket.to_owned(),
        }),
        csi::server::NodeServer::new(Node {
            node_name: node_name.to_string(),
            addr: addr.to_string(),
            port,
            socket: ms_socket.to_owned(),
            filesystems: probe_filesystems()
                .expect("Failed to probe filesystems"),
        }),
    );
    let egress_svc =
        rpc::service::server::MayastorServer::new(MayastorService {
            socket: ms_socket.to_owned(),
        });

    let mut csi_server = Server::new(csi_svc);
    let mut egress_server = Server::new(egress_svc);

    let endpoint_egress = format!("0.0.0.0:{}", port).parse().unwrap();
    let bind_egress = TcpListener::bind(&endpoint_egress).expect("bind");

    info!("Egress listening on {}", endpoint_egress);

    let accept_egress =
        bind_egress.incoming().for_each(move |sock| {
            debug!(
                "New connection from {}",
                match sock.peer_addr() {
                    Ok(addr) => addr.to_string(),
                    Err(_) => "unknown".to_owned(),
                }
            );
            if let Err(e) = sock.set_nodelay(true) {
                return Err(e);
            }

            let http = Http::new().http2_only(true).clone();
            let serve = egress_server.serve_with(sock, http);
            tokio::spawn(serve.map_err(|e| {
                error!("http2 error on egress connection: {:?}", e)
            }));
            Ok(())
        });

    // Besides the obvious unix domain socket for CSI we need to support TCP
    // as well, because grpc-node used in the tests does not support UDS:
    // https://github.com/grpc/grpc-node/issues/258.
    // Unfortunately we cannot abstract the differences between TCP and UDS
    // by using Box<Stream> because the underlying Item type of Stream is
    // different for each of TCP and UDS stream, so there is a little code
    // duplication here.
    let accept_csi: Box<dyn Future<Item = (), Error = IoError> + Send> =
        if csi_socket.starts_with('/') {
            // bind would fail if we did not remove stale socket
            let _ = fs::remove_file(csi_socket);
            let bind_csi = UnixListener::bind(csi_socket).expect("bind");
            Box::new(bind_csi.incoming().for_each(move |sock| {
                debug!("New csi connection");
                let http = Http::new().http2_only(true).clone();
                let serve = csi_server.serve_with(sock, http);
                tokio::spawn(
                    serve.map_err(|e| error!("error on CSI connection: {}", e)),
                );
                Ok(())
            }))
        } else {
            let endpoint_csi = csi_socket.parse().unwrap();
            let bind_csi = TcpListener::bind(&endpoint_csi).expect("bind");
            Box::new(bind_csi.incoming().for_each(move |sock| {
                debug!("New csi connection");
                let http = Http::new().http2_only(true).clone();
                let serve = csi_server.serve_with(sock, http);
                tokio::spawn(
                    serve.map_err(|e| error!("error on CSI connection: {}", e)),
                );
                Ok(())
            }))
        };

    info!("CSI listening on {}", csi_socket);

    tokio::run(accept_egress.join(accept_csi).then(|res| {
        if let Err(err) = res {
            error!("accept error: {}", err);
        }
        Ok(())
    }))
}
