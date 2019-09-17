//! gRPC client implementation for mayastor server.
//!
//! It is not expected to be used in production env but rather for testing and
//! debugging of the server.

#![feature(core_intrinsics)]
#![warn(unused_extern_crates)]
#[macro_use]
extern crate clap;

use bytesize::ByteSize;
use clap::{App, AppSettings, Arg, ArgMatches, SubCommand};
use futures::{future, Future};
use hyper::client::connect::{Destination, HttpConnector};
use rpc::{self, service::client::Mayastor};
use tokio::runtime::Runtime;
use tower_grpc::BoxBody;
use tower_hyper::{client, util, Connection};
use tower_request_modifier::{Builder, RequestModifier};
use tower_util::MakeService;

fn create_pool(
    mut client: Mayastor<RequestModifier<Connection<BoxBody>, BoxBody>>,
    matches: &ArgMatches,
    verbose: bool,
) -> Box<dyn Future<Item = (), Error = String> + Send> {
    let name = matches.value_of("POOL").unwrap().to_owned();
    let disks = matches
        .values_of("DISK")
        .unwrap()
        .map(|dev| dev.to_owned())
        .collect();
    let block_size = value_t!(matches.value_of("block-size"), u32).unwrap_or(0);

    if verbose {
        println!("Creating the pool {}", name);
    }

    Box::new(
        client
            .create_pool(tower_grpc::Request::new(
                rpc::mayastor::CreatePoolRequest {
                    name,
                    disks,
                    block_size,
                },
            ))
            .map_err(|err| format!("Grpc failed: {}", err))
            .map(|_null_resp| ()),
    )
}

fn destroy_pool(
    mut client: Mayastor<RequestModifier<Connection<BoxBody>, BoxBody>>,
    matches: &ArgMatches,
    verbose: bool,
) -> Box<dyn Future<Item = (), Error = String> + Send> {
    let name = matches.value_of("POOL").unwrap().to_owned();

    if verbose {
        println!("Destroying the pool {}", name);
    }

    Box::new(
        client
            .destroy_pool(tower_grpc::Request::new(
                rpc::mayastor::DestroyPoolRequest {
                    name,
                },
            ))
            .map_err(|err| format!("Grpc failed: {}", err))
            .map(|_null_resp| ()),
    )
}

fn list_pools(
    mut client: Mayastor<RequestModifier<Connection<BoxBody>, BoxBody>>,
    verbose: bool,
    quiet: bool,
) -> Box<dyn Future<Item = (), Error = String> + Send> {
    if verbose {
        println!("Requesting a list of pools");
    }

    let f = client
        .list_pools(tower_grpc::Request::new(rpc::mayastor::Null {}))
        .map_err(|err| format!("Grpc failed: {}", err))
        .map(move |resp| {
            let pools = &resp.get_ref().pools;

            if pools.is_empty() && !quiet {
                println!("No pools have been created");
            } else {
                if !quiet {
                    println!(
                        "{: <20} {: <8} {: >12} {: >12}   DISKS",
                        "NAME", "STATE", "CAPACITY", "USED"
                    );
                }
                for p in pools {
                    print!(
                        "{: <20} {: <8} {: >12} {: >12}  ",
                        p.name,
                        match rpc::mayastor::PoolState::from_i32(p.state)
                            .unwrap()
                        {
                            rpc::mayastor::PoolState::Online => "online",
                            rpc::mayastor::PoolState::Degraded => "degraded",
                            rpc::mayastor::PoolState::Faulty => "faulty",
                        },
                        ByteSize::b(p.capacity).to_string_as(true),
                        ByteSize::b(p.used).to_string_as(true),
                    );
                    for disk in &p.disks {
                        print!(" {}", disk);
                    }
                    println!();
                }
            }
        });
    Box::new(f)
}

fn create_replica(
    mut client: Mayastor<RequestModifier<Connection<BoxBody>, BoxBody>>,
    matches: &ArgMatches,
    verbose: bool,
) -> Box<dyn Future<Item = (), Error = String> + Send> {
    let pool = matches.value_of("POOL").unwrap().to_owned();
    let uuid = matches.value_of("UUID").unwrap().to_owned();
    let size = value_t!(matches.value_of("size"), u64).unwrap();
    let thin = matches.is_present("thin");
    let share = match matches.value_of("protocol") {
        None => rpc::mayastor::ShareProtocol::None as i32,
        Some("nvmf") => rpc::mayastor::ShareProtocol::Nvmf as i32,
        Some(_) => {
            return Box::new(future::err(
                "Invalid value of share protocol".to_owned(),
            ))
        }
    };

    if verbose {
        println!("Creating replica {} on pool {}", uuid, pool);
    }

    Box::new(
        client
            .create_replica(tower_grpc::Request::new(
                rpc::mayastor::CreateReplicaRequest {
                    uuid,
                    pool,
                    thin,
                    share,
                    size: size * (1024 * 1024),
                },
            ))
            .map_err(|err| format!("Grpc failed: {}", err))
            .map(|_null_resp| ()),
    )
}

fn destroy_replica(
    mut client: Mayastor<RequestModifier<Connection<BoxBody>, BoxBody>>,
    matches: &ArgMatches,
    verbose: bool,
) -> Box<dyn Future<Item = (), Error = String> + Send> {
    let uuid = matches.value_of("UUID").unwrap().to_owned();

    if verbose {
        println!("Destroying replica {}", uuid);
    }

    Box::new(
        client
            .destroy_replica(tower_grpc::Request::new(
                rpc::mayastor::DestroyReplicaRequest {
                    uuid,
                },
            ))
            .map_err(|err| format!("Grpc failed: {}", err))
            .map(|_null_resp| ()),
    )
}

fn list_replicas(
    mut client: Mayastor<RequestModifier<Connection<BoxBody>, BoxBody>>,
    verbose: bool,
    quiet: bool,
) -> Box<dyn Future<Item = (), Error = String> + Send> {
    if verbose {
        println!("Requesting a list of replicas");
    }

    Box::new(
        client
            .list_replicas(tower_grpc::Request::new(rpc::mayastor::Null {}))
            .map_err(|err| format!("Grpc failed: {}", err))
            .map(move |resp| {
                let replicas = &resp.get_ref().replicas;

                if replicas.is_empty() && !quiet {
                    println!("No replicas have been created");
                } else {
                    if !quiet {
                        println!(
                            "{: <20} {: <36} {: <8} {: <8} {: >10}",
                            "POOL", "NAME", "THIN", "SHARE", "SIZE"
                        );
                    }
                    for r in replicas {
                        println!(
                            "{: <20} {: <36} {: <8} {: <8} {: >10}",
                            r.pool,
                            r.uuid,
                            r.thin,
                            match rpc::mayastor::ShareProtocol::from_i32(
                                r.share
                            ) {
                                Some(rpc::mayastor::ShareProtocol::None) => {
                                    "none"
                                }
                                Some(rpc::mayastor::ShareProtocol::Nvmf) => {
                                    "nvmf"
                                }
                                None => "unknown",
                            },
                            ByteSize::b(r.size).to_string_as(true),
                        );
                    }
                }
            }),
    )
}

fn stat_replicas(
    mut client: Mayastor<RequestModifier<Connection<BoxBody>, BoxBody>>,
    verbose: bool,
    quiet: bool,
) -> Box<dyn Future<Item = (), Error = String> + Send> {
    if verbose {
        println!("Requesting replicas stats");
    }

    Box::new(
        client
            .stat_replicas(tower_grpc::Request::new(rpc::mayastor::Null {}))
            .map_err(|err| format!("Grpc failed: {}", err))
            .map(move |resp| {
                let replicas = &resp.get_ref().replicas;

                if replicas.is_empty() && !quiet {
                    println!("No replicas have been created");
                } else {
                    if !quiet {
                        println!(
                            "{: <20} {: <36} {: >10} {: >10} {: >10} {: >10}",
                            "POOL",
                            "NAME",
                            "RDCNT",
                            "WRCNT",
                            "RDBYTES",
                            "WRBYTES",
                        );
                    }
                    for r in replicas {
                        let stats = r.stats.as_ref().unwrap();
                        println!(
                            "{: <20} {: <36} {: >10} {: >10} {: >10} {: >10}",
                            r.pool,
                            r.uuid,
                            stats.num_read_ops,
                            stats.num_write_ops,
                            stats.bytes_read,
                            stats.bytes_written,
                        );
                    }
                }
            }),
    )
}

/// Call storage pool RPC method.
///
/// Function gets a gRPC client handle and invokes the right RPC method
/// based on command line arguments parsed by clap lib.
///
/// NOTE: We need to return Boxed future with dynamic dispatch table because
/// static templates for result futures of gRPC methods with different return
/// values do not work.
fn dispatch_pool_cmd(
    client: Mayastor<RequestModifier<Connection<BoxBody>, BoxBody>>,
    matches: &ArgMatches,
    verbose: bool,
    quiet: bool,
) -> Box<dyn Future<Item = (), Error = String> + Send> {
    match matches.subcommand() {
        ("create", Some(matches)) => create_pool(client, matches, verbose),
        ("destroy", Some(matches)) => destroy_pool(client, matches, verbose),
        ("list", Some(_matches)) => list_pools(client, verbose, quiet),
        _ => Box::new(future::err(format!(
            "Command invalid\n {}",
            matches.usage().to_string()
        ))),
    }
}

/// The same dispatch function as for the pool commands above but this one
/// is for replica commands.
fn dispatch_replica_cmd(
    client: Mayastor<RequestModifier<Connection<BoxBody>, BoxBody>>,
    matches: &ArgMatches,
    verbose: bool,
    quiet: bool,
) -> Box<dyn Future<Item = (), Error = String> + Send> {
    match matches.subcommand() {
        ("create", Some(matches)) => create_replica(client, matches, verbose),
        ("destroy", Some(matches)) => destroy_replica(client, matches, verbose),
        ("list", Some(_matches)) => list_replicas(client, verbose, quiet),
        ("stats", Some(_matches)) => stat_replicas(client, verbose, quiet),
        _ => Box::new(future::err(format!(
            "Command invalid\n {}",
            matches.usage().to_string()
        ))),
    }
}

pub fn main() {
    let matches = App::new("Mayastor grpc client")
        .version("0.1")
        .settings(&[AppSettings::SubcommandRequiredElseHelp,
                  AppSettings::ColoredHelp, AppSettings::ColorAlways])
        .about("Client for mayastor gRPC server")
        .arg(
            Arg::with_name("address")
                .short("a")
                .long("address")
                .value_name("HOST")
                .help("IP address of mayastor server (default 127.0.0.1)")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("port")
                .short("p")
                .long("port")
                .value_name("NUMBER")
                .help("Port number of mayastor server (default 10124)")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("verbose")
                .short("v")
                .long("verbose")
                .multiple(true)
                .help("Verbose output"),
        )
        .arg(
            Arg::with_name("quiet")
                .short("q")
                .long("quiet")
                .help("Do not print any output except for list records"),
        )
        .subcommand(
            SubCommand::with_name("pool")
                .about("Storage pool management")
                .subcommand(
                    SubCommand::with_name("create")
                        .about("Create storage pool")
                        .arg(
                            Arg::with_name("block-size")
                                .short("b")
                                .long("block-size")
                                .value_name("NUMBER")
                                .help("block size of the underlying devices")
                                .takes_value(true),
                        )
                        .arg(
                            Arg::with_name("POOL")
                                .help("Storage pool name")
                                .required(true)
                                .index(1),
                        )
                        .arg(
                            Arg::with_name("DISK")
                                .help("Disk device files")
                                .required(true)
                                .multiple(true)
                                .index(2),
                        ),
                )
                .subcommand(
                    SubCommand::with_name("destroy")
                        .about("Destroy storage pool")
                        .arg(
                            Arg::with_name("POOL")
                                .help("Storage pool name")
                                .required(true)
                                .index(1),
                        ),
                )
                .subcommand(SubCommand::with_name("list").about("List storage pools")),
        )
        .subcommand(
            SubCommand::with_name("replica")
                .about("Replica management")
                .subcommand(
                    SubCommand::with_name("create")
                        .about("Create replica on pool")
                        .arg(
                            Arg::with_name("POOL")
                                .help("Storage pool name")
                                .required(true)
                                .index(1),
                        )
                        .arg(
                            Arg::with_name("UUID")
                                .help("Unique replica uuid")
                                .required(true)
                                .index(2),
                        )
                        .arg(
                            Arg::with_name("protocol")
                                .short("p")
                                .long("protocol")
                                .value_name("PROTOCOL")
                                .help("Name of a protocol (nvmf) used for sharing the replica (default none)")
                                .takes_value(true),
                        )
                        .arg(
                            Arg::with_name("size")
                                .short("s")
                                .long("size")
                                .value_name("NUMBER")
                                .help("Size of the replica in MiB")
                                .takes_value(true)
                                .required(true),
                        )
                        .arg(
                            Arg::with_name("thin")
                                .short("t")
                                .long("thin")
                                .help("Replica is thin provisioned (default false)")
                                .takes_value(false),
                        ),
                )
                .subcommand(
                    SubCommand::with_name("destroy")
                        .about("Destroy replica")
                        .arg(
                            Arg::with_name("UUID")
                                .help("Replica uuid")
                                .required(true)
                                .index(1),
                        ),
                )
                .subcommand(SubCommand::with_name("list").about("List replicas"))
                .subcommand(SubCommand::with_name("stats").about("IO stats of replicas")),
        )
        .get_matches();

    let endpoint = {
        let addr = matches.value_of("address").unwrap_or("127.0.0.1");
        let port = value_t!(matches.value_of("port"), u16).unwrap_or(10124);
        format!("{}:{}", addr, port)
    };
    let verbose = matches.occurrences_of("verbose") > 0;
    let quiet = matches.is_present("quiet");

    let uri: http::Uri = format!("http://{}", endpoint).parse().unwrap();
    let dst = Destination::try_from_uri(uri.clone()).unwrap();
    let connector = util::Connector::new(HttpConnector::new(1));
    let settings = client::Builder::new().http2_only(true).clone();
    let mut make_client = client::Connect::with_builder(connector, settings);

    // We explicitly create tokio runtime and use .block_on() method instead
    // of simply calling tokio::run(f). That's because tokio::run used to hang
    // at the end waiting for gRPC connection to be closed, but we couldn't do
    // that as the connection was consumed by the gRPC service and we could no
    // longer control it.
    let mut rt = Runtime::new().unwrap();

    // connect to the server ...
    if verbose {
        println!("Connecting to {}", endpoint);
    }
    rt.block_on::<_, (), ()>(
        make_client
            .make_service(dst)
            .map_err(move |err| {
                format!("Failed to connect to {}: {}", endpoint, err)
            })
            // conn is tower_hyper::Connection<_>
            .and_then(move |conn| {
                let conn = Builder::new().set_origin(uri).build(conn).unwrap();

                Mayastor::new(conn)
                    .ready()
                    .map_err(|err| format!("Error waiting for ready: {}", err))
            })
            .and_then(move |client| {
                // dispatch command to appropriate group of handlers
                match matches.subcommand() {
                    ("pool", Some(m)) => {
                        dispatch_pool_cmd(client, &m, verbose, quiet)
                    }
                    ("replica", Some(m)) => {
                        dispatch_replica_cmd(client, &m, verbose, quiet)
                    }
                    _ => panic!("unexpected input"),
                }
            })
            // Print the error if any
            .then(|res: Result<_, String>| {
                if let Err(err) = res {
                    eprintln!("{}", err);
                    ::std::process::exit(1);
                }
                future::ok(())
            }),
    )
    .unwrap();
}
