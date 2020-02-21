//! gRPC client implementation for mayastor server.
//!
//! It is not expected to be used in production env but rather for testing and
//! debugging of the server.

#![warn(unused_extern_crates)]
#[macro_use]
extern crate clap;

use bytesize::ByteSize;
use clap::{App, AppSettings, Arg, ArgMatches, SubCommand};

use tonic::{transport::Channel, Code, Request, Status};

use rpc::service::mayastor_client::MayastorClient;

fn parse_share_protocol(pcol: Option<&str>) -> Result<i32, Status> {
    match pcol {
        None => Ok(rpc::mayastor::ShareProtocol::None as i32),
        Some("nvmf") => Ok(rpc::mayastor::ShareProtocol::Nvmf as i32),
        Some("iscsi") => Ok(rpc::mayastor::ShareProtocol::Iscsi as i32),
        Some("none") => Ok(rpc::mayastor::ShareProtocol::None as i32),
        Some(_) => Err(Status::new(
            Code::Internal,
            "Invalid value of share protocol".to_owned(),
        )),
    }
}

async fn create_pool(
    mut client: MayastorClient<Channel>,
    matches: &ArgMatches<'_>,
    verbose: bool,
) -> Result<(), Status> {
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

    let _ = client
        .create_pool(Request::new(rpc::mayastor::CreatePoolRequest {
            name,
            disks,
            block_size,
        }))
        .await?;

    Ok(())
}

async fn destroy_pool(
    mut client: MayastorClient<Channel>,
    matches: &ArgMatches<'_>,
    verbose: bool,
) -> Result<(), Status> {
    let name = matches.value_of("POOL").unwrap().to_owned();

    if verbose {
        println!("Destroying the pool {}", name);
    }

    client
        .destroy_pool(Request::new(rpc::mayastor::DestroyPoolRequest {
            name,
        }))
        .await?;

    Ok(())
}

async fn list_pools(
    mut client: MayastorClient<Channel>,
    verbose: bool,
    quiet: bool,
) -> Result<(), Status> {
    if verbose {
        println!("Requesting a list of pools");
    }

    let f = client
        .list_pools(Request::new(rpc::mayastor::Null {}))
        .await?;

    let pools = &f.get_ref().pools;

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
                match rpc::mayastor::PoolState::from_i32(p.state).unwrap() {
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
    Ok(())
}

async fn create_replica(
    mut client: MayastorClient<Channel>,
    matches: &ArgMatches<'_>,
    verbose: bool,
) -> Result<(), Status> {
    let pool = matches.value_of("POOL").unwrap().to_owned();
    let uuid = matches.value_of("UUID").unwrap().to_owned();
    let size = value_t!(matches.value_of("size"), u64).unwrap();
    let thin = matches.is_present("thin");
    let share = parse_share_protocol(matches.value_of("protocol"))?;

    if verbose {
        println!("Creating replica {} on pool {}", uuid, pool);
    }

    let resp = client
        .create_replica(Request::new(rpc::mayastor::CreateReplicaRequest {
            uuid,
            pool,
            thin,
            share,
            size: size * (1024 * 1024),
        }))
        .await?;
    println!("{}", resp.get_ref().uri);
    Ok(())
}

async fn destroy_replica(
    mut client: MayastorClient<Channel>,
    matches: &ArgMatches<'_>,
    verbose: bool,
) -> Result<(), Status> {
    let uuid = matches.value_of("UUID").unwrap().to_owned();

    if verbose {
        println!("Destroying replica {}", uuid);
    }

    client
        .destroy_replica(Request::new(rpc::mayastor::DestroyReplicaRequest {
            uuid,
        }))
        .await?;
    Ok(())
}

async fn share_replica(
    mut client: MayastorClient<Channel>,
    matches: &ArgMatches<'_>,
    verbose: bool,
) -> Result<(), Status> {
    let uuid = matches.value_of("UUID").unwrap().to_owned();
    let share = parse_share_protocol(matches.value_of("PROTOCOL"))?;

    if verbose {
        println!("Sharing replica {}", uuid);
    }

    let resp = client
        .share_replica(Request::new(rpc::mayastor::ShareReplicaRequest {
            uuid,
            share,
        }))
        .await?;
    println!("{}", resp.get_ref().uri);
    Ok(())
}

async fn list_replicas(
    mut client: MayastorClient<Channel>,
    verbose: bool,
    quiet: bool,
) -> Result<(), Status> {
    if verbose {
        println!("Requesting a list of replicas");
    }

    let resp = client
        .list_replicas(Request::new(rpc::mayastor::Null {}))
        .await?;

    let replicas = &resp.get_ref().replicas;

    if replicas.is_empty() && !quiet {
        println!("No replicas have been created");
    } else {
        if !quiet {
            println!(
                "{: <15} {: <36} {: <8} {: <8} {: <10} URI",
                "POOL", "NAME", "THIN", "SHARE", "SIZE",
            );
        }
        for r in replicas {
            println!(
                "{: <15} {: <36} {: <8} {: <8} {: <10} {}",
                r.pool,
                r.uuid,
                r.thin,
                match rpc::mayastor::ShareProtocol::from_i32(r.share) {
                    Some(rpc::mayastor::ShareProtocol::None) => {
                        "none"
                    }
                    Some(rpc::mayastor::ShareProtocol::Nvmf) => {
                        "nvmf"
                    }
                    Some(rpc::mayastor::ShareProtocol::Iscsi) => {
                        "iscsi"
                    }
                    Some(rpc::mayastor::ShareProtocol::Nbd) => {
                        "nbd"
                    }
                    None => "unknown",
                },
                ByteSize::b(r.size).to_string_as(true),
                r.uri,
            );
        }
    }
    Ok(())
}

async fn stat_replicas(
    mut client: MayastorClient<Channel>,
    verbose: bool,
    quiet: bool,
) -> Result<(), Status> {
    if verbose {
        println!("Requesting replicas stats");
    }

    let resp = client
        .stat_replicas(Request::new(rpc::mayastor::Null {}))
        .await?;
    let replicas = &resp.get_ref().replicas;

    if replicas.is_empty() && !quiet {
        println!("No replicas have been created");
    } else {
        if !quiet {
            println!(
                "{: <20} {: <36} {: >10} {: >10} {: >10} {: >10}",
                "POOL", "NAME", "RDCNT", "WRCNT", "RDBYTES", "WRBYTES",
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
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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
                                .help("Name of a protocol (nvmf, iscsi) used for sharing the replica (default none)")
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
                .subcommand(
                    SubCommand::with_name("share")
                        .about("Share or unshare replica")
                        .arg(
                            Arg::with_name("UUID")
                                .help("Replica uuid")
                                .required(true)
                                .index(1),
                        )
                        .arg(
                            Arg::with_name("PROTOCOL")
                                .help("Replica uuid")
                                .help("Name of a protocol (nvmf, iscsi) used for sharing or \"none\" to unshare the replica")
                                .required(true)
                                .index(2),
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

    let uri = format!("http://{}", endpoint);

    let client = MayastorClient::connect(uri).await?;

    match matches.subcommand() {
        ("pool", Some(m)) => match m.subcommand() {
            ("create", Some(m)) => create_pool(client, &m, verbose).await?,
            ("list", Some(_m)) => list_pools(client, verbose, quiet).await?,
            ("destroy", Some(m)) => destroy_pool(client, &m, quiet).await?,
            _ => {}
        },
        ("replica", Some(m)) => match m.subcommand() {
            ("create", Some(matches)) => {
                create_replica(client, matches, verbose).await?
            }
            ("destroy", Some(matches)) => {
                destroy_replica(client, matches, verbose).await?
            }
            ("share", Some(matches)) => {
                share_replica(client, matches, verbose).await?
            }
            ("list", Some(_matches)) => {
                list_replicas(client, verbose, quiet).await?
            }
            ("stats", Some(_matches)) => {
                stat_replicas(client, verbose, quiet).await?
            }
            _ => {}
        },

        _ => {}
    };

    Ok(())
}
