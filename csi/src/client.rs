//! gRPC client implementation for mayastor server.
//!
//! It is not expected to be used in production env but rather for testing and
//! debugging of the server.

#![warn(unused_extern_crates)]
#[macro_use]
extern crate clap;

use byte_unit::Byte;
use clap::{App, AppSettings, Arg, ArgMatches, SubCommand};
use tonic::{transport::Channel, Code, Status};

use ::rpc::{mayastor as rpc, service::mayastor_client::MayastorClient};

type MayaClient = MayastorClient<Channel>;

fn parse_replica_protocol(pcol: Option<&str>) -> Result<i32, Status> {
    match pcol {
        None => Ok(rpc::ShareProtocolReplica::ReplicaNone as i32),
        Some("nvmf") => Ok(rpc::ShareProtocolReplica::ReplicaNvmf as i32),
        Some("iscsi") => Ok(rpc::ShareProtocolReplica::ReplicaIscsi as i32),
        Some("none") => Ok(rpc::ShareProtocolReplica::ReplicaNone as i32),
        Some(_) => Err(Status::new(
            Code::Internal,
            "Invalid value of share protocol".to_owned(),
        )),
    }
}

fn replica_protocol_to_str(idx: i32) -> &'static str {
    match rpc::ShareProtocolReplica::from_i32(idx) {
        Some(rpc::ShareProtocolReplica::ReplicaNone) => "none",
        Some(rpc::ShareProtocolReplica::ReplicaNvmf) => "nvmf",
        Some(rpc::ShareProtocolReplica::ReplicaIscsi) => "iscsi",
        None => "unknown",
    }
}

fn pool_state_to_str(idx: i32) -> &'static str {
    match rpc::PoolState::from_i32(idx).unwrap() {
        rpc::PoolState::PoolUnknown => "unknown",
        rpc::PoolState::PoolOnline => "online",
        rpc::PoolState::PoolDegraded => "degraded",
        rpc::PoolState::PoolFaulted => "faulted",
    }
}

pub(crate) fn parse_size(src: &str) -> Result<Byte, String> {
    Byte::from_str(src).map_err(|_| src.to_string())
}

struct Context {
    client: MayaClient,
    verbosity: u64,
    units: char,
}

impl Context {
    fn v1(&self, s: &str) {
        if self.verbosity > 0 {
            println!("{}", s)
        }
    }
    fn v2(&self, s: &str) {
        if self.verbosity > 1 {
            println!("{}", s)
        }
    }
    fn units(&self, n: Byte) -> String {
        match self.units {
            'i' => n.get_appropriate_unit(true).to_string(),
            'd' => n.get_appropriate_unit(false).to_string(),
            _ => n.get_bytes().to_string(),
        }
    }
}

//
// POOL
//

async fn pool_create(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> Result<(), Status> {
    let name = matches.value_of("pool").unwrap().to_owned();
    let disks = matches
        .values_of("disk")
        .unwrap()
        .map(|dev| dev.to_owned())
        .collect();
    let block_size = value_t!(matches.value_of("block-size"), u32).unwrap_or(0);
    let io_if = match matches.value_of("io-if") {
        None | Some("auto") => Ok(rpc::PoolIoIf::PoolIoAuto as i32),
        Some("aio") => Ok(rpc::PoolIoIf::PoolIoAio as i32),
        Some("uring") => Ok(rpc::PoolIoIf::PoolIoUring as i32),
        Some(_) => Err(Status::new(
            Code::Internal,
            "Invalid value of I/O interface".to_owned(),
        )),
    }?;

    ctx.v2(&format!("Creating pool {}", name));
    ctx.client
        .create_pool(rpc::CreatePoolRequest {
            name: name.clone(),
            disks,
            block_size,
            io_if,
        })
        .await?;
    ctx.v1(&format!("Created pool {}", name));
    Ok(())
}

async fn pool_destroy(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> Result<(), Status> {
    let name = matches.value_of("pool").unwrap().to_owned();

    ctx.v2(&format!("Destroying pool {}", name));
    ctx.client
        .destroy_pool(rpc::DestroyPoolRequest {
            name: name.clone(),
        })
        .await?;
    ctx.v1(&format!("Destroyed pool {}", name));
    Ok(())
}

async fn pool_list(
    mut ctx: Context,
    _matches: &ArgMatches<'_>,
) -> Result<(), Status> {
    ctx.v2("Requesting a list of pools");

    let reply = ctx.client.list_pools(rpc::Null {}).await?;
    let pools = &reply.get_ref().pools;
    if pools.is_empty() {
        ctx.v1("No pools found");
        return Ok(());
    }

    ctx.v1(&format!(
        "{: <20} {: <8} {: >12} {: >12}   DISKS",
        "NAME", "STATE", "CAPACITY", "USED"
    ));

    for p in pools {
        let cap = Byte::from_bytes(p.capacity.into());
        let used = Byte::from_bytes(p.used.into());
        let state = pool_state_to_str(p.state);
        print!(
            "{: <20} {: <8} {: >12} {: >12}  ",
            p.name,
            state,
            ctx.units(cap),
            ctx.units(used)
        );
        for disk in &p.disks {
            print!(" {}", disk);
        }
        println!();
    }
    Ok(())
}

//
// NEXUS
//

async fn nexus_create(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> Result<(), Status> {
    let uuid = matches.value_of("uuid").unwrap().to_string();
    let size = parse_size(matches.value_of("size").unwrap())
        .map_err(|s| Status::invalid_argument(format!("Bad size '{}'", s)))?;
    let children = matches
        .value_of("children")
        .unwrap()
        .split_whitespace()
        .map(|c| c.to_string())
        .collect::<Vec<String>>();

    ctx.v2(&format!(
        "Creating nexus {} of size {} ",
        uuid,
        ctx.units(size)
    ));
    ctx.v2(&format!(" with children {:?}", children));
    let size = size.get_bytes() as u64;
    ctx.client
        .create_nexus(rpc::CreateNexusRequest {
            uuid: uuid.clone(),
            size,
            children,
        })
        .await?;
    ctx.v1(&format!("Nexus {} created", uuid));
    Ok(())
}

async fn nexus_destroy(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> Result<(), Status> {
    let uuid = matches.value_of("uuid").unwrap().to_string();

    ctx.v2(&format!("Destroying nexus {}", uuid));
    ctx.client
        .destroy_nexus(rpc::DestroyNexusRequest {
            uuid: uuid.clone(),
        })
        .await?;
    ctx.v1(&format!("Nexus {} destroyed", uuid));
    Ok(())
}

async fn nexus_list(
    mut ctx: Context,
    _matches: &ArgMatches<'_>,
) -> Result<(), Status> {
    let list = ctx.client.list_nexus(rpc::Null {}).await?;

    ctx.v2("Found following nexus:");
    list.into_inner().nexus_list.into_iter().for_each(|n| {
        println!("{:?}", n);
    });
    Ok(())
}

async fn nexus_publish(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> Result<(), Status> {
    let uuid = matches.value_of("uuid").unwrap().to_string();
    let key = matches.value_of("key").unwrap_or("").to_string();
    let prot = match matches.value_of("protocol") {
        None => rpc::ShareProtocolNexus::NexusNbd,
        Some("nvmf") => rpc::ShareProtocolNexus::NexusNvmf,
        Some("iscsi") => rpc::ShareProtocolNexus::NexusIscsi,
        Some(_) => {
            return Err(Status::new(
                Code::Internal,
                "Invalid value of share protocol".to_owned(),
            ))
        }
    };

    ctx.v2(&format!("Publishing nexus {} over {:?}", uuid, prot));
    let path = ctx
        .client
        .publish_nexus(rpc::PublishNexusRequest {
            uuid,
            key,
            share: prot.into(),
        })
        .await?;
    ctx.v1(&format!("Nexus published at {:?}", path));
    Ok(())
}

async fn nexus_unpublish(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> Result<(), Status> {
    let uuid = matches.value_of("uuid").unwrap().to_string();

    ctx.v2(&format!("Unpublishing nexus {}", uuid));
    ctx.client
        .unpublish_nexus(rpc::UnpublishNexusRequest {
            uuid: uuid.clone(),
        })
        .await?;
    ctx.v1(&format!("Nexus {} unpublished", uuid));
    Ok(())
}

async fn nexus_add(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> Result<(), Status> {
    let uuid = matches.value_of("uuid").unwrap().to_string();
    let uri = matches.value_of("uri").unwrap().to_string();
    let rebuild = matches.value_of("rebuild").unwrap().parse::<bool>().unwrap_or(true);

    ctx.v2(&format!("Adding {} to children of {}", uri, uuid));
    ctx.client
        .add_child_nexus(rpc::AddChildNexusRequest {
            uuid: uuid.clone(),
            uri: uri.clone(),
            rebuild: rebuild,
        })
        .await?;
    ctx.v1(&format!("Added {} to children of {}", uri, uuid));
    Ok(())
}

async fn nexus_remove(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> Result<(), Status> {
    let uuid = matches.value_of("uuid").unwrap().to_string();
    let uri = matches.value_of("uri").unwrap().to_string();

    ctx.v2(&format!("Removing {} from children of {}", uri, uuid));
    ctx.client
        .remove_child_nexus(rpc::RemoveChildNexusRequest {
            uuid: uuid.clone(),
            uri: uri.clone(),
        })
        .await?;
    ctx.v2(&format!("Removed {} from children of {}", uri, uuid));
    Ok(())
}

//
// REPLICA
//

async fn replica_create(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> Result<(), Status> {
    let pool = matches.value_of("pool").unwrap().to_owned();
    let uuid = matches.value_of("uuid").unwrap().to_owned();
    let size = parse_size(matches.value_of("size").unwrap())
        .map_err(|s| Status::invalid_argument(format!("Bad size '{}'", s)))?;
    let thin = matches.is_present("thin");
    let share = parse_replica_protocol(matches.value_of("protocol"))?;

    ctx.v2(&format!("Creating replica {} on pool {}", uuid, pool));
    let rq = rpc::CreateReplicaRequest {
        uuid,
        pool,
        thin,
        share,
        size: size.get_bytes() as u64,
    };
    let resp = ctx.client.create_replica(rq).await?;
    ctx.v1(&format!("Created {}", resp.get_ref().uri));
    Ok(())
}

async fn replica_destroy(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> Result<(), Status> {
    let uuid = matches.value_of("uuid").unwrap().to_owned();

    ctx.v2(&format!("Destroying replica {}", uuid));
    ctx.client
        .destroy_replica(rpc::DestroyReplicaRequest {
            uuid,
        })
        .await?;
    Ok(())
}

async fn replica_share(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> Result<(), Status> {
    let uuid = matches.value_of("uuid").unwrap().to_owned();
    let share = parse_replica_protocol(matches.value_of("protocol"))?;

    ctx.v2(&format!("Sharing replica {} on {}", uuid, share));

    let resp = ctx
        .client
        .share_replica(rpc::ShareReplicaRequest {
            uuid,
            share,
        })
        .await?;
    ctx.v1(&format!("Shared {}", resp.get_ref().uri));
    Ok(())
}

async fn replica_list(
    mut ctx: Context,
    _matches: &ArgMatches<'_>,
) -> Result<(), Status> {
    ctx.v2("Requesting a list of replicas");

    let resp = ctx.client.list_replicas(rpc::Null {}).await?;
    let replicas = &resp.get_ref().replicas;
    if replicas.is_empty() {
        ctx.v1("No replicas have been created");
        return Ok(());
    }

    ctx.v1(&format!(
        "{: <15} {: <36} {: <8} {: <8} {: <10} URI",
        "POOL", "NAME", "THIN", "SHARE", "SIZE"
    ));

    for r in replicas {
        let proto = replica_protocol_to_str(r.share);
        let size = ctx.units(Byte::from_bytes(r.size.into()));
        println!(
            "{: <15} {: <36} {: <8} {: <8} {: <10} {}",
            r.pool, r.uuid, r.thin, proto, size, r.uri
        );
    }
    Ok(())
}

async fn replica_stat(
    mut ctx: Context,
    _matches: &ArgMatches<'_>,
) -> Result<(), Status> {
    ctx.v2("Requesting replicas stats");

    let resp = ctx.client.stat_replicas(rpc::Null {}).await?;
    let replicas = &resp.get_ref().replicas;
    if replicas.is_empty() {
        ctx.v1("No replicas have been created");
    }

    ctx.v1(&format!(
        "{: <20} {: <36} {: >10} {: >10} {: >10} {: >10}",
        "POOL", "NAME", "RDCNT", "WRCNT", "RDBYTES", "WRBYTES"
    ));

    for r in replicas {
        let stats = r.stats.as_ref().unwrap();
        let read = ctx.units(Byte::from_bytes(stats.bytes_read.into()));
        let written = ctx.units(Byte::from_bytes(stats.bytes_written.into()));
        println!(
            "{: <20} {: <36} {: >10} {: >10} {: >10} {: >10}",
            r.pool,
            r.uuid,
            stats.num_read_ops,
            stats.num_write_ops,
            read,
            written
        );
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pools_subcommand = {
        let create = SubCommand::with_name("create")
            .about("Create storage pool")
            .arg(
                Arg::with_name("block-size")
                    .short("b")
                    .long("block-size")
                    .value_name("NUMBER")
                    .help("block size of the underlying devices"),
            )
            .arg(
                Arg::with_name("io-if")
                    .short("i")
                    .long("io-if")
                    .value_name("IF")
                    .help("I/O interface for the underlying devices"),
            )
            .arg(
                Arg::with_name("pool")
                    .required(true)
                    .index(1)
                    .help("Storage pool name"),
            )
            .arg(
                Arg::with_name("disk")
                    .required(true)
                    .multiple(true)
                    .index(2)
                    .help("Disk device files"),
            );
        let destroy = SubCommand::with_name("destroy")
            .about("Destroy storage pool")
            .arg(
                Arg::with_name("pool")
                    .required(true)
                    .index(1)
                    .help("Storage pool name"),
            );
        SubCommand::with_name("pool")
            .about("Storage pool management")
            .subcommand(create)
            .subcommand(destroy)
            .subcommand(
                SubCommand::with_name("list").about("List storage pools"),
            )
    };

    let nexus_subcommand = {
        let create = SubCommand::with_name("create")
            .about("create a new nexus device")
            .arg(
                Arg::with_name("uuid")
                    .required(true)
                    .index(1)
                    .help("uuid for the nexus"),
            )
            .arg(
                Arg::with_name("size")
                    .required(true)
                    .index(2)
                    .help("size in mb"),
            )
            .arg(
                Arg::with_name("children")
                    .required(true)
                    .multiple(true)
                    .index(3)
                    .help("list of children to add"),
            );
        let destroy = SubCommand::with_name("destroy")
            .about("destroy the nexus with given name")
            .arg(
                Arg::with_name("uuid")
                    .required(true)
                    .index(1)
                    .help("uuid for the nexus"),
            );
        let publish = SubCommand::with_name("publish").about("publish the nexus")
            .arg(Arg::with_name("protocol").short("p").long("protocol").value_name("PROTOCOL")
                .help("Name of a protocol (nvmf, iscsi) used for publishing the nexus remotely"))
            .arg(Arg::with_name("uuid").required(true).index(1)
                .help("uuid for the nexus"))
            .arg(Arg::with_name("key").required(false).index(2)
                .help("crypto key to use"));
        let add = SubCommand::with_name("add")
            .about("add a child")
            .arg(
                Arg::with_name("uuid")
                    .required(true)
                    .index(1)
                    .help("uuid for the nexus"),
            )
            .arg(
                Arg::with_name("uri")
                    .required(true)
                    .index(2)
                    .help("uri of child to add"),
            );
        let remove = SubCommand::with_name("remove")
            .about("remove a child")
            .arg(
                Arg::with_name("uuid")
                    .required(true)
                    .index(1)
                    .help("uuid for the nexus"),
            )
            .arg(
                Arg::with_name("uri")
                    .required(true)
                    .index(2)
                    .help("uri of child to remove"),
            );
        SubCommand::with_name("nexus")
            .about("nexus management")
            .subcommand(create)
            .subcommand(destroy)
            .subcommand(publish)
            .subcommand(add)
            .subcommand(remove)
            .subcommand(
                SubCommand::with_name("list").about("list all nexus devices"),
            )
            .subcommand(
                SubCommand::with_name("unpublish").about("unpublish a nexus"),
            )
    };

    let replica_subcommand = {
        let create = SubCommand::with_name("create").about("Create replica on pool")
            .arg(Arg::with_name("pool").required(true).index(1)
                .help("Storage pool name"))
            .arg(Arg::with_name("uuid").required(true).index(2)
                .help("Unique replica uuid"))
            .arg(Arg::with_name("protocol").short("p").long("protocol")
                .takes_value(true).value_name("PROTOCOL")
                .help("Name of a protocol (nvmf, iscsi) used for sharing the replica (default none)"))
            .arg(Arg::with_name("size").short("s").long("size")
                .takes_value(true).required(true).value_name("NUMBER")
                .help("Size of the replica"))
            .arg(Arg::with_name("thin").short("t").long("thin").takes_value(false)
                .help("Whether replica is thin provisioned (default false)"));
        let destroy = SubCommand::with_name("destroy")
            .about("Destroy replica")
            .arg(
                Arg::with_name("uuid")
                    .required(true)
                    .index(1)
                    .help("Replica uuid"),
            );
        let share = SubCommand::with_name("share").about("Share or unshare replica")
            .arg(Arg::with_name("uuid").required(true).index(1)
                .help("Replica uuid"))
            .arg(Arg::with_name("protocol").required(true).index(2)
                .help("Name of a protocol (nvmf, iscsi) used for sharing or \"none\" to unshare the replica"));
        SubCommand::with_name("replica")
            .about("Replica management")
            .subcommand(create)
            .subcommand(destroy)
            .subcommand(share)
            .subcommand(SubCommand::with_name("list").about("List replicas"))
            .subcommand(
                SubCommand::with_name("stats").about("IO stats of replicas"),
            )
    };

    let matches = App::new("Mayastor gRPC client")
        .version("0.1")
        .settings(&[AppSettings::SubcommandRequiredElseHelp,
                  AppSettings::ColoredHelp, AppSettings::ColorAlways])
        .about("Client for mayastor gRPC server")
        .arg(Arg::with_name("address").short("a").long("address")
            .default_value("127.0.0.1").value_name("HOST")
            .help("IP address of mayastor server"))
        .arg(Arg::with_name("port").short("p").long("port")
            .default_value("10124").value_name("NUMBER")
            .help("Port number of mayastor server"))
        .arg(Arg::with_name("quiet").short("q").long("quiet")
            .help("Do not print any output except for list records"))
        .arg(Arg::with_name("verbose").short("v").long("verbose").multiple(true)
            .help("Verbose output").conflicts_with("quiet"))
        .arg(Arg::with_name("units").short("u").long("units")
            .value_name("BASE").possible_values(&["i","d"])
            .hide_possible_values(true).next_line_help(true)
            .help("Output with large units: i for kiB, etc. or d for kB, etc."))
        .subcommand(pools_subcommand)
        .subcommand(nexus_subcommand)
        .subcommand(replica_subcommand)
        .get_matches();

    let ctx = {
        let verbosity = if matches.is_present("quiet") {
            0
        } else {
            matches.occurrences_of("verbose") + 1
        };
        let units = matches
            .value_of("units")
            .and_then(|u| u.chars().next())
            .unwrap_or('b');
        let endpoint = {
            let addr = matches.value_of("address").unwrap_or("127.0.0.1");
            let port = value_t!(matches.value_of("port"), u16).unwrap_or(10124);
            format!("{}:{}", addr, port)
        };
        let uri = format!("http://{}", endpoint);
        if verbosity > 1 {
            println!("Connecting to {}", uri);
        }
        let client = MayaClient::connect(uri).await?;
        Context {
            client,
            verbosity,
            units,
        }
    };

    match matches.subcommand() {
        ("pool", Some(m)) => match m.subcommand() {
            ("create", Some(m)) => pool_create(ctx, &m).await?,
            ("list", Some(m)) => pool_list(ctx, &m).await?,
            ("destroy", Some(m)) => pool_destroy(ctx, &m).await?,
            _ => {}
        },

        ("nexus", Some(m)) => match m.subcommand() {
            ("create", Some(m)) => nexus_create(ctx, &m).await?,
            ("destroy", Some(m)) => nexus_destroy(ctx, &m).await?,
            ("list", Some(m)) => nexus_list(ctx, &m).await?,
            ("publish", Some(m)) => nexus_publish(ctx, &m).await?,
            ("unpublish", Some(m)) => nexus_unpublish(ctx, &m).await?,
            ("add", Some(m)) => nexus_add(ctx, &m).await?,
            ("remove", Some(m)) => nexus_remove(ctx, &m).await?,
            _ => {}
        },

        ("replica", Some(m)) => match m.subcommand() {
            ("create", Some(m)) => replica_create(ctx, &m).await?,
            ("destroy", Some(m)) => replica_destroy(ctx, &m).await?,
            ("share", Some(m)) => replica_share(ctx, &m).await?,
            ("list", Some(m)) => replica_list(ctx, &m).await?,
            ("stats", Some(m)) => replica_stat(ctx, &m).await?,
            _ => {}
        },

        _ => eprintln!("Internal Error: Not implemented"),
    };
    Ok(())
}
