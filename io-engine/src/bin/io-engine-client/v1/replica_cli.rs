use crate::{
    context::{Context, OutputFormat},
    parse_size,
    ClientError,
    GrpcStatus,
};
use byte_unit::Byte;
use clap::{Arg, ArgMatches, Command};
use colored_json::ToColoredJson;
use io_engine_api::{v0 as rpc, v1 as v1_rpc};
use snafu::ResultExt;
use std::convert::TryFrom;
use tonic::{Code, Status};

pub fn subcommands() -> Command {
    let create = Command::new("create")
        .about("Create replica on pool")
        .arg(
            Arg::new("name")
                .required(true).index(1)
                .help("Replica name"))
        .arg(
            Arg::new("uuid")
                .required(true).index(2)
                .help("Unique replica uuid"))
        .arg(
            Arg::new("pooluuid")
                .required(true)
                .index(3)
                .help("Storage pool name or UUID"))
        .arg(
            Arg::new("size")
                .short('s')
                .long("size")
                .required(true)
                .value_name("NUMBER")
                .help("Size of the replica"))
        .arg(
            Arg::new("protocol")
                .short('p')
                .long("protocol")

                .value_name("PROTOCOL")
                .help("Name of a protocol (nvmf) used for sharing the replica (default none)"))
        .arg(
            Arg::new("thin")
                .short('t')
                .long("thin")
                .action(clap::ArgAction::SetTrue)
                .help("Whether replica is thin provisioned (default false)"))
        .arg(
            Arg::new("allowed-host")
                .long("allowed-host")

                .action(clap::ArgAction::Append)
                .required(false)
                .help(
                    "NQN of hosts which are allowed to connect to the target",
                ),
        );

    let destroy = Command::new("destroy")
        .about("Destroy replica")
        .arg(
            Arg::new("uuid")
                .required(true)
                .index(1)
                .help("Replica uuid"),
        )
        .arg(
            Arg::new("pool-uuid")
                .long("pool-uuid")
                .required(false)
                .conflicts_with("pool-name")
                .help("Uuid of the pool where replica resides"),
        )
        .arg(
            Arg::new("pool-name")
                .long("pool-name")
                .required(false)
                .conflicts_with("pool-uuid")
                .help("Name of the pool where replica resides"),
        );

    let share = Command::new("share").about("Share replica over specified protocol")
        .arg(
            Arg::new("uuid")
                .required(true)
                .index(1)
                .help("Replica uuid"))
        .arg(
            Arg::new("protocol")
                .required(true)
                .index(2)
                .help("Name of a protocol (nvmf) used for sharing or \"none\" to unshare the replica"))
        .arg(
            Arg::new("allowed-host")
                .long("allowed-host")

                .action(clap::ArgAction::Append)
                .required(false)
                .help("Name of a protocol (nvmf) used for sharing or \"none\" to unshare the replica"));
    let unshare = Command::new("unshare").about("Unshare replica").arg(
        Arg::new("uuid")
            .required(true)
            .index(1)
            .help("Replica uuid"),
    );
    let resize = Command::new("resize")
        .about("Resize replica")
        .arg(
            Arg::new("uuid")
                .required(true)
                .index(1)
                .help("Replica uuid"),
        )
        .arg(
            Arg::new("size")
                .required(true)
                .index(2)
                .help("Requested new size of the replica"),
        );
    Command::new("replica")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .about("Replica management")
        .subcommand(create)
        .subcommand(destroy)
        .subcommand(share)
        .subcommand(unshare)
        .subcommand(resize)
        .subcommand(Command::new("list").about("List replicas"))
        .subcommand(Command::new("stats").about("IO stats of replicas"))
}

pub async fn handler(ctx: Context, matches: &ArgMatches) -> crate::Result<()> {
    match matches.subcommand().unwrap() {
        ("create", args) => replica_create(ctx, args).await,
        ("destroy", args) => replica_destroy(ctx, args).await,
        ("list", args) => replica_list(ctx, args).await,
        ("share", args) => replica_share(ctx, args).await,
        ("unshare", args) => replica_unshare(ctx, args).await,
        ("resize", args) => replica_resize(ctx, args).await,
        ("stats", args) => replica_stat(ctx, args).await,
        (cmd, _) => {
            Err(Status::not_found(format!("command {cmd} does not exist")))
                .context(GrpcStatus)
        }
    }
}

async fn replica_create(
    mut ctx: Context,
    matches: &ArgMatches,
) -> crate::Result<()> {
    let name = matches
        .get_one::<String>("name")
        .ok_or_else(|| ClientError::MissingValue {
            field: "name".to_string(),
        })?
        .to_owned();
    let uuid = matches
        .get_one::<String>("uuid")
        .ok_or_else(|| ClientError::MissingValue {
            field: "uuid".to_string(),
        })?
        .to_owned();
    let pooluuid = matches
        .get_one::<String>("pooluuid")
        .ok_or_else(|| ClientError::MissingValue {
            field: "pool".to_string(),
        })?
        .to_owned();
    let size =
        parse_size(matches.get_one::<String>("size").ok_or_else(|| {
            ClientError::MissingValue {
                field: "size".to_string(),
            }
        })?)
        .map_err(|s| Status::invalid_argument(format!("Bad size '{s}'")))
        .context(GrpcStatus)?;
    let thin = matches.get_flag("thin");
    let share = parse_replica_protocol(matches.get_one::<String>("protocol"))
        .context(GrpcStatus)?;
    let allowed_hosts = matches
        .get_many::<String>("allowed-host")
        .unwrap_or_default()
        .cloned()
        .collect();

    let request = v1_rpc::replica::CreateReplicaRequest {
        name,
        uuid: uuid.clone(),
        entity_id: None,
        pooluuid,
        thin,
        share,
        size: size.get_bytes() as u64,
        allowed_hosts,
    };

    let response = ctx
        .v1
        .replica
        .create_replica(request)
        .await
        .context(GrpcStatus)?;

    match ctx.output {
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&response.get_ref())
                    .unwrap()
                    .to_colored_json_auto()
                    .unwrap()
            );
        }
        OutputFormat::Default => {
            println!("{}", &response.get_ref().uri);
        }
    };

    Ok(())
}

async fn replica_destroy(
    mut ctx: Context,
    matches: &ArgMatches,
) -> crate::Result<()> {
    let uuid = matches
        .get_one::<String>("uuid")
        .ok_or_else(|| ClientError::MissingValue {
            field: "uuid".to_string(),
        })?
        .to_owned();

    let pool = match matches.get_one::<String>("pool-uuid") {
        Some(uuid) => {
            Some(v1_rpc::replica::destroy_replica_request::Pool::PoolUuid(
                uuid.to_string(),
            ))
        }
        None => matches.get_one::<String>("pool-name").map(|name| {
            v1_rpc::replica::destroy_replica_request::Pool::PoolName(
                name.to_string(),
            )
        }),
    };

    let _ = ctx
        .v1
        .replica
        .destroy_replica(v1_rpc::replica::DestroyReplicaRequest {
            uuid: uuid.clone(),
            pool,
        })
        .await
        .context(GrpcStatus)?;

    match ctx.output {
        OutputFormat::Json => {}
        OutputFormat::Default => {
            println!("replica: {} is deleted", &uuid);
        }
    }

    Ok(())
}

async fn replica_list(
    mut ctx: Context,
    _matches: &ArgMatches,
) -> crate::Result<()> {
    let response = ctx
        .v1
        .replica
        .list_replicas(v1_rpc::replica::ListReplicaOptions {
            name: None,
            poolname: None,
            uuid: None,
            pooluuid: None,
            query: None,
        })
        .await
        .context(GrpcStatus)?;

    match ctx.output {
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&response.get_ref())
                    .unwrap()
                    .to_colored_json_auto()
                    .unwrap()
            );
        }
        OutputFormat::Default => {
            let replicas = &response.get_ref().replicas;
            if replicas.is_empty() {
                ctx.v1("No replicas found");
                return Ok(());
            }

            let table = replicas
                .iter()
                .map(|r| {
                    let usage = r.usage.as_ref().unwrap();
                    let proto = replica_protocol_to_str(r.share);
                    let size = ctx.units(Byte::from_bytes(r.size.into()));
                    let capacity = ctx
                        .units(Byte::from_bytes(usage.capacity_bytes.into()));
                    let allocated = ctx
                        .units(Byte::from_bytes(usage.allocated_bytes.into()));
                    vec![
                        r.poolname.clone(),
                        r.name.clone(),
                        r.uuid.clone(),
                        r.thin.to_string(),
                        proto.to_string(),
                        size,
                        capacity,
                        allocated,
                        r.uri.clone(),
                        r.is_snapshot.to_string(),
                        r.is_clone.to_string(),
                        usage.allocated_bytes_snapshots.to_string(),
                        usage
                            .allocated_bytes_snapshot_from_clone
                            .unwrap_or_default()
                            .to_string(),
                    ]
                })
                .collect();
            ctx.print_list(
                vec![
                    "POOL",
                    "NAME",
                    "UUID",
                    ">THIN",
                    ">SHARE",
                    ">SIZE",
                    ">CAP",
                    ">ALLOC",
                    "URI",
                    "IS_SNAPSHOT",
                    "IS_CLONE",
                    "SNAP_ANCESTOR_SIZE",
                    "CLONE_SNAP_ANCESTOR_SIZE",
                ],
                table,
            );
        }
    };

    Ok(())
}

async fn replica_share(
    mut ctx: Context,
    matches: &ArgMatches,
) -> crate::Result<()> {
    let uuid = matches.get_one::<String>("uuid").unwrap().to_owned();
    let share = parse_replica_protocol(matches.get_one::<String>("protocol"))
        .context(GrpcStatus)?;
    let allowed_hosts = matches
        .get_many::<String>("allowed-host")
        .unwrap_or_default()
        .cloned()
        .collect();

    let response = ctx
        .v1
        .replica
        .share_replica(v1_rpc::replica::ShareReplicaRequest {
            uuid,
            share,
            allowed_hosts,
        })
        .await
        .context(GrpcStatus)?;
    match ctx.output {
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&response.get_ref())
                    .unwrap()
                    .to_colored_json_auto()
                    .unwrap()
            );
        }
        OutputFormat::Default => {
            println!("{}", &response.get_ref().uri);
        }
    };
    Ok(())
}

async fn replica_unshare(
    mut ctx: Context,
    matches: &ArgMatches,
) -> crate::Result<()> {
    let uuid = matches.get_one::<String>("uuid").unwrap().to_owned();
    let response = ctx
        .v1
        .replica
        .unshare_replica(v1_rpc::replica::UnshareReplicaRequest {
            uuid,
        })
        .await
        .context(GrpcStatus)?;

    match ctx.output {
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&response.get_ref())
                    .unwrap()
                    .to_colored_json_auto()
                    .unwrap()
            );
        }
        OutputFormat::Default => {
            println!("{}", &response.get_ref().uri);
        }
    };

    Ok(())
}

async fn replica_resize(
    mut ctx: Context,
    matches: &ArgMatches,
) -> crate::Result<()> {
    let uuid = matches
        .get_one::<String>("uuid")
        .ok_or_else(|| ClientError::MissingValue {
            field: "uuid".to_string(),
        })?
        .to_owned();

    let requested_size =
        parse_size(matches.get_one::<String>("size").ok_or_else(|| {
            ClientError::MissingValue {
                field: "size".to_string(),
            }
        })?)
        .map_err(|s| Status::invalid_argument(format!("Bad size '{s}'")))
        .context(GrpcStatus)?;

    let _ = ctx
        .v1
        .replica
        .resize_replica(v1_rpc::replica::ResizeReplicaRequest {
            uuid: uuid.clone(),
            requested_size: requested_size.get_bytes() as u64,
        })
        .await
        .context(GrpcStatus)?;

    match ctx.output {
        OutputFormat::Json => {}
        OutputFormat::Default => {
            println!("replica {} is resized", &uuid);
        }
    }

    Ok(())
}

// TODO : There's no v1 rpc for stat.
async fn replica_stat(
    mut ctx: Context,
    _matches: &ArgMatches,
) -> crate::Result<()> {
    let response = ctx
        .client
        .stat_replicas(rpc::Null {})
        .await
        .context(GrpcStatus)?;

    match ctx.output {
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(response.get_ref())
                    .unwrap()
                    .to_colored_json_auto()
                    .unwrap()
            );
        }
        OutputFormat::Default => {
            let replicas = &response.get_ref().replicas;
            if replicas.is_empty() {
                ctx.v1("No replicas have been created");
                return Ok(());
            }

            let header =
                vec!["POOL", "NAME", "RDCNT", "WRCNT", "RDBYTES", "WRBYTES"];
            let table = replicas
                .iter()
                .map(|replica| {
                    let stats = replica.stats.as_ref().unwrap();
                    let read =
                        ctx.units(Byte::from_bytes(stats.bytes_read.into()));
                    let written =
                        ctx.units(Byte::from_bytes(stats.bytes_written.into()));
                    vec![
                        replica.pool.clone(),
                        replica.uuid.clone(),
                        stats.num_read_ops.to_string(),
                        stats.num_write_ops.to_string(),
                        read,
                        written,
                    ]
                })
                .collect();
            ctx.print_list(header, table);
        }
    };

    Ok(())
}

fn parse_replica_protocol(pcol: Option<&String>) -> Result<i32, Status> {
    match pcol.map(|s| s.as_str()) {
        None => Ok(v1_rpc::common::ShareProtocol::None as i32),
        Some("nvmf") => Ok(v1_rpc::common::ShareProtocol::Nvmf as i32),
        Some("none") => Ok(v1_rpc::common::ShareProtocol::None as i32),
        Some("iscsi") => Ok(v1_rpc::common::ShareProtocol::Iscsi as i32),
        Some(_) => Err(Status::new(
            Code::Internal,
            "Invalid value of share protocol".to_owned(),
        )),
    }
}

fn replica_protocol_to_str(idx: i32) -> &'static str {
    match v1_rpc::common::ShareProtocol::try_from(idx) {
        Ok(v1_rpc::common::ShareProtocol::None) => "none",
        Ok(v1_rpc::common::ShareProtocol::Nvmf) => "nvmf",
        Ok(v1_rpc::common::ShareProtocol::Iscsi) => "iscsi",
        Err(_) => "unknown",
    }
}
