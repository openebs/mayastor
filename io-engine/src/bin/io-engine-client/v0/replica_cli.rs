use crate::{
    context::{Context, OutputFormat},
    parse_size,
    ClientError,
    GrpcStatus,
};
use byte_unit::Byte;
use clap::{Arg, ArgMatches, Command};
use colored_json::ToColoredJson;
use mayastor_api::v0 as rpc;
use snafu::ResultExt;
use tonic::{Code, Status};

pub fn subcommands() -> Command {
    let create = Command::new("create")
        .about("Create replica on pool")
        .arg(
            Arg::new("pool")
                .required(true)
                .index(1)
                .help("Storage pool name"))
        .arg(
            Arg::new("name")
                .required(true).index(2)
                .help("Replica name"))

        .arg(
            Arg::new("protocol")
                .short('p')
                .long("protocol")

                .value_name("PROTOCOL")
                .help("Name of a protocol (nvmf) used for sharing the replica (default none)"))
        .arg(
            Arg::new("size")
                .short('s')
                .long("size")

                .required(true)
                .value_name("NUMBER")
                .help("Size of the replica"))
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

    let create_v2 = Command::new("create2")
        .about("Create replica on pool")
        .arg(
            Arg::new("pool")
                .required(true)
                .index(1)
                .help("Storage pool name"))
        .arg(
            Arg::new("name")
                .required(true)
                .index(2)
                .help("Replica name"))
        .arg(
            Arg::new("uuid")
                .required(true).index(3)
                .help("Unique replica uuid"))
        .arg(
            Arg::new("protocol")
                .short('p')
                .long("protocol")

                .value_name("PROTOCOL")
                .help("Name of a protocol (nvmf) used for sharing the replica (default none)"))
        .arg(
            Arg::new("size")
                .short('s')
                .long("size")

                .required(true)
                .value_name("NUMBER")
                .help("Size of the replica"))
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

    let destroy = Command::new("destroy").about("Destroy replica").arg(
        Arg::new("uuid")
            .required(true)
            .index(1)
            .help("Replica uuid"),
    );

    let share = Command::new("share").about("Share or unshare replica")
        .arg(
            Arg::new("name")
                .required(true)
                .index(1)
                .help("Replica name"))
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
                .help("NQN of hosts which are allowed to connect to the target"));

    Command::new("replica")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .about("Replica management")
        .subcommand(create)
        .subcommand(create_v2)
        .subcommand(destroy)
        .subcommand(share)
        .subcommand(Command::new("list").about("List replicas"))
        .subcommand(Command::new("list2").about("List replicas"))
        .subcommand(Command::new("stats").about("IO stats of replicas"))
}

pub async fn handler(ctx: Context, matches: &ArgMatches) -> crate::Result<()> {
    match matches.subcommand().unwrap() {
        ("create", args) => replica_create(ctx, args).await,
        ("create2", args) => replica_create_v2(ctx, args).await,
        ("destroy", args) => replica_destroy(ctx, args).await,
        ("list", args) => replica_list(ctx, args).await,
        ("list2", args) => replica_list2(ctx, args).await,
        ("share", args) => replica_share(ctx, args).await,
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
    let pool = matches
        .get_one::<String>("pool")
        .ok_or_else(|| ClientError::MissingValue {
            field: "pool".to_string(),
        })?
        .to_owned();
    let name = matches
        .get_one::<String>("name")
        .ok_or_else(|| ClientError::MissingValue {
            field: "name".to_string(),
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

    let rq = rpc::CreateReplicaRequest {
        uuid: name.clone(),
        pool,
        thin,
        share,
        size: size.get_bytes() as u64,
        allowed_hosts,
    };
    let response = ctx.client.create_replica(rq).await.context(GrpcStatus)?;

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

async fn replica_create_v2(
    mut ctx: Context,
    matches: &ArgMatches,
) -> crate::Result<()> {
    let pool = matches
        .get_one::<String>("pool")
        .ok_or_else(|| ClientError::MissingValue {
            field: "pool".to_string(),
        })?
        .to_owned();
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

    let rq = rpc::CreateReplicaRequestV2 {
        name,
        uuid: uuid.clone(),
        pool,
        thin,
        share,
        size: size.get_bytes() as u64,
        allowed_hosts,
    };
    let response =
        ctx.client.create_replica_v2(rq).await.context(GrpcStatus)?;

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

    let response = ctx
        .client
        .destroy_replica(rpc::DestroyReplicaRequest {
            uuid: uuid.clone(),
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
            println!("{}", &uuid);
        }
    };

    Ok(())
}

async fn replica_list(
    mut ctx: Context,
    _matches: &ArgMatches,
) -> crate::Result<()> {
    let response = ctx
        .client
        .list_replicas(rpc::Null {})
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
                    let proto = replica_protocol_to_str(r.share);
                    let size = ctx.units(Byte::from_bytes(r.size.into()));
                    vec![
                        r.pool.clone(),
                        r.uuid.clone(),
                        r.thin.to_string(),
                        proto.to_string(),
                        size,
                        r.uri.clone(),
                    ]
                })
                .collect();
            ctx.print_list(
                vec!["POOL", "NAME", ">THIN", ">SHARE", ">SIZE", "URI"],
                table,
            );
        }
    };

    Ok(())
}

async fn replica_list2(
    mut ctx: Context,
    _matches: &ArgMatches,
) -> crate::Result<()> {
    let response = ctx
        .client
        .list_replicas_v2(rpc::Null {})
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
                    let proto = replica_protocol_to_str(r.share);
                    let size = ctx.units(Byte::from_bytes(r.size.into()));
                    vec![
                        r.pool.clone(),
                        r.name.clone(),
                        r.uuid.clone(),
                        r.thin.to_string(),
                        proto.to_string(),
                        size,
                        r.uri.clone(),
                    ]
                })
                .collect();
            ctx.print_list(
                vec!["POOL", "NAME", "UUID", ">THIN", ">SHARE", ">SIZE", "URI"],
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
    let name = matches.get_one::<String>("name").unwrap().to_owned();
    let share = parse_replica_protocol(matches.get_one::<String>("protocol"))
        .context(GrpcStatus)?;
    let allowed_hosts = matches
        .get_many::<String>("allowed-host")
        .unwrap_or_default()
        .cloned()
        .collect();

    let response = ctx
        .client
        .share_replica(rpc::ShareReplicaRequest {
            uuid: name.clone(),
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
        None => Ok(rpc::ShareProtocolReplica::ReplicaNone as i32),
        Some("nvmf") => Ok(rpc::ShareProtocolReplica::ReplicaNvmf as i32),
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
