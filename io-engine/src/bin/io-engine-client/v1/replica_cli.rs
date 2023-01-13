use crate::{
    context::{Context, OutputFormat},
    parse_size,
    ClientError,
    GrpcStatus,
};
use byte_unit::Byte;
use clap::{App, AppSettings, Arg, ArgMatches, SubCommand};
use colored_json::ToColoredJson;
use mayastor_api::{v0 as rpc, v1 as v1_rpc};
use snafu::ResultExt;
use tonic::{Code, Status};

pub fn subcommands<'a, 'b>() -> App<'a, 'b> {
    let create = SubCommand::with_name("create")
        .about("Create replica on pool")
        .arg(
            Arg::with_name("name")
                .required(true).index(1)
                .help("Replica name"))
        .arg(
            Arg::with_name("uuid")
                .required(true).index(2)
                .help("Unique replica uuid"))
        .arg(
            Arg::with_name("pooluuid")
                .required(true)
                .index(3)
                .help("Storage pool name"))
        .arg(
            Arg::with_name("size")
                .short("s")
                .long("size")
                .takes_value(true)
                .required(true)
                .value_name("NUMBER")
                .help("Size of the replica"))
        .arg(
            Arg::with_name("protocol")
                .short("p")
                .long("protocol")
                .takes_value(true)
                .value_name("PROTOCOL")
                .help("Name of a protocol (nvmf) used for sharing the replica (default none)"))
        .arg(
            Arg::with_name("thin")
                .short("t")
                .long("thin")
                .takes_value(false)
                .help("Whether replica is thin provisioned (default false)"))
        .arg(
            Arg::with_name("allowed-host")
                .long("allowed-host")
                .takes_value(true)
                .multiple(true)
                .required(false)
                .help(
                    "NQN of hosts which are allowed to connect to the target",
                ),
        );

    // let create_v2 = SubCommand::with_name("create2")
    //            .about("Create replica on pool")
    //            .arg(
    //                Arg::with_name("pool")
    //                    .required(true)
    //                    .index(1)
    //                    .help("Storage pool name"))
    //            .arg(
    //                Arg::with_name("name")
    //                    .required(true)
    //                    .index(2)
    //                    .help("Replica name"))
    //            .arg(
    //                Arg::with_name("uuid")
    //                    .required(true).index(3)
    //                    .help("Unique replica uuid"))
    //            .arg(
    //                Arg::with_name("protocol")
    //                    .short("p")
    //                    .long("protocol")
    //                    .takes_value(true)
    //                    .value_name("PROTOCOL")
    //                    .help("Name of a protocol (nvmf) used for sharing the
    // replica (default none)"))            .arg(
    //                Arg::with_name("size")
    //                    .short("s")
    //                    .long("size")
    //                    .takes_value(true)
    //                    .required(true)
    //                    .value_name("NUMBER")
    //                    .help("Size of the replica"))
    //            .arg(
    //                Arg::with_name("thin")
    //                    .short("t")
    //                    .long("thin")
    //                    .takes_value(false)
    //                    .help("Whether replica is thin provisioned (default
    // false)"))            .arg(
    //                Arg::with_name("allowed-host")
    //                    .long("allowed-host")
    //                    .takes_value(true)
    //                    .multiple(true)
    //                    .required(false)
    //                    .help(
    //                        "NQN of hosts which are allowed to connect to the
    // target",                    ),
    //            );

    let destroy = SubCommand::with_name("destroy")
        .about("Destroy replica")
        .arg(
            Arg::with_name("uuid")
                .required(true)
                .index(1)
                .help("Replica uuid"),
        );

    let share = SubCommand::with_name("share").about("Share replica over specified protocol")
        .arg(
            Arg::with_name("uuid")
                .required(true)
                .index(1)
                .help("Replica uuid"))
        .arg(
            Arg::with_name("protocol")
                .required(true)
                .index(2)
                .help("Name of a protocol (nvmf) used for sharing or \"none\" to unshare the replica"))
        .arg(
            Arg::with_name("allowed-host")
                .long("allowed-host")
                .takes_value(true)
                .multiple(true)
                .required(false)
                .help("Name of a protocol (nvmf) used for sharing or \"none\" to unshare the replica"));
    let unshare = SubCommand::with_name("unshare")
        .about("Unshare replica")
        .arg(
            Arg::with_name("uuid")
                .required(true)
                .index(1)
                .help("Replica uuid"),
        );

    SubCommand::with_name("replica")
        .settings(&[
            AppSettings::SubcommandRequiredElseHelp,
            AppSettings::ColoredHelp,
            AppSettings::ColorAlways,
        ])
        .about("Replica management")
        .subcommand(create)
        .subcommand(destroy)
        .subcommand(share)
        .subcommand(unshare)
        .subcommand(SubCommand::with_name("list").about("List replicas"))
        .subcommand(
            SubCommand::with_name("stats").about("IO stats of replicas"),
        )
}

pub async fn handler(
    ctx: Context,
    matches: &ArgMatches<'_>,
) -> crate::Result<()> {
    match matches.subcommand() {
        ("create", Some(args)) => replica_create(ctx, args).await,
        ("destroy", Some(args)) => replica_destroy(ctx, args).await,
        ("list", Some(args)) => replica_list(ctx, args).await,
        ("share", Some(args)) => replica_share(ctx, args).await,
        ("unshare", Some(args)) => replica_unshare(ctx, args).await,
        ("stats", Some(args)) => replica_stat(ctx, args).await,
        (cmd, _) => {
            Err(Status::not_found(format!("command {} does not exist", cmd)))
                .context(GrpcStatus)
        }
    }
}

async fn replica_create(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> crate::Result<()> {
    let name = matches
        .value_of("name")
        .ok_or_else(|| ClientError::MissingValue {
            field: "name".to_string(),
        })?
        .to_owned();
    let uuid = matches
        .value_of("uuid")
        .ok_or_else(|| ClientError::MissingValue {
            field: "uuid".to_string(),
        })?
        .to_owned();
    let pooluuid = matches
        .value_of("pooluuid")
        .ok_or_else(|| ClientError::MissingValue {
            field: "pool".to_string(),
        })?
        .to_owned();
    let size = parse_size(matches.value_of("size").ok_or_else(|| {
        ClientError::MissingValue {
            field: "size".to_string(),
        }
    })?)
    .map_err(|s| Status::invalid_argument(format!("Bad size '{}'", s)))
    .context(GrpcStatus)?;
    let thin = matches.is_present("thin");
    let share = parse_replica_protocol(matches.value_of("protocol"))
        .context(GrpcStatus)?;
    let allowed_hosts =
        matches.values_of_lossy("allowed-host").unwrap_or_default();

    let request = v1_rpc::replica::CreateReplicaRequest {
        name,
        uuid: uuid.clone(),
        pooluuid,
        thin,
        share,
        size: size.get_bytes() as u64,
        allowed_hosts,
    };
    //let response = ctx.client.create_replica(rq).await.context(GrpcStatus)?;
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
    matches: &ArgMatches<'_>,
) -> crate::Result<()> {
    let uuid = matches
        .value_of("uuid")
        .ok_or_else(|| ClientError::MissingValue {
            field: "uuid".to_string(),
        })?
        .to_owned();

    let _ = ctx.v1.replica.destroy_replica(
        v1_rpc::replica::DestroyReplicaRequest {
            uuid: uuid.clone(),
        },
    );

    match ctx.output {
        OutputFormat::Json => {}
        OutputFormat::Default => {
            println!("replica: {} is deleted", &uuid);
        }
    };
    Ok(())
}

async fn replica_list(
    mut ctx: Context,
    _matches: &ArgMatches<'_>,
) -> crate::Result<()> {
    let response = ctx
        .v1
        .replica
        .list_replicas(v1_rpc::replica::ListReplicaOptions {
            name: None,
            poolname: None,
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
                    let proto = replica_protocol_to_str(r.share);
                    let size = ctx.units(Byte::from_bytes(r.size.into()));
                    vec![
                        r.pooluuid.clone(),
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
    matches: &ArgMatches<'_>,
) -> crate::Result<()> {
    let uuid = matches.value_of("uuid").unwrap().to_owned();
    let share = parse_replica_protocol(matches.value_of("protocol"))
        .context(GrpcStatus)?;
    let allowed_hosts =
        matches.values_of_lossy("allowed-host").unwrap_or_default();

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
    matches: &ArgMatches<'_>,
) -> crate::Result<()> {
    let uuid = matches.value_of("uuid").unwrap().to_owned();
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

// TODO : There's no v1 rpc for stat.
async fn replica_stat(
    mut ctx: Context,
    _matches: &ArgMatches<'_>,
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

fn parse_replica_protocol(pcol: Option<&str>) -> Result<i32, Status> {
    match pcol {
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
    match v1_rpc::common::ShareProtocol::from_i32(idx) {
        Some(v1_rpc::common::ShareProtocol::None) => "none",
        Some(v1_rpc::common::ShareProtocol::Nvmf) => "nvmf",
        Some(v1_rpc::common::ShareProtocol::Iscsi) => "iscsi",
        None => "unknown",
    }
}
