use byte_unit::Byte;
use clap::{App, AppSettings, Arg, ArgMatches, SubCommand};
use tonic::{Code, Status};

use ::rpc::mayastor as rpc;

use crate::{context::Context, parse_size};

pub fn subcommands<'a, 'b>() -> App<'a, 'b> {
    let create = SubCommand::with_name("create")
        .about("Create replica on pool")
        .arg(
            Arg::with_name("pool")
                .required(true)
                .index(1)
                .help("Storage pool name"))

        .arg(
            Arg::with_name("uuid")
                .required(true).index(2)
                .help("Unique replica uuid"))

        .arg(
            Arg::with_name("protocol")
                .short("p")
                .long("protocol")
                .takes_value(true)
                .value_name("PROTOCOL")
                .help("Name of a protocol (nvmf, iSCSI) used for sharing the replica (default none)"))
        .arg(
            Arg::with_name("size")
                .short("s")
                .long("size")
                .takes_value(true)
                .required(true)
                .value_name("NUMBER")
                .help("Size of the replica"))
        .arg(
            Arg::with_name("thin")
                .short("t")
                .long("thin")
                .takes_value(false)
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
        .arg(
            Arg::with_name("uuid")
                .required(true)
                .index(1)
                .help("Replica uuid"))
        .arg(
            Arg::with_name("protocol")
                .required(true)
                .index(2)
                .help("Name of a protocol (nvmf, iscsi) used for sharing or \"none\" to unshare the replica"));

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
        .subcommand(SubCommand::with_name("list").about("List replicas"))
        .subcommand(
            SubCommand::with_name("stats").about("IO stats of replicas"),
        )
}

pub async fn handler(
    ctx: Context,
    matches: &ArgMatches<'_>,
) -> Result<(), Status> {
    match matches.subcommand() {
        ("create", Some(args)) => replica_create(ctx, &args).await,
        ("destroy", Some(args)) => replica_destroy(ctx, &args).await,
        ("list", Some(args)) => replica_list(ctx, &args).await,
        ("share", Some(args)) => replica_share(ctx, &args).await,
        ("stats", Some(args)) => replica_stat(ctx, &args).await,
        (cmd, _) => {
            Err(Status::not_found(format!("command {} does not exist", cmd)))
        }
    }
}

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

async fn replica_list(
    mut ctx: Context,
    _matches: &ArgMatches<'_>,
) -> Result<(), Status> {
    ctx.v2("Requesting a list of replicas");

    let resp = ctx.client.list_replicas(rpc::Null {}).await?;
    let replicas = &resp.get_ref().replicas;
    if replicas.is_empty() {
        ctx.v1("No replicas found");
        return Ok(());
    }

    ctx.v2("Found following replicas:");

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
