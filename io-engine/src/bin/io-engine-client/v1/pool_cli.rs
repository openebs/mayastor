use crate::{
    context::{Context, OutputFormat},
    ClientError,
    GrpcStatus,
};
use byte_unit::Byte;
use clap::{App, AppSettings, Arg, ArgMatches, SubCommand};
use colored_json::ToColoredJson;
use mayastor_api::v1 as v1rpc;
use snafu::ResultExt;
use tonic::Status;

pub fn subcommands<'a, 'b>() -> App<'a, 'b> {
    let create = SubCommand::with_name("create")
        .about("Create new or import existing storage pool")
        .arg(
            Arg::with_name("pool")
                .required(true)
                .index(1)
                .help("Storage pool name"),
        )
        .arg(
            Arg::with_name("uuid")
                .long("uuid")
                .required(false)
                .takes_value(true)
                .help("Storage pool uuid"),
        )
        .arg(
            Arg::with_name("disk")
                .required(true)
                .multiple(true)
                .index(2)
                .help("Disk device files"),
        );

    let import = SubCommand::with_name("import")
        .about("Import existing storage pool, fail if pool does not exist")
        .arg(
            Arg::with_name("pool")
                .required(true)
                .index(1)
                .help("Storage pool name"),
        )
        .arg(
            Arg::with_name("uuid")
                .long("uuid")
                .required(false)
                .takes_value(true)
                .help("Storage pool uuid"),
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

    let export = SubCommand::with_name("export")
        .about("Export storage pool without destroying it")
        .arg(
            Arg::with_name("pool")
                .required(true)
                .index(1)
                .help("Storage pool name"),
        );

    SubCommand::with_name("pool")
        .settings(&[
            AppSettings::SubcommandRequiredElseHelp,
            AppSettings::ColoredHelp,
            AppSettings::ColorAlways,
        ])
        .about("Storage pool management")
        .subcommand(create)
        .subcommand(import)
        .subcommand(destroy)
        .subcommand(export)
        .subcommand(SubCommand::with_name("list").about("List storage pools"))
}

pub async fn handler(
    ctx: Context,
    matches: &ArgMatches<'_>,
) -> crate::Result<()> {
    match matches.subcommand() {
        ("create", Some(args)) => create(ctx, args).await,
        ("import", Some(args)) => import(ctx, args).await,
        ("destroy", Some(args)) => destroy(ctx, args).await,
        ("export", Some(args)) => export(ctx, args).await,
        ("list", Some(args)) => list(ctx, args).await,
        (cmd, _) => {
            Err(Status::not_found(format!("command {cmd} does not exist")))
                .context(GrpcStatus)
        }
    }
}

async fn create(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> crate::Result<()> {
    let name = matches
        .value_of("pool")
        .ok_or_else(|| ClientError::MissingValue {
            field: "pool".to_string(),
        })?
        .to_owned();
    let uuid = matches.value_of("uuid");
    let disks_list = matches
        .values_of("disk")
        .ok_or_else(|| ClientError::MissingValue {
            field: "disk".to_string(),
        })?
        .map(|dev| dev.to_owned())
        .collect();

    let response = ctx
        .v1
        .pool
        .create_pool(v1rpc::pool::CreatePoolRequest {
            name: name.clone(),
            uuid: uuid.map(ToString::to_string),
            disks: disks_list,
            pooltype: v1rpc::pool::PoolType::Lvs as i32,
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
            println!("{}", &name);
        }
    };

    Ok(())
}

async fn import(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> crate::Result<()> {
    let name = matches
        .value_of("pool")
        .ok_or_else(|| ClientError::MissingValue {
            field: "pool".to_string(),
        })?
        .to_owned();
    let uuid = matches.value_of("uuid");
    let disks_list = matches
        .values_of("disk")
        .ok_or_else(|| ClientError::MissingValue {
            field: "disk".to_string(),
        })?
        .map(|dev| dev.to_owned())
        .collect();

    let response = ctx
        .v1
        .pool
        .import_pool(v1rpc::pool::ImportPoolRequest {
            name: name.clone(),
            uuid: uuid.map(ToString::to_string),
            disks: disks_list,
            pooltype: v1rpc::pool::PoolType::Lvs as i32,
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
            println!("{}", &name);
        }
    };

    Ok(())
}

async fn destroy(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> crate::Result<()> {
    let name = matches
        .value_of("pool")
        .ok_or_else(|| ClientError::MissingValue {
            field: "pool".to_string(),
        })?
        .to_owned();

    let _ = ctx
        .v1
        .pool
        .destroy_pool(v1rpc::pool::DestroyPoolRequest {
            name: name.clone(),
            uuid: None,
        })
        .await
        .context(GrpcStatus)?;

    match ctx.output {
        OutputFormat::Json => {}
        OutputFormat::Default => {
            println!("pool: {} is deleted", &name);
        }
    };

    Ok(())
}

async fn export(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> crate::Result<()> {
    let name = matches
        .value_of("pool")
        .ok_or_else(|| ClientError::MissingValue {
            field: "pool".to_string(),
        })?
        .to_owned();

    let _ = ctx
        .v1
        .pool
        .export_pool(v1rpc::pool::ExportPoolRequest {
            name: name.clone(),
            uuid: None,
        })
        .await
        .context(GrpcStatus)?;

    match ctx.output {
        OutputFormat::Json => {}
        OutputFormat::Default => {
            println!("pool: {} is exported", &name);
        }
    };

    Ok(())
}

async fn list(
    mut ctx: Context,
    _matches: &ArgMatches<'_>,
) -> crate::Result<()> {
    ctx.v2("Requesting a list of pools");

    let response = ctx
        .v1
        .pool
        .list_pools(v1rpc::pool::ListPoolOptions {
            name: None,
            pooltype: None,
            uuid: None,
        })
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
            let pools: &Vec<v1rpc::pool::Pool> = &response.get_ref().pools;
            if pools.is_empty() {
                ctx.v1("No pools found");
                return Ok(());
            }

            let table = pools
                .iter()
                .map(|p| {
                    let cap = Byte::from_bytes(p.capacity.into());
                    let used = Byte::from_bytes(p.used.into());
                    let state = pool_state_to_str(p.state);
                    vec![
                        p.name.clone(),
                        p.uuid.clone(),
                        state.to_string(),
                        ctx.units(cap),
                        ctx.units(used),
                        p.disks.join(" "),
                    ]
                })
                .collect();
            ctx.print_list(
                vec!["NAME", "UUID", "STATE", ">CAPACITY", ">USED", "DISKS"],
                table,
            );
        }
    };

    Ok(())
}

fn pool_state_to_str(idx: i32) -> &'static str {
    match v1rpc::pool::PoolState::from_i32(idx).unwrap() {
        v1rpc::pool::PoolState::PoolUnknown => "unknown",
        v1rpc::pool::PoolState::PoolOnline => "online",
        v1rpc::pool::PoolState::PoolDegraded => "degraded",
        v1rpc::pool::PoolState::PoolFaulted => "faulted",
    }
}
