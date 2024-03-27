use crate::{
    context::{Context, OutputFormat},
    parse_size,
    ClientError,
    GrpcStatus,
};
use byte_unit::Byte;
use clap::{Arg, ArgMatches, Command};
use colored_json::ToColoredJson;
use io_engine_api::v1 as v1rpc;
use snafu::ResultExt;
use std::{convert::TryFrom, str::FromStr};
use strum::VariantNames;
use strum_macros::{AsRefStr, EnumString, EnumVariantNames};
use tonic::Status;

pub fn subcommands() -> Command {
    let create = Command::new("create")
        .about("Create new or import existing storage pool")
        .arg(
            Arg::new("pool")
                .required(true)
                .index(1)
                .help("Storage pool name"),
        )
        .arg(
            Arg::new("uuid")
                .long("uuid")
                .required(false)
                .help("Storage pool uuid"),
        )
        .arg(
            Arg::new("cluster-size")
                .long("cluster-size")
                .required(false)
                .help("SPDK cluster size"),
        )
        .arg(
            Arg::new("disk")
                .required(true)
                .action(clap::ArgAction::Append)
                .index(2)
                .help("Disk device files"),
        )
        .arg(
            Arg::new("type")
                .short('t')
                .long("type")
                .help("The type of the pool")
                .required(false)
                .value_parser(PoolType::types().to_vec())
                .default_value(PoolType::Lvs.as_ref()),
        );

    let import = Command::new("import")
        .about("Import existing storage pool, fail if pool does not exist")
        .arg(
            Arg::new("pool")
                .required(true)
                .index(1)
                .help("Storage pool name"),
        )
        .arg(
            Arg::new("uuid")
                .short('u')
                .long("uuid")
                .required(false)
                .help("Storage pool uuid"),
        )
        .arg(
            Arg::new("disk")
                .required(true)
                .action(clap::ArgAction::Append)
                .index(2)
                .help("Disk device files"),
        )
        .arg(
            Arg::new("type")
                .short('t')
                .long("type")
                .help("The type of the pool")
                .required(false)
                .value_parser(PoolType::types().to_vec())
                .default_value(PoolType::Lvs.as_ref()),
        );

    let destroy = Command::new("destroy")
        .about("Destroy storage pool")
        .arg(
            Arg::new("pool")
                .required(true)
                .index(1)
                .help("Storage pool name"),
        )
        .arg(
            Arg::new("uuid")
                .short('u')
                .long("uuid")
                .required(false)
                .help("Storage pool uuid"),
        )
        .arg(
            Arg::new("type")
                .short('t')
                .long("type")
                .help("The type of the pool")
                .required(false)
                .value_parser(PoolType::types().to_vec())
                .default_value(PoolType::Lvs.as_ref()),
        );

    let export = Command::new("export")
        .about("Export storage pool without destroying it")
        .arg(
            Arg::new("name")
                .required(true)
                .index(1)
                .help("Storage pool name"),
        )
        .arg(
            Arg::new("uuid")
                .short('u')
                .long("uuid")
                .required(false)
                .help("Storage pool uuid"),
        )
        .arg(
            Arg::new("type")
                .short('t')
                .long("type")
                .help("The type of the pool")
                .required(false)
                .value_parser(PoolType::types().to_vec())
                .default_value(PoolType::Lvs.as_ref()),
        );

    let list = Command::new("list")
        .about("List storage pools")
        .arg(Arg::new("name").required(false).help("Storage pool name"))
        .arg(Arg::new("uuid").required(false).help("Storage pool uuid"))
        .arg(
            Arg::new("type")
                .short('t')
                .long("type")
                .help("The type of the pool")
                .required(false)
                .value_parser(PoolType::types().to_vec()),
        );

    Command::new("pool")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .about("Storage pool management")
        .subcommand(create)
        .subcommand(import)
        .subcommand(destroy)
        .subcommand(export)
        .subcommand(list)
}

pub async fn handler(ctx: Context, matches: &ArgMatches) -> crate::Result<()> {
    match matches.subcommand().unwrap() {
        ("create", args) => create(ctx, args).await,
        ("import", args) => import(ctx, args).await,
        ("destroy", args) => destroy(ctx, args).await,
        ("export", args) => export(ctx, args).await,
        ("list", args) => list(ctx, args).await,
        (cmd, _) => {
            Err(Status::not_found(format!("command {cmd} does not exist")))
                .context(GrpcStatus)
        }
    }
}

async fn create(mut ctx: Context, matches: &ArgMatches) -> crate::Result<()> {
    let name = matches
        .get_one::<String>("pool")
        .ok_or_else(|| ClientError::MissingValue {
            field: "pool".to_string(),
        })?
        .to_owned();

    let uuid = matches.get_one::<String>("uuid");

    let disks_list = matches
        .get_many::<String>("disk")
        .ok_or_else(|| ClientError::MissingValue {
            field: "disk".to_string(),
        })?
        .map(|dev| dev.to_owned())
        .collect();

    let pooltype = matches
        .get_one::<String>("type")
        .map(|s| PoolType::from_str(s.as_str()))
        .unwrap()
        .map_err(|e| Status::invalid_argument(e.to_string()))
        .context(GrpcStatus)?;

    let cluster_size = match matches.get_one::<String>("cluster-size") {
        Some(s) => match parse_size(s) {
            Ok(s) => Some(s.get_bytes() as u32),
            Err(err) => {
                return Err(Status::invalid_argument(format!(
                    "Bad size '{err}'"
                )))
                .context(GrpcStatus);
            }
        },
        None => None,
    };

    let response = ctx
        .v1
        .pool
        .create_pool(v1rpc::pool::CreatePoolRequest {
            name: name.clone(),
            uuid: uuid.map(ToString::to_string),
            disks: disks_list,
            pooltype: v1rpc::pool::PoolType::from(pooltype) as i32,
            cluster_size,
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

#[derive(EnumString, EnumVariantNames, AsRefStr)]
#[strum(serialize_all = "camelCase")]
pub(super) enum PoolType {
    Lvs,
    Lvm,
}
impl PoolType {
    pub(crate) fn types() -> &'static [&'static str] {
        Self::VARIANTS
    }
}
impl From<PoolType> for v1rpc::pool::PoolType {
    fn from(value: PoolType) -> Self {
        match value {
            PoolType::Lvs => Self::Lvs,
            PoolType::Lvm => Self::Lvm,
        }
    }
}

async fn import(mut ctx: Context, matches: &ArgMatches) -> crate::Result<()> {
    let name = matches
        .get_one::<String>("pool")
        .ok_or_else(|| ClientError::MissingValue {
            field: "pool".to_string(),
        })?
        .to_owned();
    let uuid = matches.get_one::<String>("uuid");
    let disks_list = matches
        .get_many::<String>("disk")
        .ok_or_else(|| ClientError::MissingValue {
            field: "disk".to_string(),
        })?
        .map(|dev| dev.to_owned())
        .collect();
    let pooltype = matches
        .get_one::<String>("type")
        .map(|s| PoolType::from_str(s.as_str()))
        .unwrap()
        .map_err(|e| Status::invalid_argument(e.to_string()))
        .context(GrpcStatus)?;

    let response = ctx
        .v1
        .pool
        .import_pool(v1rpc::pool::ImportPoolRequest {
            name: name.clone(),
            uuid: uuid.map(ToString::to_string),
            disks: disks_list,
            pooltype: v1rpc::pool::PoolType::from(pooltype) as i32,
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

async fn destroy(mut ctx: Context, matches: &ArgMatches) -> crate::Result<()> {
    let name = matches
        .get_one::<String>("pool")
        .ok_or_else(|| ClientError::MissingValue {
            field: "pool".to_string(),
        })?
        .to_owned();
    let uuid = matches.get_one::<String>("uuid").cloned();

    let _ = ctx
        .v1
        .pool
        .destroy_pool(v1rpc::pool::DestroyPoolRequest {
            name: name.clone(),
            uuid,
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

async fn export(mut ctx: Context, matches: &ArgMatches) -> crate::Result<()> {
    let name = matches
        .get_one::<String>("name")
        .ok_or_else(|| ClientError::MissingValue {
            field: "name".to_string(),
        })?
        .to_owned();
    let uuid = matches.get_one::<String>("uuid").cloned();

    let _ = ctx
        .v1
        .pool
        .export_pool(v1rpc::pool::ExportPoolRequest {
            name: name.clone(),
            uuid,
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

async fn list(mut ctx: Context, matches: &ArgMatches) -> crate::Result<()> {
    ctx.v2("Requesting a list of pools");

    let name = matches.get_one::<String>("name").cloned();
    let uuid = matches.get_one::<String>("uuid").cloned();
    let pooltype = matches
        .get_one::<String>("type")
        .map(|s| PoolType::from_str(s.as_str()))
        .transpose()
        .map_err(|e| Status::invalid_argument(e.to_string()))
        .context(GrpcStatus)?;

    let response = ctx
        .v1
        .pool
        .list_pools(v1rpc::pool::ListPoolOptions {
            name,
            uuid,
            pooltype: pooltype.map(|pooltype| v1rpc::pool::PoolTypeValue {
                value: v1rpc::pool::PoolType::from(pooltype) as i32,
            }),
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
    match v1rpc::pool::PoolState::try_from(idx).unwrap() {
        v1rpc::pool::PoolState::PoolUnknown => "unknown",
        v1rpc::pool::PoolState::PoolOnline => "online",
        v1rpc::pool::PoolState::PoolDegraded => "degraded",
        v1rpc::pool::PoolState::PoolFaulted => "faulted",
    }
}
