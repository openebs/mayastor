use crate::{
    context::{Context, OutputFormat},
    Error,
    GrpcStatus,
};
use ::rpc::mayastor as rpc;
use byte_unit::Byte;
use clap::{App, AppSettings, Arg, ArgMatches, SubCommand};
use colored_json::ToColoredJson;
use snafu::ResultExt;
use tonic::Status;

pub fn subcommands<'a, 'b>() -> App<'a, 'b> {
    let create = SubCommand::with_name("create")
        .about("Create storage pool")
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
        .settings(&[
            AppSettings::SubcommandRequiredElseHelp,
            AppSettings::ColoredHelp,
            AppSettings::ColorAlways,
        ])
        .about("Storage pool management")
        .subcommand(create)
        .subcommand(destroy)
        .subcommand(SubCommand::with_name("list").about("List storage pools"))
}

pub async fn handler(
    ctx: Context,
    matches: &ArgMatches<'_>,
) -> crate::Result<()> {
    match matches.subcommand() {
        ("create", Some(args)) => create(ctx, args).await,
        ("destroy", Some(args)) => destroy(ctx, args).await,
        ("list", Some(args)) => list(ctx, args).await,
        (cmd, _) => {
            Err(Status::not_found(format!("command {} does not exist", cmd)))
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
        .ok_or_else(|| Error::MissingValue {
            field: "pool".to_string(),
        })?
        .to_owned();
    let disks = matches
        .values_of("disk")
        .ok_or_else(|| Error::MissingValue {
            field: "disk".to_string(),
        })?
        .map(|dev| dev.to_owned())
        .collect();

    let response = ctx
        .client
        .create_pool(rpc::CreatePoolRequest {
            name: name.clone(),
            disks,
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
        .ok_or_else(|| Error::MissingValue {
            field: "pool".to_string(),
        })?
        .to_owned();

    let response = ctx
        .client
        .destroy_pool(rpc::DestroyPoolRequest {
            name: name.clone(),
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

async fn list(
    mut ctx: Context,
    _matches: &ArgMatches<'_>,
) -> crate::Result<()> {
    ctx.v2("Requesting a list of pools");

    let response = ctx
        .client
        .list_pools(rpc::Null {})
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
            let pools: &Vec<rpc::Pool> = &response.get_ref().pools;
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
                        state.to_string(),
                        ctx.units(cap),
                        ctx.units(used),
                        p.disks.join(" "),
                    ]
                })
                .collect();
            ctx.print_list(
                vec!["NAME", "STATE", ">CAPACITY", ">USED", "DISKS"],
                table,
            );
        }
    };

    Ok(())
}

fn pool_state_to_str(idx: i32) -> &'static str {
    match rpc::PoolState::from_i32(idx).unwrap() {
        rpc::PoolState::PoolUnknown => "unknown",
        rpc::PoolState::PoolOnline => "online",
        rpc::PoolState::PoolDegraded => "degraded",
        rpc::PoolState::PoolFaulted => "faulted",
    }
}
