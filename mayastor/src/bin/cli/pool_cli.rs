use super::context::Context;
use ::rpc::mayastor as rpc;
use byte_unit::Byte;
use clap::{App, AppSettings, Arg, ArgMatches, SubCommand};
use tonic::{Code, Status};

pub fn subcommands<'a, 'b>() -> App<'a, 'b> {
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
) -> Result<(), Status> {
    match matches.subcommand() {
        ("create", Some(args)) => create(ctx, args).await,
        ("destroy", Some(args)) => destroy(ctx, args).await,
        ("list", Some(args)) => list(ctx, args).await,
        (cmd, _) => {
            Err(Status::not_found(format!("command {} does not exist", cmd)))
        }
    }
}

async fn create(
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

async fn destroy(
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

async fn list(
    mut ctx: Context,
    _matches: &ArgMatches<'_>,
) -> Result<(), Status> {
    ctx.v2("Requesting a list of pools");

    let reply = ctx.client.list_pools(rpc::Null {}).await?;
    let pools: &Vec<rpc::Pool> = &reply.get_ref().pools;
    if pools.is_empty() {
        ctx.v1("No pools found");
        return Ok(());
    }

    ctx.v2("Found following pools:");

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
    ctx.print_list(vec!["NAME", "STATE", ">CAPACITY", ">USED", "DISKS"], table);

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
