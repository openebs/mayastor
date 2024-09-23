//!
//! methods to directly interact with the bdev layer

use crate::{
    context::{Context, OutputFormat},
    ClientError,
    GrpcStatus,
};
use byte_unit::Byte;
use clap::{Arg, ArgMatches, Command};
use colored_json::prelude::*;
use io_engine_api::v1 as v1rpc;
use snafu::ResultExt;
use tonic::Status;

pub async fn handler(ctx: Context, matches: &ArgMatches) -> crate::Result<()> {
    match matches.subcommand().unwrap() {
        ("list", args) => list(ctx, args).await,
        ("create", args) => create(ctx, args).await,
        ("share", args) => share(ctx, args).await,
        ("destroy", args) => destroy(ctx, args).await,
        ("unshare", args) => unshare(ctx, args).await,
        (cmd, _) => {
            Err(Status::not_found(format!("command {cmd} does not exist")))
                .context(GrpcStatus)
        }
    }
}

pub fn subcommands() -> Command {
    let list = Command::new("list").about("List all bdevs");
    let create = Command::new("create")
        .about("Create a new bdev by specifying a URI")
        .arg(Arg::new("uri").required(true).index(1));

    let destroy = Command::new("destroy")
        .about("destroy the given bdev")
        .arg(Arg::new("name").required(true).index(1));

    let share = Command::new("share")
        .about("share the given bdev")
        .arg(Arg::new("name").required(true).index(1))
        .arg(
            Arg::new("protocol")
                .long("protocol")
                .short('p')
                .help("the protocol to used to share the given bdev.")
                .required(false)
                .value_parser(["Nvmf"])
                .default_value("Nvmf"),
        )
        .arg(
            Arg::new("allowed-host")
                .long("allowed-host")
                .action(clap::ArgAction::Append)
                .required(false)
                .help(
                    "NQN of hosts which are allowed to connect to the target",
                ),
        );

    let unshare = Command::new("unshare")
        .about("unshare the given bdev")
        .arg(Arg::new("name").required(true).index(1));

    Command::new("bdev")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .about("Block device management")
        .subcommand(list)
        .subcommand(share)
        .subcommand(unshare)
        .subcommand(create)
        .subcommand(destroy)
}

async fn list(mut ctx: Context, _args: &ArgMatches) -> crate::Result<()> {
    let response = ctx
        .v1
        .bdev
        .list(v1rpc::bdev::ListBdevOptions {
            name: None,
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
            let bdevs = &response.get_ref().bdevs;
            if bdevs.is_empty() {
                ctx.v1("No bdevs found");
                return Ok(());
            }
            let header = vec![
                "UUID",
                "NUM_BLOCKS",
                "BLK_SIZE",
                "CAPACITY",
                "CLAIMED_BY",
                "NAME",
                "SHARE_URI",
            ];
            let table = bdevs
                .iter()
                .map(|bdev| {
                    let cap = Byte::from_bytes(
                        (bdev.num_blocks * bdev.blk_size as u64).into(),
                    );
                    vec![
                        bdev.uuid.to_string(),
                        bdev.num_blocks.to_string(),
                        bdev.blk_size.to_string(),
                        ctx.units(cap),
                        bdev.claimed_by.to_string(),
                        bdev.name.to_string(),
                        bdev.share_uri.to_string(),
                    ]
                })
                .collect();
            ctx.print_list(header, table);
        }
    };

    Ok(())
}

async fn create(mut ctx: Context, args: &ArgMatches) -> crate::Result<()> {
    let uri = args
        .get_one::<String>("uri")
        .ok_or_else(|| ClientError::MissingValue {
            field: "uri".to_string(),
        })?
        .to_owned();

    let response = ctx
        .v1
        .bdev
        .create(v1rpc::bdev::CreateBdevRequest {
            uri,
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
            println!("{}", &response.get_ref().bdev.as_ref().unwrap().name);
        }
    };

    Ok(())
}

async fn destroy(mut ctx: Context, args: &ArgMatches) -> crate::Result<()> {
    let name = args
        .get_one::<String>("name")
        .ok_or_else(|| ClientError::MissingValue {
            field: "name".to_string(),
        })?
        .to_owned();

    let bdevs = ctx
        .v1
        .bdev
        .list(v1rpc::bdev::ListBdevOptions {
            name: None,
        })
        .await
        .context(GrpcStatus)?
        .into_inner();

    let found = bdevs
        .bdevs
        .iter()
        .find(|b| b.name == name)
        .ok_or_else(|| Status::not_found(name.clone()))
        .context(GrpcStatus)?;

    // un share the bdev
    let _ = ctx
        .v1
        .bdev
        .unshare(v1rpc::bdev::BdevUnshareRequest {
            name,
        })
        .await
        .context(GrpcStatus)?;

    let response = ctx
        .v1
        .bdev
        .destroy(v1rpc::bdev::DestroyBdevRequest {
            uri: found.uri.clone(),
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
            println!("{}", found.name,);
        }
    };

    Ok(())
}

async fn share(mut ctx: Context, args: &ArgMatches) -> crate::Result<()> {
    let name = args
        .get_one::<String>("name")
        .ok_or_else(|| ClientError::MissingValue {
            field: "name".to_string(),
        })?
        .to_owned();
    let protocol = args
        .get_one::<String>("protocol")
        .ok_or_else(|| ClientError::MissingValue {
            field: "protocol".to_string(),
        })?
        .to_owned();
    let allowed_hosts = args
        .get_many::<String>("allowed-host")
        .unwrap_or_default()
        .cloned()
        .collect();

    let val = if protocol == "Nvmf" {
        v1rpc::common::ShareProtocol::Nvmf as i32
    } else {
        v1rpc::common::ShareProtocol::None as i32
    };

    let response = ctx
        .v1
        .bdev
        .share(v1rpc::bdev::BdevShareRequest {
            name,
            protocol: val,
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
            println!("{}", &response.get_ref().bdev.as_ref().unwrap().uri);
        }
    }
    Ok(())
}

async fn unshare(mut ctx: Context, args: &ArgMatches) -> crate::Result<()> {
    let name = args
        .get_one::<String>("name")
        .ok_or_else(|| ClientError::MissingValue {
            field: "name".to_string(),
        })?
        .to_owned();

    let response = ctx
        .v1
        .bdev
        .unshare(v1rpc::bdev::BdevUnshareRequest {
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
            println!("{name}",);
        }
    }
    Ok(())
}
