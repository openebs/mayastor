//!
//! methods to directly interact with the bdev layer

use clap::{App, AppSettings, Arg, ArgMatches, SubCommand};
use colored_json::prelude::*;
use tonic::Status;

use rpc::mayastor::{BdevShareRequest, BdevUri, CreateReply, Null};

use crate::context::Context;

pub async fn handler(
    ctx: Context,
    matches: &ArgMatches<'_>,
) -> Result<(), Status> {
    match matches.subcommand() {
        ("list", Some(args)) => list(ctx, args).await,
        ("create", Some(args)) => create(ctx, args).await,
        ("share", Some(args)) => share(ctx, args).await,
        ("destroy", Some(args)) => destroy(ctx, args).await,
        ("unshare", Some(args)) => unshare(ctx, args).await,
        (cmd, _) => {
            Err(Status::not_found(format!("command {} does not exist", cmd)))
        }
    }
}
pub fn subcommands<'a, 'b>() -> App<'a, 'b> {
    let list = SubCommand::with_name("list").about("List all bdevs");
    let create = SubCommand::with_name("create")
        .about("Create a new bdev by specifying a URI")
        .arg(Arg::with_name("uri").required(true).index(1));

    let destroy = SubCommand::with_name("destroy")
        .about("destroy the given bdev")
        .arg(Arg::with_name("name").required(true).index(1));

    let share = SubCommand::with_name("share")
        .about("share the given bdev")
        .arg(Arg::with_name("name").required(true).index(1))
        .arg(Arg::from_usage("<protocol> 'the protocol to used to share the given bdev'")
                .short("p")
                .possible_values(&["nvmf", "iscsi"])
                .default_value("nvmf"),
        );

    let unshare = SubCommand::with_name("unshare")
        .about("unshare the given bdev")
        .arg(Arg::with_name("name").required(true).index(1));

    SubCommand::with_name("bdev")
        .settings(&[
            AppSettings::SubcommandRequiredElseHelp,
            AppSettings::ColoredHelp,
            AppSettings::ColorAlways,
        ])
        .about("Block device management")
        .subcommand(list)
        .subcommand(share)
        .subcommand(unshare)
        .subcommand(create)
        .subcommand(destroy)
}

async fn list(mut ctx: Context, _args: &ArgMatches<'_>) -> Result<(), Status> {
    let bdevs = ctx.bdev.list(Null {}).await?;
    println!(
        "{}",
        serde_json::to_string_pretty(&bdevs.into_inner())
            .unwrap()
            .to_colored_json_auto()
            .unwrap()
    );

    Ok(())
}
async fn create(mut ctx: Context, args: &ArgMatches<'_>) -> Result<(), Status> {
    let uri = args.value_of("uri").unwrap().to_owned();
    let response = ctx
        .bdev
        .create(BdevUri {
            uri,
        })
        .await?;
    println!(
        "{}",
        serde_json::to_string_pretty(&response.into_inner())
            .unwrap()
            .to_colored_json_auto()
            .unwrap()
    );
    Ok(())
}

async fn destroy(
    mut ctx: Context,
    args: &ArgMatches<'_>,
) -> Result<(), Status> {
    let name = args.value_of("name").unwrap().to_owned();
    let bdevs = ctx.bdev.list(Null {}).await?.into_inner();

    if let Some(bdev) = bdevs.bdevs.iter().find(|b| b.name == name) {
        // un share the bdev
        let _ = ctx
            .bdev
            .unshare(CreateReply {
                name,
            })
            .await?;

        let response = ctx
            .bdev
            .destroy(BdevUri {
                uri: bdev.uri.clone(),
            })
            .await?;
        println!(
            "{}",
            serde_json::to_string_pretty(&response.into_inner())
                .unwrap()
                .to_colored_json_auto()
                .unwrap()
        );
        Ok(())
    } else {
        Err(Status::not_found(name))
    }
}

async fn share(mut ctx: Context, args: &ArgMatches<'_>) -> Result<(), Status> {
    let name = args.value_of("name").unwrap().to_owned();
    let protocol = args.value_of("protocol").unwrap().to_owned();
    let response = ctx
        .bdev
        .share(BdevShareRequest {
            name,
            proto: protocol,
        })
        .await?;
    println!(
        "{}",
        serde_json::to_string_pretty(&response.into_inner())
            .unwrap()
            .to_colored_json_auto()
            .unwrap()
    );
    Ok(())
}

async fn unshare(
    mut ctx: Context,
    args: &ArgMatches<'_>,
) -> Result<(), Status> {
    let name = args.value_of("name").unwrap().to_owned();
    let response = ctx
        .bdev
        .unshare(CreateReply {
            name,
        })
        .await?;
    println!(
        "{}",
        serde_json::to_string_pretty(&response.into_inner())
            .unwrap()
            .to_colored_json_auto()
            .unwrap()
    );
    Ok(())
}
