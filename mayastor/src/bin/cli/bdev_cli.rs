//!
//! methods to directly interact with the bdev layer

use crate::context::Context;
use clap::{App, AppSettings, ArgMatches, SubCommand};
use colored_json::prelude::*;
use rpc::mayastor::Null;
use tonic::Status;

pub async fn handler(
    ctx: Context,
    matches: &ArgMatches<'_>,
) -> Result<(), Status> {
    match matches.subcommand() {
        ("list", Some(args)) => list(ctx, args).await,
        (cmd, _) => {
            Err(Status::not_found(format!("command {} does not exist", cmd)))
        }
    }
}
pub fn subcommands<'a, 'b>() -> App<'a, 'b> {
    let list = SubCommand::with_name("list").about("List all bdevs");
    SubCommand::with_name("bdev")
        .settings(&[
            AppSettings::SubcommandRequiredElseHelp,
            AppSettings::ColoredHelp,
            AppSettings::ColorAlways,
        ])
        .about("Block device management")
        .subcommand(list)
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
