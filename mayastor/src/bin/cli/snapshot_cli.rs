//!
//! methods to interact with snapshot management

use crate::context::Context;
use ::rpc::mayastor as rpc;
use clap::{App, AppSettings, Arg, ArgMatches, SubCommand};
use tonic::Status;

pub async fn handler(
    ctx: Context,
    matches: &ArgMatches<'_>,
) -> Result<(), Status> {
    match matches.subcommand() {
        ("create", Some(args)) => create(ctx, &args).await,
        (cmd, _) => {
            Err(Status::not_found(format!("command {} does not exist", cmd)))
        }
    }
}

pub fn subcommands<'a, 'b>() -> App<'a, 'b> {
    let create = SubCommand::with_name("create")
        .about("create a snapshot")
        .arg(
            Arg::with_name("uuid")
                .required(true)
                .index(1)
                .help("uuid of the nexus"),
        );

    SubCommand::with_name("snapshot")
        .settings(&[
            AppSettings::SubcommandRequiredElseHelp,
            AppSettings::ColoredHelp,
            AppSettings::ColorAlways,
        ])
        .about("Snapshot management")
        .subcommand(create)
}

async fn create(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> Result<(), Status> {
    let uuid = matches.value_of("uuid").unwrap().to_string();

    ctx.client
        .create_snapshot(rpc::CreateSnapshotRequest {
            uuid: uuid.clone(),
        })
        .await?;
    ctx.v1(&format!("Creating snapshot on nexus {}", uuid));
    Ok(())
}
