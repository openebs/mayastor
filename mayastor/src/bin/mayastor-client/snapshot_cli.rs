//!
//! methods to interact with snapshot management

use crate::{
    context::{Context, OutputFormat},
    Error,
    GrpcStatus,
};
use ::rpc::mayastor as rpc;
use clap::{App, AppSettings, Arg, ArgMatches, SubCommand};
use colored_json::ToColoredJson;
use snafu::ResultExt;
use tonic::Status;

pub async fn handler(
    ctx: Context,
    matches: &ArgMatches<'_>,
) -> crate::Result<()> {
    match matches.subcommand() {
        ("create", Some(args)) => create(ctx, args).await,
        (cmd, _) => {
            Err(Status::not_found(format!("command {} does not exist", cmd)))
                .context(GrpcStatus)
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
) -> crate::Result<()> {
    let uuid = matches
        .value_of("uuid")
        .ok_or_else(|| Error::MissingValue {
            field: "uuid".to_string(),
        })?
        .to_string();

    let response = ctx
        .client
        .create_snapshot(rpc::CreateSnapshotRequest {
            uuid: uuid.clone(),
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
            println!("{}", &uuid);
        }
    };

    Ok(())
}
