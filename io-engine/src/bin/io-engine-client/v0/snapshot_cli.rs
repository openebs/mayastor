//!
//! methods to interact with snapshot management

use crate::{
    context::{Context, OutputFormat},
    ClientError,
    GrpcStatus,
};
use clap::{Arg, ArgMatches, Command};
use colored_json::ToColoredJson;
use mayastor_api::v0 as rpc;
use snafu::ResultExt;
use tonic::Status;

pub async fn handler(ctx: Context, matches: &ArgMatches) -> crate::Result<()> {
    match matches.subcommand().unwrap() {
        ("create", args) => create(ctx, args).await,
        (cmd, _) => {
            Err(Status::not_found(format!("command {cmd} does not exist")))
                .context(GrpcStatus)
        }
    }
}

pub fn subcommands() -> Command {
    let create = Command::new("create").about("create a snapshot").arg(
        Arg::new("uuid")
            .required(true)
            .index(1)
            .help("uuid of the nexus"),
    );

    Command::new("snapshot")
        .arg_required_else_help(true)
        .about("Snapshot management")
        .subcommand(create)
}

async fn create(mut ctx: Context, matches: &ArgMatches) -> crate::Result<()> {
    let uuid = matches
        .get_one::<String>("uuid")
        .ok_or_else(|| ClientError::MissingValue {
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
