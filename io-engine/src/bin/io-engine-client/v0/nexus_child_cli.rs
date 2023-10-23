//!
//! methods to interact with the nexus child

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
        ("fault", args) => fault(ctx, args).await,
        ("offline", args) => child_operation(ctx, args, 0).await,
        ("online", args) => child_operation(ctx, args, 1).await,
        ("retire", args) => child_operation(ctx, args, 2).await,
        (cmd, _) => {
            Err(Status::not_found(format!("command {cmd} does not exist")))
                .context(GrpcStatus)
        }
    }
}

pub fn subcommands() -> Command {
    let fault = Command::new("fault")
        .about("fault a child")
        .arg(
            Arg::new("uuid")
                .required(true)
                .index(1)
                .help("uuid of the nexus"),
        )
        .arg(
            Arg::new("uri")
                .required(true)
                .index(2)
                .help("uri of the child"),
        );

    let offline = Command::new("offline")
        .about("offline a child")
        .arg(
            Arg::new("uuid")
                .required(true)
                .index(1)
                .help("uuid of the nexus"),
        )
        .arg(
            Arg::new("uri")
                .required(true)
                .index(2)
                .help("uri of the child"),
        );

    let online = Command::new("online")
        .about("online a child")
        .arg(
            Arg::new("uuid")
                .required(true)
                .index(1)
                .help("uuid of the nexus"),
        )
        .arg(
            Arg::new("uri")
                .required(true)
                .index(2)
                .help("uri of the child"),
        );

    let retire = Command::new("retire")
        .about("retire a child")
        .arg(
            Arg::new("uuid")
                .required(true)
                .index(1)
                .help("uuid of the nexus"),
        )
        .arg(
            Arg::new("uri")
                .required(true)
                .index(2)
                .help("uri of the child"),
        );

    Command::new("child")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .about("Nexus child management")
        .subcommand(fault)
        .subcommand(offline)
        .subcommand(online)
        .subcommand(retire)
}

async fn fault(mut ctx: Context, matches: &ArgMatches) -> crate::Result<()> {
    let uuid = matches
        .get_one::<String>("uuid")
        .ok_or_else(|| ClientError::MissingValue {
            field: "uuid".to_string(),
        })?
        .to_string();
    let uri = matches
        .get_one::<String>("uri")
        .ok_or_else(|| ClientError::MissingValue {
            field: "uri".to_string(),
        })?
        .to_string();

    let response = ctx
        .client
        .fault_nexus_child(rpc::FaultNexusChildRequest {
            uuid: uuid.clone(),
            uri: uri.clone(),
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
            println!("{uri}");
        }
    };

    Ok(())
}

async fn child_operation(
    mut ctx: Context,
    matches: &ArgMatches,
    action: i32,
) -> crate::Result<()> {
    let uuid = matches
        .get_one::<String>("uuid")
        .ok_or_else(|| ClientError::MissingValue {
            field: "uuid".to_string(),
        })?
        .to_string();
    let uri = matches
        .get_one::<String>("uri")
        .ok_or_else(|| ClientError::MissingValue {
            field: "uri".to_string(),
        })?
        .to_string();

    let response = ctx
        .client
        .child_operation(rpc::ChildNexusRequest {
            uuid: uuid.clone(),
            uri: uri.clone(),
            action,
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
            println!("{uri}");
        }
    };

    Ok(())
}
