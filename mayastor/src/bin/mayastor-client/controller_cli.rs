//!
//! methods to interact with NVMe controllers

use super::context::Context;
use crate::{context::OutputFormat, GrpcStatus};
use ::rpc::mayastor as rpc;
use clap::{App, AppSettings, ArgMatches, SubCommand};
use colored_json::ToColoredJson;
use snafu::ResultExt;
use tonic::Status;

pub fn subcommands<'a, 'b>() -> App<'a, 'b> {
    let list =
        SubCommand::with_name("list").about("List existing NVMe controllers");

    SubCommand::with_name("controller")
        .settings(&[
            AppSettings::SubcommandRequiredElseHelp,
            AppSettings::ColoredHelp,
            AppSettings::ColorAlways,
        ])
        .about("NVMe controllers")
        .subcommand(list)
}

pub async fn handler(
    ctx: Context,
    matches: &ArgMatches<'_>,
) -> crate::Result<()> {
    match matches.subcommand() {
        ("list", Some(args)) => list_controllers(ctx, args).await,
        (cmd, _) => {
            Err(Status::not_found(format!("command {} does not exist", cmd)))
                .context(GrpcStatus)
        }
    }
}

fn controller_state_to_str(idx: i32) -> String {
    match rpc::NvmeControllerState::from_i32(idx).unwrap() {
        rpc::NvmeControllerState::New => "new",
        rpc::NvmeControllerState::Initializing => "init",
        rpc::NvmeControllerState::Running => "running",
        rpc::NvmeControllerState::Faulted => "faulted",
        rpc::NvmeControllerState::Unconfiguring => "unconfiguring",
        rpc::NvmeControllerState::Unconfigured => "unconfigured",
    }
    .to_string()
}

async fn list_controllers(
    mut ctx: Context,
    _matches: &ArgMatches<'_>,
) -> crate::Result<()> {
    let response = ctx
        .client
        .list_nvme_controllers(rpc::Null {})
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
            let controllers = &response.get_ref().controllers;
            if controllers.is_empty() {
                ctx.v1("No NVMe controllers found");
                return Ok(());
            }

            let table = controllers
                .iter()
                .map(|c| {
                    let size = c.size.to_string();
                    let blk_size = c.blk_size.to_string();
                    let state = controller_state_to_str(c.state);

                    vec![c.name.clone(), size, state, blk_size]
                })
                .collect();

            let hdr = vec!["NAMEs", "SIZE", "STATE", "BLKSIZE"];
            ctx.print_list(hdr, table);
        }
    }

    Ok(())
}
