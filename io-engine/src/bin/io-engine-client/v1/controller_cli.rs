//!
//! methods to interact with NVMe controllers

use super::context::Context;
use crate::{context::OutputFormat, GrpcStatus};
use clap::{Args, Subcommand};
use colored_json::ToColoredJson;
use io_engine_api::v1 as v1rpc;
use snafu::ResultExt;
use std::convert::TryFrom;

#[derive(Debug, Args)]
#[command(subcommand_required = true, arg_required_else_help = true)]
pub struct ControllerArgs {
    #[command(subcommand)]
    command: ControllerCommands,
}

#[derive(Debug, Subcommand)]
enum ControllerCommands {
    /// List existing NVMe controllers
    List,
    /// Display I/O statistics for NVMe controllers
    Stats(StatsArgs),
}

#[derive(Debug, Args)]
struct StatsArgs {
    /// name of the controller
    name: String,
}

pub async fn handler(ctx: Context, args: ControllerArgs) -> crate::Result<()> {
    match args.command {
        ControllerCommands::List => list_controllers(ctx).await,
        ControllerCommands::Stats(args) => controller_stats(ctx, args).await,
    }
}

fn controller_state_to_str(idx: i32) -> String {
    match v1rpc::host::NvmeControllerState::try_from(idx).unwrap() {
        v1rpc::host::NvmeControllerState::New => "new",
        v1rpc::host::NvmeControllerState::Initializing => "init",
        v1rpc::host::NvmeControllerState::Running => "running",
        v1rpc::host::NvmeControllerState::Faulted => "faulted",
        v1rpc::host::NvmeControllerState::Unconfiguring => "unconfiguring",
        v1rpc::host::NvmeControllerState::Unconfigured => "unconfigured",
    }
    .to_string()
}

async fn controller_stats(mut ctx: Context, args: StatsArgs) -> crate::Result<()> {
    let name = args.name;
    let response = ctx
        .v1
        .host
        .stat_nvme_controller(v1rpc::host::StatNvmeControllerRequest {
            name: name.to_string(),
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
            let controllers = &response.get_ref().stats;
            if controllers.is_none() {
                ctx.v1("No NVMe controllers found");
                return Ok(());
            }

            let row = match controllers {
                None => {
                    ctx.v1("No NVMe controllers found");
                    return Ok(());
                }
                Some(c) => {
                    let num_read_ops = c.num_read_ops.to_string();
                    let num_write_ops = c.num_write_ops.to_string();
                    let bytes_read = c.bytes_read.to_string();
                    let bytes_written = c.bytes_written.to_string();
                    let num_unmap_ops = c.num_unmap_ops.to_string();
                    let bytes_unmapped = c.bytes_unmapped.to_string();
                    vec![
                        name.to_string(),
                        num_read_ops,
                        num_write_ops,
                        bytes_read,
                        bytes_written,
                        num_unmap_ops,
                        bytes_unmapped,
                    ]
                }
            };

            let hdr = vec![
                "NAME",
                "READS",
                "WRITES",
                "READ/B",
                "WRITTEN/B",
                "NUM_UNMAPS",
                "BYTES_UNMAPPED",
            ];
            ctx.print_list(hdr, vec![row]);
        }
    }

    Ok(())
}

async fn list_controllers(mut ctx: Context) -> crate::Result<()> {
    let response = ctx
        .v1
        .host
        .list_nvme_controllers(())
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
