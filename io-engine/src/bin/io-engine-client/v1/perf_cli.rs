//!
//! Methods related to the gathering of performance statistics.
//!
//! At present we only have get_resource_usage() which is
//! essentially the result of a getrusage(2) system call.

use super::{
    context::{Context, OutputFormat},
    GrpcStatus,
};
use clap::{Args, Subcommand};
use colored_json::ToColoredJson;
use io_engine_api::v0 as rpc;
use snafu::ResultExt;

#[derive(Debug, Args)]
#[command(subcommand_required = true, arg_required_else_help = true)]
pub struct PerfArgs {
    #[command(subcommand)]
    command: PerfCommands,
}

#[derive(Debug, Subcommand)]
enum PerfCommands {
    /// Resource usage statistics
    Resource,
}

pub async fn handler(ctx: Context, args: PerfArgs) -> crate::Result<()> {
    match args.command {
        PerfCommands::Resource => get_resource_usage(ctx).await,
    }
}

// TODO: There's no rpc for this API in v1.
async fn get_resource_usage(mut ctx: Context) -> crate::Result<()> {
    ctx.v2("Requesting resource usage statistics");

    let mut table: Vec<Vec<String>> = Vec::new();

    let response = ctx
        .client
        .get_resource_usage(rpc::Null {})
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
            if let Some(usage) = &response.get_ref().usage {
                table.push(vec![
                    usage.soft_faults.to_string(),
                    usage.hard_faults.to_string(),
                    usage.vol_csw.to_string(),
                    usage.invol_csw.to_string(),
                ]);
            }

            ctx.print_list(
                vec![
                    ">SOFT_FAULTS",
                    ">HARD_FAULTS",
                    ">VOLUNTARY_CSW",
                    ">INVOLUNTARY_CSW",
                ],
                table,
            );
        }
    };

    Ok(())
}
