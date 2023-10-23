//!
//! Methods related to the gathering of performance statistics.
//!
//! At present we only have get_resource_usage() which is
//! essentially the result of a getrusage(2) system call.

use super::{
    context::{Context, OutputFormat},
    GrpcStatus,
};
use clap::{ArgMatches, Command};
use colored_json::ToColoredJson;
use mayastor_api::v0 as rpc;
use snafu::ResultExt;
use tonic::Status;

pub fn subcommands() -> Command {
    let resource = Command::new("resource").about("Resource usage statistics");

    Command::new("perf")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .about("Performance statistics")
        .subcommand(resource)
}

pub async fn handler(ctx: Context, matches: &ArgMatches) -> crate::Result<()> {
    match matches.subcommand().unwrap() {
        ("resource", args) => get_resource_usage(ctx, args).await,
        (cmd, _) => {
            Err(Status::not_found(format!("command {cmd} does not exist")))
                .context(GrpcStatus)
        }
    }
}

async fn get_resource_usage(
    mut ctx: Context,
    _matches: &ArgMatches,
) -> crate::Result<()> {
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
