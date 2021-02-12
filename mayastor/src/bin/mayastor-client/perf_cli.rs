//!
//! Methods related to the gathering of performance statistics.
//!
//! At present we only have get_resource_usage() which is
//! essentially the result of a getrusage(2) system call.

use super::context::Context;
use ::rpc::mayastor as rpc;
use clap::{App, AppSettings, ArgMatches, SubCommand};
use tonic::Status;

pub fn subcommands<'a, 'b>() -> App<'a, 'b> {
    let resource =
        SubCommand::with_name("resource").about("Resource usage statistics");

    SubCommand::with_name("perf")
        .settings(&[
            AppSettings::SubcommandRequiredElseHelp,
            AppSettings::ColoredHelp,
            AppSettings::ColorAlways,
        ])
        .about("Performance statistics")
        .subcommand(resource)
}

pub async fn handler(
    ctx: Context,
    matches: &ArgMatches<'_>,
) -> Result<(), Status> {
    match matches.subcommand() {
        ("resource", Some(args)) => get_resource_usage(ctx, args).await,
        (cmd, _) => {
            Err(Status::not_found(format!("command {} does not exist", cmd)))
        }
    }
}

async fn get_resource_usage(
    mut ctx: Context,
    _matches: &ArgMatches<'_>,
) -> Result<(), Status> {
    ctx.v2("Requesting resource usage statistics");

    let mut table: Vec<Vec<String>> = Vec::new();

    let reply = ctx.client.get_resource_usage(rpc::Null {}).await?;

    if let Some(usage) = &reply.get_ref().usage {
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

    Ok(())
}
