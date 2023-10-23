pub mod bdev_cli;
pub mod controller_cli;
pub mod device_cli;
pub mod jsonrpc_cli;
mod nexus_child_cli;
pub mod nexus_cli;
pub mod perf_cli;
pub mod pool_cli;
pub mod rebuild_cli;
pub mod replica_cli;
pub mod snapshot_cli;
mod test_cli;

pub(crate) use super::context;
use crate::ContextCreate;
pub(crate) use crate::GrpcStatus;
use clap::{Arg, Command};
use snafu::ResultExt;
use version_info::version_info_str;

pub(super) async fn main_() -> crate::Result<()> {
    let matches = Command::new("Mayastor CLI V1")
        .version(version_info_str!())
        .about("CLI utility for Mayastor")
        .arg(
            Arg::new("bind")
                .short('b')
                .long("bind")
                .env("MY_POD_IP")
                .default_value("http://127.0.0.1:10124")
                .value_name("HOST")
                .help("The URI of mayastor instance")
                .global(true))
        .arg(
            Arg::new("quiet")
                .short('q')
                .action(clap::ArgAction::SetTrue)
                .global(true)
                .long("quiet")
                .help("Do not print any output except for list records"))
        .arg(
            Arg::new("verbose")
                .short('v')
                .long("verbose")
                .action(clap::ArgAction::Count)
                .help("Verbose output")
                .conflicts_with("quiet")
                .global(true))
        .arg(
            Arg::new("units")
                .short('u')
                .long("units")
                .value_name("BASE")
                .value_parser(["i", "d"])
                .hide_possible_values(true)
                .next_line_help(true)
                .help("Output with large units: i for kiB, etc. or d for kB, etc."))
        .arg(
            Arg::new("output")
                .short('o')
                .long("output")
                .value_name("FORMAT")
                .default_value("default")
                .value_parser(["default", "json"])
                .global(true)
                .help("Output format.")
        )
        .subcommand(pool_cli::subcommands())
        .subcommand(nexus_cli::subcommands())
        .subcommand(replica_cli::subcommands())
        .subcommand(bdev_cli::subcommands())
        .subcommand(device_cli::subcommands())
        .subcommand(perf_cli::subcommands())
        .subcommand(rebuild_cli::subcommands())
        .subcommand(snapshot_cli::subcommands())
        .subcommand(jsonrpc_cli::subcommands())
        .subcommand(controller_cli::subcommands())
        .subcommand(test_cli::subcommands())
        .subcommand_required(true)
        .arg_required_else_help(true)
        .get_matches();

    let ctx = context::Context::new(&matches)
        .await
        .context(ContextCreate)?;

    let status = match matches.subcommand().unwrap() {
        ("bdev", args) => bdev_cli::handler(ctx, args).await,
        ("device", args) => device_cli::handler(ctx, args).await,
        ("nexus", args) => nexus_cli::handler(ctx, args).await,
        ("perf", args) => perf_cli::handler(ctx, args).await,
        ("pool", args) => pool_cli::handler(ctx, args).await,
        ("replica", args) => replica_cli::handler(ctx, args).await,
        ("rebuild", args) => rebuild_cli::handler(ctx, args).await,
        ("snapshot", args) => snapshot_cli::handler(ctx, args).await,
        ("controller", args) => controller_cli::handler(ctx, args).await,
        ("jsonrpc", args) => jsonrpc_cli::json_rpc_call(ctx, args).await,
        ("test", args) => test_cli::handler(ctx, args).await,
        _ => panic!("Command not found"),
    };
    status
}
