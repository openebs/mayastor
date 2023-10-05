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
use clap::{App, AppSettings, Arg};
use snafu::ResultExt;
use version_info::version_info_str;

pub(super) async fn main_() -> crate::Result<()> {
    let matches = App::new("Mayastor CLI V1")
        .version(version_info_str!())
        .settings(&[
            AppSettings::SubcommandRequiredElseHelp,
            AppSettings::ColoredHelp,
            AppSettings::ColorAlways])
        .about("CLI utility for Mayastor")
        .arg(
            Arg::with_name("bind")
                .short("b")
                .long("bind")
                .env("MY_POD_IP")
                .default_value("http://127.0.0.1:10124")
                .value_name("HOST")
                .help("The URI of mayastor instance")
                .global(true))
        .arg(
            Arg::with_name("quiet")
                .short("q")
                .long("quiet")
                .help("Do not print any output except for list records"))
        .arg(
            Arg::with_name("verbose")
                .short("v")
                .long("verbose")
                .multiple(true)
                .help("Verbose output")
                .conflicts_with("quiet")
                .global(true))
        .arg(
            Arg::with_name("units")
                .short("u")
                .long("units")
                .value_name("BASE")
                .possible_values(&["i", "d"])
                .hide_possible_values(true)
                .next_line_help(true)
                .help("Output with large units: i for kiB, etc. or d for kB, etc."))
        .arg(
            Arg::with_name("output")
                .short("o")
                .long("output")
                .value_name("FORMAT")
                .default_value("default")
                .possible_values(&["default", "json"])
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
        .get_matches();

    let ctx = context::Context::new(&matches)
        .await
        .context(ContextCreate)?;

    let status = match matches.subcommand() {
        ("bdev", Some(args)) => bdev_cli::handler(ctx, args).await,
        ("device", Some(args)) => device_cli::handler(ctx, args).await,
        ("nexus", Some(args)) => nexus_cli::handler(ctx, args).await,
        ("perf", Some(args)) => perf_cli::handler(ctx, args).await,
        ("pool", Some(args)) => pool_cli::handler(ctx, args).await,
        ("replica", Some(args)) => replica_cli::handler(ctx, args).await,
        ("rebuild", Some(args)) => rebuild_cli::handler(ctx, args).await,
        ("snapshot", Some(args)) => snapshot_cli::handler(ctx, args).await,
        ("controller", Some(args)) => controller_cli::handler(ctx, args).await,
        ("jsonrpc", Some(args)) => jsonrpc_cli::json_rpc_call(ctx, args).await,
        ("test", Some(args)) => test_cli::handler(ctx, args).await,
        _ => panic!("Command not found"),
    };
    status
}
