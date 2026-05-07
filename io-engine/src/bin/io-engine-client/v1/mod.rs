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
mod snapshot_rebuild_cli;
pub mod stats_cli;
mod test_cli;

pub(crate) use super::context;
use crate::ContextCreate;
pub(crate) use crate::GrpcStatus;
use clap::{Parser, Subcommand};
use context::{OutputFormat, Units};
use snafu::ResultExt;
use version_info::version_info_str;

#[derive(Parser, Debug)]
#[command(
    name = "Mayastor CLI V1",
    version = version_info_str!(),
    about = "CLI utility for Mayastor"
)]
struct Opts {
    #[arg(
        short = 'b',
        long,
        env = "MY_POD_IP",
        default_value = "http://127.0.0.1",
        value_name = "HOST",
        help = "The URI of mayastor instance",
        global = true,
        help_heading = "Global Options"
    )]
    bind: String,
    #[arg(
        short = 'q',
        long,
        global = true,
        help = "Do not print any output except for list records",
        help_heading = "Global Options"
    )]
    quiet: bool,
    #[arg(short = 'v', long, action = clap::ArgAction::Count, help = "Verbose output", conflicts_with = "quiet", global = true, help_heading = "Global Options")]
    verbose: u8,
    #[arg(
        short = 'u',
        long,
        default_value = "d",
        value_name = "BASE",
        help = "Output units: b for bytes, i for binary (KiB/MiB/GiB), d for decimal (KB/MB/GB)",
        help_heading = "Global Options"
    )]
    units: Units,
    #[arg(
        short = 'o',
        long,
        value_name = "FORMAT",
        default_value = "default",
        global = true,
        help = "Output format.",
        help_heading = "Global Options"
    )]
    output: OutputFormat,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Pool(pool_cli::PoolArgs),
    Nexus(nexus_cli::NexusArgs),
    Replica(replica_cli::ReplicaArgs),
    Bdev(bdev_cli::BdevArgs),
    Device(device_cli::DeviceArgs),
    Perf(perf_cli::PerfArgs),
    Rebuild(rebuild_cli::RebuildArgs),
    #[command(name = "snapshot-rebuild")]
    SnapshotRebuild(snapshot_rebuild_cli::SnapshotRebuildArgs),
    Snapshot(snapshot_cli::SnapshotArgs),
    Stats(stats_cli::StatsArgs),
    Controller(controller_cli::ControllerArgs),
    Jsonrpc(jsonrpc_cli::JsonrpcArgs),
    Test(test_cli::TestArgs),
}

pub(super) async fn main_() -> crate::Result<()> {
    let opts = Opts::parse();

    let ctx = context::Context::new(
        &opts.bind,
        opts.quiet,
        opts.verbose,
        opts.units,
        opts.output,
    )
    .await
    .context(ContextCreate)?;

    let status = match opts.command {
        Commands::Bdev(args) => bdev_cli::handler(ctx, args).await,
        Commands::Device(args) => device_cli::handler(ctx, args).await,
        Commands::Nexus(args) => nexus_cli::handler(ctx, args).await,
        Commands::Perf(args) => perf_cli::handler(ctx, args).await,
        Commands::Pool(args) => pool_cli::handler(ctx, args).await,
        Commands::Replica(args) => replica_cli::handler(ctx, args).await,
        Commands::Rebuild(args) => rebuild_cli::handler(ctx, args).await,
        Commands::SnapshotRebuild(args) => snapshot_rebuild_cli::handler(ctx, args).await,
        Commands::Snapshot(args) => snapshot_cli::handler(ctx, args).await,
        Commands::Stats(args) => stats_cli::handler(ctx, args).await,
        Commands::Controller(args) => controller_cli::handler(ctx, args).await,
        Commands::Jsonrpc(args) => jsonrpc_cli::json_rpc_call(ctx, args).await,
        Commands::Test(args) => test_cli::handler(ctx, args).await,
    };
    status
}
