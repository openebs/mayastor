use crate::{
    context::{Context, OutputFormat},
    GrpcStatus,
};
use byte_unit::Byte;
use clap::{Args, Subcommand};
use colored_json::ToColoredJson;
use io_engine_api::v1 as v1rpc;
use snafu::ResultExt;

#[derive(Debug, Args)]
#[command(subcommand_required = true, arg_required_else_help = true)]
pub struct StatsArgs {
    #[command(subcommand)]
    command: StatsCommands,
}

#[derive(Debug, Subcommand)]
enum StatsCommands {
    /// Get Pool IO Stats
    Pool(NameArgs),
    /// Get Nexus IO Stats
    Nexus(NameArgs),
    /// Get Replica IO Stats
    Replica(NameArgs),
    /// Reset all resource IO Stats
    Reset,
}

#[derive(Debug, Args)]
struct NameArgs {
    /// Optional resource name filter
    name: Option<String>,
}

pub async fn handler(ctx: Context, args: StatsArgs) -> crate::Result<()> {
    match args.command {
        StatsCommands::Pool(args) => pool(ctx, args).await,
        StatsCommands::Nexus(args) => nexus(ctx, args).await,
        StatsCommands::Replica(args) => replica(ctx, args).await,
        StatsCommands::Reset => reset(ctx).await,
    }
}

async fn pool(mut ctx: Context, args: NameArgs) -> crate::Result<()> {
    ctx.v2("Requesting Pool metrics");
    let response = ctx
        .v1
        .stats
        .get_pool_io_stats(v1rpc::stats::ListStatsOption {
            name: args.name.clone(),
        })
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
            let stats: &Vec<v1rpc::stats::IoStats> = &response.get_ref().stats;
            if stats.is_empty() {
                if let Some(name) = args.name {
                    ctx.v1(&format!(
                        "No IoStats found for {name}, Check if device exist"
                    ));
                } else {
                    ctx.v1("No Pool IoStats found");
                }
                return Ok(());
            }
            ctx.print_list(
                vec![
                    "NAME",
                    "NUM_RD_OPS",
                    "TOTAL_RD",
                    "NUM_WR_OPS",
                    "TOTAL_WR",
                    "NUM_UNMAP_OPS",
                    "TOTAL_UNMAPPED",
                    "RD_LAT",
                    "WR_LAT",
                    "UNMAP_LATENCY",
                    "MAX_RD_LAT",
                    "MIN_RD_LAT",
                    "MAX_WR_LAT",
                    "MIN_WR_LAT",
                ],
                stats.iter().map(io_stats_row).collect(),
            );
        }
    };
    Ok(())
}

async fn nexus(mut ctx: Context, args: NameArgs) -> crate::Result<()> {
    ctx.v2("Requesting Nexus metrics");
    let response = ctx
        .v1
        .stats
        .get_nexus_io_stats(v1rpc::stats::ListStatsOption {
            name: args.name.clone(),
        })
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
            let stats: &Vec<v1rpc::stats::IoStats> = &response.get_ref().stats;
            if stats.is_empty() {
                if let Some(name) = args.name {
                    ctx.v1(&format!(
                        "No IoStats found for {name}, Check if device exists"
                    ));
                } else {
                    ctx.v1("No Nexus IoStats found");
                }
                return Ok(());
            }
            ctx.print_list(
                vec![
                    "NAME",
                    "NUM_RD_OPS",
                    "TOTAL_RD",
                    "NUM_WR_OPS",
                    "TOTAL_WR",
                    "NUM_UNMAP_OPS",
                    "TOTAL_UNMAPPED",
                    "RD_LAT",
                    "WR_LAT",
                    "UNMAP_LATENCY",
                    "MAX_RD_LAT",
                    "MIN_RD_LAT",
                    "MAX_WR_LAT",
                    "MIN_WR_LAT",
                ],
                stats.iter().map(io_stats_row).collect(),
            );
        }
    };
    Ok(())
}

async fn replica(mut ctx: Context, args: NameArgs) -> crate::Result<()> {
    ctx.v2("Requesting Replica metrics");
    let response = ctx
        .v1
        .stats
        .get_replica_io_stats(v1rpc::stats::ListStatsOption {
            name: args.name.clone(),
        })
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
            let stats: &Vec<v1rpc::stats::ReplicaIoStats> = &response.get_ref().stats;
            if stats.is_empty() {
                if let Some(name) = args.name {
                    ctx.v1(&format!(
                        "No IoStats found for {name}, Check if device exists"
                    ));
                } else {
                    ctx.v1("No Replica IoStats found");
                }
                return Ok(());
            }
            ctx.print_list(
                vec![
                    "NAME",
                    "NUM_RD_OPS",
                    "TOTAL_RD",
                    "NUM_WR_OPS",
                    "TOTAL_WR",
                    "NUM_UNMAP_OPS",
                    "TOTAL_UNMAPPED",
                    "RD_LAT",
                    "WR_LAT",
                    "UNMAP_LATENCY",
                    "MAX_RD_LAT",
                    "MIN_RD_LAT",
                    "MAX_WR_LAT",
                    "MIN_WR_LAT",
                ],
                stats
                    .iter()
                    .map(|p| io_stats_row(p.stats.as_ref().unwrap()))
                    .collect(),
            );
        }
    };
    Ok(())
}

async fn reset(mut ctx: Context) -> crate::Result<()> {
    ctx.v2("Resetting all metrics");
    let _ = ctx.v1.stats.reset_io_stats(()).await.context(GrpcStatus)?;
    println!("Stats Reset Completed");
    Ok(())
}

fn io_stats_row(stats: &v1rpc::stats::IoStats) -> Vec<String> {
    let tick_rate = stats.tick_rate;
    let ticks_time = |ticks| -> String { ticks_to_time(ticks, tick_rate).to_string() };
    vec![
        stats.name.clone(),
        stats.num_read_ops.to_string(),
        adjust_bytes(stats.bytes_read),
        stats.num_write_ops.to_string(),
        adjust_bytes(stats.bytes_written),
        stats.num_unmap_ops.to_string(),
        adjust_bytes(stats.bytes_unmapped),
        ticks_time(stats.read_latency_ticks),
        ticks_time(stats.write_latency_ticks),
        ticks_time(stats.unmap_latency_ticks),
        ticks_time(stats.max_read_latency_ticks),
        ticks_time(stats.min_read_latency_ticks),
        ticks_time(stats.max_write_latency_ticks),
        ticks_time(stats.min_write_latency_ticks),
    ]
}

fn adjust_bytes(bytes: u64) -> String {
    let byte = Byte::from_u64(bytes);
    let adjusted_byte = byte.get_appropriate_unit(byte_unit::UnitType::Binary);
    format!("{adjusted_byte:.2}")
}

fn ticks_to_time(tick: u64, tick_rate: u64) -> u64 {
    ((tick as u128 * 1000000) / tick_rate as u128) as u64
}
