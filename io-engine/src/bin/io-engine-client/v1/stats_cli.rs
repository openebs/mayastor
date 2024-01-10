use crate::{
    context::{Context, OutputFormat},
    GrpcStatus,
};
use byte_unit::Byte;
use clap::{Arg, ArgMatches, Command};
use colored_json::ToColoredJson;
use io_engine_api::v1 as v1rpc;
use snafu::ResultExt;
use tonic::Status;

pub fn subcommands() -> Command {
    let pool = Command::new("pool").about("Get Pool IO Stats").arg(
        Arg::new("name")
            .required(false)
            .index(1)
            .help("Storage pool name"),
    );

    let reset = Command::new("reset").about("Reset all resource IO Stats");

    Command::new("stats")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .about("Resource IOStats")
        .subcommand(pool)
        .subcommand(reset)
}

pub async fn handler(ctx: Context, matches: &ArgMatches) -> crate::Result<()> {
    match matches.subcommand().unwrap() {
        ("pool", args) => pool(ctx, args).await,
        ("reset", _) => reset(ctx).await,
        (cmd, _) => {
            Err(Status::not_found(format!("command {cmd} does not exist")))
                .context(GrpcStatus)
        }
    }
}

async fn pool(mut ctx: Context, _matches: &ArgMatches) -> crate::Result<()> {
    ctx.v2("Requesting Pool metrics");
    let response = ctx
        .v1
        .stats
        .get_pool_io_stats(v1rpc::stats::ListStatsOption {
            name: None,
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
                ctx.v1("No Pool IoStats found");
                return Ok(());
            }

            let table = stats
                .iter()
                .map(|p| {
                    let read_latency =
                        ticks_to_time(p.read_latency_ticks, p.tick_rate);
                    let write_latency =
                        ticks_to_time(p.write_latency_ticks, p.tick_rate);
                    let unmap_latency =
                        ticks_to_time(p.unmap_latency_ticks, p.tick_rate);
                    let max_read_latency =
                        ticks_to_time(p.max_read_latency_ticks, p.tick_rate);
                    let min_read_latency =
                        ticks_to_time(p.min_read_latency_ticks, p.tick_rate);
                    let max_write_latency =
                        ticks_to_time(p.max_write_latency_ticks, p.tick_rate);
                    let min_write_latency =
                        ticks_to_time(p.min_write_latency_ticks, p.tick_rate);
                    vec![
                        p.name.clone(),
                        p.num_read_ops.to_string(),
                        adjust_bytes(p.bytes_read),
                        p.num_write_ops.to_string(),
                        adjust_bytes(p.bytes_written),
                        p.num_unmap_ops.to_string(),
                        adjust_bytes(p.bytes_unmapped),
                        read_latency.to_string(),
                        write_latency.to_string(),
                        unmap_latency.to_string(),
                        max_read_latency.to_string(),
                        min_read_latency.to_string(),
                        max_write_latency.to_string(),
                        min_write_latency.to_string(),
                    ]
                })
                .collect();
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
                table,
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

fn adjust_bytes(bytes: u64) -> String {
    let byte = Byte::from_bytes(bytes as u128);
    let adjusted_byte = byte.get_appropriate_unit(true);
    adjusted_byte.to_string()
}

fn ticks_to_time(tick: u64, tick_rate: u64) -> u64 {
    ((tick as u128 * 1000000) / tick_rate as u128) as u64
}
