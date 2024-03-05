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

    let nexus = Command::new("nexus").about("Get Nexus IO Stats").arg(
        Arg::new("name")
            .required(false)
            .index(1)
            .help("Volume target/nexus name"),
    );

    let replica = Command::new("replica").about("Get Replica IO Stats").arg(
        Arg::new("name")
            .required(false)
            .index(1)
            .help("Replica name"),
    );

    let reset = Command::new("reset").about("Reset all resource IO Stats");

    Command::new("stats")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .about("Resource IOStats")
        .subcommand(pool)
        .subcommand(nexus)
        .subcommand(replica)
        .subcommand(reset)
}

pub async fn handler(ctx: Context, matches: &ArgMatches) -> crate::Result<()> {
    match matches.subcommand().unwrap() {
        ("pool", args) => pool(ctx, args).await,
        ("nexus", args) => nexus(ctx, args).await,
        ("replica", args) => replica(ctx, args).await,
        ("reset", _) => reset(ctx).await,
        (cmd, _) => {
            Err(Status::not_found(format!("command {cmd} does not exist")))
                .context(GrpcStatus)
        }
    }
}

async fn pool(mut ctx: Context, matches: &ArgMatches) -> crate::Result<()> {
    ctx.v2("Requesting Pool metrics");
    let pool_name = matches.get_one::<String>("name");
    let response = ctx
        .v1
        .stats
        .get_pool_io_stats(v1rpc::stats::ListStatsOption {
            name: pool_name.cloned(),
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
                if let Some(name) = pool_name {
                    ctx.v1(&format!(
                        "No IoStats found for {}, Check if device exist",
                        name
                    ));
                } else {
                    ctx.v1("No Pool IoStats found");
                }
                return Ok(());
            }

            let table = stats
                .iter()
                .map(|stats| {
                    let tick_rate = stats.tick_rate;
                    let ticks_time = |ticks| -> String {
                        ticks_to_time(ticks, tick_rate).to_string()
                    };
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

async fn nexus(mut ctx: Context, matches: &ArgMatches) -> crate::Result<()> {
    ctx.v2("Requesting Nexus metrics");
    let nexus_name = matches.get_one::<String>("name");
    let response = ctx
        .v1
        .stats
        .get_nexus_io_stats(v1rpc::stats::ListStatsOption {
            name: nexus_name.cloned(),
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
                if let Some(name) = nexus_name {
                    ctx.v1(&format!(
                        "No IoStats found for {}, Check if device exists",
                        name
                    ));
                } else {
                    ctx.v1("No Nexus IoStats found");
                }
                return Ok(());
            }

            let table = stats
                .iter()
                .map(|stats| {
                    let tick_rate = stats.tick_rate;
                    let ticks_time = |ticks| -> String {
                        ticks_to_time(ticks, tick_rate).to_string()
                    };
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

async fn replica(mut ctx: Context, matches: &ArgMatches) -> crate::Result<()> {
    ctx.v2("Requesting Replica metrics");
    let replica_name = matches.get_one::<String>("name");
    let response = ctx
        .v1
        .stats
        .get_replica_io_stats(v1rpc::stats::ListStatsOption {
            name: replica_name.cloned(),
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
            let stats: &Vec<v1rpc::stats::ReplicaIoStats> =
                &response.get_ref().stats;
            if stats.is_empty() {
                if let Some(name) = replica_name {
                    ctx.v1(&format!(
                        "No IoStats found for {}, Check if device exists",
                        name
                    ));
                } else {
                    ctx.v1("No Replica IoStats found");
                }
                return Ok(());
            }

            let table = stats
                .iter()
                .map(|p| {
                    let io_stat = p.stats.as_ref().unwrap();
                    let tick_rate = io_stat.tick_rate;
                    let ticks_time = |ticks| -> String {
                        ticks_to_time(ticks, tick_rate).to_string()
                    };
                    vec![
                        io_stat.name.clone(),
                        io_stat.num_read_ops.to_string(),
                        adjust_bytes(io_stat.bytes_read),
                        io_stat.num_write_ops.to_string(),
                        adjust_bytes(io_stat.bytes_written),
                        io_stat.num_unmap_ops.to_string(),
                        adjust_bytes(io_stat.bytes_unmapped),
                        ticks_time(io_stat.read_latency_ticks),
                        ticks_time(io_stat.write_latency_ticks),
                        ticks_time(io_stat.unmap_latency_ticks),
                        ticks_time(io_stat.max_read_latency_ticks),
                        ticks_time(io_stat.min_read_latency_ticks),
                        ticks_time(io_stat.max_write_latency_ticks),
                        ticks_time(io_stat.min_write_latency_ticks),
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
