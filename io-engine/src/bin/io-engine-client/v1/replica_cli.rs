//!
//! methods to interact with the replica's of the mayastor

use crate::{
    context::{Context, OutputFormat},
    GrpcStatus,
};
use byte_unit::Byte;
use clap::{Args, Subcommand};
use colored_json::ToColoredJson;
use io_engine_api::v1;
use snafu::ResultExt;
use uuid::Uuid;

#[derive(Debug, Args)]
#[command(subcommand_required = true, arg_required_else_help = true)]
pub struct ReplicaArgs {
    #[command(subcommand)]
    command: ReplicaCommands,
}

#[derive(Debug, Subcommand)]
enum ReplicaCommands {
    /// Create a replica
    Create(CreateArgs),
    /// Destroy a replica
    Destroy(UuidArgs),
    /// List replicas
    List,
    /// Share a replica
    Share(ShareArgs),
    /// Unshare a replica
    Unshare(UuidArgs),
    /// Resize a replica
    Resize(ResizeArgs),
    /// Get replica IO stats
    Stats,
}

#[derive(Debug, Args)]
struct CreateArgs {
    /// name of the replica
    name: String,
    /// uuid of the replica
    uuid: Uuid,
    /// storage pool name or UUID
    pool: String,
    /// size of the replica
    #[arg(long, short = 's', value_parser = parse_byte)]
    size: Byte,
    /// sharing protocol (nvmf or none)
    #[arg(long, short = 'p')]
    protocol: Option<ShareProtocol>,
    /// enable thin provisioning
    #[arg(long, short)]
    thin: bool,
    /// NQN of hosts allowed to connect to the target
    #[arg(long = "allowed-host", action = clap::ArgAction::Append)]
    allowed_hosts: Vec<String>,
}

#[derive(Debug, Args)]
struct UuidArgs {
    /// uuid of the replica
    uuid: Uuid,
}

/// Share protocol for replicas
#[derive(Debug, Clone, clap::ValueEnum)]
pub(super) enum ShareProtocol {
    None,
    Nvmf,
}

#[derive(Debug, Args)]
struct ShareArgs {
    /// uuid of the replica
    uuid: Uuid,
    /// protocol for sharing
    protocol: ShareProtocol,
}

#[derive(Debug, Args)]
struct ResizeArgs {
    /// uuid of the replica
    uuid: Uuid,
    /// new size of the replica
    #[arg(value_parser = parse_byte)]
    size: Byte,
}

pub async fn handler(ctx: Context, args: ReplicaArgs) -> crate::Result<()> {
    match args.command {
        ReplicaCommands::Create(args) => create(ctx, args).await,
        ReplicaCommands::Destroy(args) => destroy(ctx, args).await,
        ReplicaCommands::List => list(ctx).await,
        ReplicaCommands::Share(args) => share(ctx, args).await,
        ReplicaCommands::Unshare(args) => unshare(ctx, args).await,
        ReplicaCommands::Resize(args) => resize(ctx, args).await,
        ReplicaCommands::Stats => stats(ctx).await,
    }
}

fn parse_byte(s: &str) -> Result<Byte, String> {
    Byte::parse_str(s, true).map_err(|e| e.to_string())
}

async fn create(mut ctx: Context, args: CreateArgs) -> crate::Result<()> {
    let uuid = args.uuid.to_string();
    let share: i32 = match args.protocol {
        None | Some(ShareProtocol::None) => 0,
        Some(ShareProtocol::Nvmf) => 1,
    };
    let response = ctx
        .v1
        .replica
        .create_replica(v1::replica::CreateReplicaRequest {
            name: args.name,
            uuid,
            pooluuid: args.pool,
            size: args.size.as_u64(),
            thin: args.thin,
            share,
            allowed_hosts: args.allowed_hosts,
            ..Default::default()
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
            let uri = &response.get_ref().uri;
            println!("{uri}");
        }
    };

    Ok(())
}

async fn destroy(mut ctx: Context, args: UuidArgs) -> crate::Result<()> {
    let uuid = args.uuid.to_string();

    ctx.v1
        .replica
        .destroy_replica(v1::replica::DestroyReplicaRequest {
            uuid: uuid.clone(),
            pool: None,
        })
        .await
        .context(GrpcStatus)?;

    match ctx.output {
        OutputFormat::Json => {}
        OutputFormat::Default => {
            println!("{uuid}");
        }
    };

    Ok(())
}

async fn list(mut ctx: Context) -> crate::Result<()> {
    let response = ctx
        .v1
        .replica
        .list_replicas(v1::replica::ListReplicaOptions {
            name: None,
            poolname: None,
            uuid: None,
            pooluuid: None,
            query: None,
            pooltypes: vec![],
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
            let replicas = &response.get_ref().replicas;

            if replicas.is_empty() {
                ctx.v1("No replicas found");
                return Ok(());
            }

            let table = replicas
                .iter()
                .map(|r| {
                    let usage = r.usage.as_ref().unwrap();
                    let proto = share_proto_to_str(r.share);
                    let size = ctx.units(Byte::from_u64(r.size));
                    let capacity = ctx.units(Byte::from_u64(usage.capacity_bytes));
                    let allocated = ctx.units(Byte::from_u64(usage.allocated_bytes));
                    vec![
                        r.poolname.clone(),
                        r.name.clone(),
                        r.uuid.clone(),
                        r.thin.to_string(),
                        proto.to_string(),
                        size,
                        capacity,
                        allocated,
                        r.uri.clone(),
                        r.is_snapshot.to_string(),
                        r.is_clone.to_string(),
                        usage.allocated_bytes_snapshots.to_string(),
                        usage
                            .allocated_bytes_snapshot_from_clone
                            .unwrap_or_default()
                            .to_string(),
                        r.encrypted.unwrap_or_default().to_string(),
                    ]
                })
                .collect();
            ctx.print_list(
                vec![
                    "POOL",
                    "NAME",
                    "UUID",
                    ">THIN",
                    ">SHARE",
                    ">SIZE",
                    ">CAP",
                    ">ALLOC",
                    "URI",
                    "IS_SNAPSHOT",
                    "IS_CLONE",
                    "SNAP_ANCESTOR_SIZE",
                    "CLONE_SNAP_ANCESTOR_SIZE",
                    "ENCRYPTED",
                ],
                table,
            );
        }
    };

    Ok(())
}

async fn share(mut ctx: Context, args: ShareArgs) -> crate::Result<()> {
    let uuid = args.uuid.to_string();
    let protocol_num: i32 = match args.protocol {
        ShareProtocol::None => 0,
        ShareProtocol::Nvmf => 1,
    };

    let response = ctx
        .v1
        .replica
        .share_replica(v1::replica::ShareReplicaRequest {
            uuid: uuid.clone(),
            share: protocol_num,
            ..Default::default()
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
            let val = &response.get_ref().uri;
            println!("{val}");
        }
    };

    Ok(())
}

async fn unshare(mut ctx: Context, args: UuidArgs) -> crate::Result<()> {
    let uuid = args.uuid.to_string();

    ctx.v1
        .replica
        .unshare_replica(v1::replica::UnshareReplicaRequest { uuid: uuid.clone() })
        .await
        .context(GrpcStatus)?;

    match ctx.output {
        OutputFormat::Json => {}
        OutputFormat::Default => {
            println!("{uuid}");
        }
    };

    Ok(())
}

async fn resize(mut ctx: Context, args: ResizeArgs) -> crate::Result<()> {
    let response = ctx
        .v1
        .replica
        .resize_replica(v1::replica::ResizeReplicaRequest {
            uuid: args.uuid.to_string(),
            requested_size: args.size.as_u64(),
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
            let uuid = &response.get_ref().uuid;
            println!("{uuid}");
        }
    };

    Ok(())
}

async fn stats(mut ctx: Context) -> crate::Result<()> {
    let response = ctx
        .v1
        .stats
        .get_replica_io_stats(v1::stats::ListStatsOption { name: None })
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
            let replicas = &response.get_ref().stats;
            if replicas.is_empty() {
                return Ok(());
            }

            let table = replicas
                .iter()
                .filter_map(|r| {
                    let s = r.stats.as_ref()?;
                    Some(vec![
                        s.uuid.clone(),
                        s.num_read_ops.to_string(),
                        s.num_write_ops.to_string(),
                        s.bytes_read.to_string(),
                        s.bytes_written.to_string(),
                    ])
                })
                .collect();
            ctx.print_list(
                vec!["NAME", ">READS", ">WRITES", ">RBYTES", ">WBYTES"],
                table,
            );
        }
    };

    Ok(())
}

fn share_proto_to_str(i: i32) -> &'static str {
    match i {
        0 => "none",
        1 => "nvmf",
        2 => "iscsi",
        _ => "unknown",
    }
}
