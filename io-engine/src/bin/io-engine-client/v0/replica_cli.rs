//!
//! methods to interact with the replica's of the mayastor

use crate::{
    context::{Context, OutputFormat},
    GrpcStatus,
};
use byte_unit::Byte;
use clap::{Args, Subcommand};
use colored_json::ToColoredJson;
use io_engine_api::v0 as rpc;
use snafu::ResultExt;

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
    /// Create a replica (v2)
    Create2(Create2Args),
    /// Destroy a replica
    Destroy(UuidArgs),
    /// List replicas
    List,
    /// List replicas (v2)
    List2,
    /// Share a replica
    Share(ShareArgs),
    /// Get replica IO stats
    Stats,
}

#[derive(Debug, Args)]
struct CreateArgs {
    /// storage pool name
    pool: String,
    /// name of the replica
    name: String,
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
struct Create2Args {
    /// storage pool name
    pool: String,
    /// name of the replica
    name: String,
    /// uuid of the replica
    uuid: String,
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
    uuid: String,
}

/// Share protocol for replicas
#[derive(Debug, Clone, clap::ValueEnum)]
enum ShareProtocol {
    None,
    Nvmf,
}

#[derive(Debug, Args)]
struct ShareArgs {
    /// uuid of the replica
    uuid: String,
    /// protocol for sharing
    protocol: ShareProtocol,
}

pub async fn handler(ctx: Context, args: ReplicaArgs) -> crate::Result<()> {
    match args.command {
        ReplicaCommands::Create(args) => create(ctx, args).await,
        ReplicaCommands::Create2(args) => create2(ctx, args).await,
        ReplicaCommands::Destroy(args) => destroy(ctx, args).await,
        ReplicaCommands::List => list(ctx).await,
        ReplicaCommands::List2 => list2(ctx).await,
        ReplicaCommands::Share(args) => share(ctx, args).await,
        ReplicaCommands::Stats => stats(ctx).await,
    }
}

fn parse_byte(s: &str) -> Result<Byte, String> {
    Byte::parse_str(s, true).map_err(|e| e.to_string())
}

async fn create(mut ctx: Context, args: CreateArgs) -> crate::Result<()> {
    let name = args.name;
    let share: i32 = match args.protocol {
        None | Some(ShareProtocol::None) => 0,
        Some(ShareProtocol::Nvmf) => 1,
    };

    let response = ctx
        .client
        .create_replica(rpc::CreateReplicaRequest {
            pool: args.pool,
            uuid: name.clone(),
            size: args.size.as_u64(),
            thin: args.thin,
            share,
            allowed_hosts: args.allowed_hosts,
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

async fn create2(mut ctx: Context, args: Create2Args) -> crate::Result<()> {
    let share: i32 = match args.protocol {
        None | Some(ShareProtocol::None) => 0,
        Some(ShareProtocol::Nvmf) => 1,
    };

    let response = ctx
        .client
        .create_replica_v2(rpc::CreateReplicaRequestV2 {
            pool: args.pool,
            uuid: args.uuid,
            size: args.size.as_u64(),
            thin: args.thin,
            share,
            name: args.name,
            allowed_hosts: args.allowed_hosts,
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
    let uuid = args.uuid;

    ctx.client
        .destroy_replica(rpc::DestroyReplicaRequest { uuid: uuid.clone() })
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
        .client
        .list_replicas(rpc::Null {})
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
                return Ok(());
            }

            let table = replicas
                .iter()
                .map(|r| {
                    let proto = replica_protocol_to_str(r.share);

                    vec![
                        r.pool.clone(),
                        r.uuid.clone(),
                        r.thin.to_string(),
                        proto.to_string(),
                        ctx.units(Byte::from_u64(r.size)),
                        r.uri.clone(),
                    ]
                })
                .collect();
            ctx.print_list(vec!["POOL", "NAME", "THIN", "SHARE", ">SIZE", "URI"], table);
        }
    };

    Ok(())
}

async fn list2(mut ctx: Context) -> crate::Result<()> {
    let response = ctx
        .client
        .list_replicas_v2(rpc::Null {})
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
                return Ok(());
            }

            let table = replicas
                .iter()
                .map(|r| {
                    let proto = replica_protocol_to_str(r.share);

                    vec![
                        r.pool.clone(),
                        r.uuid.clone(),
                        r.name.clone(),
                        proto.to_string(),
                        r.size.to_string(),
                        r.uri.clone(),
                    ]
                })
                .collect();
            ctx.print_list(
                vec!["POOL", "UUID", "NAME", "PROTOCOL", ">SIZE", "URI"],
                table,
            );
        }
    };

    Ok(())
}

async fn share(mut ctx: Context, args: ShareArgs) -> crate::Result<()> {
    let uuid = args.uuid;
    let protocol_num: i32 = match args.protocol {
        ShareProtocol::None => 0,
        ShareProtocol::Nvmf => 1,
    };

    let response = ctx
        .client
        .share_replica(rpc::ShareReplicaRequest {
            uuid: uuid.clone(),
            share: protocol_num,
            allowed_hosts: vec![],
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

async fn stats(mut ctx: Context) -> crate::Result<()> {
    let response = ctx
        .client
        .stat_replicas(rpc::Null {})
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
                return Ok(());
            }

            let table = replicas
                .iter()
                .map(|r| {
                    let stats = r.stats.as_ref().unwrap();
                    vec![
                        r.pool.clone(),
                        r.uuid.clone(),
                        stats.num_read_ops.to_string(),
                        stats.num_write_ops.to_string(),
                        stats.bytes_read.to_string(),
                        stats.bytes_written.to_string(),
                    ]
                })
                .collect();
            ctx.print_list(
                vec!["POOL", "NAME", ">READS", ">WRITES", ">RBYTES", ">WBYTES"],
                table,
            );
        }
    };

    Ok(())
}

fn replica_protocol_to_str(i: i32) -> &'static str {
    match i {
        0 => "none",
        1 => "nvmf",
        2 => "iscsi",
        _ => "unknown",
    }
}
