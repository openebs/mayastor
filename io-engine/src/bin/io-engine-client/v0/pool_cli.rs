use crate::{
    context::{Context, OutputFormat},
    GrpcStatus,
};
use byte_unit::Byte;
use clap::{Args, Subcommand};
use colored_json::ToColoredJson;
use io_engine_api::v0 as rpc;
use snafu::ResultExt;
use std::convert::TryFrom;

#[derive(Debug, Args)]
#[command(subcommand_required = true, arg_required_else_help = true)]
pub struct PoolArgs {
    #[command(subcommand)]
    command: PoolCommands,
}

#[derive(Debug, Subcommand)]
enum PoolCommands {
    /// Create storage pool
    Create(CreateArgs),
    /// Destroy storage pool
    Destroy(DestroyArgs),
    /// List storage pools
    List,
}

#[derive(Debug, Args)]
struct CreateArgs {
    /// Storage pool name
    pool: String,
    /// Disk device files
    #[arg(action = clap::ArgAction::Append)]
    disk: Vec<String>,
}

#[derive(Debug, Args)]
struct DestroyArgs {
    /// Storage pool name
    pool: String,
}

pub async fn handler(ctx: Context, args: PoolArgs) -> crate::Result<()> {
    match args.command {
        PoolCommands::Create(args) => create(ctx, args).await,
        PoolCommands::Destroy(args) => destroy(ctx, args).await,
        PoolCommands::List => list(ctx).await,
    }
}

async fn create(mut ctx: Context, args: CreateArgs) -> crate::Result<()> {
    let name = args.pool;
    let disks = args.disk;

    let response = ctx
        .client
        .create_pool(rpc::CreatePoolRequest {
            name: name.clone(),
            disks,
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
            println!("{name}");
        }
    };

    Ok(())
}

async fn destroy(mut ctx: Context, args: DestroyArgs) -> crate::Result<()> {
    let name = args.pool;

    let response = ctx
        .client
        .destroy_pool(rpc::DestroyPoolRequest { name: name.clone() })
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
            println!("{name}");
        }
    };

    Ok(())
}

async fn list(mut ctx: Context) -> crate::Result<()> {
    ctx.v2("Requesting a list of pools");

    let response = ctx
        .client
        .list_pools(rpc::Null {})
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
            let pools: &Vec<rpc::Pool> = &response.get_ref().pools;
            if pools.is_empty() {
                ctx.v1("No pools found");
                return Ok(());
            }

            let table = pools
                .iter()
                .map(|p| {
                    let cap = Byte::from_u64(p.capacity);
                    let used = Byte::from_u64(p.used);
                    let state = pool_state_to_str(p.state);
                    vec![
                        p.name.clone(),
                        state.to_string(),
                        ctx.units(cap),
                        ctx.units(used),
                        p.disks.join(" "),
                    ]
                })
                .collect();
            ctx.print_list(vec!["NAME", "STATE", ">CAPACITY", ">USED", "DISKS"], table);
        }
    };

    Ok(())
}

fn pool_state_to_str(idx: i32) -> &'static str {
    match rpc::PoolState::try_from(idx).unwrap() {
        rpc::PoolState::PoolUnknown => "unknown",
        rpc::PoolState::PoolOnline => "online",
        rpc::PoolState::PoolDegraded => "degraded",
        rpc::PoolState::PoolFaulted => "faulted",
    }
}
