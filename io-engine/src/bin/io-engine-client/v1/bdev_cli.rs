use crate::{
    context::{Context, OutputFormat},
    GrpcStatus,
};
use byte_unit::Byte;
use clap::{Args, Subcommand};
use colored_json::prelude::*;
use io_engine_api::v1 as v1rpc;
use snafu::ResultExt;

#[derive(Debug, Args)]
#[command(subcommand_required = true, arg_required_else_help = true)]
pub struct BdevArgs {
    #[command(subcommand)]
    command: BdevCommands,
}

#[derive(Debug, Subcommand)]
enum BdevCommands {
    /// List all bdevs
    List,
    /// Create a new bdev by specifying a URI
    Create(CreateArgs),
    /// Share the given bdev
    Share(ShareArgs),
    /// Destroy the given bdev
    Destroy(DestroyArgs),
    /// Unshare the given bdev
    Unshare(UnshareArgs),
}

#[derive(Debug, Args)]
struct CreateArgs {
    /// URI to create
    uri: url::Url,
}

/// Share protocol for bdevs
#[derive(Debug, Clone, clap::ValueEnum)]
enum BdevShareProtocol {
    Nvmf,
}

#[derive(Debug, Args)]
struct ShareArgs {
    /// bdev name
    name: String,
    #[arg(
        long,
        short = 'p',
        default_value = "nvmf",
        help = "the protocol to use to share the given bdev."
    )]
    protocol: BdevShareProtocol,
    #[arg(long = "allowed-host", action = clap::ArgAction::Append, help = "NQN of hosts which are allowed to connect to the target")]
    allowed_host: Vec<String>,
}

#[derive(Debug, Args)]
struct DestroyArgs {
    /// bdev name
    name: String,
}

#[derive(Debug, Args)]
struct UnshareArgs {
    /// bdev name
    name: String,
}

pub async fn handler(ctx: Context, args: BdevArgs) -> crate::Result<()> {
    match args.command {
        BdevCommands::List => list(ctx).await,
        BdevCommands::Create(args) => create(ctx, args).await,
        BdevCommands::Share(args) => share(ctx, args).await,
        BdevCommands::Destroy(args) => destroy(ctx, args).await,
        BdevCommands::Unshare(args) => unshare(ctx, args).await,
    }
}

async fn list(mut ctx: Context) -> crate::Result<()> {
    let response = ctx
        .v1
        .bdev
        .list(v1rpc::bdev::ListBdevOptions { name: None })
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
            let bdevs = &response.get_ref().bdevs;
            if bdevs.is_empty() {
                ctx.v1("No bdevs found");
                return Ok(());
            }
            let header = vec![
                "UUID",
                "NUM_BLOCKS",
                "BLK_SIZE",
                "CAPACITY",
                "CLAIMED_BY",
                "NAME",
                "SHARE_URI",
            ];
            let table = bdevs
                .iter()
                .map(|bdev| {
                    let cap = Byte::from_u64(bdev.num_blocks * bdev.blk_size as u64);
                    vec![
                        bdev.uuid.to_string(),
                        bdev.num_blocks.to_string(),
                        bdev.blk_size.to_string(),
                        ctx.units(cap),
                        bdev.claimed_by.to_string(),
                        bdev.name.to_string(),
                        bdev.share_uri.to_string(),
                    ]
                })
                .collect();
            ctx.print_list(header, table);
        }
    };
    Ok(())
}

async fn create(mut ctx: Context, args: CreateArgs) -> crate::Result<()> {
    let response = ctx
        .v1
        .bdev
        .create(v1rpc::bdev::CreateBdevRequest {
            uri: args.uri.to_string(),
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
            let val = &response.get_ref().bdev.as_ref().unwrap().name;
            println!("{val}");
        }
    };
    Ok(())
}

async fn destroy(mut ctx: Context, args: DestroyArgs) -> crate::Result<()> {
    let name = args.name;
    let bdevs = ctx
        .v1
        .bdev
        .list(v1rpc::bdev::ListBdevOptions { name: None })
        .await
        .context(GrpcStatus)?
        .into_inner();

    let found = bdevs
        .bdevs
        .iter()
        .find(|b| b.name == name)
        .ok_or_else(|| tonic::Status::not_found(name.clone()))
        .context(GrpcStatus)?;

    let _ = ctx
        .v1
        .bdev
        .unshare(v1rpc::bdev::BdevUnshareRequest { name })
        .await
        .context(GrpcStatus)?;

    let response = ctx
        .v1
        .bdev
        .destroy(v1rpc::bdev::DestroyBdevRequest {
            uri: found.uri.clone(),
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
            let name = &found.name;
            println!("{name}");
        }
    };
    Ok(())
}

async fn share(mut ctx: Context, args: ShareArgs) -> crate::Result<()> {
    let protocol = match args.protocol {
        BdevShareProtocol::Nvmf => v1rpc::common::ShareProtocol::Nvmf as i32,
    };

    let response = ctx
        .v1
        .bdev
        .share(v1rpc::bdev::BdevShareRequest {
            name: args.name,
            protocol,
            allowed_hosts: args.allowed_host,
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
            let val = &response.get_ref().bdev.as_ref().unwrap().uri;
            println!("{val}");
        }
    }
    Ok(())
}

async fn unshare(mut ctx: Context, args: UnshareArgs) -> crate::Result<()> {
    let name = args.name;
    let response = ctx
        .v1
        .bdev
        .unshare(v1rpc::bdev::BdevUnshareRequest { name: name.clone() })
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
    }
    Ok(())
}
