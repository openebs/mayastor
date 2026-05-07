//!
//! methods to interact with the rebuild process

use crate::{
    context::{Context, OutputFormat},
    GrpcStatus,
};
use clap::{Args, Subcommand};
use colored_json::ToColoredJson;
use io_engine_api::v0 as rpc;
use snafu::ResultExt;
use uuid::Uuid;

#[derive(Debug, Args)]
#[command(subcommand_required = true, arg_required_else_help = true)]
pub struct RebuildArgs {
    #[command(subcommand)]
    command: RebuildCommands,
}

#[derive(Debug, Subcommand)]
enum RebuildCommands {
    /// Starts a rebuild
    Start(UuidUriArgs),
    /// Stops a rebuild
    Stop(UuidUriArgs),
    /// Pauses a rebuild
    Pause(UuidUriArgs),
    /// Resumes a rebuild
    Resume(UuidUriArgs),
    /// Gets the rebuild state of the child
    State(UuidUriArgs),
    /// Gets the rebuild stats of the child
    Stats(UuidUriArgs),
    /// Shows the progress of a rebuild
    Progress(UuidUriArgs),
}

#[derive(Debug, Args)]
struct UuidUriArgs {
    /// uuid of the nexus
    uuid: Uuid,
    /// uri of child
    uri: String,
}

pub async fn handler(ctx: Context, args: RebuildArgs) -> crate::Result<()> {
    match args.command {
        RebuildCommands::Start(args) => start(ctx, args).await,
        RebuildCommands::Stop(args) => stop(ctx, args).await,
        RebuildCommands::Pause(args) => pause(ctx, args).await,
        RebuildCommands::Resume(args) => resume(ctx, args).await,
        RebuildCommands::State(args) => state(ctx, args).await,
        RebuildCommands::Stats(args) => stats(ctx, args).await,
        RebuildCommands::Progress(args) => progress(ctx, args).await,
    }
}

async fn start(mut ctx: Context, args: UuidUriArgs) -> crate::Result<()> {
    let uuid = args.uuid.to_string();
    let uri = args.uri;

    let response = ctx
        .client
        .start_rebuild(rpc::StartRebuildRequest {
            uuid: uuid.clone(),
            uri: uri.clone(),
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
            println!("{uri}");
        }
    };

    Ok(())
}

async fn stop(mut ctx: Context, args: UuidUriArgs) -> crate::Result<()> {
    let uuid = args.uuid.to_string();
    let uri = args.uri;

    let response = ctx
        .client
        .stop_rebuild(rpc::StopRebuildRequest {
            uuid: uuid.clone(),
            uri: uri.clone(),
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
            println!("{uri}");
        }
    };

    Ok(())
}

async fn pause(mut ctx: Context, args: UuidUriArgs) -> crate::Result<()> {
    let uuid = args.uuid.to_string();
    let uri = args.uri;

    let response = ctx
        .client
        .pause_rebuild(rpc::PauseRebuildRequest {
            uuid: uuid.clone(),
            uri: uri.clone(),
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
            println!("{uri}");
        }
    };

    Ok(())
}

async fn resume(mut ctx: Context, args: UuidUriArgs) -> crate::Result<()> {
    let uuid = args.uuid.to_string();
    let uri = args.uri;

    let response = ctx
        .client
        .resume_rebuild(rpc::ResumeRebuildRequest {
            uuid: uuid.clone(),
            uri: uri.clone(),
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
            println!("{uri}");
        }
    };

    Ok(())
}

async fn state(mut ctx: Context, args: UuidUriArgs) -> crate::Result<()> {
    let uuid = args.uuid.to_string();
    let uri = args.uri;

    let response = ctx
        .client
        .get_rebuild_state(rpc::RebuildStateRequest {
            uuid: uuid.clone(),
            uri: uri.clone(),
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
            ctx.print_list(vec!["state"], vec![vec![response.get_ref().state.clone()]]);
        }
    };

    Ok(())
}

async fn stats(mut ctx: Context, args: UuidUriArgs) -> crate::Result<()> {
    let uuid = args.uuid.to_string();
    let uri = args.uri;

    ctx.v2(&format!(
        "Getting the rebuild stats of child {uri} on nexus {uuid}",
    ));
    let response = ctx
        .client
        .get_rebuild_stats(rpc::RebuildStatsRequest {
            uuid: uuid.clone(),
            uri: uri.clone(),
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
            let response = &response.get_ref();
            ctx.print_list(
                vec![
                    "blocks_total",
                    "blocks_recovered",
                    "progress (%)",
                    "segment_size_blks",
                    "block_size",
                    "tasks_total",
                    "tasks_active",
                ],
                vec![[
                    response.blocks_total,
                    response.blocks_recovered,
                    response.progress,
                    response.segment_size_blks,
                    response.block_size,
                    response.tasks_total,
                    response.tasks_active,
                ]
                .iter()
                .map(|s| s.to_string())
                .collect()],
            );
        }
    };

    Ok(())
}

async fn progress(mut ctx: Context, args: UuidUriArgs) -> crate::Result<()> {
    let uuid = args.uuid.to_string();
    let uri = args.uri;

    let response = ctx
        .client
        .get_rebuild_progress(rpc::RebuildProgressRequest {
            uuid: uuid.clone(),
            uri: uri.clone(),
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
            ctx.print_list(
                vec!["progress (%)"],
                vec![vec![response.get_ref().progress.to_string()]],
            );
        }
    };
    Ok(())
}
