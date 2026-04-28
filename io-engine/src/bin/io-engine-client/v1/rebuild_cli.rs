//!
//! methods to interact with the rebuild process

use crate::{
    context::{Context, OutputFormat},
    GrpcStatus,
};
use clap::{Args, Subcommand};
use colored_json::ToColoredJson;
use io_engine_api::v1;
use snafu::ResultExt;
use std::convert::TryFrom;
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
    /// Shows the rebuild history for children of a nexus
    History(UuidArgs),
}

#[derive(Debug, Args)]
struct UuidUriArgs {
    /// uuid of the nexus
    uuid: Uuid,
    /// uri of child
    uri: String,
}

#[derive(Debug, Args)]
struct UuidArgs {
    /// uuid of the nexus
    uuid: Uuid,
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
        RebuildCommands::History(args) => history(ctx, args).await,
    }
}

async fn start(mut ctx: Context, args: UuidUriArgs) -> crate::Result<()> {
    let uri = args.uri.clone();

    let response = ctx
        .v1
        .nexus
        .start_rebuild(v1::nexus::StartRebuildRequest {
            nexus_uuid: args.uuid.to_string(),
            uri: args.uri,
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
    let uri = args.uri.clone();

    let response = ctx
        .v1
        .nexus
        .stop_rebuild(v1::nexus::StopRebuildRequest {
            nexus_uuid: args.uuid.to_string(),
            uri: args.uri,
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
    let uri = args.uri.clone();

    let response = ctx
        .v1
        .nexus
        .pause_rebuild(v1::nexus::PauseRebuildRequest {
            nexus_uuid: args.uuid.to_string(),
            uri: args.uri,
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
    let uri = args.uri.clone();

    let response = ctx
        .v1
        .nexus
        .resume_rebuild(v1::nexus::ResumeRebuildRequest {
            nexus_uuid: args.uuid.to_string(),
            uri: args.uri,
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
    let response = ctx
        .v1
        .nexus
        .get_rebuild_state(v1::nexus::RebuildStateRequest {
            nexus_uuid: args.uuid.to_string(),
            uri: args.uri,
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

async fn history(mut ctx: Context, args: UuidArgs) -> crate::Result<()> {
    let uuid = args.uuid.to_string();
    let response = ctx
        .v1
        .nexus
        .get_rebuild_history(v1::nexus::RebuildHistoryRequest { uuid: uuid.clone() })
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
            if response.records.is_empty() {
                return Ok(());
            }
            let table = response
                .records
                .iter()
                .map(|r| {
                    let state = rebuild_state_to_str(
                        v1::nexus::RebuildJobState::try_from(r.state).unwrap(),
                    )
                    .to_string();

                    vec![
                        r.child_uri.clone(),
                        r.src_uri.clone(),
                        r.blocks_total.to_string(),
                        r.blocks_transferred.to_string(),
                        state,
                        r.blocks_per_task.to_string(),
                        r.block_size.to_string(),
                        r.is_partial.to_string(),
                        r.start_time.as_ref().unwrap().to_string(),
                        r.end_time.as_ref().unwrap().to_string(),
                    ]
                })
                .collect();
            ctx.print_list(
                vec![
                    "CHILD",
                    "SOURCE",
                    ">TOTAL",
                    ">TRANSFERRED",
                    ">STATE",
                    ">BLK_PER_TASK",
                    ">BLK_SIZE",
                    ">PARTIAL",
                    "START",
                    "END",
                ],
                table,
            );
        }
    };

    Ok(())
}

async fn stats(mut ctx: Context, args: UuidUriArgs) -> crate::Result<()> {
    let uri = args.uri.clone();
    let uuid = args.uuid.to_string();

    ctx.v2(&format!(
        "Getting the rebuild stats of child {uri} on nexus {uuid}"
    ));
    let response = ctx
        .v1
        .nexus
        .get_rebuild_stats(v1::nexus::RebuildStatsRequest {
            nexus_uuid: uuid,
            uri: args.uri,
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
                    ">TOTAL",
                    ">RECOVERED",
                    ">TRANSFERRED",
                    ">REMAINING",
                    ">PROGRESS (%)",
                    ">BLK_PER_TASK",
                    ">BLK_SIZE",
                    ">PARTIAL",
                    ">TASKS_TOTAL",
                    ">TASKS_ACTIVE",
                ],
                vec![vec![
                    response.blocks_total.to_string(),
                    response.blocks_recovered.to_string(),
                    response.blocks_transferred.to_string(),
                    response.blocks_remaining.to_string(),
                    response.progress.to_string(),
                    response.blocks_per_task.to_string(),
                    response.block_size.to_string(),
                    response.is_partial.to_string(),
                    response.tasks_total.to_string(),
                    response.tasks_active.to_string(),
                ]],
            );
        }
    };

    Ok(())
}

async fn progress(mut ctx: Context, args: UuidUriArgs) -> crate::Result<()> {
    let response = ctx
        .v1
        .nexus
        .get_rebuild_stats(v1::nexus::RebuildStatsRequest {
            nexus_uuid: args.uuid.to_string(),
            uri: args.uri,
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

fn rebuild_state_to_str(s: v1::nexus::RebuildJobState) -> &'static str {
    match s {
        v1::nexus::RebuildJobState::Init => "init",
        v1::nexus::RebuildJobState::Rebuilding => "rebuilding",
        v1::nexus::RebuildJobState::Stopped => "stopped",
        v1::nexus::RebuildJobState::Paused => "paused",
        v1::nexus::RebuildJobState::Failed => "failed",
        v1::nexus::RebuildJobState::Completed => "completed",
    }
}
