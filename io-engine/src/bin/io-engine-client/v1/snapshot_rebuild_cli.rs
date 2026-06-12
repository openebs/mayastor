//!
//! methods to interact with the rebuild process

use crate::{
    context::{Context, OutputFormat},
    GrpcStatus,
};
use clap::{Args, Subcommand};
use colored_json::ToColoredJson;
use io_engine_api::{v1, v1::snapshot_rebuild::RebuildStatus};
use snafu::ResultExt;
use uuid::Uuid;

#[derive(Debug, Args)]
#[command(subcommand_required = true, arg_required_else_help = true)]
pub struct SnapshotRebuildArgs {
    #[command(subcommand)]
    command: SnapshotRebuildCommands,
}

#[derive(Debug, Subcommand)]
enum SnapshotRebuildCommands {
    /// Create and start a snapshot rebuild
    Create(CreateArgs),
    /// Destroy a snapshot rebuild
    Destroy(UuidArgs),
    /// List snapshot rebuilds
    List(ListArgs),
}

#[derive(Debug, Args)]
struct CreateArgs {
    /// uuid of the replica to snap rebuild
    uuid: Uuid,
    /// uri of the snapshot source to rebuild from
    uri: String,
}

#[derive(Debug, Args)]
struct UuidArgs {
    /// uuid of the snapshot rebuild
    uuid: Uuid,
}

#[derive(Debug, Args)]
struct ListArgs {
    /// uuid of the snapshot rebuild (optional)
    uuid: Option<Uuid>,
}

pub async fn handler(ctx: Context, args: SnapshotRebuildArgs) -> crate::Result<()> {
    match args.command {
        SnapshotRebuildCommands::Create(args) => create(ctx, args).await,
        SnapshotRebuildCommands::Destroy(args) => destroy(ctx, args).await,
        SnapshotRebuildCommands::List(args) => list(ctx, args).await,
    }
}

async fn create(mut ctx: Context, args: CreateArgs) -> crate::Result<()> {
    let uuid = args.uuid.to_string();
    let uri = args.uri;

    let response = ctx
        .v1
        .snapshot_rebuild
        .create_snapshot_rebuild(v1::snapshot_rebuild::CreateSnapshotRebuildRequest {
            replica_uuid: uuid.clone(),
            uuid: uuid.clone(),
            snapshot_uuid: "".to_string(),
            replica_uri: "".to_string(),
            snapshot_uri: uri,
            resume: false,
            bitmap: None,
            use_bitmap: false,
            error_policy: None,
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
            let uuid = response.into_inner().uuid;
            println!("Snapshot Rebuild {uuid} created");
        }
    };

    Ok(())
}

async fn destroy(mut ctx: Context, args: UuidArgs) -> crate::Result<()> {
    let uuid = args.uuid.to_string();

    let _response = ctx
        .v1
        .snapshot_rebuild
        .destroy_snapshot_rebuild(v1::snapshot_rebuild::DestroySnapshotRebuildRequest {
            uuid: uuid.clone(),
        })
        .await
        .context(GrpcStatus)?;
    println!("Snapshot Rebuild {uuid} deleted");

    Ok(())
}

async fn list(mut ctx: Context, args: ListArgs) -> crate::Result<()> {
    let response = ctx
        .v1
        .snapshot_rebuild
        .list_snapshot_rebuild(v1::snapshot_rebuild::ListSnapshotRebuildRequest {
            uuid: args.uuid.map(|u| u.to_string()),
            replica_uuid: None,
            snapshot_uuid: None,
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
            let response = response.into_inner();
            if response.rebuilds.is_empty() {
                return Ok(());
            }
            let table = response
                .rebuilds
                .into_iter()
                .map(|r| {
                    let status = r.status();
                    vec![
                        r.uuid,
                        r.snapshot_uri,
                        rebuild_status_to_str(status),
                        r.total.to_string(),
                        r.rebuilt.to_string(),
                        r.remaining.to_string(),
                        r.start_timestamp.map(|s| s.to_string()).unwrap_or_default(),
                        r.end_timestamp.map(|s| s.to_string()).unwrap_or_default(),
                    ]
                })
                .collect();
            ctx.print_list(
                vec![
                    "REPLICA",
                    "SNAPSHOT",
                    "STATUS",
                    "TOTAL",
                    "REBUILT",
                    "REMAINING",
                    "START",
                    "END",
                ],
                table,
            );
        }
    };

    Ok(())
}

fn rebuild_status_to_str(status: RebuildStatus) -> String {
    match status {
        RebuildStatus::Unknown => "unknown",
        RebuildStatus::Created => "created",
        RebuildStatus::Running => "running",
        RebuildStatus::Paused => "paused",
        RebuildStatus::Successful => "successful",
        RebuildStatus::Failed => "failed",
    }
    .to_string()
}
