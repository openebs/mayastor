//!
//! methods to interact with the rebuild process

use crate::{
    context::{Context, OutputFormat},
    ClientError,
    GrpcStatus,
};
use clap::{Arg, ArgMatches, Command};
use colored_json::ToColoredJson;
use io_engine_api::{v1, v1::snapshot_rebuild::RebuildStatus};
use snafu::ResultExt;
use tonic::Status;

pub async fn handler(ctx: Context, matches: &ArgMatches) -> crate::Result<()> {
    match matches.subcommand().unwrap() {
        ("create", args) => create(ctx, args).await,
        ("destroy", args) => destroy(ctx, args).await,
        ("list", args) => list(ctx, args).await,
        (cmd, _) => {
            Err(Status::not_found(format!("command {cmd} does not exist")))
                .context(GrpcStatus)
        }
    }
}

pub fn subcommands() -> Command {
    let create = Command::new("create")
        .about("create and start a snapshot rebuild")
        .arg(
            Arg::new("uuid")
                .required(true)
                .index(1)
                .help("uuid of the replica to snap rebuild"),
        )
        .arg(
            Arg::new("uri")
                .required(true)
                .index(2)
                .help("uri of the snapshot source to rebuild from"),
        );

    let destroy = Command::new("destroy")
        .about("destroy a snapshot rebuild")
        .arg(
            Arg::new("uuid")
                .required(true)
                .index(1)
                .help("uuid of the snapshot rebuild"),
        );

    let list = Command::new("list")
        .about("list a specific one or all snapshot rebuilds")
        .arg(
            Arg::new("uuid")
                .required(false)
                .index(1)
                .help("uuid of the snapshot rebuild"),
        );

    Command::new("snapshot-rebuild")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .about("Snapshot Rebuild Management")
        .subcommand(create)
        .subcommand(destroy)
        .subcommand(list)
}

async fn create(mut ctx: Context, matches: &ArgMatches) -> crate::Result<()> {
    let uuid = matches
        .get_one::<String>("uuid")
        .ok_or_else(|| ClientError::MissingValue {
            field: "uuid".to_string(),
        })?
        .to_string();
    let uri = matches
        .get_one::<String>("uri")
        .ok_or_else(|| ClientError::MissingValue {
            field: "uri".to_string(),
        })?
        .to_string();

    let response = ctx
        .v1
        .snapshot_rebuild
        .create_snapshot_rebuild(
            v1::snapshot_rebuild::CreateSnapshotRebuildRequest {
                replica_uuid: uuid.to_string(),
                uuid,
                snapshot_uuid: "".to_string(),
                replica_uri: "".to_string(),
                snapshot_uri: uri,
                resume: false,
                bitmap: None,
                use_bitmap: false,
                error_policy: None,
            },
        )
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

async fn destroy(mut ctx: Context, matches: &ArgMatches) -> crate::Result<()> {
    let uuid = matches
        .get_one::<String>("uuid")
        .ok_or_else(|| ClientError::MissingValue {
            field: "uuid".to_string(),
        })?
        .to_string();

    let _response = ctx
        .v1
        .snapshot_rebuild
        .destroy_snapshot_rebuild(
            v1::snapshot_rebuild::DestroySnapshotRebuildRequest {
                uuid: uuid.to_string(),
            },
        )
        .await
        .context(GrpcStatus)?;
    println!("Snapshot Rebuild {uuid} deleted");

    Ok(())
}

async fn list(mut ctx: Context, matches: &ArgMatches) -> crate::Result<()> {
    let replica_uuid = matches.get_one::<String>("uuid").cloned();

    let response = ctx
        .v1
        .snapshot_rebuild
        .list_snapshot_rebuild(
            v1::snapshot_rebuild::ListSnapshotRebuildRequest {
                uuid: replica_uuid,
                replica_uuid: None,
                snapshot_uuid: None,
            },
        )
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
                        r.start_timestamp
                            .map(|s| s.to_string())
                            .unwrap_or_default(),
                        r.end_timestamp
                            .map(|s| s.to_string())
                            .unwrap_or_default(),
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
