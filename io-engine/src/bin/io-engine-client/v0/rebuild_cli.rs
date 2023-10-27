//!
//! methods to interact with the rebuild process

use crate::{
    context::{Context, OutputFormat},
    ClientError,
    GrpcStatus,
};
use clap::{Arg, ArgMatches, Command};
use colored_json::ToColoredJson;
use io_engine_api::v0 as rpc;
use snafu::ResultExt;
use tonic::Status;

pub async fn handler(ctx: Context, matches: &ArgMatches) -> crate::Result<()> {
    match matches.subcommand().unwrap() {
        ("start", args) => start(ctx, args).await,
        ("stop", args) => stop(ctx, args).await,
        ("pause", args) => pause(ctx, args).await,
        ("resume", args) => resume(ctx, args).await,
        ("state", args) => state(ctx, args).await,
        ("stats", args) => stats(ctx, args).await,
        ("progress", args) => progress(ctx, args).await,
        (cmd, _) => {
            Err(Status::not_found(format!("command {cmd} does not exist")))
                .context(GrpcStatus)
        }
    }
}

pub fn subcommands() -> Command {
    let start = Command::new("start")
        .about("starts a rebuild")
        .arg(
            Arg::new("uuid")
                .required(true)
                .index(1)
                .help("uuid of the nexus"),
        )
        .arg(
            Arg::new("uri")
                .required(true)
                .index(2)
                .help("uri of child to start rebuilding"),
        );

    let stop = Command::new("stop")
        .about("stops a rebuild")
        .arg(
            Arg::new("uuid")
                .required(true)
                .index(1)
                .help("uuid of the nexus"),
        )
        .arg(
            Arg::new("uri")
                .required(true)
                .index(2)
                .help("uri of child to stop rebuilding"),
        );

    let pause = Command::new("pause")
        .about("pauses a rebuild")
        .arg(
            Arg::new("uuid")
                .required(true)
                .index(1)
                .help("uuid of the nexus"),
        )
        .arg(
            Arg::new("uri")
                .required(true)
                .index(2)
                .help("uri of child to pause rebuilding"),
        );

    let resume = Command::new("resume")
        .about("resumes a rebuild")
        .arg(
            Arg::new("uuid")
                .required(true)
                .index(1)
                .help("uuid of the nexus"),
        )
        .arg(
            Arg::new("uri")
                .required(true)
                .index(2)
                .help("uri of child to resume rebuilding"),
        );

    let state = Command::new("state")
        .about("gets the rebuild state of the child")
        .arg(
            Arg::new("uuid")
                .required(true)
                .index(1)
                .help("uuid of the nexus"),
        )
        .arg(
            Arg::new("uri")
                .required(true)
                .index(2)
                .help("uri of child to get the rebuild state from"),
        );

    let stats = Command::new("stats")
        .about("gets the rebuild stats of the child")
        .arg(
            Arg::new("uuid")
                .required(true)
                .index(1)
                .help("uuid of the nexus"),
        )
        .arg(
            Arg::new("uri")
                .required(true)
                .index(2)
                .help("uri of child to get the rebuild stats from"),
        );

    let progress = Command::new("progress")
        .about("shows the progress of a rebuild")
        .arg(
            Arg::new("uuid")
                .required(true)
                .index(1)
                .help("uuid of the nexus"),
        )
        .arg(
            Arg::new("uri")
                .required(true)
                .index(2)
                .help("uri of child to get the rebuild progress from"),
        );

    Command::new("rebuild")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .about("Rebuild management")
        .subcommand(start)
        .subcommand(stop)
        .subcommand(pause)
        .subcommand(resume)
        .subcommand(state)
        .subcommand(stats)
        .subcommand(progress)
}

async fn start(mut ctx: Context, matches: &ArgMatches) -> crate::Result<()> {
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
            println!("{}", &uri);
        }
    };

    Ok(())
}

async fn stop(mut ctx: Context, matches: &ArgMatches) -> crate::Result<()> {
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
            println!("{}", &uri);
        }
    };

    Ok(())
}

async fn pause(mut ctx: Context, matches: &ArgMatches) -> crate::Result<()> {
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
            println!("{}", &uri);
        }
    };

    Ok(())
}

async fn resume(mut ctx: Context, matches: &ArgMatches) -> crate::Result<()> {
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
            println!("{}", &uri);
        }
    };

    Ok(())
}

async fn state(mut ctx: Context, matches: &ArgMatches) -> crate::Result<()> {
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
            ctx.print_list(
                vec!["state"],
                vec![vec![response.get_ref().state.clone()]],
            );
        }
    };

    Ok(())
}

async fn stats(mut ctx: Context, matches: &ArgMatches) -> crate::Result<()> {
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

async fn progress(mut ctx: Context, matches: &ArgMatches) -> crate::Result<()> {
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
