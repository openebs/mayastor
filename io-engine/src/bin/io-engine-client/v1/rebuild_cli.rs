//!
//! methods to interact with the rebuild process

use crate::{
    context::{Context, OutputFormat},
    ClientError,
    GrpcStatus,
};
use clap::{App, AppSettings, Arg, ArgMatches, SubCommand};
use colored_json::ToColoredJson;
use mayastor_api::v1;
use snafu::ResultExt;
use tonic::Status;

pub async fn handler(
    ctx: Context,
    matches: &ArgMatches<'_>,
) -> crate::Result<()> {
    match matches.subcommand() {
        ("start", Some(args)) => start(ctx, args).await,
        ("stop", Some(args)) => stop(ctx, args).await,
        ("pause", Some(args)) => pause(ctx, args).await,
        ("resume", Some(args)) => resume(ctx, args).await,
        ("state", Some(args)) => state(ctx, args).await,
        ("stats", Some(args)) => stats(ctx, args).await,
        ("progress", Some(args)) => progress(ctx, args).await,
        ("history", Some(args)) => history(ctx, args).await,
        (cmd, _) => {
            Err(Status::not_found(format!("command {cmd} does not exist")))
                .context(GrpcStatus)
        }
    }
}

pub fn subcommands<'a, 'b>() -> App<'a, 'b> {
    let start = SubCommand::with_name("start")
        .about("starts a rebuild")
        .arg(
            Arg::with_name("uuid")
                .required(true)
                .index(1)
                .help("uuid of the nexus"),
        )
        .arg(
            Arg::with_name("uri")
                .required(true)
                .index(2)
                .help("uri of child to start rebuilding"),
        );

    let stop = SubCommand::with_name("stop")
        .about("stops a rebuild")
        .arg(
            Arg::with_name("uuid")
                .required(true)
                .index(1)
                .help("uuid of the nexus"),
        )
        .arg(
            Arg::with_name("uri")
                .required(true)
                .index(2)
                .help("uri of child to stop rebuilding"),
        );

    let pause = SubCommand::with_name("pause")
        .about("pauses a rebuild")
        .arg(
            Arg::with_name("uuid")
                .required(true)
                .index(1)
                .help("uuid of the nexus"),
        )
        .arg(
            Arg::with_name("uri")
                .required(true)
                .index(2)
                .help("uri of child to pause rebuilding"),
        );

    let resume = SubCommand::with_name("resume")
        .about("resumes a rebuild")
        .arg(
            Arg::with_name("uuid")
                .required(true)
                .index(1)
                .help("uuid of the nexus"),
        )
        .arg(
            Arg::with_name("uri")
                .required(true)
                .index(2)
                .help("uri of child to resume rebuilding"),
        );

    let state = SubCommand::with_name("state")
        .about("gets the rebuild state of the child")
        .arg(
            Arg::with_name("uuid")
                .required(true)
                .index(1)
                .help("uuid of the nexus"),
        )
        .arg(
            Arg::with_name("uri")
                .required(true)
                .index(2)
                .help("uri of child to get the rebuild state from"),
        );

    let stats = SubCommand::with_name("stats")
        .about("gets the rebuild stats of the child")
        .arg(
            Arg::with_name("uuid")
                .required(true)
                .index(1)
                .help("uuid of the nexus"),
        )
        .arg(
            Arg::with_name("uri")
                .required(true)
                .index(2)
                .help("uri of child to get the rebuild stats from"),
        );

    let progress = SubCommand::with_name("progress")
        .about("shows the progress of a rebuild")
        .arg(
            Arg::with_name("uuid")
                .required(true)
                .index(1)
                .help("uuid of the nexus"),
        )
        .arg(
            Arg::with_name("uri")
                .required(true)
                .index(2)
                .help("uri of child to get the rebuild progress from"),
        );

    let history = SubCommand::with_name("history")
        .about("shows the rebuild history for children of a nexus")
        .arg(
            Arg::with_name("uuid")
                .required(true)
                .index(1)
                .help("uuid of the nexus"),
        );

    SubCommand::with_name("rebuild")
        .settings(&[
            AppSettings::SubcommandRequiredElseHelp,
            AppSettings::ColoredHelp,
            AppSettings::ColorAlways,
        ])
        .about("Rebuild management")
        .subcommand(start)
        .subcommand(stop)
        .subcommand(pause)
        .subcommand(resume)
        .subcommand(state)
        .subcommand(stats)
        .subcommand(progress)
        .subcommand(history)
}

async fn start(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> crate::Result<()> {
    let uuid = matches
        .value_of("uuid")
        .ok_or_else(|| ClientError::MissingValue {
            field: "uuid".to_string(),
        })?
        .to_string();
    let uri = matches
        .value_of("uri")
        .ok_or_else(|| ClientError::MissingValue {
            field: "uri".to_string(),
        })?
        .to_string();

    let response = ctx
        .v1
        .nexus
        .start_rebuild(v1::nexus::StartRebuildRequest {
            nexus_uuid: uuid,
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

async fn stop(mut ctx: Context, matches: &ArgMatches<'_>) -> crate::Result<()> {
    let uuid = matches
        .value_of("uuid")
        .ok_or_else(|| ClientError::MissingValue {
            field: "uuid".to_string(),
        })?
        .to_string();
    let uri = matches
        .value_of("uri")
        .ok_or_else(|| ClientError::MissingValue {
            field: "uri".to_string(),
        })?
        .to_string();

    let response = ctx
        .v1
        .nexus
        .stop_rebuild(v1::nexus::StopRebuildRequest {
            nexus_uuid: uuid,
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

async fn pause(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> crate::Result<()> {
    let uuid = matches
        .value_of("uuid")
        .ok_or_else(|| ClientError::MissingValue {
            field: "uuid".to_string(),
        })?
        .to_string();
    let uri = matches
        .value_of("uri")
        .ok_or_else(|| ClientError::MissingValue {
            field: "uri".to_string(),
        })?
        .to_string();

    let response = ctx
        .v1
        .nexus
        .pause_rebuild(v1::nexus::PauseRebuildRequest {
            nexus_uuid: uuid,
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

async fn resume(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> crate::Result<()> {
    let uuid = matches
        .value_of("uuid")
        .ok_or_else(|| ClientError::MissingValue {
            field: "uuid".to_string(),
        })?
        .to_string();
    let uri = matches
        .value_of("uri")
        .ok_or_else(|| ClientError::MissingValue {
            field: "uri".to_string(),
        })?
        .to_string();

    let response = ctx
        .v1
        .nexus
        .resume_rebuild(v1::nexus::ResumeRebuildRequest {
            nexus_uuid: uuid,
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

async fn state(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> crate::Result<()> {
    let uuid = matches
        .value_of("uuid")
        .ok_or_else(|| ClientError::MissingValue {
            field: "uuid".to_string(),
        })?
        .to_string();
    let uri = matches
        .value_of("uri")
        .ok_or_else(|| ClientError::MissingValue {
            field: "uri".to_string(),
        })?
        .to_string();

    let response = ctx
        .v1
        .nexus
        .get_rebuild_state(v1::nexus::RebuildStateRequest {
            nexus_uuid: uuid,
            uri,
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

async fn history(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> crate::Result<()> {
    let uuid = matches
        .value_of("uuid")
        .ok_or_else(|| ClientError::MissingValue {
            field: "uuid".to_string(),
        })?
        .to_string();
    let response = ctx
        .v1
        .nexus
        .get_rebuild_history(v1::nexus::RebuildHistoryRequest {
            uuid: uuid.clone(),
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
            if response.records.is_empty() {
                return Ok(());
            }
            let table = response
                .records
                .iter()
                .map(|r| {
                    let state = rebuild_state_to_str(
                        v1::nexus::RebuildJobState::from_i32(r.state).unwrap(),
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

async fn stats(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> crate::Result<()> {
    let uuid = matches
        .value_of("uuid")
        .ok_or_else(|| ClientError::MissingValue {
            field: "uuid".to_string(),
        })?
        .to_string();
    let uri = matches
        .value_of("uri")
        .ok_or_else(|| ClientError::MissingValue {
            field: "uri".to_string(),
        })?
        .to_string();

    ctx.v2(&format!(
        "Getting the rebuild stats of child {uri} on nexus {uuid}"
    ));
    let response = ctx
        .v1
        .nexus
        .get_rebuild_stats(v1::nexus::RebuildStatsRequest {
            nexus_uuid: uuid,
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

async fn progress(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> crate::Result<()> {
    let uuid = matches
        .value_of("uuid")
        .ok_or_else(|| ClientError::MissingValue {
            field: "uuid".to_string(),
        })?
        .to_string();
    let uri = matches
        .value_of("uri")
        .ok_or_else(|| ClientError::MissingValue {
            field: "uri".to_string(),
        })?
        .to_string();

    let response = ctx
        .v1
        .nexus
        .get_rebuild_stats(v1::nexus::RebuildStatsRequest {
            nexus_uuid: uuid,
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
