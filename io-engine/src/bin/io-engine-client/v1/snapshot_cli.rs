//!
//! methods to interact with snapshot management

use crate::{
    context::{Context, OutputFormat},
    ClientError,
    GrpcStatus,
};
use clap::{App, AppSettings, Arg, ArgMatches, SubCommand};
use colored_json::ToColoredJson;
use mayastor_api::{v0 as rpc, v1 as v1_rpc};
use snafu::ResultExt;
use tonic::Status;

pub async fn handler(
    ctx: Context,
    matches: &ArgMatches<'_>,
) -> crate::Result<()> {
    match matches.subcommand() {
        ("create_for_nexus", Some(args)) => create_for_nexus(ctx, args).await,
        ("create_for_replica", Some(args)) => {
            create_for_replica(ctx, args).await
        }
        ("list", Some(args)) => list(ctx, args).await,
        ("destroy", Some(args)) => destroy(ctx, args).await,
        (cmd, _) => {
            Err(Status::not_found(format!("command {cmd} does not exist")))
                .context(GrpcStatus)
        }
    }
}

pub fn subcommands<'a, 'b>() -> App<'a, 'b> {
    let create_for_nexus = SubCommand::with_name("create_for_nexus")
        .about("create a snapshot for nexus")
        .arg(
            Arg::with_name("uuid")
                .required(true)
                .index(1)
                .help("uuid of the nexus"),
        );
    let create_for_replica = SubCommand::with_name("create_for_replica")
        .about("create a snapshot for replica")
        .arg(
            Arg::with_name("replica_uuid")
                .required(true)
                .index(1)
                .help("Replica uuid"),
        )
        .arg(
            Arg::with_name("snapshot_name")
                .required(true)
                .index(2)
                .help("Snapshot name"),
        )
        .arg(
            Arg::with_name("entity_id")
                .required(true)
                .index(3)
                .help("Entity Id"),
        )
        .arg(
            Arg::with_name("txn_id")
                .required(true)
                .index(4)
                .help("Transaction id"),
        )
        .arg(
            Arg::with_name("snapshot_uuid")
                .required(true)
                .index(5)
                .help("Snapshot uuid"),
        );
    let list = SubCommand::with_name("list")
        .about("List Snapshot Detail")
        .arg(
            Arg::with_name("source_uuid")
                .required(false)
                .index(1)
                .help("Source uuid from which snapshot is created"),
        )
        .arg(
            Arg::with_name("snapshot_uuid")
                .required(false)
                .index(2)
                .help("Snapshot uuid"),
        );
    let destroy = SubCommand::with_name("destroy")
        .about("Destroy Snapshot")
        .arg(
            Arg::with_name("snapshot_uuid")
                .required(true)
                .index(1)
                .help("Snapshot uuid"),
        )
        .arg(
            Arg::with_name("pool-uuid")
                .long("pool-uuid")
                .required(false)
                .takes_value(true)
                .conflicts_with("pool-name")
                .help("Uuid of the pool where snapshot resides"),
        )
        .arg(
            Arg::with_name("pool-name")
                .long("pool-name")
                .required(false)
                .takes_value(true)
                .conflicts_with("pool-uuid")
                .help("Name of the pool where snapshot resides"),
        );
    SubCommand::with_name("snapshot")
        .settings(&[
            AppSettings::SubcommandRequiredElseHelp,
            AppSettings::ColoredHelp,
            AppSettings::ColorAlways,
        ])
        .about("Snapshot management")
        .subcommand(create_for_nexus)
        .subcommand(create_for_replica)
        .subcommand(list)
        .subcommand(destroy)
}

async fn create_for_nexus(
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
        .client
        .create_snapshot(rpc::CreateSnapshotRequest {
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
            println!("{}", &uuid);
        }
    };

    Ok(())
}

/// Replica Snapshot Create CLI Function.
async fn create_for_replica(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> crate::Result<()> {
    let replica_uuid = matches
        .value_of("replica_uuid")
        .ok_or_else(|| ClientError::MissingValue {
            field: "replica_uuid".to_string(),
        })?
        .to_owned();
    let snapshot_name = matches
        .value_of("snapshot_name")
        .ok_or_else(|| ClientError::MissingValue {
            field: "snapshot_name".to_string(),
        })?
        .to_owned();
    let entity_id = matches
        .value_of("entity_id")
        .ok_or_else(|| ClientError::MissingValue {
            field: "entity_id".to_string(),
        })?
        .to_owned();
    let txn_id = matches
        .value_of("txn_id")
        .ok_or_else(|| ClientError::MissingValue {
            field: "txn_id".to_string(),
        })?
        .to_owned();
    let snapshot_uuid = matches
        .value_of("snapshot_uuid")
        .ok_or_else(|| ClientError::MissingValue {
            field: "snapshot_uuid".to_string(),
        })?
        .to_owned();
    // let snapshot_uuid = Uuid::generate().to_string();
    let request = v1_rpc::snapshot::CreateReplicaSnapshotRequest {
        replica_uuid,
        snapshot_uuid,
        snapshot_name,
        entity_id,
        txn_id,
    };

    let response = ctx
        .v1
        .snapshot
        .create_replica_snapshot(request)
        .await
        .context(GrpcStatus)?;

    match ctx.output {
        OutputFormat::Json => {
            println!(
                "Snapshot Created {}",
                serde_json::to_string_pretty(&response.get_ref())
                    .unwrap()
                    .to_colored_json_auto()
                    .unwrap()
            );
        }
        OutputFormat::Default => {
            let snapshots = &response.get_ref().snapshot;
            let table = snapshots
                .iter()
                .map(|r| {
                    vec![
                        r.snapshot_uuid.clone(),
                        r.snapshot_name.clone(),
                        r.snapshot_size.clone().to_string(),
                        r.timestamp.clone().unwrap_or_default().to_string(),
                        r.num_clones.to_string(),
                        r.source_uuid.clone(),
                        r.source_size.to_string(),
                        r.pool_uuid.to_string(),
                        r.entity_id.clone(),
                        r.txn_id.clone(),
                        r.valid_snapshot.to_string(),
                    ]
                })
                .collect();
            ctx.print_list(
                vec![
                    "SNAP_UUID",
                    "SNAP_NAME",
                    "SNAP_SIZE",
                    "CREATE_TIME",
                    "CLONE_COUNT",
                    "SOURCE_UUID",
                    "SOURCE_SIZE",
                    "POOL_UUID",
                    "ENTITY_ID",
                    "TXN_ID",
                    "VALID_SNAPSHOT",
                ],
                table,
            );
        }
    };

    Ok(())
}
/// Replica Snapshot List CLI Function.
async fn list(mut ctx: Context, matches: &ArgMatches<'_>) -> crate::Result<()> {
    let source_uuid = matches.value_of("source_uuid").map(|s| s.to_owned());
    let snapshot_uuid = matches.value_of("snapshot_uuid").map(|s| s.to_owned());
    let request = v1_rpc::snapshot::ListSnapshotsRequest {
        source_uuid,
        snapshot_uuid,
    };

    let response = ctx
        .v1
        .snapshot
        .list_snapshot(request)
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
            let snapshots = &response.get_ref().snapshots;
            if snapshots.is_empty() {
                ctx.v1("No snapshots found");
                return Ok(());
            }

            let table = snapshots
                .iter()
                .map(|r| {
                    vec![
                        r.snapshot_uuid.clone(),
                        r.snapshot_name.clone(),
                        r.snapshot_size.to_string(),
                        r.timestamp.clone().unwrap_or_default().to_string(),
                        r.num_clones.to_string(),
                        r.source_uuid.clone(),
                        r.source_size.to_string(),
                        r.pool_uuid.to_string(),
                        r.entity_id.clone(),
                        r.txn_id.clone(),
                        r.valid_snapshot.to_string(),
                    ]
                })
                .collect();
            ctx.print_list(
                vec![
                    "SNAP_UUID",
                    "SNAP_NAME",
                    "SNAP_SIZE",
                    "CREATE_TIME",
                    "CLONE_COUNT",
                    "REPLICA_UUID",
                    "REPLICA_SIZE",
                    "POOL_UUID",
                    "ENTITY_ID",
                    "TXN_ID",
                    "VALID_SNAPSHOT",
                ],
                table,
            );
        }
    };

    Ok(())
}
/// Snapshot Destroy CLI Function.
async fn destroy(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> crate::Result<()> {
    let snapshot_uuid = matches
        .value_of("snapshot_uuid")
        .ok_or_else(|| ClientError::MissingValue {
            field: "snapshot_uuid".to_string(),
        })?
        .to_owned();
    let pool = match matches.value_of("pool-uuid") {
        Some(uuid) => {
            Some(v1_rpc::snapshot::destroy_snapshot_request::Pool::PoolUuid(
                uuid.to_string(),
            ))
        }
        None => matches.value_of("pool-name").map(|name| {
            v1_rpc::snapshot::destroy_snapshot_request::Pool::PoolName(
                name.to_string(),
            )
        }),
    };
    let _ = ctx
        .v1
        .snapshot
        .destroy_snapshot(v1_rpc::snapshot::DestroySnapshotRequest {
            snapshot_uuid: snapshot_uuid.clone(),
            pool,
        })
        .await
        .context(GrpcStatus)?;

    match ctx.output {
        OutputFormat::Json => {}
        OutputFormat::Default => {
            println!("snapshot: {} is deleted", &snapshot_uuid);
        }
    }

    Ok(())
}
