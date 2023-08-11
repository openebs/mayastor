//!
//! methods to interact with snapshot management

use crate::{
    context::{Context, OutputFormat},
    ClientError,
    GrpcStatus,
};
use clap::{App, AppSettings, Arg, ArgMatches, SubCommand};
use colored_json::ToColoredJson;
use mayastor_api::v1 as v1_rpc;
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
        ("create_clone", Some(args)) => create_clone(ctx, args).await,
        ("list_clone", Some(args)) => list_clone(ctx, args).await,
        (cmd, _) => {
            Err(Status::not_found(format!("command {cmd} does not exist")))
                .context(GrpcStatus)
        }
    }
}

pub fn subcommands<'a, 'b>() -> App<'a, 'b> {
    let create_for_nexus = SubCommand::with_name("create_for_nexus")
        .about("Create a snapshot for nexus")
        .arg(
            Arg::with_name("nexus_uuid")
                .required(true)
                .index(1)
                .help("uuid of the nexus"),
        )
        .arg(
            Arg::with_name("entity_id")
                .required(true)
                .index(2)
                .help("Entity Id"),
        )
        .arg(
            Arg::with_name("txn_id")
                .required(true)
                .index(3)
                .help("Transaction id"),
        )
        .arg(
            Arg::with_name("snapshot_name")
                .required(true)
                .index(4)
                .help("Snapshot name"),
        )
        .arg(
            Arg::with_name("replica_uuid")
                .required(true)
                .index(5)
                .help("replica uuid"),
        )
        .arg(
            Arg::with_name("snapshot_uuid")
                .required(true)
                .index(6)
                .help("snapshot uuid"),
        );
    let create_for_replica = SubCommand::with_name("create_for_replica")
        .about("Create a snapshot for replica")
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
        .about("List snapshots details")
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
        .about("Destroy snapshot")
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
    let create_clone = SubCommand::with_name("create_clone")
        .about("Create a clone from snapshot")
        .arg(
            Arg::with_name("snapshot_uuid")
                .required(true)
                .index(1)
                .help("Snapshot uuid"),
        )
        .arg(
            Arg::with_name("clone_name")
                .required(true)
                .index(2)
                .help("Clone name"),
        )
        .arg(
            Arg::with_name("clone_uuid")
                .required(true)
                .index(3)
                .help("Clone uuid"),
        );
    let list_clone = SubCommand::with_name("list_clone")
        .about("List clones details")
        .arg(
            Arg::with_name("snapshot_uuid")
                .required(false)
                .index(1)
                .help("Snapshot uuid"),
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
        .subcommand(create_clone)
        .subcommand(list_clone)
}

async fn create_for_nexus(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> crate::Result<()> {
    let nexus_uuid = matches
        .value_of("nexus_uuid")
        .ok_or_else(|| ClientError::MissingValue {
            field: "nexus_uuid".to_string(),
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
    let snapshot_name = matches
        .value_of("snapshot_name")
        .ok_or_else(|| ClientError::MissingValue {
            field: "snapshot_name".to_string(),
        })?
        .to_owned();
    let replica_uuid = matches
        .values_of("replica_uuid")
        .ok_or_else(|| ClientError::MissingValue {
            field: "replica_uuid".to_string(),
        })?
        .map(|c| c.to_string())
        .collect::<Vec<String>>();
    let snapshot_uuid = matches
        .values_of("snapshot_uuid")
        .ok_or_else(|| ClientError::MissingValue {
            field: "snapshot_uuid".to_string(),
        })?
        .map(|c| c.to_string())
        .collect::<Vec<String>>();
    if replica_uuid.len() != snapshot_uuid.len() {
        return Err(ClientError::MissingValue {
                field: "Parameter count doesn't match between replica_uuid and snapshot_uuid".to_string()
            });
    }
    let replicas: Vec<v1_rpc::snapshot::NexusCreateSnapshotReplicaDescriptor> =
        replica_uuid
            .into_iter()
            .zip(snapshot_uuid)
            .map(|(a, b)| {
                v1_rpc::snapshot::NexusCreateSnapshotReplicaDescriptor {
                    replica_uuid: a,
                    snapshot_uuid: Some(b),
                    skip: false,
                }
            })
            .collect();

    let request = v1_rpc::snapshot::NexusCreateSnapshotRequest {
        nexus_uuid: nexus_uuid.clone(),
        entity_id,
        txn_id,
        snapshot_name,
        replicas,
    };

    let response = ctx
        .v1
        .snapshot
        .create_nexus_snapshot(request)
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
            let replica_done = &response.get_ref().replicas_done;
            let nexus = &response.get_ref().nexus;

            let table = replica_done
                .iter()
                .map(|r| {
                    vec![
                        nexus.clone().unwrap().uuid,
                        nexus.clone().unwrap().size.to_string(),
                        nexus.clone().unwrap().state.to_string(),
                        r.replica_uuid.clone(),
                        r.status_code.clone().to_string(),
                    ]
                })
                .collect();
            ctx.print_list(
                vec![
                    "NEXUS_UUID",
                    "NEXUS_SIZE",
                    "NEXUS_STATE",
                    "REPLICA_UUID",
                    "STATUS",
                ],
                table,
            );
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
                        r.discarded_snapshot.to_string(),
                    ]
                })
                .collect();
            ctx.print_list(
                vec![
                    "SNAP_UUID",
                    "SNAP_NAME",
                    "SNAP_SIZE",
                    "CREATE_TIME",
                    "CLONES",
                    "SOURCE_UUID",
                    "SOURCE_SIZE",
                    "POOL_UUID",
                    "ENTITY_ID",
                    "TXN_ID",
                    "VALID_SNAPSHOT",
                    "discarded_snapshot",
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
        snapshot_query_type: v1_rpc::snapshot::SnapshotQueryType::AllSnapshots
            as i32,
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
                        r.discarded_snapshot.to_string(),
                        r.referenced_bytes.to_string(),
                    ]
                })
                .collect();
            ctx.print_list(
                vec![
                    "SNAP_UUID",
                    "SNAP_NAME",
                    "SNAP_SIZE",
                    "CREATE_TIME",
                    "CLONES",
                    "REPLICA_UUID",
                    "REPLICA_SIZE",
                    "POOL_UUID",
                    "ENTITY_ID",
                    "TXN_ID",
                    "VALID_SNAPSHOT",
                    "DISCARD_SNAPSHOT",
                    "ANCESTOR_SIZE",
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
/// CLI to create snapshot clone.
async fn create_clone(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> crate::Result<()> {
    let snapshot_uuid = matches
        .value_of("snapshot_uuid")
        .ok_or_else(|| ClientError::MissingValue {
            field: "snapshot_uuid".to_string(),
        })?
        .to_owned();
    let clone_name = matches
        .value_of("clone_name")
        .ok_or_else(|| ClientError::MissingValue {
            field: "clone_name".to_string(),
        })?
        .to_owned();
    let clone_uuid = matches
        .value_of("clone_uuid")
        .ok_or_else(|| ClientError::MissingValue {
            field: "clone_uuid".to_string(),
        })?
        .to_owned();
    // let snapshot_uuid = Uuid::generate().to_string();
    let request = v1_rpc::snapshot::CreateSnapshotCloneRequest {
        snapshot_uuid,
        clone_name,
        clone_uuid,
    };

    let response = ctx
        .v1
        .snapshot
        .create_snapshot_clone(request)
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
            let r = &response.get_ref();
            let data = vec![vec![
                r.name.clone(),
                r.uuid.clone(),
                r.size.clone().to_string(),
                r.usage.as_ref().unwrap().allocated_bytes.to_string(),
                r.thin.clone().to_string(),
                r.poolname.clone(),
                r.is_clone.clone().to_string(),
                r.snapshot_uuid.clone().unwrap_or_default(),
            ]];
            ctx.print_list(
                vec![
                    "CLONE_NAME",
                    "CLONE_UUID",
                    "CLONE_CAPACITY",
                    "CLONE_ALLOC",
                    "THIN",
                    "POOL",
                    "IS_CLONE",
                    "SNAPSHOT_UUID",
                ],
                data,
            );
        }
    };

    Ok(())
}
async fn list_clone(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> crate::Result<()> {
    let snapshot_uuid = matches.value_of("snapshot_uuid").map(|s| s.to_owned());
    let request = v1_rpc::snapshot::ListSnapshotCloneRequest {
        snapshot_uuid,
    };

    let response = ctx
        .v1
        .snapshot
        .list_snapshot_clone(request)
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
            let clones = &response.get_ref().replicas;
            if clones.is_empty() {
                ctx.v1("No clones found");
                return Ok(());
            }

            let table = clones
                .iter()
                .map(|r| {
                    vec![
                        r.name.clone(),
                        r.uuid.clone(),
                        r.size.clone().to_string(),
                        r.usage.as_ref().unwrap().allocated_bytes.to_string(),
                        r.thin.clone().to_string(),
                        r.poolname.clone(),
                        r.is_clone.clone().to_string(),
                        r.snapshot_uuid.clone().unwrap_or_default(),
                    ]
                })
                .collect();
            ctx.print_list(
                vec![
                    "CLONE_NAME",
                    "CLONE_UUID",
                    "CLONE_CAPACITY",
                    "CLONE_ALLOC",
                    "THIN",
                    "POOL",
                    "IS_CLONE",
                    "SNAPSHOT_UUID",
                ],
                table,
            );
        }
    };

    Ok(())
}
