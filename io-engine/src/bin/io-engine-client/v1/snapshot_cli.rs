use crate::{
    context::{Context, OutputFormat},
    ClientError, GrpcStatus,
};
use clap::{Args, Subcommand};
use colored_json::ToColoredJson;
use io_engine_api::v1 as v1_rpc;
use snafu::ResultExt;
use uuid::Uuid;

#[derive(Debug, Args)]
#[command(subcommand_required = true, arg_required_else_help = true)]
pub struct SnapshotArgs {
    #[command(subcommand)]
    command: SnapshotCommands,
}

#[derive(Debug, Subcommand)]
enum SnapshotCommands {
    #[command(name = "create_for_nexus")]
    CreateForNexus(CreateForNexusArgs),
    #[command(name = "create_for_replica")]
    CreateForReplica(CreateForReplicaArgs),
    List(ListArgs),
    Destroy(DestroyArgs),
    #[command(name = "create_clone")]
    CreateClone(CreateCloneArgs),
    #[command(name = "list_clone")]
    ListClone(ListCloneArgs),
}

#[derive(Debug, Args)]
struct CreateForNexusArgs {
    nexus_uuid: Uuid,
    entity_id: String,
    txn_id: String,
    snapshot_name: String,
    /// replica UUIDs (repeat the flag for multiple)
    #[arg(long = "replica-uuid", action = clap::ArgAction::Append)]
    replica_uuid: Vec<Uuid>,
    /// snapshot UUIDs (repeat the flag for multiple, must match replica count)
    #[arg(long = "snapshot-uuid", action = clap::ArgAction::Append)]
    snapshot_uuid: Vec<Uuid>,
}

#[derive(Debug, Args)]
struct CreateForReplicaArgs {
    replica_uuid: Uuid,
    snapshot_name: String,
    entity_id: String,
    txn_id: String,
    snapshot_uuid: Uuid,
}

#[derive(Debug, Args)]
struct ListArgs {
    source_uuid: Option<Uuid>,
    snapshot_uuid: Option<Uuid>,
}

#[derive(Debug, Args)]
struct DestroyArgs {
    snapshot_uuid: Uuid,
    #[arg(long = "pool-uuid", conflicts_with = "pool_name")]
    pool_uuid: Option<Uuid>,
    #[arg(long = "pool-name", conflicts_with = "pool_uuid")]
    pool_name: Option<String>,
}

#[derive(Debug, Args)]
struct CreateCloneArgs {
    snapshot_uuid: Uuid,
    clone_name: String,
    clone_uuid: Uuid,
}

#[derive(Debug, Args)]
struct ListCloneArgs {
    snapshot_uuid: Option<Uuid>,
}

pub async fn handler(ctx: Context, args: SnapshotArgs) -> crate::Result<()> {
    match args.command {
        SnapshotCommands::CreateForNexus(args) => create_for_nexus(ctx, args).await,
        SnapshotCommands::CreateForReplica(args) => create_for_replica(ctx, args).await,
        SnapshotCommands::List(args) => list(ctx, args).await,
        SnapshotCommands::Destroy(args) => destroy(ctx, args).await,
        SnapshotCommands::CreateClone(args) => create_clone(ctx, args).await,
        SnapshotCommands::ListClone(args) => list_clone(ctx, args).await,
    }
}

async fn create_for_nexus(mut ctx: Context, args: CreateForNexusArgs) -> crate::Result<()> {
    if args.replica_uuid.len() != args.snapshot_uuid.len() {
        return Err(ClientError::MissingValue {
            field: "Parameter count doesn't match between replica_uuid and snapshot_uuid"
                .to_string(),
        });
    }
    let replicas: Vec<v1_rpc::snapshot::NexusCreateSnapshotReplicaDescriptor> = args
        .replica_uuid
        .into_iter()
        .zip(args.snapshot_uuid)
        .map(
            |(r, s)| v1_rpc::snapshot::NexusCreateSnapshotReplicaDescriptor {
                replica_uuid: r.to_string(),
                snapshot_uuid: Some(s.to_string()),
                skip: false,
            },
        )
        .collect();
    let request = v1_rpc::snapshot::NexusCreateSnapshotRequest {
        nexus_uuid: args.nexus_uuid.to_string(),
        entity_id: args.entity_id,
        txn_id: args.txn_id,
        snapshot_name: args.snapshot_name,
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
            let json = serde_json::to_string_pretty(&response.get_ref())
                .unwrap()
                .to_colored_json_auto()
                .unwrap();
            println!("{json}");
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

async fn create_for_replica(mut ctx: Context, args: CreateForReplicaArgs) -> crate::Result<()> {
    let request = v1_rpc::snapshot::CreateReplicaSnapshotRequest {
        replica_uuid: args.replica_uuid.to_string(),
        snapshot_uuid: args.snapshot_uuid.to_string(),
        snapshot_name: args.snapshot_name,
        entity_id: args.entity_id,
        txn_id: args.txn_id,
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
                        r.timestamp.unwrap_or_default().to_string(),
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

async fn list(mut ctx: Context, args: ListArgs) -> crate::Result<()> {
    let request = v1_rpc::snapshot::ListSnapshotsRequest {
        source_uuid: args.source_uuid.map(|u| u.to_string()),
        snapshot_uuid: args.snapshot_uuid.map(|u| u.to_string()),
        query: None,
    };
    let response = ctx
        .v1
        .snapshot
        .list_snapshot(request)
        .await
        .context(GrpcStatus)?;
    match ctx.output {
        OutputFormat::Json => {
            let json = serde_json::to_string_pretty(&response.get_ref())
                .unwrap()
                .to_colored_json_auto()
                .unwrap();
            println!("{json}");
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
                        r.timestamp.unwrap_or_default().to_string(),
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

async fn destroy(mut ctx: Context, args: DestroyArgs) -> crate::Result<()> {
    let pool = match args.pool_uuid {
        Some(uuid) => Some(v1_rpc::snapshot::destroy_snapshot_request::Pool::PoolUuid(
            uuid.to_string(),
        )),
        None => args
            .pool_name
            .map(v1_rpc::snapshot::destroy_snapshot_request::Pool::PoolName),
    };
    let snapshot_uuid = args.snapshot_uuid.to_string();
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
            println!("snapshot: {snapshot_uuid} is deleted");
        }
    }
    Ok(())
}

async fn create_clone(mut ctx: Context, args: CreateCloneArgs) -> crate::Result<()> {
    let request = v1_rpc::snapshot::CreateSnapshotCloneRequest {
        snapshot_uuid: args.snapshot_uuid.to_string(),
        clone_name: args.clone_name,
        clone_uuid: args.clone_uuid.to_string(),
    };
    let response = ctx
        .v1
        .snapshot
        .create_snapshot_clone(request)
        .await
        .context(GrpcStatus)?;
    match ctx.output {
        OutputFormat::Json => {
            let json = serde_json::to_string_pretty(&response.get_ref())
                .unwrap()
                .to_colored_json_auto()
                .unwrap();
            println!("{json}");
        }
        OutputFormat::Default => {
            let r = &response.get_ref();
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
                vec![vec![
                    r.name.clone(),
                    r.uuid.clone(),
                    r.size.clone().to_string(),
                    r.usage.as_ref().unwrap().allocated_bytes.to_string(),
                    r.thin.clone().to_string(),
                    r.poolname.clone(),
                    r.is_clone.clone().to_string(),
                    r.snapshot_uuid.clone().unwrap_or_default(),
                ]],
            );
        }
    };
    Ok(())
}

async fn list_clone(mut ctx: Context, args: ListCloneArgs) -> crate::Result<()> {
    let request = v1_rpc::snapshot::ListSnapshotCloneRequest {
        snapshot_uuid: args.snapshot_uuid.map(|u| u.to_string()),
    };
    let response = ctx
        .v1
        .snapshot
        .list_snapshot_clone(request)
        .await
        .context(GrpcStatus)?;
    match ctx.output {
        OutputFormat::Json => {
            let json = serde_json::to_string_pretty(&response.get_ref())
                .unwrap()
                .to_colored_json_auto()
                .unwrap();
            println!("{json}");
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
