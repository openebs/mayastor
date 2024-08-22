pub mod common;

use futures::channel::oneshot;
use io_engine::{bdev::nexus::NexusSnapshotStatus, core::ISnapshotDescriptor};
use once_cell::sync::OnceCell;

use chrono::{DateTime, Utc};
use common::{
    compose::{
        rpc::v1::{
            bdev::ListBdevOptions,
            pool::CreatePoolRequest,
            replica::{CreateReplicaRequest, ListReplicaOptions},
            snapshot::{
                ListSnapshotsRequest,
                NexusCreateSnapshotReplicaDescriptor,
                SnapshotInfo,
            },
            GrpcConnect,
        },
        Binary,
        Builder,
        ComposeTest,
        MayastorTest,
    },
    nexus::NexusBuilder,
    nvme::{list_mayastor_nvme_devices, nvme_connect, nvme_disconnect_all},
    pool::PoolBuilder,
    replica::ReplicaBuilder,
};

use io_engine::{
    bdev::{
        device_create,
        device_destroy,
        device_open,
        nexus::{
            nexus_create,
            nexus_lookup_mut,
            NexusReplicaSnapshotDescriptor,
        },
        Nexus,
    },
    constants::NVME_NQN_PREFIX,
    core::{MayastorCliArgs, Protocol, SnapshotParams},
    subsys::{Config, NvmeBdevOpts},
};
use io_engine_tests::file_io::{test_write_to_file, DataSize};
use nix::errno::Errno;

use io_engine_api::v1::{
    replica::list_replica_options,
    snapshot::{
        destroy_snapshot_request::Pool,
        list_snapshots_request,
        CreateReplicaSnapshotRequest,
        CreateSnapshotCloneRequest,
        DestroySnapshotRequest,
    },
};
use std::{pin::Pin, str};
use uuid::Uuid;

static MAYASTOR: OnceCell<MayastorTest> = OnceCell::new();

const REPLICA_SIZE: u64 = 16 * 1024 * 1024;

/// Get the global Mayastor test suite instance.
fn get_ms() -> &'static MayastorTest<'static> {
    MAYASTOR.get_or_init(|| MayastorTest::new(MayastorCliArgs::default()))
}

fn replica1_name() -> String {
    "volume1".to_string()
}

fn replica2_name() -> String {
    "volume2".to_string()
}

fn replica1_uuid() -> String {
    "65acdaac-14c4-41d8-a55e-d03bfd7185a4".to_string()
}

fn replica2_uuid() -> String {
    "f51ccd64-74b4-401f-a269-aa69071b3d2f".to_string()
}

fn pool_uuid() -> String {
    "6e3c062c-293b-46e6-8ab3-ff13c1643437".to_string()
}

fn nexus_name() -> String {
    "nexus1".to_string()
}

fn nexus_uuid() -> String {
    "9f1014be-7653-4960-a48b-6d08b275e3ac".to_string()
}

fn get_mayastor_nvme_device() -> String {
    let nvme_ms = list_mayastor_nvme_devices();
    assert_eq!(nvme_ms.len(), 1);
    format!("/dev/{}", nvme_ms[0].device)
}

/// Launch a containerized I/O agent with 2 shared volumes on it.
async fn launch_instance(create_replicas: bool) -> (ComposeTest, Vec<String>) {
    common::composer_init();

    Config::get_or_init(|| Config {
        nvme_bdev_opts: NvmeBdevOpts {
            timeout_us: 2_000_000,
            keep_alive_timeout_ms: 5_000,
            transport_retry_count: 2,
            ..Default::default()
        },
        ..Default::default()
    })
    .apply();

    let test = Builder::new()
        .name("cargo-test")
        .network("10.1.0.0/16")
        .unwrap()
        .add_container_dbg("ms1")
        .with_clean(true)
        .build()
        .await
        .unwrap();

    if !create_replicas {
        return (test, Vec::new());
    }

    let conn = GrpcConnect::new(&test);

    let mut ms1 = conn.grpc_handle("ms1").await.unwrap();

    ms1.pool
        .create_pool(CreatePoolRequest {
            name: "pool1".to_string(),
            uuid: Some(pool_uuid()),
            pooltype: 0,
            disks: vec!["malloc:///disk0?size_mb=128".into()],
            cluster_size: None,
            md_args: None,
        })
        .await
        .unwrap();

    ms1.replica
        .create_replica(CreateReplicaRequest {
            name: replica1_name(),
            uuid: replica1_uuid(),
            pooluuid: pool_uuid(),
            size: REPLICA_SIZE,
            thin: false,
            share: 1,
            ..Default::default()
        })
        .await
        .unwrap();

    ms1.replica
        .create_replica(CreateReplicaRequest {
            name: replica2_name(),
            uuid: replica2_uuid(),
            pooluuid: pool_uuid(),
            size: REPLICA_SIZE,
            thin: false,
            share: 1,
            ..Default::default()
        })
        .await
        .unwrap();

    let mut bdev_urls = Vec::new();

    for n in [replica1_name(), replica2_name()] {
        let bdev_url = format!(
            "nvmf://{}:8420/{NVME_NQN_PREFIX}:{}",
            ms1.endpoint.ip(),
            n,
        );

        bdev_urls.push(bdev_url);
    }

    (test, bdev_urls)
}

async fn create_nexus<'n>(replicas: &[String]) -> Pin<&'n mut Nexus<'n>> {
    let name = nexus_name();

    // Destroy nexus before recreating it.
    if let Some(n) = nexus_lookup_mut(&name) {
        n.destroy().await.expect("Failed to destroy existing nexus");
    }

    nexus_create(&name, REPLICA_SIZE, Some(&nexus_uuid()), replicas)
        .await
        .expect("Failed to create a nexus");

    nexus_lookup_mut(&nexus_name()).expect("Failed to lookup target nexus")
}

async fn create_device(url: &str) -> String {
    // Destroy the device and re-create it from scratch.
    let _r = device_destroy(url).await;

    device_create(url).await.expect("Failed to create device")
}

fn check_replica_snapshot(params: &SnapshotParams, snapshot: &SnapshotInfo) {
    assert_eq!(
        snapshot.snapshot_uuid,
        params.snapshot_uuid().unwrap(),
        "Snapshot UUID doesn't match",
    );

    assert_eq!(
        snapshot.source_uuid,
        params.parent_id().unwrap(),
        "Snapshot replica UUID doesn't match",
    );

    assert_eq!(
        snapshot.txn_id,
        params.txn_id().unwrap(),
        "Snapshot transaction ID doesn't match",
    );

    assert_eq!(
        snapshot.entity_id,
        params.entity_id().unwrap(),
        "Snapshot entity ID doesn't match",
    );

    assert_eq!(
        snapshot.snapshot_name,
        params.name().unwrap(),
        "Snapshot name ID doesn't match",
    );

    assert_eq!(
        snapshot.timestamp,
        params
            .create_time()
            .map(|s| s.parse::<DateTime<Utc>>().unwrap_or_default().into()),
        "Snapshot CreateTime doesn't match",
    );
}

#[tokio::test]
async fn test_replica_handle_snapshot() {
    let ms = get_ms();
    let (test, urls) = launch_instance(true).await;
    let conn = GrpcConnect::new(&test);
    static SNAP_NAME: &str = "snap21";

    let mut ms1 = conn
        .grpc_handle("ms1")
        .await
        .expect("Can't connect to remote I/O agent");

    // Make sure no snapshot exists on the remote node prior testing.
    let snapshots = ms1
        .snapshot
        .list_snapshot(ListSnapshotsRequest {
            source_uuid: None,
            snapshot_uuid: None,
            query: None,
        })
        .await
        .expect("Failed to list snapshots on replica node")
        .into_inner()
        .snapshots;

    assert_eq!(
        snapshots.len(),
        0,
        "Snapshot already exists on remote replica"
    );

    let snapshot_params = SnapshotParams::new(
        Some(String::from("e21")),
        Some(String::from("p21")),
        Some(Uuid::new_v4().to_string()),
        Some(String::from(SNAP_NAME)),
        Some(Uuid::new_v4().to_string()),
        Some(Utc::now().to_string()),
        false,
    );
    let mut snapshot_params_clone = snapshot_params.clone();

    ms.spawn(async move {
        let device_name = create_device(&urls[0]).await;
        let descr = device_open(&device_name, false)
            .expect("Can't open remote lvol device");
        let handle = descr.into_handle().unwrap();

        handle
            .create_snapshot(snapshot_params)
            .await
            .expect("Failed to create snapshot");
    })
    .await;

    // Make sure snapshot exists on the remote node.
    let snapshots = ms1
        .snapshot
        .list_snapshot(ListSnapshotsRequest {
            source_uuid: None,
            snapshot_uuid: None,
            query: None,
        })
        .await
        .expect("Failed to list snapshots on replica node")
        .into_inner()
        .snapshots;
    snapshot_params_clone.set_parent_id(String::default());
    check_replica_snapshot(
        &snapshot_params_clone,
        snapshots
            .get(0)
            .expect("Snapshot is not created on remote replica"),
    );
}

#[tokio::test]
async fn test_list_no_snapshots() {
    let (test, _urls) = launch_instance(false).await;

    let conn = GrpcConnect::new(&test);
    let mut ms1 = conn.grpc_handle("ms1").await.unwrap();

    // Make sure no devices exist.
    let bdevs = ms1
        .bdev
        .list(ListBdevOptions {
            name: None,
        })
        .await
        .expect("Failed to list existing devices")
        .into_inner()
        .bdevs;

    assert_eq!(bdevs.len(), 0, "Some devices still present");

    // Make sure snapshots can be properly enumerated when no devices exist.
    let snapshots = ms1
        .snapshot
        .list_snapshot(ListSnapshotsRequest {
            source_uuid: None,
            snapshot_uuid: None,
            query: None,
        })
        .await
        .expect("Failed to list snapshots on replica node")
        .into_inner()
        .snapshots;

    assert_eq!(snapshots.len(), 0, "Some snapshots present");
}

fn check_nexus_snapshot_status(
    res: &NexusSnapshotStatus,
    status: &Vec<(String, u32)>,
) {
    assert_eq!(
        res.replicas_skipped.len(),
        0,
        "Some replicas were skipped while taking nexus snapshot"
    );

    assert_eq!(
        res.replicas_done.len(),
        1,
        "Not all replicas were processed while taking nexus snapshot"
    );

    assert_eq!(
        res.replicas_done.len(),
        status.len(),
        "Size of replica status array returned doesn't match"
    );

    for (uuid, e) in status {
        assert!(
            res.replicas_done.iter().any(|r| {
                if r.replica_uuid.eq(uuid) {
                    assert_eq!(
                        r.status, *e,
                        "Replica snapshot status doesn't match"
                    );
                    true
                } else {
                    false
                }
            }),
            "Replica not found: {}",
            uuid,
        );
    }
}

#[tokio::test]
async fn test_nexus_snapshot() {
    let ms = get_ms();
    let (test, urls) = launch_instance(true).await;
    let conn = GrpcConnect::new(&test);
    static SNAP_NAME: &str = "snap31";
    static ENTITY_ID: &str = "e1";
    static TXN_ID: &str = "t1";
    let snapshot_uuid = Uuid::new_v4().to_string();

    let mut ms1 = conn
        .grpc_handle("ms1")
        .await
        .expect("Can't connect to remote I/O agent");

    let snapshot_params = SnapshotParams::new(
        Some(ENTITY_ID.to_string()),
        Some(replica1_uuid()),
        Some(TXN_ID.to_string()),
        Some(String::from(SNAP_NAME)),
        Some(snapshot_uuid.clone()),
        Some(Utc::now().to_string()),
        false,
    );
    let snapshot_params_clone = snapshot_params.clone();

    // Make sure no snapshots exist on the remote node prior testing.
    let snapshots = ms1
        .snapshot
        .list_snapshot(ListSnapshotsRequest {
            source_uuid: None,
            snapshot_uuid: None,
            query: None,
        })
        .await
        .expect("Failed to list snapshots on replica node")
        .into_inner()
        .snapshots;

    assert_eq!(
        snapshots.len(),
        0,
        "Snapshot already exists on remote replica"
    );

    ms.spawn(async move {
        // Create a single replica nexus.
        let uris = [format!("{}?uuid={}", urls[0].clone(), replica1_uuid())];

        let nexus = create_nexus(&uris).await;

        let mut replicas = Vec::new();

        let r = NexusReplicaSnapshotDescriptor {
            replica_uuid: replica1_uuid(),
            skip: false,
            snapshot_uuid: Some(snapshot_uuid.clone()),
        };
        replicas.push(r);

        let res = nexus
            .create_snapshot(snapshot_params_clone, replicas)
            .await
            .expect("Failed to create nexus snapshot");

        let replica_status: Vec<(String, u32)> = vec![(replica1_uuid(), 0)];
        check_nexus_snapshot_status(&res, &replica_status);
    })
    .await;

    let snapshots = ms1
        .snapshot
        .list_snapshot(ListSnapshotsRequest {
            source_uuid: None,
            snapshot_uuid: None,
            query: None,
        })
        .await
        .expect("Failed to list snapshots on replica node")
        .into_inner()
        .snapshots;

    check_replica_snapshot(
        &snapshot_params,
        snapshots
            .get(0)
            .expect("Snapshot is not created on remote replica"),
    );
}

#[tokio::test]
async fn test_duplicated_snapshot_uuid_name() {
    let ms = get_ms();
    let (test, urls) = launch_instance(true).await;
    let conn = GrpcConnect::new(&test);

    let mut ms1 = conn
        .grpc_handle("ms1")
        .await
        .expect("Can't connect to remote I/O agent");

    let snapshot_uuid = Uuid::new_v4().to_string();
    let mut snapshot_params = SnapshotParams::new(
        Some("e31".to_string()),
        Some(replica1_uuid()),
        Some("t31".to_string()),
        Some(String::from("snapshot51")),
        Some(snapshot_uuid.clone()),
        Some(Utc::now().to_string()),
        false,
    );
    let snapshot_params_clone = snapshot_params.clone();

    ms.spawn(async move {
        // Create a single replica nexus.
        let uris = [format!("{}?uuid={}", urls[0].clone(), replica1_uuid())];
        let mut nexus = create_nexus(&uris).await;

        let replicas = vec![NexusReplicaSnapshotDescriptor {
            replica_uuid: replica1_uuid(),
            skip: false,
            snapshot_uuid: Some(snapshot_uuid.clone()),
        }];

        // Step 1: create a snapshot.
        let res = nexus
            .as_mut()
            .create_snapshot(snapshot_params.clone(), replicas.clone())
            .await
            .expect("Failed to create nexus snapshot");

        let mut replica_status: Vec<(String, u32)> = vec![(replica1_uuid(), 0)];
        check_nexus_snapshot_status(&res, &replica_status);

        // Step 2: try to create another snapshot with the same UUID: must see
        // EEXIST.
        let res = nexus
            .as_mut()
            .create_snapshot(snapshot_params.clone(), replicas.clone())
            .await
            .expect("Failed to create nexus snapshot");

        replica_status[0].1 = Errno::EEXIST as u32;
        check_nexus_snapshot_status(&res, &replica_status);

        // Step 3: try to create another snapshot with the same name and
        // different UUID: must see EEXIST.
        snapshot_params.set_snapshot_uuid(Uuid::new_v4().to_string());

        let res = nexus
            .create_snapshot(snapshot_params, replicas)
            .await
            .expect("Failed to create nexus snapshot");

        check_nexus_snapshot_status(&res, &replica_status);
    })
    .await;

    let snapshots = ms1
        .snapshot
        .list_snapshot(ListSnapshotsRequest {
            source_uuid: None,
            snapshot_uuid: None,
            query: None,
        })
        .await
        .expect("Failed to list snapshots on replica node")
        .into_inner()
        .snapshots;

    // Must see only one initial snapshot.
    check_replica_snapshot(
        &snapshot_params_clone,
        snapshots
            .get(0)
            .expect("Snapshot is not created on remote replica"),
    );
}

#[tokio::test]
async fn test_snapshot_ancestor_usage() {
    const SNAP1_NAME: &str = "snapshot61";
    const SNAP2_NAME: &str = "snapshot62";
    const SNAP3_NAME: &str = "snapshot63";

    let ms = get_ms();
    let (test, urls) = launch_instance(true).await;
    let conn = GrpcConnect::new(&test);

    nvme_disconnect_all();

    ms.spawn(async move {
        // Create a single replica nexus.
        let uris = [format!("{}?uuid={}", urls[0].clone(), replica1_uuid())];

        let mut nexus = create_nexus(&uris).await;

        nexus
            .as_mut()
            .share(Protocol::Nvmf, None)
            .await
            .expect("Failed to publish nexus");

        let mut replicas = Vec::new();

        let snapshot_uuid = Uuid::new_v4().to_string();
        let snapshot_params = SnapshotParams::new(
            Some("e61".to_string()),
            Some(replica1_uuid()),
            Some("t61".to_string()),
            Some(String::from(SNAP1_NAME)),
            Some(snapshot_uuid.clone()),
            Some(Utc::now().to_string()),
            false,
        );

        let r = NexusReplicaSnapshotDescriptor {
            replica_uuid: replica1_uuid(),
            skip: false,
            snapshot_uuid: Some(snapshot_uuid.clone()),
        };
        replicas.push(r);

        let res = nexus
            .create_snapshot(snapshot_params, replicas)
            .await
            .expect("Failed to create nexus snapshot");

        let replica_status: Vec<(String, u32)> = vec![(replica1_uuid(), 0)];
        check_nexus_snapshot_status(&res, &replica_status);
    })
    .await;

    let mut ms1 = conn
        .grpc_handle("ms1")
        .await
        .expect("Can't connect to remote I/O agent");

    let mut replicas = ms1
        .replica
        .list_replicas(ListReplicaOptions {
            uuid: Some(replica1_uuid()),
            ..Default::default()
        })
        .await
        .expect("Can't get replicas from I/O agent")
        .into_inner()
        .replicas;

    assert_eq!(replicas.len(), 1, "Number of test replicas doesn't match");
    let usage = replicas
        .pop()
        .expect("Failed to get replica from response data")
        .usage
        .expect("No replica usage information provided");

    let cluster_size = usage.cluster_size;

    // Initial snapshot must own all replica's clusters.
    assert_eq!(
        usage.allocated_bytes_snapshots, REPLICA_SIZE,
        "Amount of bytes allocated by snapshots doesn't match"
    );

    assert_eq!(
        usage.num_allocated_clusters_snapshots * cluster_size,
        usage.allocated_bytes_snapshots,
        "Disparity in snapshot size and number of allocated clusters"
    );

    /*
     * Write some data to nexus and create a second snapshot which
     * should now own all new data.
     */
    let nqn = format!("{NVME_NQN_PREFIX}:{}", nexus_name());
    nvme_connect("127.0.0.1", &nqn, true);

    let (s, r) = oneshot::channel::<()>();
    tokio::spawn(async move {
        let device = get_mayastor_nvme_device();

        test_write_to_file(
            device,
            DataSize::default(),
            1,
            DataSize::from_mb(1),
        )
        .await
        .expect("Failed to write to nexus");

        s.send(()).expect("Failed to notify the waiter");
    });

    // Wait for I/O to complete.
    r.await.expect("Failed to write data to nexus");

    // After writing data to nexus snapshot space usage must remain unchanged.
    replicas = ms1
        .replica
        .list_replicas(ListReplicaOptions {
            uuid: Some(replica1_uuid()),
            ..Default::default()
        })
        .await
        .expect("Can't get replicas from I/O agent")
        .into_inner()
        .replicas;

    assert_eq!(replicas.len(), 1, "Number of test replicas doesn't match");
    let usage2 = replicas
        .pop()
        .expect("Failed to get replica from response data")
        .usage
        .expect("No replica usage information provided");

    // Snapshot data usage must not change since no other sapshots
    // were taken.
    assert_eq!(
        usage.allocated_bytes_snapshots, usage2.allocated_bytes_snapshots,
        "Amount of bytes allocated by snapshots has changed"
    );

    assert_eq!(
        usage.num_allocated_clusters_snapshots,
        usage2.num_allocated_clusters_snapshots,
        "Amount of clusters allocated by snapshots has changed"
    );

    // Create a second snapshot after data has been written to nexus.
    ms.spawn(async move {
        let nexus =
            nexus_lookup_mut(&nexus_name()).expect("Can't find the nexus");

        let snapshot_params = SnapshotParams::new(
            Some("e62".to_string()),
            Some(replica1_uuid()),
            Some("t62".to_string()),
            Some(String::from(SNAP2_NAME)),
            Some(Uuid::new_v4().to_string()),
            Some(Utc::now().to_string()),
            false,
        );

        let replicas = vec![NexusReplicaSnapshotDescriptor {
            replica_uuid: replica1_uuid(),
            skip: false,
            snapshot_uuid: snapshot_params.snapshot_uuid(),
        }];

        let res = nexus
            .create_snapshot(snapshot_params, replicas)
            .await
            .expect("Failed to create nexus snapshot");

        let replica_status: Vec<(String, u32)> = vec![(replica1_uuid(), 0)];
        check_nexus_snapshot_status(&res, &replica_status);
    })
    .await;

    // After taking the second snapshot its allocated space must
    // be properly accounted in replica usage.
    replicas = ms1
        .replica
        .list_replicas(ListReplicaOptions {
            uuid: Some(replica1_uuid()),
            ..Default::default()
        })
        .await
        .expect("Can't get replicas from I/O agent")
        .into_inner()
        .replicas;

    assert_eq!(replicas.len(), 1, "Number of test replicas doesn't match");
    let usage3 = replicas
        .pop()
        .expect("Failed to get replica from response data")
        .usage
        .expect("No replica usage information provided");

    // Check that one extra cluster of data written is properly accounted.
    assert_eq!(
        usage3.allocated_bytes_snapshots,
        usage2.allocated_bytes_snapshots + usage3.cluster_size,
        "Amount of bytes allocated by snapshots has changed"
    );

    assert_eq!(
        usage3.num_allocated_clusters_snapshots,
        usage2.num_allocated_clusters_snapshots + 1,
        "Amount of clusters allocated by snapshots has changed"
    );

    /* Make sure 2 test snapshots properly expose referenced bytes.
     * The newest snapshot should expose allocated bytes of the oldest
     * snapshot as its referenced bytes, whereas the oldest snapshot
     * should expose zero as referenced bytes (since it's the last
     * snapshot in the chain and has no successors).
     */
    let snapshots = ms1
        .snapshot
        .list_snapshot(ListSnapshotsRequest {
            source_uuid: None,
            snapshot_uuid: None,
            query: None,
        })
        .await
        .expect("Failed to list snapshots on replica node")
        .into_inner()
        .snapshots;

    assert_eq!(
        snapshots.len(),
        2,
        "Invalid number of snapshots reported for test volume"
    );
    let snap1 = snapshots.get(0).expect("Can't get the first sbapshot");
    let snap2 = snapshots.get(1).expect("Can't get the second snaoshot");

    let (oldest, newest) = if snap1.snapshot_name.eq(SNAP1_NAME) {
        (snap1, snap2)
    } else {
        (snap2, snap1)
    };

    assert_eq!(
        oldest.referenced_bytes, 0,
        "Oldest snapshot has non-zero referenced bytes"
    );
    assert_eq!(
        newest.referenced_bytes, REPLICA_SIZE,
        "Number of bytes referenced by the ancestor snapshot doesn't match"
    );

    // Create the third snapshot and make sure it correctly references space of
    // 2 pre-existing snapshots.
    ms.spawn(async move {
        let nexus =
            nexus_lookup_mut(&nexus_name()).expect("Can't find the nexus");

        let snapshot_params = SnapshotParams::new(
            Some("e63".to_string()),
            Some(replica1_uuid()),
            Some("t63".to_string()),
            Some(String::from(SNAP3_NAME)),
            Some(Uuid::new_v4().to_string()),
            Some(Utc::now().to_string()),
            false,
        );

        let replicas = vec![NexusReplicaSnapshotDescriptor {
            replica_uuid: replica1_uuid(),
            skip: false,
            snapshot_uuid: snapshot_params.snapshot_uuid(),
        }];

        let res = nexus
            .create_snapshot(snapshot_params, replicas)
            .await
            .expect("Failed to create nexus snapshot");

        let replica_status: Vec<(String, u32)> = vec![(replica1_uuid(), 0)];
        check_nexus_snapshot_status(&res, &replica_status);
    })
    .await;

    // The third snapshot must reference the space of the other two snapshots.
    let snap3 = ms1
        .snapshot
        .list_snapshot(ListSnapshotsRequest {
            source_uuid: None,
            snapshot_uuid: None,
            query: None,
        })
        .await
        .expect("Failed to list snapshots on replica node")
        .into_inner()
        .snapshots
        .into_iter()
        .find(|s| s.snapshot_name.eq(SNAP3_NAME))
        .expect("Can't list the third snapshot");

    assert_eq!(
        snap3.referenced_bytes,
        REPLICA_SIZE + cluster_size,
        "Number of bytes referenced by ancestor snapshots doesn't match"
    );

    nvme_disconnect_all();
}

/// This tests creates 2 replicas --> 2 snapshots --> 1 restore --> deletes snap
/// 1 --> Validates listing.
#[tokio::test]
async fn test_replica_snapshot_listing_with_query() {
    let _ = get_ms();
    let (test, _urls) = launch_instance(true).await;
    let conn = GrpcConnect::new(&test);
    let snap1 = Uuid::new_v4();

    let mut ms1 = conn
        .grpc_handle("ms1")
        .await
        .expect("Can't connect to remote I/O agent");

    ms1.snapshot
        .create_replica_snapshot(CreateReplicaSnapshotRequest {
            replica_uuid: replica1_uuid(),
            snapshot_uuid: snap1.to_string(),
            snapshot_name: "snaprep1/2".to_string(),
            entity_id: "snaprep1".to_string(),
            txn_id: "1".to_string(),
        })
        .await
        .expect("Should create replica snapshot");

    ms1.snapshot
        .create_replica_snapshot(CreateReplicaSnapshotRequest {
            replica_uuid: replica2_uuid(),
            snapshot_uuid: Uuid::new_v4().to_string(),
            snapshot_name: "snaprep2/1".to_string(),
            entity_id: "snaprep2".to_string(),
            txn_id: "1".to_string(),
        })
        .await
        .expect("Should create replica snapshot");

    // List only non discarded snapshots before restore.
    let snaps = ms1
        .snapshot
        .list_snapshot(ListSnapshotsRequest {
            source_uuid: None,
            snapshot_uuid: None,
            query: Some(list_snapshots_request::Query {
                invalid: None,
                discarded: Some(false),
            }),
        })
        .await
        .expect("List should not fail")
        .into_inner()
        .snapshots;
    assert_eq!(snaps.len(), 2);
    assert!(!snaps[0].discarded_snapshot);
    assert!(!snaps[1].discarded_snapshot);

    ms1.snapshot
        .create_snapshot_clone(CreateSnapshotCloneRequest {
            snapshot_uuid: snap1.to_string(),
            clone_name: "snaprep1clone".to_string(),
            clone_uuid: Uuid::new_v4().to_string(),
        })
        .await
        .expect("Should create snapshot clone");

    // List only non discarded snapshots after restore.
    let snaps = ms1
        .snapshot
        .list_snapshot(ListSnapshotsRequest {
            source_uuid: None,
            snapshot_uuid: None,
            query: Some(list_snapshots_request::Query {
                invalid: None,
                discarded: Some(false),
            }),
        })
        .await
        .expect("List should not fail")
        .into_inner()
        .snapshots;
    assert_eq!(snaps.len(), 2);
    assert!(!snaps[0].discarded_snapshot);
    assert!(!snaps[1].discarded_snapshot);

    ms1.snapshot
        .destroy_snapshot(DestroySnapshotRequest {
            snapshot_uuid: snap1.to_string(),
            pool: Some(Pool::PoolUuid(pool_uuid())),
        })
        .await
        .expect("Destroy should not fail");

    // List only non discarded snapshots.
    let snaps = ms1
        .snapshot
        .list_snapshot(ListSnapshotsRequest {
            source_uuid: None,
            snapshot_uuid: None,
            query: Some(list_snapshots_request::Query {
                invalid: None,
                discarded: Some(false),
            }),
        })
        .await
        .expect("List should not fail")
        .into_inner()
        .snapshots;
    assert_eq!(snaps.len(), 1);
    assert!(!snaps[0].discarded_snapshot);

    // List only discarded snapshots.
    let snaps = ms1
        .snapshot
        .list_snapshot(ListSnapshotsRequest {
            source_uuid: None,
            snapshot_uuid: None,
            query: Some(list_snapshots_request::Query {
                invalid: None,
                discarded: Some(true),
            }),
        })
        .await
        .expect("List should not fail")
        .into_inner()
        .snapshots;
    assert_eq!(snaps.len(), 1);
    assert!(snaps[0].discarded_snapshot);

    // List all with query fields as None.
    let snaps = ms1
        .snapshot
        .list_snapshot(ListSnapshotsRequest {
            source_uuid: None,
            snapshot_uuid: None,
            query: Some(list_snapshots_request::Query {
                invalid: None,
                discarded: None,
            }),
        })
        .await
        .expect("List should not fail")
        .into_inner()
        .snapshots;
    assert_eq!(snaps.len(), 2);

    // List all with query None.
    let snaps = ms1
        .snapshot
        .list_snapshot(ListSnapshotsRequest {
            source_uuid: None,
            snapshot_uuid: None,
            query: None,
        })
        .await
        .expect("List should not fail")
        .into_inner()
        .snapshots;
    assert_eq!(snaps.len(), 2);
}

/// This tests creates 2 replicas --> 2 snapshots --> 1 restore --> Validates
/// listing.
#[tokio::test]
async fn test_replica_listing_with_query() {
    let _ = get_ms();
    let (test, _urls) = launch_instance(true).await;
    let conn = GrpcConnect::new(&test);
    let snap1 = Uuid::new_v4();

    let mut ms1 = conn
        .grpc_handle("ms1")
        .await
        .expect("Can't connect to remote I/O agent");

    ms1.snapshot
        .create_replica_snapshot(CreateReplicaSnapshotRequest {
            replica_uuid: replica1_uuid(),
            snapshot_uuid: snap1.to_string(),
            snapshot_name: "snap1rep1/2".to_string(),
            entity_id: "snap1rep1".to_string(),
            txn_id: "1".to_string(),
        })
        .await
        .expect("Should create replica snapshot");

    ms1.snapshot
        .create_replica_snapshot(CreateReplicaSnapshotRequest {
            replica_uuid: replica2_uuid(),
            snapshot_uuid: Uuid::new_v4().to_string(),
            snapshot_name: "snap2rep2/1".to_string(),
            entity_id: "snap2rep2".to_string(),
            txn_id: "1".to_string(),
        })
        .await
        .expect("Should create replica snapshot");

    ms1.snapshot
        .create_snapshot_clone(CreateSnapshotCloneRequest {
            snapshot_uuid: snap1.to_string(),
            clone_name: "snaprep1clone".to_string(),
            clone_uuid: Uuid::new_v4().to_string(),
        })
        .await
        .expect("Should create snapshot clone");

    // List all with query None, all replicas including snapshots and clones.
    let replicas = ms1
        .replica
        .list_replicas(ListReplicaOptions {
            name: None,
            poolname: None,
            uuid: None,
            pooluuid: None,
            query: None,
            pooltypes: vec![],
        })
        .await
        .expect("List should not fail")
        .into_inner()
        .replicas;
    assert_eq!(replicas.len(), 5);

    // List all replicas except snapshots.
    let replicas = ms1
        .replica
        .list_replicas(ListReplicaOptions {
            name: None,
            poolname: None,
            uuid: None,
            pooluuid: None,
            query: Some(list_replica_options::Query {
                replica: true,
                snapshot: false,
                clone: true,
            }),
            pooltypes: vec![],
        })
        .await
        .expect("List should not fail")
        .into_inner()
        .replicas;
    assert_eq!(replicas.len(), 3);
    assert!(!replicas[0].is_snapshot);
    assert!(!replicas[1].is_snapshot);
    assert!(!replicas[2].is_snapshot);

    // List all replicas except clones.
    let replicas = ms1
        .replica
        .list_replicas(ListReplicaOptions {
            name: None,
            poolname: None,
            uuid: None,
            pooluuid: None,
            query: Some(list_replica_options::Query {
                replica: true,
                snapshot: true,
                clone: false,
            }),
            pooltypes: vec![],
        })
        .await
        .expect("List should not fail")
        .into_inner()
        .replicas;
    assert_eq!(replicas.len(), 4);
    assert!(!replicas[0].is_clone);
    assert!(!replicas[1].is_clone);
    assert!(!replicas[2].is_clone);
    assert!(!replicas[3].is_clone);

    // List only clones and snapshots.
    let replicas = ms1
        .replica
        .list_replicas(ListReplicaOptions {
            name: None,
            poolname: None,
            uuid: None,
            pooluuid: None,
            query: Some(list_replica_options::Query {
                replica: false,
                snapshot: true,
                clone: true,
            }),
            pooltypes: vec![],
        })
        .await
        .expect("List should not fail")
        .into_inner()
        .replicas;
    assert_eq!(replicas.len(), 3);
    assert!(replicas[0].is_clone || replicas[0].is_snapshot);
    assert!(replicas[1].is_clone || replicas[1].is_snapshot);
    assert!(replicas[2].is_clone || replicas[2].is_snapshot);

    // List only snapshots.
    let replicas = ms1
        .replica
        .list_replicas(ListReplicaOptions {
            name: None,
            poolname: None,
            uuid: None,
            pooluuid: None,
            query: Some(list_replica_options::Query {
                replica: false,
                snapshot: true,
                clone: false,
            }),
            pooltypes: vec![],
        })
        .await
        .expect("List should not fail")
        .into_inner()
        .replicas;
    assert_eq!(replicas.len(), 2);
    assert!(replicas[0].is_snapshot);
    assert!(replicas[1].is_snapshot);

    // List only clones.
    let replicas = ms1
        .replica
        .list_replicas(ListReplicaOptions {
            name: None,
            poolname: None,
            uuid: None,
            pooluuid: None,
            query: Some(list_replica_options::Query {
                replica: false,
                snapshot: false,
                clone: true,
            }),
            pooltypes: vec![],
        })
        .await
        .expect("List should not fail")
        .into_inner()
        .replicas;
    assert_eq!(replicas.len(), 1);
    assert!(replicas[0].is_clone);

    // List all only replicas.
    let replicas = ms1
        .replica
        .list_replicas(ListReplicaOptions {
            name: None,
            poolname: None,
            uuid: None,
            pooluuid: None,
            query: Some(list_replica_options::Query {
                replica: true,
                snapshot: false,
                clone: false,
            }),
            pooltypes: vec![],
        })
        .await
        .expect("List should not fail")
        .into_inner()
        .replicas;
    assert_eq!(replicas.len(), 2);
    assert!(!replicas[0].is_clone && !replicas[0].is_snapshot);
    assert!(!replicas[1].is_clone && !replicas[1].is_snapshot);
}

#[tokio::test]
async fn test_multireplica_nexus_snapshot() {
    const POOL_SIZE: u64 = 60;
    const REPL_SIZE: u64 = 22;

    common::composer_init();
    let test = Builder::new()
        .name("cargo-test")
        .network("10.1.0.0/16")
        .unwrap()
        .add_container_bin(
            "ms_nexus",
            Binary::from_dbg("io-engine").with_args(vec!["-l", "1"]),
        )
        .add_container_bin(
            "ms_repl_1",
            Binary::from_dbg("io-engine").with_args(vec!["-l", "2"]),
        )
        .add_container_bin(
            "ms_repl_2",
            Binary::from_dbg("io-engine").with_args(vec!["-l", "3"]),
        )
        .add_container_bin(
            "ms_repl_3",
            Binary::from_dbg("io-engine").with_args(vec!["-l", "4"]),
        )
        .with_clean(true)
        .build()
        .await
        .unwrap();

    let conn = GrpcConnect::new(&test);
    let ms_nexus = conn.grpc_handle_shared("ms_nexus").await.unwrap();
    let ms_repl_1 = conn.grpc_handle_shared("ms_repl_1").await.unwrap();
    let ms_repl_2 = conn.grpc_handle_shared("ms_repl_2").await.unwrap();
    let ms_repl_3 = conn.grpc_handle_shared("ms_repl_3").await.unwrap();
    // Create Pool-1 and Replica-1.
    let mut pool_1 = PoolBuilder::new(ms_repl_1.clone())
        .with_name("pool1")
        .with_new_uuid()
        .with_malloc("mem1", POOL_SIZE);

    let mut repl_1 = ReplicaBuilder::new(ms_repl_1.clone())
        .with_pool(&pool_1)
        .with_name("r1")
        .with_new_uuid()
        .with_size_mb(REPL_SIZE)
        .with_thin(true);

    pool_1.create().await.unwrap();
    repl_1.create().await.unwrap();
    repl_1.share().await.unwrap();

    // Create Pool-2 and Replica-2.
    let mut pool_2 = PoolBuilder::new(ms_repl_2.clone())
        .with_name("pool2")
        .with_new_uuid()
        .with_malloc("mem2", POOL_SIZE);

    let mut repl_2 = ReplicaBuilder::new(ms_repl_2.clone())
        .with_pool(&pool_2)
        .with_name("r2")
        .with_new_uuid()
        .with_size_mb(REPL_SIZE)
        .with_thin(true);

    pool_2.create().await.unwrap();
    repl_2.create().await.unwrap();
    repl_2.share().await.unwrap();
    // Create Pool-3 and Replica-3.
    let mut pool_3 = PoolBuilder::new(ms_repl_3.clone())
        .with_name("pool3")
        .with_new_uuid()
        .with_malloc("mem3", POOL_SIZE);

    let mut repl_3 = ReplicaBuilder::new(ms_repl_3.clone())
        .with_pool(&pool_3)
        .with_name("r3")
        .with_new_uuid()
        .with_size_mb(REPL_SIZE)
        .with_thin(true);

    pool_3.create().await.unwrap();
    repl_3.create().await.unwrap();
    repl_3.share().await.unwrap();

    // Create nexus.
    let mut nex_0 = NexusBuilder::new(ms_nexus.clone())
        .with_name("nexus0")
        .with_new_uuid()
        .with_size_mb(REPL_SIZE)
        .with_replica(&repl_1)
        .with_replica(&repl_2)
        .with_replica(&repl_3);

    nex_0.create().await.unwrap();
    nex_0.publish().await.unwrap();

    let snapshot_params = SnapshotParams::new(
        Some(String::from("e1")),
        Some(String::from("p1")),
        Some(Uuid::new_v4().to_string()),
        Some(String::from("s1")),
        Some(Uuid::new_v4().to_string()),
        Some(Utc::now().to_string()),
        false,
    );

    let mut replicas = vec![
        NexusCreateSnapshotReplicaDescriptor {
            replica_uuid: repl_1.uuid(),
            snapshot_uuid: Some(Uuid::new_v4().to_string()),
            skip: false,
        },
        NexusCreateSnapshotReplicaDescriptor {
            replica_uuid: repl_2.uuid(),
            snapshot_uuid: Some(Uuid::new_v4().to_string()),
            skip: false,
        },
    ];
    // check for error when snapshot uuid is not provided for all replicas.
    let result = nex_0
        .create_nexus_snapshot(&snapshot_params, &replicas)
        .await;
    assert!(result.is_err());

    replicas.push(NexusCreateSnapshotReplicaDescriptor {
        replica_uuid: repl_2.uuid(),
        snapshot_uuid: replicas[1].snapshot_uuid.clone(),
        skip: false,
    });
    // check for error when snapshot uuid is duplicated.
    let result = nex_0
        .create_nexus_snapshot(&snapshot_params, &replicas)
        .await;
    assert!(result.is_err());
    replicas.pop();
    replicas.push(NexusCreateSnapshotReplicaDescriptor {
        replica_uuid: repl_3.uuid(),
        snapshot_uuid: Some(Uuid::new_v4().to_string()),
        skip: false,
    });
    let snap_list = nex_0
        .create_nexus_snapshot(&snapshot_params, &replicas)
        .await
        .unwrap();
    assert_eq!(snap_list.len(), 3);
}
