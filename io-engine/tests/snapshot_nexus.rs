pub mod common;

use futures::channel::oneshot;
use io_engine::{bdev::nexus::NexusSnapshotStatus, core::SnapshotDescriptor};
use once_cell::sync::OnceCell;

use chrono::{DateTime, Utc};
use common::{
    compose::{
        rpc::v1::{
            bdev::ListBdevOptions,
            pool::CreatePoolRequest,
            replica::{CreateReplicaRequest, ListReplicaOptions},
            snapshot::{ListSnapshotsRequest, SnapshotInfo},
            GrpcConnect,
        },
        Builder,
        ComposeTest,
        MayastorTest,
    },
    nvme::{list_mayastor_nvme_devices, nvme_connect, nvme_disconnect_all},
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
            "nvmf://{}:8420/nqn.2019-05.io.openebs:{}",
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
async fn test_multireplica_nexus_snapshot() {
    let ms = get_ms();
    let (_test, urls) = launch_instance(true).await;

    ms.spawn(async move {
        let nexus = create_nexus(&urls).await;

        let snapshot_params = SnapshotParams::new(
            Some(String::from("e1")),
            Some(String::from("p1")),
            Some(Uuid::new_v4().to_string()),
            Some(String::from("s1")),
            Some(Uuid::new_v4().to_string()),
            Some(Utc::now().to_string()),
        );

        let replicas = vec![
            NexusReplicaSnapshotDescriptor {
                replica_uuid: replica1_uuid(),
                skip: false,
                snapshot_uuid: Some(Uuid::new_v4().to_string()),
            },
            NexusReplicaSnapshotDescriptor {
                replica_uuid: replica2_uuid(),
                skip: false,
                snapshot_uuid: Some(Uuid::new_v4().to_string()),
            },
        ];

        nexus
            .create_snapshot(snapshot_params, replicas)
            .await
            .expect_err(
                "Snapshot successfully created for a multireplica nexus",
            );
    })
    .await;
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
    );
    let snapshot_params_clone = snapshot_params.clone();

    // Make sure no snapshots exist on the remote node prior testing.
    let snapshots = ms1
        .snapshot
        .list_snapshot(ListSnapshotsRequest {
            source_uuid: None,
            snapshot_uuid: None,
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
            Some(String::from("snapshot61")),
            Some(snapshot_uuid.clone()),
            Some(Utc::now().to_string()),
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
            Some("e71".to_string()),
            Some(replica1_uuid()),
            Some("t71".to_string()),
            Some(String::from("snapshot71")),
            Some(Uuid::new_v4().to_string()),
            Some(Utc::now().to_string()),
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

    nvme_disconnect_all();
}
