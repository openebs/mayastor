pub mod common;

use common::{
    compose::{
        rpc::v1::{
            nexus::NexusState,
            snapshot::NexusCreateSnapshotReplicaDescriptor,
            GrpcConnect,
            SharedRpcHandle,
        },
        Binary,
        Builder,
    },
    fio::{Fio, FioBuilder, FioJobBuilder},
    nexus::NexusBuilder,
    pool::PoolBuilder,
    replica::ReplicaBuilder,
};
use io_engine::core::SnapshotParams;
use once_cell::sync::OnceCell;
use std::path::PathBuf;
use tokio::task::JoinHandle;

use async_trait::async_trait;

const POOL_SIZE: u64 = 800; // 800MiB
const REPL_SIZE: u64 = 200; // 200MiB
const EXPANDED_SIZE: u64 = 262144000; //250 MiB
const DEFAULT_REPLICA_CNT: usize = 3;

static NEXUS_CONNECT_PATH: OnceCell<PathBuf> = OnceCell::new();

async fn compose_ms_nodes() -> io_engine_tests::compose::ComposeTest {
    common::composer_init();

    Builder::new()
        .name("cargo-test")
        .network("10.1.0.0/16")
        .unwrap()
        .add_container_bin(
            "ms_nex_0",
            Binary::from_dbg("io-engine").with_args(vec!["-l", "1,2"]),
        )
        .add_container_bin(
            "ms_rep_1",
            Binary::from_dbg("io-engine").with_args(vec!["-l", "3,4"]),
        )
        .add_container_bin(
            "ms_rep_2",
            Binary::from_dbg("io-engine").with_args(vec!["-l", "5,6"]),
        )
        .with_clean(true)
        .build()
        .await
        .unwrap()
}

struct StorConfig {
    ms_nex_0: SharedRpcHandle,
    ms_rep_1: SharedRpcHandle,
    ms_rep_2: SharedRpcHandle,
}

// Define an enum to represent the functions
enum ResizeTest {
    WithoutReplicaResize,
    AfterReplicaResize,
    WithRebuildingReplica,
    ResizeAfterSnapshot,
}

// Define a trait for the test functions
#[async_trait(?Send)]
trait ResizeTestTrait {
    async fn call(
        &self,
        nexus: &NexusBuilder,
        replicas: Vec<&mut ReplicaBuilder>,
        fio_instance: JoinHandle<Fio>,
    );
}

// Implement the trait for the functions
#[async_trait(?Send)]
impl ResizeTestTrait for ResizeTest {
    async fn call(
        &self,
        nexus: &NexusBuilder,
        replicas: Vec<&mut ReplicaBuilder>,
        fio_instance: JoinHandle<Fio>,
    ) {
        match self {
            ResizeTest::WithoutReplicaResize => {
                do_resize_without_replica_resize(nexus, replicas).await
            }
            ResizeTest::AfterReplicaResize => {
                do_resize_after_replica_resize(nexus, replicas).await
            }
            ResizeTest::WithRebuildingReplica => {
                do_resize_with_rebuilding_replica(nexus, replicas).await
            }
            ResizeTest::ResizeAfterSnapshot => {
                do_resize_after_snapshot(nexus, replicas, fio_instance).await
            }
        }
    }
}

async fn do_resize_without_replica_resize(
    nexus: &NexusBuilder,
    replicas: Vec<&mut ReplicaBuilder>,
) {
    let _ = nexus
        .resize(EXPANDED_SIZE)
        .await
        .expect_err("Resize of nexus without resizing replicas must fail");

    // And even if a replica is resized and others are not - then also operation
    // must fail.
    assert!(replicas.len() == DEFAULT_REPLICA_CNT);
    let mut resize_repl = replicas[0].clone();
    let ret = &mut resize_repl.resize(EXPANDED_SIZE).await.unwrap();
    assert!(ret.size >= EXPANDED_SIZE);
    let _ = nexus
        .resize(EXPANDED_SIZE)
        .await
        .expect_err("Resize of nexus without resizing ALL replicas must fail");
}

async fn do_resize_after_replica_resize(
    nexus: &NexusBuilder,
    replicas: Vec<&mut ReplicaBuilder>,
) {
    for replica in replicas {
        let ret = replica.resize(EXPANDED_SIZE).await.unwrap();
        assert!(ret.size >= EXPANDED_SIZE);
    }

    // Slight wait to let replica resize events consolidate.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let nexus_obj = nexus
        .resize(EXPANDED_SIZE)
        .await
        .expect("Resize of nexus after resizing replicas failed");
    assert!(nexus_obj.size == EXPANDED_SIZE);
}

async fn do_resize_with_rebuilding_replica(
    nexus: &NexusBuilder,
    replicas: Vec<&mut ReplicaBuilder>,
) {
    assert!(replicas.len() == DEFAULT_REPLICA_CNT);
    // Last one is the chosen one!
    let rebuild_replica = &replicas[replicas.len() - 1];

    // Scale down and then scale up to initiate a rebuild.
    nexus.remove_child_replica(rebuild_replica).await.unwrap();

    nexus.add_replica(rebuild_replica, false).await.unwrap();

    // Make sure nexus is Degraded i.e. a rebuild is ongoing before we attempt
    // volume resize.
    assert_eq!(
        nexus.get_nexus().await.unwrap().state,
        NexusState::NexusDegraded as i32
    );
    do_resize_after_replica_resize(nexus, replicas).await
}

async fn do_resize_after_snapshot(
    nexus: &NexusBuilder,
    replicas: Vec<&mut ReplicaBuilder>,
    fio_instance: JoinHandle<Fio>,
) {
    // Params for first snapshot (before volume expansion).
    let snapshot1_params = SnapshotParams::new(
        Some(String::from("ent1")),
        Some(String::from("p1")),
        Some(uuid::Uuid::new_v4().to_string()),
        Some(String::from("snap_pre_vol_resize")),
        Some(uuid::Uuid::new_v4().to_string()),
        Some(chrono::Utc::now().to_string()),
        false,
    );

    let num_descs = replicas.len();
    let mut replica_descs: Vec<NexusCreateSnapshotReplicaDescriptor> =
        Vec::with_capacity(num_descs);
    for replica in &replicas {
        replica_descs.push(NexusCreateSnapshotReplicaDescriptor {
            replica_uuid: replica.uuid().to_string(),
            snapshot_uuid: Some(uuid::Uuid::new_v4().to_string()),
            skip: false,
        })
    }

    // Create the snapshot prior to expansion.
    nexus
        .create_nexus_snapshot(&snapshot1_params, &replica_descs)
        .await
        .expect("Snapshot creation failed for a multireplica nexus");

    // Params for second snapshot (after volume expansion).
    let snapshot2_params = SnapshotParams::new(
        Some(String::from("ent1")),
        Some(String::from("p1")),
        Some(uuid::Uuid::new_v4().to_string()),
        Some(String::from("snap_post_vol_resize")),
        Some(uuid::Uuid::new_v4().to_string()),
        Some(chrono::Utc::now().to_string()),
        false,
    );
    let mut replica_descs2: Vec<NexusCreateSnapshotReplicaDescriptor> =
        Vec::with_capacity(num_descs);
    for replica in &replicas {
        replica_descs2.push(NexusCreateSnapshotReplicaDescriptor {
            replica_uuid: replica.uuid().to_string(),
            snapshot_uuid: Some(uuid::Uuid::new_v4().to_string()),
            skip: false,
        })
    }

    // Expand the nexus and underlying replicas now.
    do_resize_after_replica_resize(nexus, replicas).await;
    // Let the running fio finish and start a new fio spanning the expanded
    // capacity.
    let _ = fio_instance.await;

    // Create the snapshot after expansion.
    nexus
        .create_nexus_snapshot(&snapshot2_params, &replica_descs2)
        .await
        .expect("Snapshot creation failed for a multireplica nexus");

    // Run I/O on the nexus again and expect no error.
    let cpath = NEXUS_CONNECT_PATH.get().unwrap();

    let fio = FioBuilder::new()
        .with_job(
            FioJobBuilder::new()
                .with_name("fio_post_vol_resize")
                .with_filename(cpath)
                .with_ioengine("libaio")
                .with_iodepth(4)
                .with_numjobs(1)
                .with_direct(true)
                .with_rw("randrw")
                .build(),
        )
        .build();

    tokio::spawn(async { fio.run() }).await.unwrap();
}

/// Creates a nexus of 3 replicas and resize the replicas and nexus bdev while
/// fio is running.
async fn setup_cluster_and_run(cfg: StorConfig, test: ResizeTest) {
    let StorConfig {
        ms_nex_0,
        ms_rep_1,
        ms_rep_2,
    } = cfg;

    //
    let mut pool_0 = PoolBuilder::new(ms_nex_0.clone())
        .with_name("pool0")
        .with_new_uuid()
        .with_malloc("mem0", POOL_SIZE);

    let mut repl_0 = ReplicaBuilder::new(ms_nex_0.clone())
        .with_pool(&pool_0)
        .with_name("r0")
        .with_new_uuid()
        .with_size_mb(REPL_SIZE);

    pool_0.create().await.unwrap();
    repl_0.create().await.unwrap();

    //
    let mut pool_1 = PoolBuilder::new(ms_rep_1.clone())
        .with_name("pool1")
        .with_new_uuid()
        .with_malloc("mem1", POOL_SIZE);

    let mut repl_1 = ReplicaBuilder::new(ms_rep_1.clone())
        .with_pool(&pool_1)
        .with_name("r1")
        .with_new_uuid()
        .with_size_mb(REPL_SIZE);

    pool_1.create().await.unwrap();
    repl_1.create().await.unwrap();
    repl_1.share().await.unwrap();

    //
    let mut pool_2 = PoolBuilder::new(ms_rep_2.clone())
        .with_name("pool2")
        .with_new_uuid()
        .with_malloc("mem2", POOL_SIZE);

    let mut repl_2 = ReplicaBuilder::new(ms_rep_2.clone())
        .with_pool(&pool_2)
        .with_name("r2")
        .with_new_uuid()
        .with_size_mb(REPL_SIZE);

    pool_2.create().await.unwrap();
    repl_2.create().await.unwrap();
    repl_2.share().await.unwrap();

    //
    let mut nex_0 = NexusBuilder::new(ms_nex_0.clone())
        .with_name("nexus_rsz0")
        .with_new_uuid()
        .with_size_mb(REPL_SIZE)
        .with_replica(&repl_0)
        .with_replica(&repl_1)
        .with_replica(&repl_2);

    nex_0.create().await.unwrap();
    nex_0.publish().await.unwrap();

    // Run I/O on the nexus in a thread, and resize the underlying replicas
    // and the nexus's size.
    let (_cg, path) = nex_0.nvmf_location().open().unwrap();
    // Save for later use.
    let _ = NEXUS_CONNECT_PATH.set(path.clone());

    let fio = FioBuilder::new()
        .with_job(
            FioJobBuilder::new()
                .with_name("fio_vol_resize")
                .with_filename(path)
                .with_ioengine("libaio")
                .with_iodepth(4)
                .with_numjobs(1)
                .with_direct(true)
                .with_rw("randrw")
                .with_runtime(20)
                .build(),
        )
        .build();

    let fparam: JoinHandle<Fio> = tokio::task::spawn_blocking(|| fio.run());

    // Wait a few secs for fio to have started.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    test.call(&nex_0, vec![&mut repl_0, &mut repl_1, &mut repl_2], fparam)
        .await;
}

#[tokio::test]
async fn resize_without_replica_resize() {
    let test = compose_ms_nodes().await;

    let conn = GrpcConnect::new(&test);

    let ms_nex_0 = conn.grpc_handle_shared("ms_nex_0").await.unwrap();
    let ms_rep_1 = conn.grpc_handle_shared("ms_rep_1").await.unwrap();
    let ms_rep_2 = conn.grpc_handle_shared("ms_rep_2").await.unwrap();

    setup_cluster_and_run(
        StorConfig {
            ms_nex_0,
            ms_rep_1,
            ms_rep_2,
        },
        ResizeTest::WithoutReplicaResize,
    )
    .await
}

#[tokio::test]
async fn resize_after_replica_resize() {
    let test = compose_ms_nodes().await;

    let conn = GrpcConnect::new(&test);

    let ms_nex_0 = conn.grpc_handle_shared("ms_nex_0").await.unwrap();
    let ms_rep_1 = conn.grpc_handle_shared("ms_rep_1").await.unwrap();
    let ms_rep_2 = conn.grpc_handle_shared("ms_rep_2").await.unwrap();

    setup_cluster_and_run(
        StorConfig {
            ms_nex_0,
            ms_rep_1,
            ms_rep_2,
        },
        ResizeTest::AfterReplicaResize,
    )
    .await
}

#[tokio::test]
async fn resize_with_rebuilding_replica() {
    let test = compose_ms_nodes().await;

    let conn = GrpcConnect::new(&test);

    let ms_nex_0 = conn.grpc_handle_shared("ms_nex_0").await.unwrap();
    let ms_rep_1 = conn.grpc_handle_shared("ms_rep_1").await.unwrap();
    let ms_rep_2 = conn.grpc_handle_shared("ms_rep_2").await.unwrap();

    setup_cluster_and_run(
        StorConfig {
            ms_nex_0,
            ms_rep_1,
            ms_rep_2,
        },
        ResizeTest::WithRebuildingReplica,
    )
    .await
}

/// This test creates a volume and runs IO on it. Takes a snapshot of that
/// volume. Expands the volume and lets fio finish, and takes a second snapshot.
/// Runs the fio again on the volume.
#[tokio::test]
async fn resize_after_snapshot() {
    let test = compose_ms_nodes().await;

    let conn = GrpcConnect::new(&test);

    let ms_nex_0 = conn.grpc_handle_shared("ms_nex_0").await.unwrap();
    let ms_rep_1 = conn.grpc_handle_shared("ms_rep_1").await.unwrap();
    let ms_rep_2 = conn.grpc_handle_shared("ms_rep_2").await.unwrap();

    setup_cluster_and_run(
        StorConfig {
            ms_nex_0,
            ms_rep_1,
            ms_rep_2,
        },
        ResizeTest::ResizeAfterSnapshot,
    )
    .await
}
