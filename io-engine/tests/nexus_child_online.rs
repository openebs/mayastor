pub mod common;

use common::{
    compose::{
        rpc::v1::{
            nexus::{ChildState, ChildStateReason, NexusNvmePreemption, NvmeReservation},
            GrpcConnect,
        },
        Binary, Builder, ComposeTest,
    },
    file_io::DataSize,
    nexus::{test_write_to_nexus, NexusBuilder},
    pool::PoolBuilder,
    replica::ReplicaBuilder,
};

use std::time::Duration;

static POOL_SIZE: u64 = 60;
static REPL_SIZE: u64 = 50;

#[allow(dead_code)]
struct StorageBuilder {
    pool_0: PoolBuilder,
    pool_1: PoolBuilder,
    repl_0: ReplicaBuilder,
    repl_1: ReplicaBuilder,
    nex_0: NexusBuilder,
}

/// Creates a composer test
async fn create_compose_test() -> ComposeTest {
    common::composer_init();

    Builder::new()
        .name("cargo-test")
        .network("10.1.0.0/16")
        .unwrap()
        .add_container_bin(
            "ms_0",
            Binary::from_dbg("io-engine").with_args(vec!["-l", "1"]),
        )
        .add_container_bin(
            "ms_1",
            Binary::from_dbg("io-engine").with_args(vec!["-l", "2"]),
        )
        .add_container_bin(
            "ms_nex",
            Binary::from_dbg("io-engine").with_args(vec!["-l", "3", "-Fcolor"]),
        )
        .with_clean(true)
        .build()
        .await
        .unwrap()
}

/// Creates test storage.
async fn create_test_storage(test: &ComposeTest) -> StorageBuilder {
    let conn = GrpcConnect::new(test);

    let ms_0 = conn.grpc_handle_shared("ms_0").await.unwrap();
    let ms_1 = conn.grpc_handle_shared("ms_1").await.unwrap();
    let ms_nex = conn.grpc_handle_shared("ms_nex").await.unwrap();

    let mut pool_0 = PoolBuilder::new(ms_0.clone())
        .with_name("pool0")
        .with_new_uuid()
        .with_malloc("mem0", POOL_SIZE);

    let mut repl_0 = ReplicaBuilder::new(ms_0.clone())
        .with_pool(&pool_0)
        .with_name("r0")
        .with_new_uuid()
        .with_size_mb(REPL_SIZE)
        .with_thin(true);

    pool_0.create().await.unwrap();
    repl_0.create().await.unwrap();
    repl_0.share().await.unwrap();

    let mut pool_1 = PoolBuilder::new(ms_1.clone())
        .with_name("pool1")
        .with_new_uuid()
        .with_malloc("mem0", POOL_SIZE);

    let mut repl_1 = ReplicaBuilder::new(ms_1.clone())
        .with_pool(&pool_1)
        .with_name("r1")
        .with_new_uuid()
        .with_size_mb(REPL_SIZE)
        .with_thin(true);

    pool_1.create().await.unwrap();
    repl_1.create().await.unwrap();
    repl_1.share().await.unwrap();

    let mut nex_0 = NexusBuilder::new(ms_nex.clone())
        .with_name("nexus0")
        .with_new_uuid()
        .with_size_mb(REPL_SIZE)
        .with_replica(&repl_0)
        .with_replica(&repl_1);

    nex_0.create().await.unwrap();
    nex_0.publish().await.unwrap();

    StorageBuilder {
        pool_0,
        pool_1,
        repl_0,
        repl_1,
        nex_0,
    }
}

#[tokio::test]
async fn nexus_child_online() {
    let test = create_compose_test().await;

    let StorageBuilder {
        pool_0: _,
        pool_1: _,
        repl_0,
        repl_1: _,
        nex_0,
    } = create_test_storage(&test).await;

    test_write_to_nexus(&nex_0, DataSize::from_bytes(0), 1, DataSize::from_kb(1))
        .await
        .unwrap();

    nex_0
        .offline_child_replica_wait(&repl_0, Duration::from_secs(1))
        .await
        .unwrap();

    nex_0
        .online_child_replica_wait(&repl_0, Duration::from_secs(1))
        .await
        .unwrap();

    nex_0.offline_child_replica(&repl_0).await.unwrap();
    nex_0
        .wait_replica_state(
            &repl_0,
            ChildState::Degraded,
            Some(ChildStateReason::NoSpace),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
}

/// Reproduces https://github.com/openebs/mayastor/issues/2087
/// If the child online fails after the child device has been re-created and
/// opened (here: reservation acquire fails, in the issue: I/O qpair connect
/// fails with ENXIO), the child is closed. The close waits for the device
/// removal event, but the nexus event listener is not yet registered on the
/// re-created device, so the close (and thus the online request) may hang
/// forever, keeping the nexus locked.
#[tokio::test]
async fn nexus_child_online_resv_fail() {
    common::composer_init();

    let test = Builder::new()
        .name("cargo-test")
        .network("10.1.0.0/16")
        .unwrap()
        .add_container_bin(
            "ms_0",
            Binary::from_dbg("io-engine")
                .with_env("NEXUS_NVMF_RESV_ENABLE", "1")
                .with_env(
                    "MAYASTOR_NVMF_HOSTID",
                    "c1affd2d-ef79-4ba4-b5cf-8eb48f9c07d0",
                )
                .with_args(vec!["-l", "1"]),
        )
        .add_container_bin(
            "ms_nex",
            Binary::from_dbg("io-engine")
                .with_env("NEXUS_NVMF_RESV_ENABLE", "1")
                .with_env(
                    "MAYASTOR_NVMF_HOSTID",
                    "53b35ce9-8e71-49a9-ab9b-cba7c5670fad",
                )
                .with_args(vec!["-l", "2", "-Fcolor"]),
        )
        .with_clean(true)
        .build()
        .await
        .unwrap();

    let conn = GrpcConnect::new(&test);
    let ms_0 = conn.grpc_handle_shared("ms_0").await.unwrap();
    let ms_nex = conn.grpc_handle_shared("ms_nex").await.unwrap();

    // Remote replica, the child we offline and online.
    let mut pool_0 = PoolBuilder::new(ms_0.clone())
        .with_name("pool0")
        .with_new_uuid()
        .with_malloc("mem0", POOL_SIZE);
    let mut repl_0 = ReplicaBuilder::new(ms_0.clone())
        .with_pool(&pool_0)
        .with_name("r0")
        .with_new_uuid()
        .with_size_mb(REPL_SIZE)
        .with_thin(true);
    pool_0.create().await.unwrap();
    repl_0.create().await.unwrap();
    repl_0.share().await.unwrap();

    // Local replica, keeps the nexus healthy.
    let mut pool_1 = PoolBuilder::new(ms_nex.clone())
        .with_name("pool1")
        .with_new_uuid()
        .with_malloc("mem0", POOL_SIZE);
    let mut repl_1 = ReplicaBuilder::new(ms_nex.clone())
        .with_pool(&pool_1)
        .with_name("r1")
        .with_new_uuid()
        .with_size_mb(REPL_SIZE)
        .with_thin(true);
    pool_1.create().await.unwrap();
    repl_1.create().await.unwrap();

    // ArgKey preemption without a preempt key: cannot acquire the reservation
    // if another host holds it.
    let mut nex_0 = NexusBuilder::new(ms_nex.clone())
        .with_name("nexus0")
        .with_new_uuid()
        .with_size_mb(REPL_SIZE)
        .with_resv_key(1)
        .with_resv_type(NvmeReservation::WriteExclusive)
        .with_preempt_policy(NexusNvmePreemption::ArgKey)
        .with_replica(&repl_0)
        .with_local_replica(&repl_1);
    nex_0.create().await.unwrap();
    nex_0.publish().await.unwrap();

    nex_0
        .offline_child_replica_wait(&repl_0, Duration::from_secs(1))
        .await
        .unwrap();

    // Another host (ms_0, over nvmf loopback) steals the reservation of repl_0.
    let mut nex_1 = NexusBuilder::new(ms_0.clone())
        .with_name("nexus1")
        .with_new_uuid()
        .with_size_mb(REPL_SIZE)
        .with_resv_key(2)
        .with_resv_type(NvmeReservation::WriteExclusive)
        .with_preempt_policy(NexusNvmePreemption::Holder)
        .with_bdev(&repl_0.shared_uri());
    nex_1.create().await.unwrap();

    // Online re-creates and opens the child device, then fails to acquire the
    // reservation and closes the child. This must fail, not hang.
    let online = tokio::time::timeout(Duration::from_secs(5), nex_0.online_child_replica(&repl_0))
        .await
        .expect("online child request hung: child close never completed");
    let error = online.expect_err("online should fail to acquire reservation");
    assert!(
        !matches!(
            error.code(),
            tonic::Code::DeadlineExceeded | tonic::Code::Cancelled
        ),
        "online child request timed out: {:?}",
        error
    );

    // The nexus must not remain locked.
    tokio::time::timeout(Duration::from_secs(5), nex_0.destroy())
        .await
        .expect("nexus destroy hung: nexus is still locked")
        .unwrap();
}
