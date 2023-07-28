#![cfg(feature = "fault-injection")]

pub mod common;

use common::{
    compose::{
        rpc::v1::{
            nexus::{ChildState, ChildStateReason, RebuildJobState},
            GrpcConnect,
            SharedRpcHandle,
        },
        Binary,
        Builder,
    },
    nexus::NexusBuilder,
    pool::PoolBuilder,
    replica::ReplicaBuilder,
    test::add_fault_injection,
};

use std::time::Duration;

const POOL_SIZE: u64 = 80;
const REPL_SIZE: u64 = 60;
const NEXUS_SIZE: u64 = REPL_SIZE;

async fn test_rebuild_verify(
    ms_nex: SharedRpcHandle,
    repl_0: ReplicaBuilder,
    repl_1: ReplicaBuilder,
) {
    let mut nex_0 = NexusBuilder::new(ms_nex.clone())
        .with_name("nexus0")
        .with_new_uuid()
        .with_size_mb(NEXUS_SIZE)
        .with_replica(&repl_0)
        .with_replica(&repl_1);

    nex_0.create().await.unwrap();
    nex_0.publish().await.unwrap();

    let children = nex_0.get_nexus().await.unwrap().children;
    assert_eq!(children.len(), 2);
    let dev_name = children[0].device_name.as_ref().unwrap();

    // Offline the replica.
    nex_0
        .offline_child_replica_wait(&repl_0, Duration::from_secs(1))
        .await
        .unwrap();

    // Add an injection as block device level.
    let inj_part = "domain=block&op=write&stage=submission&type=data\
                    &offset=10240&num_blk=1";
    let inj_uri = format!("inject://{dev_name}?{inj_part}");
    add_fault_injection(nex_0.rpc(), &inj_uri).await.unwrap();

    // Online the replica. Rebuild must fail at some point because of injected
    // miscompare.
    nex_0.online_child_replica(&repl_0).await.unwrap();

    // Wait until the rebuild fails.
    nex_0
        .wait_replica_state(
            &repl_0,
            ChildState::Faulted,
            Some(ChildStateReason::RebuildFailed),
            Duration::from_secs(5),
        )
        .await
        .unwrap();

    // Check that the rebuild history has a single failed record.
    let hist = nex_0.get_rebuild_history().await.unwrap();
    assert_eq!(hist.len(), 1);
    assert_eq!(hist[0].state(), RebuildJobState::Failed);
}

#[tokio::test]
async fn nexus_rebuild_verify_remote() {
    common::composer_init();

    let test = Builder::new()
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
            Binary::from_dbg("io-engine")
                // Disable partial rebuild to force rebuild I/O.
                .with_env("NEXUS_PARTIAL_REBUILD", "0")
                // Set rebuild revify mode to fail.
                .with_env("NEXUS_REBUILD_VERIFY", "fail")
                .with_args(vec!["-l", "3", "-Fcolor,compact"]),
        )
        .with_clean(true)
        .build()
        .await
        .unwrap();

    let conn = GrpcConnect::new(&test);

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
        .with_thin(false);

    pool_0.create().await.unwrap();
    repl_0.create().await.unwrap();
    repl_0.share().await.unwrap();

    let mut pool_1 = PoolBuilder::new(ms_1.clone())
        .with_name("pool1")
        .with_new_uuid()
        .with_malloc("mem1", POOL_SIZE);

    let mut repl_1 = ReplicaBuilder::new(ms_1.clone())
        .with_pool(&pool_1)
        .with_name("r1")
        .with_new_uuid()
        .with_size_mb(REPL_SIZE)
        .with_thin(false);

    pool_1.create().await.unwrap();
    repl_1.create().await.unwrap();
    repl_1.share().await.unwrap();

    test_rebuild_verify(ms_nex, repl_0, repl_1).await;
}

#[tokio::test]
async fn nexus_rebuild_verify_local() {
    common::composer_init();

    let test = Builder::new()
        .name("cargo-test")
        .network("10.1.0.0/16")
        .unwrap()
        .add_container_bin(
            "ms_1",
            Binary::from_dbg("io-engine").with_args(vec!["-l", "2"]),
        )
        .add_container_bin(
            "ms_nex",
            Binary::from_dbg("io-engine")
                // Disable partial rebuild to force rebuild I/O.
                .with_env("NEXUS_PARTIAL_REBUILD", "0")
                // Set rebuild revify mode to fail.
                .with_env("NEXUS_REBUILD_VERIFY", "fail")
                .with_args(vec!["-l", "3", "-Fcolor,compact"]),
        )
        .with_clean(true)
        .build()
        .await
        .unwrap();

    let conn = GrpcConnect::new(&test);

    let ms_1 = conn.grpc_handle_shared("ms_1").await.unwrap();
    let ms_nex = conn.grpc_handle_shared("ms_nex").await.unwrap();

    let mut pool_0 = PoolBuilder::new(ms_nex.clone())
        .with_name("pool0")
        .with_new_uuid()
        .with_malloc("mem0", POOL_SIZE);

    let mut repl_0 = ReplicaBuilder::new(ms_nex.clone())
        .with_pool(&pool_0)
        .with_name("r0")
        .with_new_uuid()
        .with_size_mb(REPL_SIZE)
        .with_thin(false);

    pool_0.create().await.unwrap();
    repl_0.create().await.unwrap();

    let mut pool_1 = PoolBuilder::new(ms_1.clone())
        .with_name("pool1")
        .with_new_uuid()
        .with_malloc("mem1", POOL_SIZE);

    let mut repl_1 = ReplicaBuilder::new(ms_1.clone())
        .with_pool(&pool_1)
        .with_name("r1")
        .with_new_uuid()
        .with_size_mb(REPL_SIZE)
        .with_thin(false);

    pool_1.create().await.unwrap();
    repl_1.create().await.unwrap();
    repl_1.share().await.unwrap();

    test_rebuild_verify(ms_nex, repl_0, repl_1).await;
}
