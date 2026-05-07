pub mod common;

use common::{
    compose::{
        rpc::v1::{GrpcConnect, SharedRpcHandle},
        Binary, Builder,
    },
    file_io::DataSize,
    nexus::{test_write_to_nexus, NexusBuilder},
    pool::{validate_pools_used_space, PoolBuilder},
    replica::{validate_replicas, ReplicaBuilder},
};
use std::time::Duration;

struct StorConfig {
    ms_nex: SharedRpcHandle,
    ms_src_0: SharedRpcHandle,
    ms_src_1: SharedRpcHandle,
    ms_dst: SharedRpcHandle,
}

/// Creates a nexus of two replicas (ms_src_0, ms_src_1).
/// Adds a new replica (ms_dst). It must rebuild and stay thinly provisioned.
async fn test_thin_rebuild(cfg: StorConfig) {
    let StorConfig {
        ms_nex,
        ms_src_0,
        ms_src_1,
        ms_dst,
    } = cfg;

    const POOL_SIZE: u64 = 60;
    const REPL_SIZE: u64 = 22;

    //
    let mut pool_0 = PoolBuilder::new(ms_src_0.clone())
        .with_name("pool0")
        .with_new_uuid()
        .with_malloc("mem0", POOL_SIZE);

    let mut repl_0 = ReplicaBuilder::new(ms_src_0.clone())
        .with_pool(&pool_0)
        .with_name("r0")
        .with_new_uuid()
        .with_size_mb(REPL_SIZE)
        .with_thin(true);

    pool_0.create().await.unwrap();
    repl_0.create().await.unwrap();
    repl_0.share().await.unwrap();

    //
    let mut pool_1 = PoolBuilder::new(ms_src_1.clone())
        .with_name("pool1")
        .with_new_uuid()
        .with_malloc("mem1", POOL_SIZE);

    let mut repl_1 = ReplicaBuilder::new(ms_src_1.clone())
        .with_pool(&pool_1)
        .with_name("r1")
        .with_new_uuid()
        .with_size_mb(REPL_SIZE)
        .with_thin(true);

    pool_1.create().await.unwrap();
    repl_1.create().await.unwrap();
    repl_1.share().await.unwrap();

    //
    let mut pool_2 = PoolBuilder::new(ms_dst.clone())
        .with_name("pool2")
        .with_new_uuid()
        .with_malloc("mem2", POOL_SIZE);

    let mut repl_2 = ReplicaBuilder::new(ms_dst.clone())
        .with_pool(&pool_2)
        .with_name("r2")
        .with_new_uuid()
        .with_size_mb(REPL_SIZE)
        .with_thin(true);

    pool_2.create().await.unwrap();
    repl_2.create().await.unwrap();
    repl_2.share().await.unwrap();

    //
    let mut nex_0 = NexusBuilder::new(ms_nex.clone())
        .with_name("nexus0")
        .with_new_uuid()
        .with_size_mb(REPL_SIZE)
        .with_replica(&repl_0)
        .with_replica(&repl_1);

    nex_0.create().await.unwrap();
    nex_0.publish().await.unwrap();

    test_write_to_nexus(&nex_0, DataSize::from_bytes(0), 14, DataSize::from_mb(1))
        .await
        .unwrap();

    nex_0.add_replica(&repl_2, false).await.unwrap();

    nex_0
        .wait_children_online(Duration::from_secs(10))
        .await
        .unwrap();

    validate_pools_used_space(&[pool_0, pool_1, pool_2]).await;
    validate_replicas(&[repl_0, repl_1, repl_2]).await;
}

#[tokio::test]
async fn nexus_thin_rebuild_from_remote_to_local() {
    common::composer_init();

    let test = Builder::new()
        .name("cargo-test")
        .network("10.1.0.0/16")
        .unwrap()
        .add_container_bin(
            "ms_nex",
            Binary::from_dbg("io-engine").with_args(vec!["-l", "1"]),
        )
        .add_container_bin(
            "ms_src_0",
            Binary::from_dbg("io-engine").with_args(vec!["-l", "2"]),
        )
        .add_container_bin(
            "ms_src_1",
            Binary::from_dbg("io-engine").with_args(vec!["-l", "3"]),
        )
        .with_clean(true)
        .build()
        .await
        .unwrap();

    let conn = GrpcConnect::new(&test);

    let ms_nex = conn.grpc_handle_shared("ms_nex").await.unwrap();
    let ms_src_0 = conn.grpc_handle_shared("ms_src_0").await.unwrap();
    let ms_src_1 = conn.grpc_handle_shared("ms_src_1").await.unwrap();
    let ms_dst = ms_nex.clone();

    test_thin_rebuild(StorConfig {
        ms_nex,
        ms_src_0,
        ms_src_1,
        ms_dst,
    })
    .await;
}

#[tokio::test]
async fn nexus_thin_rebuild_from_remote_to_remote() {
    common::composer_init();

    let test = Builder::new()
        .name("cargo-test")
        .network("10.1.0.0/16")
        .unwrap()
        .add_container_bin(
            "ms_nex",
            Binary::from_dbg("io-engine").with_args(vec!["-l", "1"]),
        )
        .add_container_bin(
            "ms_src_0",
            Binary::from_dbg("io-engine").with_args(vec!["-l", "2"]),
        )
        .add_container_bin(
            "ms_src_1",
            Binary::from_dbg("io-engine").with_args(vec!["-l", "3"]),
        )
        .add_container_bin(
            "ms_dst",
            Binary::from_dbg("io-engine").with_args(vec!["-l", "4"]),
        )
        .with_clean(true)
        .build()
        .await
        .unwrap();

    let conn = GrpcConnect::new(&test);

    let ms_nex = conn.grpc_handle_shared("ms_nex").await.unwrap();
    let ms_src_0 = conn.grpc_handle_shared("ms_src_0").await.unwrap();
    let ms_src_1 = conn.grpc_handle_shared("ms_src_1").await.unwrap();
    let ms_dst = conn.grpc_handle_shared("ms_dst").await.unwrap();

    test_thin_rebuild(StorConfig {
        ms_nex,
        ms_src_0,
        ms_src_1,
        ms_dst,
    })
    .await;
}

#[tokio::test]
async fn nexus_thin_rebuild_from_local_to_remote() {
    common::composer_init();

    let test = Builder::new()
        .name("cargo-test")
        .network("10.1.0.0/16")
        .unwrap()
        .add_container_bin(
            "ms_nex",
            Binary::from_dbg("io-engine").with_args(vec!["-l", "1,2,3,4"]),
        )
        .add_container_bin(
            "ms_dst",
            Binary::from_dbg("io-engine").with_args(vec!["-l", "5"]),
        )
        .with_clean(true)
        .build()
        .await
        .unwrap();

    let conn = GrpcConnect::new(&test);

    let ms_nex = conn.grpc_handle_shared("ms_nex").await.unwrap();
    let ms_src_0 = ms_nex.clone();
    let ms_src_1 = ms_nex.clone();
    let ms_dst = conn.grpc_handle_shared("ms_dst").await.unwrap();

    test_thin_rebuild(StorConfig {
        ms_nex,
        ms_src_0,
        ms_src_1,
        ms_dst,
    })
    .await;
}

#[tokio::test]
async fn nexus_thin_rebuild_from_local_to_local() {
    common::composer_init();

    let test = Builder::new()
        .name("cargo-test")
        .network("10.1.0.0/16")
        .unwrap()
        .add_container_bin(
            "ms_nex",
            Binary::from_dbg("io-engine").with_args(vec!["-l", "1,2,3,4"]),
        )
        .with_clean(true)
        .build()
        .await
        .unwrap();

    let conn = GrpcConnect::new(&test);

    let ms_nex = conn.grpc_handle_shared("ms_nex").await.unwrap();
    let ms_src_0 = ms_nex.clone();
    let ms_src_1 = ms_nex.clone();
    let ms_dst = ms_nex.clone();

    test_thin_rebuild(StorConfig {
        ms_nex,
        ms_src_0,
        ms_src_1,
        ms_dst,
    })
    .await;
}
