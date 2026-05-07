pub mod common;

use common::{
    compose::{
        rpc::v1::{GrpcConnect, SharedRpcHandle},
        Binary, Builder,
    },
    file_io::DataSize,
    nexus::{test_trim_to_nexus, test_write_to_nexus, NexusBuilder},
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

/// Creates a nexus from two source replicas, writes data to two clusters via
/// the nexus, unmaps those clusters via the nexus (which propagates the unmap
/// to both source replicas), then adds a destination replica that is rebuilt
/// from the sources.  After rebuild the destination must have the same number
/// of allocated clusters as the sources: i.e. the unmapped regions in the
/// sources must **not** be re-allocated on the destination.
async fn test_thin_rebuild_with_unmap(cfg: StorConfig) {
    let StorConfig {
        ms_nex,
        ms_src_0,
        ms_src_1,
        ms_dst,
    } = cfg;

    const POOL_SIZE: u64 = 100;
    const REPL_SIZE: u64 = 22;
    // Use an explicit 1 MiB cluster size so that the test can write/unmap
    // exactly one cluster at a time without relying on the pool default.
    const CLUSTER_SIZE: u32 = 1024 * 1024;

    let mut pool_0 = PoolBuilder::new(ms_src_0.clone())
        .with_name("pool0")
        .with_new_uuid()
        .with_malloc("mem0", POOL_SIZE)
        .with_cluster_size(CLUSTER_SIZE);

    let mut repl_0 = ReplicaBuilder::new(ms_src_0.clone())
        .with_pool(&pool_0)
        .with_name("r0")
        .with_new_uuid()
        .with_size_mb(REPL_SIZE)
        .with_thin(true);

    pool_0.create().await.unwrap();
    repl_0.create().await.unwrap();
    repl_0.share().await.unwrap();

    let mut pool_1 = PoolBuilder::new(ms_src_1.clone())
        .with_name("pool1")
        .with_new_uuid()
        .with_malloc("mem1", POOL_SIZE)
        .with_cluster_size(CLUSTER_SIZE);

    let mut repl_1 = ReplicaBuilder::new(ms_src_1.clone())
        .with_pool(&pool_1)
        .with_name("r1")
        .with_new_uuid()
        .with_size_mb(REPL_SIZE)
        .with_thin(true);

    pool_1.create().await.unwrap();
    repl_1.create().await.unwrap();
    repl_1.share().await.unwrap();

    let mut pool_2 = PoolBuilder::new(ms_dst.clone())
        .with_name("pool2")
        .with_new_uuid()
        .with_malloc("mem2", POOL_SIZE)
        .with_cluster_size(CLUSTER_SIZE);

    let mut repl_2 = ReplicaBuilder::new(ms_dst.clone())
        .with_pool(&pool_2)
        .with_name("r2")
        .with_new_uuid()
        .with_size_mb(REPL_SIZE)
        .with_thin(true);

    pool_2.create().await.unwrap();
    repl_2.create().await.unwrap();
    repl_2.share().await.unwrap();

    let mut nex_0 = NexusBuilder::new(ms_nex.clone())
        .with_name("nexus0")
        .with_new_uuid()
        .with_size_mb(REPL_SIZE)
        .with_replica(&repl_0)
        .with_replica(&repl_1);

    nex_0.create().await.unwrap();
    nex_0.publish().await.unwrap();

    let cluster_bytes = CLUSTER_SIZE as u64;

    // Write two full clusters worth of data (clusters 2 and 3, skipping cluster
    // 0 which is reserved for the superblock / metadata).
    test_write_to_nexus(
        &nex_0,
        DataSize::from_bytes(2 * cluster_bytes),
        2,
        DataSize::from_bytes(cluster_bytes),
    )
    .await
    .unwrap();

    // Confirm that each source replica has allocated some clusters.
    let r0_before = repl_0.get_replica().await.unwrap().usage.unwrap();
    let r1_before = repl_1.get_replica().await.unwrap().usage.unwrap();
    assert!(
        r0_before.num_allocated_clusters >= 2,
        "Source replica 0 should have at least 2 allocated clusters after write, got {}",
        r0_before.num_allocated_clusters
    );
    assert!(
        r1_before.num_allocated_clusters >= 2,
        "Source replica 1 should have at least 2 allocated clusters after write, got {}",
        r1_before.num_allocated_clusters
    );

    // Unmap exactly one cluster (cluster 3) through the nexus.  The nexus
    // forwards the unmap to both source replicas; with --bs-cluster-unmap
    // enabled the underlying blobstore releases the corresponding cluster.
    test_trim_to_nexus(
        &nex_0,
        DataSize::from_bytes(3 * cluster_bytes),
        DataSize::from_bytes(cluster_bytes),
    )
    .await
    .unwrap();

    // Wait a moment for the asynchronous cluster-release to settle.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Each source replica must now have one fewer allocated cluster.
    let r0_after_trim = repl_0.get_replica().await.unwrap().usage.unwrap();
    let r1_after_trim = repl_1.get_replica().await.unwrap().usage.unwrap();
    assert_eq!(
        r0_before.num_allocated_clusters,
        r0_after_trim.num_allocated_clusters + 1,
        "Source replica 0: expected one cluster released after unmap"
    );
    assert_eq!(
        r1_before.num_allocated_clusters,
        r1_after_trim.num_allocated_clusters + 1,
        "Source replica 1: expected one cluster released after unmap"
    );

    // Add the destination replica. This triggers a nexus rebuild: the
    // destination is synchronised with the sources.  Unmapped clusters in the
    // source must be unmapped (not allocated) in the destination.
    nex_0.add_replica(&repl_2, false).await.unwrap();

    nex_0
        .wait_children_online(Duration::from_secs(60))
        .await
        .unwrap();

    // After rebuild the destination must match source allocation.
    let r0_final = repl_0.get_replica().await.unwrap().usage.unwrap();
    let r2_final = repl_2.get_replica().await.unwrap().usage.unwrap();
    assert_eq!(
        r0_final.num_allocated_clusters, r2_final.num_allocated_clusters,
        "After nexus rebuild destination cluster count ({}) must match source ({})",
        r2_final.num_allocated_clusters, r0_final.num_allocated_clusters,
    );
}

#[tokio::test]
async fn nexus_thin_rebuild_unmap_from_remote_to_local() {
    common::composer_init();

    let test = Builder::new()
        .name("cargo-test")
        .network("10.1.0.0/16")
        .unwrap()
        .add_container_bin(
            "ms_nex",
            Binary::from_dbg("io-engine").with_args(vec!["-l", "1", "--bs-cluster-unmap"]),
        )
        .add_container_bin(
            "ms_src_0",
            Binary::from_dbg("io-engine").with_args(vec!["-l", "2", "--bs-cluster-unmap"]),
        )
        .add_container_bin(
            "ms_src_1",
            Binary::from_dbg("io-engine").with_args(vec!["-l", "3", "--bs-cluster-unmap"]),
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

    test_thin_rebuild_with_unmap(StorConfig {
        ms_nex,
        ms_src_0,
        ms_src_1,
        ms_dst,
    })
    .await;
}

#[tokio::test]
async fn nexus_thin_rebuild_unmap_from_remote_to_remote() {
    common::composer_init();

    let test = Builder::new()
        .name("cargo-test")
        .network("10.1.0.0/16")
        .unwrap()
        .add_container_bin(
            "ms_nex",
            Binary::from_dbg("io-engine").with_args(vec!["-l", "1", "--bs-cluster-unmap"]),
        )
        .add_container_bin(
            "ms_src_0",
            Binary::from_dbg("io-engine").with_args(vec!["-l", "2", "--bs-cluster-unmap"]),
        )
        .add_container_bin(
            "ms_src_1",
            Binary::from_dbg("io-engine").with_args(vec!["-l", "3", "--bs-cluster-unmap"]),
        )
        .add_container_bin(
            "ms_dst",
            Binary::from_dbg("io-engine").with_args(vec!["-l", "4", "--bs-cluster-unmap"]),
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

    test_thin_rebuild_with_unmap(StorConfig {
        ms_nex,
        ms_src_0,
        ms_src_1,
        ms_dst,
    })
    .await;
}

#[tokio::test]
async fn nexus_thin_rebuild_unmap_from_local_to_local() {
    common::composer_init();

    let test = Builder::new()
        .name("cargo-test")
        .network("10.1.0.0/16")
        .unwrap()
        .add_container_bin(
            "ms_nex",
            Binary::from_dbg("io-engine").with_args(vec!["-l", "1,2,3,4", "--bs-cluster-unmap"]),
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

    test_thin_rebuild_with_unmap(StorConfig {
        ms_nex,
        ms_src_0,
        ms_src_1,
        ms_dst,
    })
    .await;
}
