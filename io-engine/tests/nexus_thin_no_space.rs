pub mod common;

use crate::common::nexus::find_nexus_by_uuid;
use common::{
    compose::{
        rpc::v1::{
            nexus::{ChildState, ChildStateReason},
            GrpcConnect,
        },
        Binary,
        Builder,
    },
    nexus::{test_write_to_nexus, NexusBuilder},
    pool::PoolBuilder,
    replica::ReplicaBuilder,
};

#[tokio::test]
/// Create a nexus with a single thin-provisioned replica with size larger than
/// pool capacity, and try to write more data than pool capacity.
/// Write must fail with ENOSPC.
async fn nexus_thin_nospc_local_single() {
    common::composer_init();

    let test = Builder::new()
        .name("cargo-test")
        .network("10.1.0.0/16")
        .unwrap()
        .add_container_bin(
            "ms1",
            Binary::from_dbg("io-engine").with_args(vec!["-l", "1"]),
        )
        .with_clean(true)
        .build()
        .await
        .unwrap();

    let conn = GrpcConnect::new(&test);

    let ms1 = conn.grpc_handle_shared("ms1").await.unwrap();

    let mut pool_0 = PoolBuilder::new(ms1.clone())
        .with_name("pool0")
        .with_new_uuid()
        .with_malloc("mem0", 60);

    let mut repl_0 = ReplicaBuilder::new(ms1.clone())
        .with_pool(&pool_0)
        .with_name("r0")
        .with_new_uuid()
        .with_size_mb(50)
        .with_thin(true);

    let mut nex_0 = NexusBuilder::new(ms1.clone())
        .with_name("nexus0")
        .with_new_uuid()
        .with_size_mb(50)
        .with_replica(&repl_0);

    pool_0.create().await.unwrap();
    repl_0.create().await.unwrap();
    nex_0.create().await.unwrap();
    nex_0.publish().await.unwrap();

    // Write less than pool size.
    test_write_to_nexus(&nex_0, 30, 1).await.unwrap();

    // Write more than pool size. Must result in ENOSPC.
    let res = test_write_to_nexus(&nex_0, 80, 1).await;

    assert_eq!(res.unwrap_err().raw_os_error().unwrap(), libc::ENOSPC);
}

#[tokio::test]
async fn nexus_thin_nospc_remote_single() {
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
            "ms_nex",
            Binary::from_dbg("io-engine").with_args(vec!["-l", "2"]),
        )
        .with_clean(true)
        .build()
        .await
        .unwrap();

    let conn = GrpcConnect::new(&test);

    let ms_0 = conn.grpc_handle_shared("ms_0").await.unwrap();
    let ms_nex = conn.grpc_handle_shared("ms_nex").await.unwrap();

    let mut pool_0 = PoolBuilder::new(ms_0.clone())
        .with_name("pool0")
        .with_new_uuid()
        .with_malloc("mem0", 60);

    let mut repl_0 = ReplicaBuilder::new(ms_0.clone())
        .with_pool(&pool_0)
        .with_name("r0")
        .with_new_uuid()
        .with_size_mb(50)
        .with_thin(true);

    pool_0.create().await.unwrap();
    repl_0.create().await.unwrap();
    repl_0.share().await.unwrap();

    let mut nex_0 = NexusBuilder::new(ms_nex.clone())
        .with_name("nexus0")
        .with_new_uuid()
        .with_size_mb(50)
        .with_replica(&repl_0);

    nex_0.create().await.unwrap();
    nex_0.publish().await.unwrap();

    // Write less than pool size.
    test_write_to_nexus(&nex_0, 30, 1).await.unwrap();

    // Write more than pool size. Must result in ENOSPC.
    let res = test_write_to_nexus(&nex_0, 80, 1).await;

    assert_eq!(res.unwrap_err().raw_os_error().unwrap(), libc::ENOSPC);
}

#[tokio::test]
/// 1. Create two pools of equal size.
/// 2. Create a thick-provisioned replica occupying some space on pool #1.
/// 3. Create two thin-provisioned replica on these pools.
/// 4. Create a nexus on these two replicas.
/// 5. Write amount data less than replica size and pool capacity,
///    but more than pool #1 free space.
/// 6. First child must degrade with no space.
/// 7. Delete the thick-provisioned replica, thus freeing space on pool #1.
/// 8. Online child #1.
/// 9. Child must start rebuild.
async fn nexus_thin_nospc_local() {
    common::composer_init();

    let test = Builder::new()
        .name("cargo-test")
        .network("10.1.0.0/16")
        .unwrap()
        .add_container_bin(
            "ms1",
            Binary::from_dbg("io-engine").with_args(vec!["-l", "1"]),
        )
        .with_clean(true)
        .build()
        .await
        .unwrap();

    let conn = GrpcConnect::new(&test);

    let ms1 = conn.grpc_handle_shared("ms1").await.unwrap();

    let mut pool_0 = PoolBuilder::new(ms1.clone())
        .with_name("pool0")
        .with_new_uuid()
        .with_malloc("mem0", 80);

    let mut repl_0 = ReplicaBuilder::new(ms1.clone())
        .with_pool(&pool_0)
        .with_name("r0")
        .with_new_uuid()
        .with_size_mb(60)
        .with_thin(true);

    let mut fill_0 = ReplicaBuilder::new(ms1.clone())
        .with_pool(&pool_0)
        .with_name("f0")
        .with_new_uuid()
        .with_size_mb(60)
        .with_thin(false);

    pool_0.create().await.unwrap();
    repl_0.create().await.unwrap();
    fill_0.create().await.unwrap();

    let mut pool_1 = PoolBuilder::new(ms1.clone())
        .with_name("pool1")
        .with_new_uuid()
        .with_malloc("mem1", 80);

    let mut repl_1 = ReplicaBuilder::new(ms1.clone())
        .with_pool(&pool_1)
        .with_name("r1")
        .with_new_uuid()
        .with_size_mb(60)
        .with_thin(true);

    pool_1.create().await.unwrap();
    repl_1.create().await.unwrap();

    let mut nex_0 = NexusBuilder::new(ms1.clone())
        .with_name("nexus0")
        .with_new_uuid()
        .with_size_mb(60)
        .with_replica(&repl_0)
        .with_replica(&repl_1);

    nex_0.create().await.unwrap();
    nex_0.publish().await.unwrap();

    test_recover_from_enospc(nex_0, repl_0, fill_0, 5, 10).await;
}

#[tokio::test]
async fn nexus_thin_nospc_remote() {
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
            Binary::from_dbg("io-engine").with_args(vec!["-l", "3"]),
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
        .with_malloc("mem0", 80);

    let mut repl_0 = ReplicaBuilder::new(ms_0.clone())
        .with_pool(&pool_0)
        .with_name("r0")
        .with_new_uuid()
        .with_size_mb(60)
        .with_thin(true);

    let mut fill_0 = ReplicaBuilder::new(ms_0.clone())
        .with_pool(&pool_0)
        .with_name("f0")
        .with_new_uuid()
        .with_size_mb(60)
        .with_thin(false);

    pool_0.create().await.unwrap();
    fill_0.create().await.unwrap();
    repl_0.create().await.unwrap();
    repl_0.share().await.unwrap();

    let mut pool_1 = PoolBuilder::new(ms_1.clone())
        .with_name("pool1")
        .with_new_uuid()
        .with_malloc("mem1", 80);

    let mut repl_1 = ReplicaBuilder::new(ms_1.clone())
        .with_pool(&pool_1)
        .with_name("r1")
        .with_new_uuid()
        .with_size_mb(60)
        .with_thin(true);

    pool_1.create().await.unwrap();
    repl_1.create().await.unwrap();
    repl_1.share().await.unwrap();

    let mut nex_0 = NexusBuilder::new(ms_nex.clone())
        .with_name("nexus0")
        .with_new_uuid()
        .with_size_mb(60)
        .with_replica(&repl_0)
        .with_replica(&repl_1);

    nex_0.create().await.unwrap();
    nex_0.publish().await.unwrap();

    test_recover_from_enospc(nex_0, repl_0, fill_0, 5, 10).await;
}

async fn test_recover_from_enospc(
    nex: NexusBuilder,
    repl_to_online: ReplicaBuilder,
    mut repl_to_remove: ReplicaBuilder,
    count: usize,
    buf_size_mb: usize,
) {
    // Write more data than pool free space.
    // Must succeed.
    test_write_to_nexus(&nex, count, buf_size_mb).await.unwrap();

    // First child must be degraded.
    let n = find_nexus_by_uuid(nex.rpc(), &nex.uuid()).await.unwrap();

    // First child must be degraded with no space.
    assert_eq!(n.children.len(), 2);
    let child = &n.children[0];
    assert_eq!(child.state, ChildState::Degraded as i32);
    assert_eq!(child.state_reason, ChildStateReason::NoSpace as i32);

    // Destroy the replica that occupies space on pool.
    repl_to_remove.destroy().await.unwrap();

    // And online degraded child.
    let n = nex.online_child_replica(&repl_to_online).await.unwrap();

    // First child must now be in a rebuilding state, which is indicated
    // by OutOfSync degrade reason.
    assert_eq!(n.children.len(), 2);
    let child = &n.children[0];
    assert_eq!(child.state, ChildState::Degraded as i32);
    assert_eq!(child.state_reason, ChildStateReason::OutOfSync as i32);
}
