pub mod common;

use common::{
    compose::{
        rpc::v1::{
            nexus::{ChildState, ChildStateReason},
            GrpcConnect,
        },
        Binary,
        Builder,
        ComposeTest,
    },
    file_io::BufferSize,
    nexus::{test_write_to_nexus, NexusBuilder},
    pool::PoolBuilder,
    replica::{validate_replicas, ReplicaBuilder},
};
use std::time::Duration;

/// Pool size.
const POOL_SIZE_MB: u64 = 40;

/// Make replica size 10MiB.
/// This will make a nexus with start block=10240 and end block=24542,
/// which is 14302 blocks, which is 111 full rebuild segments, plus 94 blocks.
const REPL_SIZE_KB: u64 = 10 * 1024;

/// Rebuild segment size in bytes.
#[allow(dead_code)]
const SEG: u64 = 65536;

/// Each rebuild segment in blocks.
const SEG_BLK: u64 = 128;

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
            "ms_nex",
            Binary::from_dbg("io-engine")
                .with_args(vec!["-l", "1,2,3,4", "-Fcompact,color"])
                .with_env("NEXUS_PARTIAL_REBUILD", "1"),
        )
        .add_container_bin(
            "ms_src_0",
            Binary::from_dbg("io-engine").with_args(vec!["-l", "5"]),
            // Binary::from_dbg("io-engine").with_args(vec!["-l", "5,6"]),
        )
        .add_container_bin(
            "ms_src_1",
            Binary::from_dbg("io-engine").with_args(vec!["-l", "7"]),
            // Binary::from_dbg("io-engine").with_args(vec!["-l", "7,8"]),
        )
        .with_clean(true)
        .build()
        .await
        .unwrap()
}

/// Creates test storage.
async fn create_test_storage(test: &ComposeTest) -> StorageBuilder {
    let conn = GrpcConnect::new(test);

    let ms_nex = conn.grpc_handle_shared("ms_nex").await.unwrap();
    let ms_src_0 = conn.grpc_handle_shared("ms_src_0").await.unwrap();
    let ms_src_1 = conn.grpc_handle_shared("ms_src_1").await.unwrap();

    //
    let mut pool_0 = PoolBuilder::new(ms_src_0.clone())
        .with_name("pool0")
        .with_new_uuid()
        .with_malloc("mem0", POOL_SIZE_MB);

    let mut repl_0 = ReplicaBuilder::new(ms_src_0.clone())
        .with_pool(&pool_0)
        .with_name("r0")
        .with_new_uuid()
        .with_size_kb(REPL_SIZE_KB)
        .with_thin(false);

    pool_0.create().await.unwrap();
    repl_0.create().await.unwrap();
    repl_0.share().await.unwrap();

    //
    let mut pool_1 = PoolBuilder::new(ms_src_1.clone())
        .with_name("pool1")
        .with_new_uuid()
        .with_malloc("mem0", POOL_SIZE_MB);

    let mut repl_1 = ReplicaBuilder::new(ms_src_1.clone())
        .with_pool(&pool_1)
        .with_name("r1")
        .with_new_uuid()
        .with_size_kb(REPL_SIZE_KB)
        .with_thin(false);

    pool_1.create().await.unwrap();
    repl_1.create().await.unwrap();
    repl_1.share().await.unwrap();

    //
    let mut nex_0 = NexusBuilder::new(ms_nex.clone())
        .with_name("nexus0")
        .with_new_uuid()
        .with_size_kb(REPL_SIZE_KB)
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
#[cfg(feature = "nexus-fault-injection")]
// 1. Create a nexus with two replicas.
// 2. Create a fault injection on one replica, and write some data.
// 3. Online the failed replica and wait until it gets back.
// 4. Verify replica data.
async fn nexus_partial_rebuild_io_fault() {
    let test = create_compose_test().await;

    let StorageBuilder {
        pool_0: _,
        pool_1: _,
        repl_0,
        repl_1,
        nex_0,
    } = create_test_storage(&test).await;

    // Validate the nexus.
    let children = nex_0.get_nexus().await.unwrap().children;
    assert_eq!(children.len(), 2);

    // Write data to the nexus, causing the injection to trigger and
    // achild to fail.
    //
    // We write several chunks that span a number of rebuild segments:
    //
    // 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 ... 110 111
    //   |___|       |___|        |_______|       |___|
    //     A           B              C             D
    // Chunk A: 65KiB spans 3 segs.
    // Chunk B: 95KiB spans 3 segs.
    // Chunk C: 129KiB spans 4 segs.
    // Chunk D: 128 + 94 = 222 blocks fully fills 2 segs.
    //
    // Total = 12 segs.

    // Inject a child failure.
    // All write operations starting of segment #7 will fail.
    let dev_name_1 = children[1].device_name.as_ref().unwrap();
    let inj_uri = format!(
        "inject://{dev_name_1}?op=write&offset={offset}",
        offset = 7 * SEG_BLK
    );
    nex_0.inject_nexus_fault(&inj_uri).await.unwrap();

    // This write must be okay as the injection is not triggered yet.
    test_write_to_nexus(&nex_0, 2 * SEG - 1, 65, BufferSize::Kb(1))
        .await
        .unwrap();

    let children = nex_0.get_nexus().await.unwrap().children;
    assert_eq!(children.len(), 2);
    assert_eq!(children[1].state, ChildState::Online as i32);

    // Chunk A.
    test_write_to_nexus(&nex_0, 8 * SEG - 1, 95, BufferSize::Kb(1))
        .await
        .unwrap();

    // Check that the nexus child is now faulted, with I/O failure reason.
    let children = nex_0.get_nexus().await.unwrap().children;
    assert_eq!(children.len(), 2);
    assert_eq!(children[1].state, ChildState::Faulted as i32);
    assert_eq!(children[1].state_reason, ChildStateReason::IoFailure as i32);

    // Chunk B.
    test_write_to_nexus(&nex_0, 2 * SEG - 1, 65, BufferSize::Kb(1))
        .await
        .unwrap();

    // Chunk C.
    test_write_to_nexus(&nex_0, 13 * SEG - 1, 129, BufferSize::Kb(1))
        .await
        .unwrap();

    // Chunk D.
    test_write_to_nexus(&nex_0, 110 * SEG, 111, BufferSize::Kb(1))
        .await
        .unwrap();

    // Remove injection.
    nex_0.remove_injected_nexus_fault(&inj_uri).await.unwrap();

    // Bring the child online. That will trigger partial rebuild.
    nex_0.online_child_replica(&repl_1).await.unwrap();
    nex_0
        .wait_children_online(std::time::Duration::from_secs(10))
        .await
        .unwrap();

    // Check the replicas are identical now.
    validate_replicas(&vec![repl_0.clone(), repl_1.clone()]).await;

    // Validate that the rebuild did occur, and it was a partial one.
    let hist = nex_0.get_rebuild_history().await.unwrap();
    assert_eq!(hist.len(), 1);
    assert_eq!(hist[0].child_uri, repl_1.shared_uri());
    assert_eq!(hist[0].src_uri, repl_0.shared_uri());
    assert!(hist[0].is_partial);

    // Check that 10 segments in total were rebuilt.
    assert_eq!(hist[0].blocks_transferred, 12 * SEG_BLK);
}

#[tokio::test]
/// 1. Create a nexus with two replicas.
/// 2. Write some data.
/// 3. Offline a replica.
/// 4. Write more data.
/// 3. Online the offlined replica and wait until it rebuilds.
/// 4. Verify replica data.
async fn nexus_partial_rebuild_offline_online() {
    let test = create_compose_test().await;

    let StorageBuilder {
        pool_0: _,
        pool_1: _,
        repl_0,
        repl_1,
        nex_0,
    } = create_test_storage(&test).await;

    // Validate the nexus.
    let children = nex_0.get_nexus().await.unwrap().children;
    assert_eq!(children.len(), 2);

    // Write 10 x 16 KiB buffers.
    test_write_to_nexus(&nex_0, 0, 10, BufferSize::Kb(16))
        .await
        .unwrap();

    // Offline the replica.
    nex_0.offline_child_replica(&repl_0).await.unwrap();

    // Transition to offline state is not immediate,
    nex_0
        .wait_replica_state(
            &repl_0,
            ChildState::Degraded,
            None,
            Duration::from_secs(1),
        )
        .await
        .unwrap();

    let children = nex_0.get_nexus().await.unwrap().children;
    assert_eq!(children[0].state, ChildState::Degraded as i32);
    assert_eq!(children[0].state_reason, ChildStateReason::ByClient as i32);

    validate_replicas(&vec![repl_0.clone(), repl_1.clone()]).await;

    // We write 9 x 16-KiB buffers = 147456 bytes = 288 blocks = 2.25 rebuild
    // segments after previously written 10 x 16 KiB buffers.
    // That rounds to 3 segments = 384 blocks.
    test_write_to_nexus(&nex_0, 10, 9, BufferSize::Kb(16))
        .await
        .unwrap();

    // Bring the child online. That will trigger partial rebuild.
    nex_0.online_child_replica(&repl_0).await.unwrap();
    nex_0
        .wait_children_online(std::time::Duration::from_secs(10))
        .await
        .unwrap();

    validate_replicas(&vec![repl_0.clone(), repl_1.clone()]).await;

    let hist = nex_0.get_rebuild_history().await.unwrap();
    assert_eq!(hist.len(), 1);
    assert_eq!(hist[0].child_uri, repl_0.shared_uri());
    assert_eq!(hist[0].src_uri, repl_1.shared_uri());
    assert!(hist[0].is_partial);

    // check that 3 segments were rebuilt.
    assert_eq!(hist[0].blocks_transferred, 3 * SEG_BLK);
}
