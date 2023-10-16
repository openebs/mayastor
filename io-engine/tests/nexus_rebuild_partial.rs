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
    file_io::DataSize,
    nexus::{test_write_to_nexus, NexusBuilder},
    pool::PoolBuilder,
    replica::{validate_replicas, ReplicaBuilder},
};

#[cfg(feature = "fault-injection")]
use io_engine_tests::{
    fio::{Fio, FioJob},
    nexus::test_fio_to_nexus,
};

#[cfg(feature = "fault-injection")]
use io_engine::core::fault_injection::{
    FaultDomain,
    FaultIoOperation,
    FaultIoStage,
    InjectionBuilder,
};

#[cfg(feature = "fault-injection")]
use common::compose::rpc::v1::nexus::RebuildJobState;

#[cfg(feature = "fault-injection")]
use common::test::{add_fault_injection, remove_fault_injection};

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
            Binary::from_dbg("io-engine").with_args(vec![
                "-l",
                "1,2,3,4",
                "-Fcompact,color",
            ]),
            // Binary::from_dbg("io-engine").with_args(vec!["-l", "1,2,3,4"]),
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
#[cfg(feature = "fault-injection")]
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
    let inj_uri = InjectionBuilder::default()
        .with_device_name(dev_name_1.clone())
        .with_domain(FaultDomain::NexusChild)
        .with_io_operation(FaultIoOperation::Write)
        .with_io_stage(FaultIoStage::Completion)
        .with_block_range(7 * SEG_BLK .. u64::MAX)
        .build_uri()
        .unwrap();
    add_fault_injection(nex_0.rpc(), &inj_uri).await.unwrap();

    // This write must be okay as the injection is not triggered yet.
    test_write_to_nexus(
        &nex_0,
        DataSize::from_bytes(2 * SEG - 1),
        65,
        DataSize::from_kb(1),
    )
    .await
    .unwrap();

    let children = nex_0.get_nexus().await.unwrap().children;
    assert_eq!(children.len(), 2);
    assert_eq!(children[1].state(), ChildState::Online);

    // Chunk A.
    test_write_to_nexus(
        &nex_0,
        DataSize::from_bytes(8 * SEG - 1),
        95,
        DataSize::from_kb(1),
    )
    .await
    .unwrap();

    // Check that the nexus child is now faulted, with I/O failure reason.
    let children = nex_0.get_nexus().await.unwrap().children;
    assert_eq!(children.len(), 2);
    assert_eq!(children[1].state(), ChildState::Faulted);
    assert_eq!(children[1].state_reason(), ChildStateReason::IoFailure);
    assert_eq!(children[1].has_io_log, true);

    // Chunk B.
    test_write_to_nexus(
        &nex_0,
        DataSize::from_bytes(2 * SEG - 1),
        65,
        DataSize::from_kb(1),
    )
    .await
    .unwrap();

    // Chunk C.
    test_write_to_nexus(
        &nex_0,
        DataSize::from_bytes(13 * SEG - 1),
        129,
        DataSize::from_kb(1),
    )
    .await
    .unwrap();

    // Chunk D.
    test_write_to_nexus(
        &nex_0,
        DataSize::from_bytes(110 * SEG),
        111,
        DataSize::from_kb(1),
    )
    .await
    .unwrap();

    // Remove injection.
    remove_fault_injection(nex_0.rpc(), &inj_uri).await.unwrap();

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
    test_write_to_nexus(
        &nex_0,
        DataSize::from_bytes(0),
        10,
        DataSize::from_kb(16),
    )
    .await
    .unwrap();

    // Offline the replica.
    nex_0
        .offline_child_replica_wait(&repl_0, Duration::from_secs(1))
        .await
        .unwrap();

    let children = nex_0.get_nexus().await.unwrap().children;
    assert_eq!(children[0].state(), ChildState::Degraded);
    assert_eq!(children[0].state_reason(), ChildStateReason::ByClient);

    validate_replicas(&vec![repl_0.clone(), repl_1.clone()]).await;

    // We write 9 x 16-KiB buffers = 147456 bytes = 288 blocks = 2.25 rebuild
    // segments after previously written 10 x 16 KiB buffers.
    // That rounds to 3 segments = 384 blocks.
    test_write_to_nexus(
        &nex_0,
        DataSize::from_kb_blocks(10, 16),
        9,
        DataSize::from_kb(16),
    )
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[cfg(feature = "fault-injection")]
/// I/O failure during rebuild.
/// Initiate a (partial) rebuild, and force a replica to fail with I/O error
/// while the rebuild job is running.
/// Now, if the replica is onlined again, a full rebuild must start.
///
/// Steps:
/// 1) Offline replica #0.
/// 2) Write some data to nexus, ending at < FAULT_POS.
/// 3) Create injection that will fail at offset == FAULT_POS.
/// 4a) Online r0 so it rebuilds in background.
/// 4b) Write new data to the nexus: data start < FAULT_POS < data end.
/// 5) I/O must fail _before_ rebuild finishes. This will prevent creartion of a
///    rebuild log.
/// 6) Remove the injection.
/// 7) Online r0: a full rebuild must now run.
/// 8) Offline, write and online again to have a successfull partial rebuild.
async fn nexus_partial_rebuild_double_fault() {
    const POOL_SIZE: u64 = 1000;
    const REPL_SIZE: u64 = 900;
    const NEXUS_SIZE: u64 = REPL_SIZE;
    const BLK_SIZE: u64 = 512;
    const DATA_A_POS: u64 = 0;
    const DATA_A_SIZE: u64 = 400;
    const DATA_B_POS: u64 = 410;
    const DATA_B_SIZE: u64 = 400;
    const FAULT_POS: u64 = 450;

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
            Binary::from_dbg("io-engine").with_args(vec!["-l", "3,4"]),
        )
        .with_clean(true)
        .build()
        .await
        .unwrap();

    let conn = GrpcConnect::new(&test);

    let ms_0 = conn.grpc_handle_shared("ms_0").await.unwrap();
    let ms_1 = conn.grpc_handle_shared("ms_1").await.unwrap();
    let ms_nex = conn.grpc_handle_shared("ms_nex").await.unwrap();

    // Node #0
    let mut pool_0 = PoolBuilder::new(ms_0.clone())
        .with_name("pool0")
        .with_new_uuid()
        .with_malloc("mem0", POOL_SIZE);

    let mut repl_0 = ReplicaBuilder::new(ms_0.clone())
        .with_pool(&pool_0)
        .with_name("r0")
        .with_new_uuid()
        .with_thin(false)
        .with_size_mb(REPL_SIZE);

    pool_0.create().await.unwrap();
    repl_0.create().await.unwrap();
    repl_0.share().await.unwrap();

    // Node #1
    let mut pool_1 = PoolBuilder::new(ms_1.clone())
        .with_name("pool1")
        .with_new_uuid()
        .with_malloc("mem1", POOL_SIZE);

    let mut repl_1 = ReplicaBuilder::new(ms_1.clone())
        .with_pool(&pool_1)
        .with_name("r1")
        .with_new_uuid()
        .with_thin(false)
        .with_size_mb(REPL_SIZE);

    pool_1.create().await.unwrap();
    repl_1.create().await.unwrap();
    repl_1.share().await.unwrap();

    // Nexus
    let mut nex_0 = NexusBuilder::new(ms_nex.clone())
        .with_name("nexus0")
        .with_new_uuid()
        .with_size_mb(NEXUS_SIZE)
        .with_replica(&repl_0)
        .with_replica(&repl_1);

    nex_0.create().await.unwrap();
    nex_0.publish().await.unwrap();

    let children = nex_0.get_nexus().await.unwrap().children;
    let child_0_dev_name = children[0].device_name.as_ref().unwrap();

    // Offline the replica again.
    nex_0
        .offline_child_replica_wait(&repl_0, Duration::from_secs(1))
        .await
        .unwrap();

    let children = nex_0.get_nexus().await.unwrap().children;
    assert_eq!(children[0].state(), ChildState::Degraded);
    assert_eq!(children[0].state_reason(), ChildStateReason::ByClient);

    // Write some data to have something to rebuild.
    test_fio_to_nexus(
        &nex_0,
        Fio::new().with_job(
            FioJob::new()
                .with_bs(4096)
                .with_iodepth(16)
                .with_offset(DataSize::from_mb(DATA_A_POS))
                .with_size(DataSize::from_mb(DATA_A_SIZE)),
        ),
    )
    .await
    .unwrap();

    // Inject a failure at FAULT_POS.
    let inj_uri = InjectionBuilder::default()
        .with_device_name(child_0_dev_name.clone())
        .with_domain(FaultDomain::NexusChild)
        .with_io_operation(FaultIoOperation::Write)
        .with_io_stage(FaultIoStage::Completion)
        .with_offset(FAULT_POS * 1024 * 1024 / BLK_SIZE, 1)
        .build_uri()
        .unwrap();
    add_fault_injection(nex_0.rpc(), &inj_uri).await.unwrap();

    // Online the replica, triggering the rebuild.
    let j0 = tokio::spawn({
        let nex_0 = nex_0.clone();
        let repl_0 = repl_0.clone();
        async move {
            nex_0.online_child_replica(&repl_0).await.unwrap();
        }
    });

    // In parallel, write some data to trigger injected fault at FAULT_POS.
    let j1 = tokio::spawn({
        let nex_0 = nex_0.clone();
        async move {
            test_fio_to_nexus(
                &nex_0,
                Fio::new().with_job(
                    FioJob::new()
                        .with_bs(4096)
                        .with_iodepth(16)
                        .with_offset(DataSize::from_mb(DATA_B_POS))
                        .with_size(DataSize::from_mb(DATA_B_SIZE)),
                ),
            )
            .await
            .unwrap();
        }
    });

    let _ = tokio::join!(j0, j1);

    // Replica must now be faulted with I/O failure, or, rarely, rebuild failure
    // may come first.
    let children = nex_0.get_nexus().await.unwrap().children;
    assert_eq!(children[0].state(), ChildState::Faulted);
    assert!(matches!(
        children[0].state_reason(),
        ChildStateReason::IoFailure | ChildStateReason::RebuildFailed
    ));

    // [6]
    nex_0.online_child_replica(&repl_0).await.unwrap();
    nex_0
        .wait_children_online(std::time::Duration::from_secs(20))
        .await
        .unwrap();

    // Offline the replica again.
    nex_0
        .offline_child_replica_wait(&repl_0, Duration::from_secs(1))
        .await
        .unwrap();

    let children = nex_0.get_nexus().await.unwrap().children;
    assert_eq!(children[0].state(), ChildState::Degraded);
    assert_eq!(children[0].state_reason(), ChildStateReason::ByClient);

    // Write some data to have something to rebuild.
    test_fio_to_nexus(
        &nex_0,
        Fio::new().with_job(
            FioJob::new()
                .with_bs(4096)
                .with_iodepth(16)
                .with_offset(DataSize::from_mb(DATA_A_POS))
                .with_size(DataSize::from_mb(DATA_A_SIZE)),
        ),
    )
    .await
    .unwrap();

    // Now online the replica and wait until rebuild completes.
    nex_0.online_child_replica(&repl_0).await.unwrap();
    nex_0
        .wait_children_online(std::time::Duration::from_secs(20))
        .await
        .unwrap();

    // Check rebuild history.
    let hist = nex_0.get_rebuild_history().await.unwrap();
    assert_eq!(hist.len(), 3);

    // First rebuild must have been failed, because I/O failed while the job
    // was running.
    assert_eq!(hist[0].state(), RebuildJobState::Failed);
    assert_eq!(hist[0].is_partial, true);
    assert!(hist[0].blocks_transferred < hist[0].blocks_total);

    // 3rd rebuid job must have been a successfully full rebuild.
    assert_eq!(hist[1].state(), RebuildJobState::Completed);
    assert_eq!(hist[1].is_partial, false);
    assert_eq!(hist[1].blocks_transferred, hist[1].blocks_total);

    // 3rd rebuid job must have been a successfully partial rebuild.
    assert_eq!(hist[2].state(), RebuildJobState::Completed);
    assert_eq!(hist[2].is_partial, true);
    assert!(hist[2].blocks_transferred < hist[2].blocks_total);

    // First rebuild job must have been prematurely stopped, so the amount of
    // bytes transferreed must be lesser then the partial rebuild that finished
    // successfully.
    assert!(hist[0].blocks_transferred < hist[2].blocks_transferred);

    // Verify replicas.
    validate_replicas(&vec![repl_0.clone(), repl_1.clone()]).await;
}
