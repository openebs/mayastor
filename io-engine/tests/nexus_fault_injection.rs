#![cfg(feature = "fault-injection")]

pub mod common;

use std::time::Duration;

use io_engine::core::fault_injection::{
    FaultDomain,
    FaultIoOperation,
    FaultIoStage,
    FaultMethod,
    Injection,
    InjectionBuilder,
};

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
    replica::ReplicaBuilder,
    test::{add_fault_injection, list_fault_injections},
};
use io_engine::core::IoCompletionStatus;
use io_engine_tests::{
    fio::{Fio, FioJob},
    nvme::{find_mayastor_nvme_device_path, NmveConnectGuard},
};
use spdk_rs::NvmeStatus;

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
            Binary::from_dbg("io-engine").with_args(vec![
                "-l",
                "3",
                "-Fcolor,compact",
            ]),
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

/// TODO
async fn test_injection_uri(inj_part: &str) {
    let test = create_compose_test().await;

    let StorageBuilder {
        pool_0: _,
        pool_1: _,
        repl_0: _,
        repl_1: _,
        nex_0,
    } = create_test_storage(&test).await;

    //
    let children = nex_0.get_nexus().await.unwrap().children;
    assert_eq!(children.len(), 2);
    let dev_name = children[0].device_name.as_ref().unwrap();

    let inj_uri = format!("inject://{dev_name}?{inj_part}");
    add_fault_injection(nex_0.rpc(), &inj_uri).await.unwrap();

    // List injected fault.
    let lst = list_fault_injections(nex_0.rpc()).await.unwrap();
    assert_eq!(lst.len(), 1);
    assert_eq!(&lst[0].device_name, dev_name);

    // Write less than pool size.
    test_write_to_nexus(
        &nex_0,
        DataSize::from_bytes(0),
        30,
        DataSize::from_mb(1),
    )
    .await
    .unwrap();

    //
    let children = nex_0.get_nexus().await.unwrap().children;
    assert_eq!(children[0].state, ChildState::Faulted as i32);
    assert_eq!(children[0].state, ChildStateReason::CannotOpen as i32);
}

#[tokio::test]
async fn nexus_fault_injection_write_submission() {
    test_injection_uri("domain=child&op=write&stage=submit&offset=64").await;
}

#[tokio::test]
async fn nexus_fault_injection_write() {
    test_injection_uri("domain=child&op=write&stage=compl&offset=64").await;
}

#[tokio::test]
async fn nexus_fault_injection_read_submission() {
    test_injection_uri("domain=child&op=read&stage=submit&offset=64").await;
}

#[tokio::test]
async fn nexus_fault_injection_read() {
    test_injection_uri("domain=child&op=read&stage=compl&offset=64").await;
}

#[tokio::test]
async fn nexus_fault_injection_time_based() {
    let test = create_compose_test().await;

    let StorageBuilder {
        pool_0: _,
        pool_1: _,
        repl_0,
        repl_1: _,
        nex_0,
    } = create_test_storage(&test).await;

    //
    let children = nex_0.get_nexus().await.unwrap().children;
    assert_eq!(children.len(), 2);
    let dev_name = children[0].device_name.as_ref().unwrap();

    // Create an injection that will start in 1 sec after first I/O
    // to the device, and end after 5s.
    let inj_part =
        "domain=child&op=write&stage=compl&begin_at=1000&end_at=5000";
    let inj_uri = format!("inject://{dev_name}?{inj_part}");
    add_fault_injection(nex_0.rpc(), &inj_uri).await.unwrap();

    // List injected fault.
    let lst = list_fault_injections(nex_0.rpc()).await.unwrap();
    assert_eq!(lst.len(), 1);
    assert_eq!(&lst[0].device_name, dev_name);

    // Write some data. Injection is not yet active.
    test_write_to_nexus(
        &nex_0,
        DataSize::from_bytes(0),
        1,
        DataSize::from_kb(1),
    )
    .await
    .unwrap();
    let children = nex_0.get_nexus().await.unwrap().children;
    assert_eq!(children[0].state, ChildState::Online as i32);

    // Wait a sec to allow the injection to kick in.
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Write again. Now the child must fail.
    test_write_to_nexus(
        &nex_0,
        DataSize::from_bytes(0),
        1,
        DataSize::from_kb(1),
    )
    .await
    .unwrap();
    let children = nex_0.get_nexus().await.unwrap().children;
    assert_eq!(children[0].state, ChildState::Faulted as i32);

    // Wait a sec to allow the injection to end.
    tokio::time::sleep(Duration::from_millis(4000)).await;

    // Bring the child online.
    nex_0.online_child_replica(&repl_0).await.unwrap();
    nex_0
        .wait_children_online(std::time::Duration::from_secs(10))
        .await
        .unwrap();

    // Write again. Now since the injection time ended, it must not fail.
    test_write_to_nexus(
        &nex_0,
        DataSize::from_bytes(0),
        1,
        DataSize::from_kb(1),
    )
    .await
    .unwrap();
    let children = nex_0.get_nexus().await.unwrap().children;
    assert_eq!(children[0].state, ChildState::Online as i32);
}

#[tokio::test]
async fn nexus_fault_injection_range_based() {
    let test = create_compose_test().await;

    let StorageBuilder {
        pool_0: _,
        pool_1: _,
        repl_0,
        repl_1: _,
        nex_0,
    } = create_test_storage(&test).await;

    //
    let children = nex_0.get_nexus().await.unwrap().children;
    assert_eq!(children.len(), 2);
    let dev_name = children[0].device_name.as_ref().unwrap();

    // Create injection that will fail at offset of 128 blocks, for a span
    // of 16 blocks.
    let inj_uri = InjectionBuilder::default()
        .with_device_name(dev_name.clone())
        .with_domain(FaultDomain::NexusChild)
        .with_io_operation(FaultIoOperation::Write)
        .with_io_stage(FaultIoStage::Completion)
        .with_offset(128, 16)
        .build_uri()
        .unwrap();
    add_fault_injection(nex_0.rpc(), &inj_uri).await.unwrap();

    // List injected fault.
    let lst = list_fault_injections(nex_0.rpc()).await.unwrap();
    assert_eq!(lst.len(), 1);
    assert_eq!(&lst[0].device_name, dev_name);

    // Write two blocks from 0 offset. It must not fail.
    test_write_to_nexus(
        &nex_0,
        DataSize::from_bytes(0),
        1,
        DataSize::from_kb(1),
    )
    .await
    .unwrap();
    let children = nex_0.get_nexus().await.unwrap().children;
    assert_eq!(children[0].state, ChildState::Online as i32);

    // Write at offset 128. Now the child must fail.
    test_write_to_nexus(
        &nex_0,
        DataSize::from_blocks(128, 512),
        1,
        DataSize::from_kb(1),
    )
    .await
    .unwrap();
    let children = nex_0.get_nexus().await.unwrap().children;
    assert_eq!(children[0].state, ChildState::Faulted as i32);

    tokio::time::sleep(Duration::from_millis(4000)).await;

    // Bring the child online.
    nex_0.online_child_replica(&repl_0).await.unwrap();
    nex_0
        .wait_children_online(std::time::Duration::from_secs(10))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(4000)).await;

    // Write at offset 128 + 16. It must not fail.
    test_write_to_nexus(
        &nex_0,
        DataSize::from_blocks(144, 512),
        1,
        DataSize::from_kb(1),
    )
    .await
    .unwrap();
    let children = nex_0.get_nexus().await.unwrap().children;
    assert_eq!(children[0].state, ChildState::Online as i32);

    // Write at offset 128 + 15. It must fail.
    test_write_to_nexus(
        &nex_0,
        DataSize::from_blocks(143, 512),
        1,
        DataSize::from_kb(1),
    )
    .await
    .unwrap();

    let children = nex_0.get_nexus().await.unwrap().children;
    assert_eq!(children[0].state, ChildState::Faulted as i32);
}

#[tokio::test]
async fn injection_uri_creation() {
    let src = InjectionBuilder::default()
        .with_domain(FaultDomain::BlockDevice)
        .with_device_name("dev0".to_string())
        .with_method(FaultMethod::Status(IoCompletionStatus::NvmeError(
            NvmeStatus::NO_SPACE,
        )))
        .with_io_operation(FaultIoOperation::Read)
        .with_io_stage(FaultIoStage::Completion)
        .with_block_range(123 .. 456)
        .with_time_range(Duration::from_secs(5) .. Duration::from_secs(10))
        .with_retries(789)
        .build()
        .unwrap();

    // Test that debug output works.
    println!("{src:?}");
    println!("{src:#?}");

    let uri = src.as_uri();
    let res = Injection::from_uri(&uri).unwrap();

    assert_eq!(src.uri(), uri);
    assert_eq!(src.uri(), res.uri());
    assert_eq!(src.domain, res.domain);
    assert_eq!(src.device_name, res.device_name);
    assert_eq!(src.method, res.method);
    assert_eq!(src.io_operation, res.io_operation);
    assert_eq!(src.io_stage, res.io_stage);
    assert_eq!(src.time_range, res.time_range);
    assert_eq!(src.block_range, res.block_range);
    assert_eq!(src.retries, res.retries);
}

#[tokio::test]
async fn replica_bdev_io_injection() {
    common::composer_init();

    const BLK_SIZE: u64 = 512;

    let test = Builder::new()
        .name("cargo-test")
        .network("10.1.0.0/16")
        .unwrap()
        .add_container_bin(
            "ms_0",
            Binary::from_dbg("io-engine").with_args(vec![
                "-l",
                "1",
                "-Fcompact,color,nodate",
            ]),
        )
        .with_clean(true)
        .build()
        .await
        .unwrap();

    let conn = GrpcConnect::new(&test);
    let rpc = conn.grpc_handle_shared("ms_0").await.unwrap();

    let mut pool_0 = PoolBuilder::new(rpc.clone())
        .with_name("pool0")
        .with_new_uuid()
        .with_malloc_blk_size("mem0", 100, BLK_SIZE);
    pool_0.create().await.unwrap();

    let mut repl_0 = ReplicaBuilder::new(rpc.clone())
        .with_pool(&pool_0)
        .with_name("r0")
        .with_new_uuid()
        .with_size_mb(80)
        .with_thin(true);

    repl_0.create().await.unwrap();
    repl_0.share().await.unwrap();

    let inj_uri = InjectionBuilder::default()
        .with_device_name("r0".to_string())
        .with_domain(FaultDomain::BdevIo)
        .with_io_operation(FaultIoOperation::Write)
        .with_io_stage(FaultIoStage::Submission)
        .with_method(FaultMethod::Status(IoCompletionStatus::NvmeError(
            NvmeStatus::DATA_TRANSFER_ERROR,
        )))
        .with_offset(20, 1)
        .build_uri()
        .unwrap();

    add_fault_injection(rpc.clone(), &inj_uri).await.unwrap();

    let nvmf = repl_0.nvmf_location();
    let _cg = NmveConnectGuard::connect_addr(&nvmf.addr, &nvmf.nqn);
    let path = find_mayastor_nvme_device_path(&nvmf.serial)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    // With offset of 30 blocks, the job mustn't hit the injected fault, which
    // is set on block #20.
    let fio_ok = Fio::new().with_job(
        FioJob::new()
            .with_direct(true)
            .with_ioengine("libaio")
            .with_iodepth(1)
            .with_filename(&path)
            .with_offset(DataSize::from_blocks(30, BLK_SIZE))
            .with_rw("write"),
    );

    // With the entire device, the job must hit the injected fault.
    let fio_fail = Fio::new().with_job(
        FioJob::new()
            .with_direct(true)
            .with_ioengine("libaio")
            .with_iodepth(1)
            .with_filename(&path)
            .with_rw("write"),
    );

    tokio::spawn(async move { fio_ok.run() })
        .await
        .unwrap()
        .expect("This FIO job must succeed");

    let r = tokio::spawn(async move { fio_fail.run() })
        .await
        .unwrap()
        .expect_err("This FIO job must fail");

    assert_eq!(r.kind(), std::io::ErrorKind::Other);
}
