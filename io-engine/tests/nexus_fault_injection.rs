#![cfg(feature = "nexus-fault-injection")]

pub mod common;

use std::time::Duration;

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
    replica::ReplicaBuilder,
};

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
    nex_0.inject_nexus_fault(&inj_uri).await.unwrap();

    // List injected fault.
    let lst = nex_0.list_injected_faults().await.unwrap();
    assert_eq!(lst.len(), 1);
    assert_eq!(&lst[0].device_name, dev_name);

    // Write less than pool size.
    test_write_to_nexus(&nex_0, 0, 30, BufferSize::Mb(1))
        .await
        .unwrap();

    //
    let children = nex_0.get_nexus().await.unwrap().children;
    assert_eq!(children[0].state, ChildState::Faulted as i32);
    assert_eq!(children[0].state, ChildStateReason::CannotOpen as i32);
}

#[tokio::test]
async fn nexus_fault_injection_write_submission() {
    test_injection_uri("op=swrite&offset=64").await;
}

#[tokio::test]
async fn nexus_fault_injection_write() {
    test_injection_uri("op=write&offset=64").await;
}

#[tokio::test]
async fn nexus_fault_injection_read_submission() {
    test_injection_uri("op=sread&offset=64").await;
}

#[tokio::test]
async fn nexus_fault_injection_read() {
    test_injection_uri("op=read&offset=64").await;
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
    let inj_part = "op=write&begin=1000&end=5000";
    let inj_uri = format!("inject://{dev_name}?{inj_part}");
    nex_0.inject_nexus_fault(&inj_uri).await.unwrap();

    // List injected fault.
    let lst = nex_0.list_injected_faults().await.unwrap();
    assert_eq!(lst.len(), 1);
    assert_eq!(&lst[0].device_name, dev_name);

    // Write some data. Injection is not yet active.
    test_write_to_nexus(&nex_0, 0, 1, BufferSize::Kb(1))
        .await
        .unwrap();
    let children = nex_0.get_nexus().await.unwrap().children;
    assert_eq!(children[0].state, ChildState::Online as i32);

    // Wait a sec to allow the injection to kick in.
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Write again. Now the child must fail.
    test_write_to_nexus(&nex_0, 0, 1, BufferSize::Kb(1))
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
    test_write_to_nexus(&nex_0, 0, 1, BufferSize::Kb(1))
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
    let inj_part = "op=write&offset=128&num_blk=16";
    let inj_uri = format!("inject://{dev_name}?{inj_part}");
    nex_0.inject_nexus_fault(&inj_uri).await.unwrap();

    // List injected fault.
    let lst = nex_0.list_injected_faults().await.unwrap();
    assert_eq!(lst.len(), 1);
    assert_eq!(&lst[0].device_name, dev_name);

    // Write two blocks from 0 offset. It must not fail.
    test_write_to_nexus(&nex_0, 0, 1, BufferSize::Kb(1))
        .await
        .unwrap();
    let children = nex_0.get_nexus().await.unwrap().children;
    assert_eq!(children[0].state, ChildState::Online as i32);

    // Write at offset 128. Now the child must fail.
    test_write_to_nexus(&nex_0, 128 * 512, 1, BufferSize::Kb(1))
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
    test_write_to_nexus(&nex_0, 144 * 512, 1, BufferSize::Kb(1))
        .await
        .unwrap();
    let children = nex_0.get_nexus().await.unwrap().children;
    assert_eq!(children[0].state, ChildState::Online as i32);

    // Write at offset 128 + 15. It must fail.
    test_write_to_nexus(&nex_0, 143 * 512, 1, BufferSize::Kb(1))
        .await
        .unwrap();

    let children = nex_0.get_nexus().await.unwrap().children;
    assert_eq!(children[0].state, ChildState::Faulted as i32);
}
