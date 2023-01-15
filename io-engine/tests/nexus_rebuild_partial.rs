pub mod common;

use common::{
    compose::{
        rpc::v1::{nexus::ChildState, GrpcConnect},
        Binary,
        Builder,
    },
    file_io::BufferSize,
    nexus::NexusBuilder,
    pool::PoolBuilder,
    replica::ReplicaBuilder,
};
use io_engine_tests::{
    nexus::test_write_to_nexus,
    nice_json,
    replica::validate_replicas,
};

#[tokio::test]
/// Create a nexus with two replica, fill it with data.
/// Offline second replica, write more data.
/// Online second replica: it should rebuild only newly written blocks.
async fn nexus_thin_rebuild_from_remote_to_remote() {
    common::composer_init();

    let test = Builder::new()
        .name("cargo-test")
        .network("10.1.0.0/16")
        .unwrap()
        .add_container_bin(
            "ms_nex",
            Binary::from_dbg("io-engine").with_args(vec![
                "-l",
                "1,2,3,4",
                "-Fcompact,nodate,host,color",
            ]),
        )
        .add_container_bin(
            "ms_src_0",
            Binary::from_dbg("io-engine").with_args(vec![
                "-l",
                "5,6",
                "-Fcompact,nodate,host,color",
            ]),
        )
        .add_container_bin(
            "ms_src_1",
            Binary::from_dbg("io-engine").with_args(vec![
                "-l",
                "7,8",
                "-Fcompact,nodate,host,color",
            ]),
        )
        .with_clean(true)
        .build()
        .await
        .unwrap();

    let conn = GrpcConnect::new(&test);

    let ms_nex = conn.grpc_handle_shared("ms_nex").await.unwrap();
    let ms_src_0 = conn.grpc_handle_shared("ms_src_0").await.unwrap();
    let ms_src_1 = conn.grpc_handle_shared("ms_src_1").await.unwrap();

    const POOL_SIZE: u64 = 40;
    const REPL_SIZE: u64 = 10;

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
        .with_thin(false);

    pool_0.create().await.unwrap();
    repl_0.create().await.unwrap();
    repl_0.share().await.unwrap();

    //
    let mut pool_1 = PoolBuilder::new(ms_src_1.clone())
        .with_name("pool1")
        .with_new_uuid()
        .with_malloc("mem0", POOL_SIZE);

    let mut repl_1 = ReplicaBuilder::new(ms_src_1.clone())
        .with_pool(&pool_1)
        .with_name("r1")
        .with_new_uuid()
        .with_size_mb(REPL_SIZE)
        .with_thin(false);

    pool_1.create().await.unwrap();
    repl_1.create().await.unwrap();
    repl_1.share().await.unwrap();

    //
    let mut nex_0 = NexusBuilder::new(ms_nex.clone())
        .with_name("nexus0")
        .with_new_uuid()
        .with_size_mb(REPL_SIZE)
        .with_replica(&repl_0)
        .with_replica(&repl_1);

    nex_0.create().await.unwrap();
    nex_0.publish().await.unwrap();

    // TODO ??
    let children = nex_0.get_nexus().await.unwrap().children;
    assert_eq!(children.len(), 2);

    // TODO ??
    let dev_name_1 = children[1].device_name.as_ref().unwrap();
    println!("==== [1] devname {}", dev_name_1);
    let inj_uri = format!("inject://{}?op=write&start_cnt=10", dev_name_1);

    nex_0.inject_nexus_fault(&inj_uri).await.unwrap();

    println!("==== [2]");
    test_write_to_nexus(&nex_0, 0, 1000, BufferSize::Kb(1))
        .await
        .unwrap();

    println!("==== [3]");
    nex_0.remove_injected_nexus_fault(&inj_uri).await.unwrap();
    println!("==== [4]");

    // TODO ???
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    //
    let children = nex_0.get_nexus().await.unwrap().children;
    assert_eq!(children.len(), 2);
    assert_eq!(children[1].state, ChildState::Faulted as i32);
    println!("{}", nice_json(&children));

    println!("==== [5]");
    nex_0.online_child_replica(&repl_1).await.unwrap();
    println!("==== [6]");

    nex_0
        .wait_children_online(std::time::Duration::from_secs(10))
        .await
        .unwrap();

    println!("==== [7]");

    validate_replicas(&vec![repl_0, repl_1]).await;
    println!("==== [8]");
    panic!("qqq")
}
