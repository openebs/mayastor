//!
//! This test is roughly the same as the tests in nexus_add_remove. However,
//! this test does not use nvmf targets rather uring bdevs

use io_engine::{
    bdev::nexus::{nexus_create, nexus_lookup_mut},
    core::{MayastorCliArgs, Share},
};
use once_cell::sync::OnceCell;

static DISKNAME1: &str = "/tmp/disk1.img";
static DISKNAME2: &str = "/tmp/disk2.img";
static DISKNAME3: &str = "/tmp/disk3.img";

const POOL_SIZE: u64 = 1000;
const REPL_SIZE: u64 = 900;
const NEXUS_SIZE: u64 = REPL_SIZE;

use crate::common::MayastorTest;

pub mod common;

use common::{
    compose::{rpc::v1::GrpcConnect, Binary, Builder},
    fio::{Fio, FioJob},
    nexus::{test_fio_to_nexus, NexusBuilder},
    pool::PoolBuilder,
    replica::ReplicaBuilder,
};

pub fn mayastor() -> &'static MayastorTest<'static> {
    static MAYASTOR: OnceCell<MayastorTest> = OnceCell::new();

    MAYASTOR.get_or_init(|| {
        MayastorTest::new(MayastorCliArgs {
            reactor_mask: "0x2".to_string(),
            no_pci: true,
            grpc_endpoint: "0.0.0.0".to_string(),
            ..Default::default()
        })
    })
}

/// create a nexus with two file based devices
/// and then, once created, share it and then
/// remove one of the children
#[tokio::test]
async fn remove_children_from_nexus() {
    // we can only start mayastor once we run it within the same process, and
    // during start mayastor will create a thread for each of the cores
    // (0x2) here.
    //
    // grpc is not used in this case, and we use channels to send work to
    // mayastor from the runtime here.

    let ms = mayastor();

    common::truncate_file(DISKNAME1, 64 * 1024);
    common::truncate_file(DISKNAME2, 64 * 1024);

    // create a nexus with two children
    ms.spawn(async {
        nexus_create(
            "remove_from_nexus",
            60 * 1024 * 1024,
            None,
            &[
                format!("uring:///{DISKNAME1}"),
                format!("uring:///{DISKNAME2}"),
            ],
        )
        .await
    })
    .await
    .expect("failed to create nexus");

    // lookup the nexus and share it over nvmf
    ms.spawn(async {
        let nexus =
            nexus_lookup_mut("remove_from_nexus").expect("nexus is not found!");
        nexus.share_nvmf(None).await
    })
    .await
    .expect("failed to share nexus over nvmf");

    // lookup the nexus, and remove a child
    ms.spawn(async {
        let nexus =
            nexus_lookup_mut("remove_from_nexus").expect("nexus is not found!");
        nexus.remove_child(&format!("uring:///{DISKNAME1}")).await
    })
    .await
    .expect("failed to remove child from nexus");

    ms.spawn(async {
        let nexus =
            nexus_lookup_mut("remove_from_nexus").expect("nexus is not found!");
        nexus.remove_child(&format!("uring:///{DISKNAME2}")).await
    })
    .await
    .expect_err("cannot remove the last child from nexus");

    // add new child but don't rebuild, so it's not healthy!
    ms.spawn(async {
        let nexus =
            nexus_lookup_mut("remove_from_nexus").expect("nexus is not found!");
        nexus
            .add_child(&format!("uring:///{DISKNAME1}"), true)
            .await
    })
    .await
    .expect("should be able to add a child back");

    ms.spawn(async {
        let nexus =
            nexus_lookup_mut("remove_from_nexus").expect("nexus is not found!");
        nexus.remove_child(&format!("uring:///{DISKNAME2}")).await
    })
    .await
    .expect_err("cannot remove the last healthy child from nexus");

    // destroy it
    ms.spawn(async {
        let nexus =
            nexus_lookup_mut("remove_from_nexus").expect("nexus is not found!");
        nexus.destroy().await.unwrap();
    })
    .await;

    common::delete_file(&[DISKNAME1.into(), DISKNAME2.into()]);
}

/// similar as the above test case however, instead of removal we add one
#[tokio::test]
async fn nexus_add_child() {
    let ms = mayastor();
    // we can only start mayastor once
    common::truncate_file(DISKNAME1, 64 * 1024);
    common::truncate_file(DISKNAME2, 64 * 1024);
    common::truncate_file(DISKNAME3, 64 * 1024);

    ms.spawn(async {
        nexus_create(
            "nexus_add_child",
            60 * 1024 * 1024,
            None,
            &[
                format!("uring:///{DISKNAME1}"),
                format!("uring:///{DISKNAME2}"),
            ],
        )
        .await
        .expect("failed to create nexus");
    })
    .await;

    ms.spawn(async {
        let nexus =
            nexus_lookup_mut("nexus_add_child").expect("nexus is not found!");
        nexus
            .share_nvmf(None)
            .await
            .expect("failed to share nexus over nvmf");
    })
    .await;

    ms.spawn(async {
        let nexus =
            nexus_lookup_mut("nexus_add_child").expect("nexus is not found!");
        nexus
            .add_child(&format!("uring:///{DISKNAME3}"), false)
            .await
    })
    .await
    .unwrap();

    ms.spawn(async {
        let nexus =
            nexus_lookup_mut("nexus_add_child").expect("nexus is not found!");
        nexus.destroy().await.unwrap();
    })
    .await;

    common::delete_file(&[
        DISKNAME1.into(),
        DISKNAME2.into(),
        DISKNAME3.into(),
    ]);
}

/// Remove a child while I/O is running.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nexus_remove_child_with_io() {
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
            Binary::from_dbg("io-engine").with_args(vec![
                "-l",
                "3,4",
                "-Fnodate,compact,color",
            ]),
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

    let j0 = tokio::spawn({
        let nex_0 = nex_0.clone();
        let repl_0 = repl_0.clone();
        async move {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            nex_0.remove_child_replica(&repl_0).await.unwrap();
        }
    });

    let j1 = tokio::spawn({
        let nex_0 = nex_0.clone();
        async move {
            test_fio_to_nexus(
                &nex_0,
                Fio::new().with_job(
                    FioJob::new()
                        .with_runtime(10)
                        .with_bs(4096)
                        .with_iodepth(16),
                ),
            )
            .await
            .unwrap();
        }
    });

    let _ = tokio::join!(j0, j1);
}

/// Test added to reproduce assertion failure caused by parallel running
/// async and sync connect for a qpair.
/// This test:
/// 1. Creates 3 pools, and a replica on each pool(repl_0, repl_1, repl_2).
/// 2. Creates nexus with repl_1 and repl_2.
/// 3. In a loop:
///     3.a. Adds replica repl_0.
///     3.b. Removes replica repl_2 (not being used as rebuild source).
///     3.c. Removes repl_0 and adds repl_2 back.
#[tokio::test]
#[ignore]
async fn nexus_channel_get_handles() {
    common::composer_init();

    let test = Builder::new()
        .name("cargo-test")
        .network("10.1.0.0/16")
        .unwrap()
        .add_container_bin(
            "qms_0",
            Binary::from_dbg("io-engine").with_args(vec!["-l", "1,2"]),
        )
        .add_container_bin(
            "qms_1",
            Binary::from_dbg("io-engine").with_args(vec!["-l", "3,4"]),
        )
        .add_container_bin(
            "qms_nex",
            Binary::from_dbg("io-engine").with_args(vec![
                "-l",
                "5,6",
                "-Fnodate,compact,color",
            ]),
        )
        .with_clean(true)
        .build()
        .await
        .unwrap();

    let conn = GrpcConnect::new(&test);
    let ms_0 = conn.grpc_handle_shared("qms_0").await.unwrap();
    let ms_1 = conn.grpc_handle_shared("qms_1").await.unwrap();
    let ms_nex = conn.grpc_handle_shared("qms_nex").await.unwrap();

    // Node #0
    let mut pool_0 = PoolBuilder::new(ms_0.clone())
        .with_name("pool0")
        .with_new_uuid()
        .with_malloc("qmem0", POOL_SIZE);

    let mut repl_0 = ReplicaBuilder::new(ms_0.clone())
        .with_pool(&pool_0)
        .with_name("qr0")
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
        .with_malloc("qmem1", POOL_SIZE);

    let mut repl_1 = ReplicaBuilder::new(ms_1.clone())
        .with_pool(&pool_1)
        .with_name("qr1")
        .with_new_uuid()
        .with_thin(false)
        .with_size_mb(REPL_SIZE);

    pool_1.create().await.unwrap();
    repl_1.create().await.unwrap();
    repl_1.share().await.unwrap();

    // Node #2 - local to nexus
    let mut pool_2 = PoolBuilder::new(ms_nex.clone())
        .with_name("pool2")
        .with_new_uuid()
        .with_malloc("qmem2", POOL_SIZE);

    let mut repl_2 = ReplicaBuilder::new(ms_nex.clone())
        .with_pool(&pool_2)
        .with_name("qr2")
        .with_new_uuid()
        .with_thin(false)
        .with_size_mb(REPL_SIZE);

    pool_2.create().await.unwrap();
    repl_2.create().await.unwrap();
    repl_2.share().await.unwrap();

    // Nexus
    let mut nex_0 = NexusBuilder::new(ms_nex.clone())
        .with_name("qnexus0")
        .with_new_uuid()
        .with_size_mb(NEXUS_SIZE)
        .with_replica(&repl_1)
        .with_replica(&repl_2);

    nex_0.create().await.unwrap();
    nex_0.publish().await.unwrap();

    for _ in 0 .. 30 {
        let j_repl0 = tokio::spawn({
            let nex_0 = nex_0.clone();
            let repl_0 = repl_0.clone();
            async move {
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                nex_0.add_replica(&repl_0, false).await.unwrap();
            }
        });

        let j_repl2 = tokio::spawn({
            let nex_0 = nex_0.clone();
            let repl_2 = repl_2.clone();
            async move {
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                nex_0.remove_child_replica(&repl_2).await.unwrap();
            }
        });

        let _ = tokio::join!(j_repl0, j_repl2);

        // Come back to same state for next loop iteration.
        nex_0.remove_child_replica(&repl_0).await.unwrap();
        nex_0.add_replica(&repl_2, false).await.unwrap();
        assert_eq!(nex_0.get_nexus().await.unwrap().children.len(), 2);
        tokio::time::sleep(std::time::Duration::from_micros(500)).await;
    }
}
