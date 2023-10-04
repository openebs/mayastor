#![cfg(feature = "fault-injection")]

use std::time::Duration;

use once_cell::sync::OnceCell;

pub mod common;

use common::{
    bdev_io,
    compose::{
        rpc::{
            v1,
            v1::{GrpcConnect, SharedRpcHandle},
        },
        Binary,
        Builder,
        ComposeTest,
    },
    file_io::DataSize,
    fio::{Fio, FioJob},
    nexus::{test_fio_to_nexus, NexusBuilder},
    pool::PoolBuilder,
    reactor_poll,
    replica::ReplicaBuilder,
    MayastorTest,
};

pub use spdk_rs::{GenericStatusCode, NvmeStatus};

use io_engine::{
    bdev::{
        nexus::{
            nexus_create,
            nexus_lookup_mut,
            ChildState,
            FaultReason,
            NexusStatus,
        },
        NexusInfo,
    },
    core::{
        fault_injection::{
            add_fault_injection,
            FaultDomain,
            FaultInjection,
            FaultIoStage,
            FaultIoType,
            FaultType,
        },
        CoreError,
        IoCompletionStatus,
        MayastorCliArgs,
        Protocol,
    },
    lvs::Lvs,
    persistent_store::PersistentStoreBuilder,
    pool_backend::PoolArgs,
};

static MAYASTOR: OnceCell<MayastorTest> = OnceCell::new();

fn get_ms() -> &'static MayastorTest<'static> {
    MAYASTOR.get_or_init(|| {
        MayastorTest::new(MayastorCliArgs {
            enable_io_all_thrd_nexus_channels: true,
            ..Default::default()
        })
    })
}

/// Test cluster.
#[allow(dead_code)]
struct TestCluster {
    test: Box<ComposeTest>,
    etcd_endpoint: String,
    etcd: etcd_client::Client,
    ms_0: SharedRpcHandle,
    ms_1: SharedRpcHandle,
    ms_nex: SharedRpcHandle,
}

impl TestCluster {
    async fn create() -> Self {
        let etcd_endpoint = format!("http://10.1.0.2:2379");

        let test = Box::new(
            Builder::new()
                .name("io-race")
                .network("10.1.0.0/16")
                .unwrap()
                .add_container_spec(
                    common::compose::ContainerSpec::from_binary(
                        "etcd",
                        Binary::from_path(env!("ETCD_BIN")).with_args(vec![
                            "--data-dir",
                            "/tmp/etcd-data",
                            "--advertise-client-urls",
                            &etcd_endpoint,
                            "--listen-client-urls",
                            &etcd_endpoint,
                        ]),
                    )
                    .with_portmap("2379", "2379")
                    .with_portmap("2380", "2380"),
                )
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
                        "-Fcolor,nodate",
                        "-l",
                        "3,4",
                        "-p",
                        &etcd_endpoint,
                    ]),
                )
                .with_clean(true)
                .with_logs(true)
                .build()
                .await
                .unwrap(),
        );

        let conn = GrpcConnect::new(&test);

        let etcd = etcd_client::Client::connect([&etcd_endpoint], None)
            .await
            .unwrap();

        let ms_0 = conn.grpc_handle_shared("ms_0").await.unwrap();
        let ms_1 = conn.grpc_handle_shared("ms_1").await.unwrap();
        let ms_nex = conn.grpc_handle_shared("ms_nex").await.unwrap();

        Self {
            test,
            etcd_endpoint,
            etcd,
            ms_0,
            ms_1,
            ms_nex,
        }
    }
}

const FIO_DATA_SIZE: u64 = 100;

/// Test storage.
#[allow(dead_code)]
struct TestStorage {
    pool_0: PoolBuilder,
    repl_0: ReplicaBuilder,
    pool_1: PoolBuilder,
    repl_1: ReplicaBuilder,
    nex_0: NexusBuilder,
}

impl TestStorage {
    async fn create(cluster: &TestCluster) -> Self {
        const POOL_SIZE: u64 = FIO_DATA_SIZE + 20;
        const REPL_SIZE: u64 = FIO_DATA_SIZE + 10;
        const NEXUS_SIZE: u64 = REPL_SIZE;

        let ms_0 = cluster.ms_0.clone();
        let ms_1 = cluster.ms_1.clone();
        let ms_nex = cluster.ms_nex.clone();

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

        Self {
            pool_0,
            repl_0,
            pool_1,
            repl_1,
            nex_0,
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nexus_child_retire_persist_unresponsive_with_fio() {
    common::composer_init();

    let mut cluster = TestCluster::create().await;

    let TestStorage {
        pool_0: _,
        repl_0,
        pool_1: _,
        repl_1,
        nex_0,
    } = TestStorage::create(&cluster).await;

    // Fault replica #0 at block 10.
    nex_0
        .add_injection_at_replica(
            &repl_0,
            &format!("domain=nexus&op=write&offset={offset}", offset = 10),
        )
        .await
        .unwrap();

    // Pause ETCD.
    cluster.test.pause("etcd").await.unwrap();

    let r1 = tokio::spawn({
        let nex_0 = nex_0.clone();
        async move {
            test_fio_to_nexus(
                &nex_0,
                Fio::new().with_job(
                    FioJob::new()
                        .with_bs(4096)
                        .with_iodepth(8)
                        .with_size(DataSize::from_mb(FIO_DATA_SIZE)),
                ),
            )
            .await
            .unwrap();
        }
    });
    tokio::pin!(r1);

    assert!(
        tokio::time::timeout(Duration::from_secs(1), &mut r1)
            .await
            .is_err(),
        "I/O to nexus must freeze when ETCD is paused"
    );

    // Thaw ETCD.
    cluster.test.thaw("etcd").await.unwrap();

    assert!(
        tokio::time::timeout(Duration::from_secs(1), &mut r1)
            .await
            .is_ok(),
        "I/O to nexus must proceed when ETCD is thawed"
    );

    // Check that 1st child is fauled, and 2nd is open.
    let n = nex_0.get_nexus().await.unwrap();
    assert_eq!(n.children[0].state(), v1::nexus::ChildState::Faulted);
    assert_eq!(
        n.children[0].state_reason(),
        v1::nexus::ChildStateReason::IoFailure
    );
    assert_eq!(n.children[1].state(), v1::nexus::ChildState::Online);

    // Check that the ETCD has the correct record for the nexus:
    // one child is failed and the other is healthy.
    let response = cluster
        .etcd
        .get(nex_0.name(), None)
        .await
        .expect("No entry found");

    let value = response.kvs().first().unwrap().value();
    let ni: NexusInfo = serde_json::from_slice(value).unwrap();

    assert!(!ni.clean_shutdown);
    let r0 = ni
        .children
        .iter()
        .find(|c| c.uuid == repl_0.uuid())
        .unwrap();
    assert!(!r0.healthy);

    let r1 = ni
        .children
        .iter()
        .find(|c| c.uuid == repl_1.uuid())
        .unwrap();
    assert!(r1.healthy);
}

const ETCD_ENDPOINT: &str = "http://localhost:2379";

const POOL_SIZE: u64 = 32 * 1024 * 1024;

const DISK_NAME_0: &str = "/tmp/disk1.img";
const BDEV_NAME_0: &str = "aio:///tmp/disk1.img?blk_size=512";
const POOL_NAME_0: &str = "pool_0";
const REPL_NAME_0: &str = "repl_0";
const REPL_UUID_0: &str = "65acdaac-14c4-41d8-a55e-d03bfd7185a4";

const DISK_NAME_1: &str = "/tmp/disk2.img";
const POOL_NAME_1: &str = "pool_1";
const REPL_NAME_1: &str = "repl_1";
const REPL_UUID_1: &str = "1c7152fd-d2a6-4ee7-8729-2822906d44a4";

const NEXUS_NAME: &str = "nexus_0";
const NEXUS_UUID: &str = "cdc2a7db-3ac3-403a-af80-7fadc1581c47";

#[tokio::test]
/// Test ETCD misbehaviour during a child retirement: a nexus must not ack I/Os
/// to a client if a persistent store cannot be updated while a child is being
/// retired.
///
/// [1] Create etcd, pools, replicas, and nexus.
/// [2] Inject an fault to a replica.
/// [3] Pause ETCD container.
/// [4] Write to the nexus. A replica fails due to injected fault, and I/O on
///     nexus must stuck.
/// [5] Thaw ETCD container.
/// [6] I/Os must now be acknowledged to the client.
async fn nexus_child_retire_persist_unresponsive_with_bdev_io() {
    let test = init_ms_etcd_test().await;

    // Inject a fault on the nexus.
    let nex = nexus_lookup_mut(NEXUS_NAME).unwrap();

    let inj_device = nex.child_at(0).get_device_name().unwrap();

    add_fault_injection(FaultInjection::new(
        FaultDomain::Nexus,
        &inj_device,
        FaultIoType::Write,
        FaultIoStage::Completion,
        FaultType::status_data_transfer_error(),
        Duration::ZERO,
        Duration::MAX,
        0 .. 1,
    ));

    // Pause etcd.
    test.pause("etcd").await.unwrap();
    println!("\nTest: ETCD paused\n");

    // Now, when running an I/O to the nexus, it will fail due to injected
    // fault. Child retirement procedure will kick in, and since ETCD is
    // paused, all I/Os must freeze.
    let io = get_ms().spawn(async {
        println!("Test: Writing to nexus bdev ...");
        bdev_io::write_blocks(NEXUS_NAME, 0, 1, 0xaa).await.unwrap();
        println!("\nTest: Writing to nexus bdev finished\n");
    });
    tokio::pin!(io);

    assert!(
        tokio::time::timeout(Duration::from_secs(1), &mut io)
            .await
            .is_err(),
        "I/O to nexus must freeze when ETCD is paused"
    );

    // Thaw etcd.
    test.thaw("etcd").await.unwrap();
    println!("\nTest: ETCD thawed\n");

    // Now, as ETCD is running again, I/Os must proceed.
    assert!(
        tokio::time::timeout(Duration::from_secs(1), &mut io)
            .await
            .is_ok(),
        "I/O to nexus must proceed when ETCD is thawed"
    );

    // Check that 1st child is fauled, and 2nd is open.
    assert!(matches!(
        nex.child_at(0).state(),
        ChildState::Faulted(FaultReason::IoError)
    ));
    assert!(matches!(nex.child_at(1).state(), ChildState::Open));

    // Check that the ETCD has the correct record for the nexus:
    // one child is failed and the other is healthy.
    let mut etcd = etcd_client::Client::connect([ETCD_ENDPOINT], None)
        .await
        .unwrap();
    let response = etcd.get(NEXUS_UUID, None).await.expect("No entry found");
    let value = response.kvs().first().unwrap().value();
    let ni: NexusInfo = serde_json::from_slice(value).unwrap();

    assert!(!ni.clean_shutdown);
    let r0 = ni.children.iter().find(|c| c.uuid == REPL_UUID_0).unwrap();
    assert!(!r0.healthy);

    let r1 = ni.children.iter().find(|c| c.uuid == REPL_UUID_1).unwrap();
    assert!(r1.healthy);

    deinit_ms_etcd_test().await;
}

#[tokio::test]
async fn nexus_child_retire_persist_failure_with_bdev_io() {
    let test = init_ms_etcd_test().await;

    // Inject a fault on the nexus.
    let nex = nexus_lookup_mut(NEXUS_NAME).unwrap();

    let inj_device = nex.child_at(0).get_device_name().unwrap();

    add_fault_injection(FaultInjection::new(
        FaultDomain::Nexus,
        &inj_device,
        FaultIoType::Write,
        FaultIoStage::Completion,
        FaultType::status_data_transfer_error(),
        Duration::ZERO,
        Duration::MAX,
        0 .. 1,
    ));

    // Pause etcd.
    test.pause("etcd").await.unwrap();
    println!("\nTest: ETCD paused\n");

    // 1) Now, when running an I/O to the nexus, it will fail due to injected
    // fault.
    // 2) Child retirement procedure will kick in.
    // 3) Since ETCD is paused, all I/Os must freeze at first.
    // 4) After a while, ETCD operation must time out, and frozen I/Os to the
    // nexus must fail.
    let io = get_ms().spawn(async {
        println!("Test: Writing to nexus bdev ...");
        let res = bdev_io::write_blocks(NEXUS_NAME, 0, 1, 0xaa).await;
        println!("\nTest: Writing to nexus bdev finished\n");

        assert!(matches!(
            res,
            Err(CoreError::WriteFailed {
                status: IoCompletionStatus::NvmeError(NvmeStatus::Generic(
                    GenericStatusCode::InternalDeviceError
                )),
                ..
            })
        ));
    });
    tokio::pin!(io);

    assert!(
        tokio::time::timeout(Duration::from_secs(3), &mut io)
            .await
            .is_err(),
        "I/O to nexus fail when ETCD is timed out"
    );

    tokio::time::timeout(Duration::from_secs(10), &mut io)
        .await
        .expect("All I/Os must complete");

    // Wait until the nexus goes shutdown.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if nex.status() == NexusStatus::Shutdown {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("Nexus must shutdown");

    // Check that the 1st child is fauled, and 2nd is closed.
    assert!(matches!(
        nex.child_at(0).state(),
        ChildState::Faulted(FaultReason::IoError)
    ));
    assert!(matches!(nex.child_at(1).state(), ChildState::Closed));

    deinit_ms_etcd_test().await;
}

async fn init_ms_etcd_test() -> ComposeTest {
    common::composer_init();

    // Create a container with ETCD.
    let test = Builder::new()
        .name("io-race")
        .add_container_spec(
            common::compose::ContainerSpec::from_binary(
                "etcd",
                Binary::from_path(env!("ETCD_BIN")).with_args(vec![
                    "--data-dir",
                    "/tmp/etcd-data",
                    "--advertise-client-urls",
                    "http://0.0.0.0:2379",
                    "--listen-client-urls",
                    "http://0.0.0.0:2379",
                ]),
            )
            .with_portmap("2379", "2379")
            .with_portmap("2380", "2380"),
        )
        .with_logs(false)
        .build()
        .await
        .unwrap();

    PersistentStoreBuilder::new()
        .with_endpoint(ETCD_ENDPOINT)
        .with_timeout(Duration::from_secs(1))
        .with_retries(5)
        .connect()
        .await;

    // Create test backing store.
    common::delete_file(&[DISK_NAME_0.into(), DISK_NAME_1.into()]);
    common::truncate_file(DISK_NAME_0, 44 * 1024);
    common::truncate_file(DISK_NAME_1, 44 * 1024);

    // Create pools, replicas and nexus.
    get_ms()
        .spawn(async move {
            // Pool #0 and replica #0.
            let pool_0 = Lvs::create_or_import(PoolArgs {
                name: POOL_NAME_0.to_string(),
                disks: vec![BDEV_NAME_0.to_string()],
                uuid: None,
            })
            .await
            .unwrap();

            // Pool #1 and replica #1.
            pool_0
                .create_lvol(REPL_NAME_0, POOL_SIZE, Some(REPL_UUID_0), false)
                .await
                .unwrap();

            let pool_1 = Lvs::create_or_import(PoolArgs {
                name: POOL_NAME_1.to_string(),
                disks: vec![DISK_NAME_1.to_string()],
                uuid: None,
            })
            .await
            .unwrap();

            pool_1
                .create_lvol(REPL_NAME_1, POOL_SIZE, Some(REPL_UUID_1), false)
                .await
                .unwrap();

            // Create a nexus with 2 children.
            nexus_create(
                NEXUS_NAME,
                POOL_SIZE,
                Some(NEXUS_UUID),
                &[
                    format!("loopback:///{REPL_NAME_0}?uuid={REPL_UUID_0}"),
                    format!("loopback:///{REPL_NAME_1}?uuid={REPL_UUID_1}"),
                ],
            )
            .await
            .unwrap();

            nexus_lookup_mut(NEXUS_NAME)
                .unwrap()
                .share(Protocol::Nvmf, None)
                .await
                .unwrap();

            reactor_poll!(600);
        })
        .await;

    test
}

async fn deinit_ms_etcd_test() {
    get_ms()
        .spawn(async {
            nexus_lookup_mut(NEXUS_NAME)
                .unwrap()
                .destroy()
                .await
                .unwrap();
            Lvs::lookup(POOL_NAME_0).unwrap().destroy().await.unwrap();
            Lvs::lookup(POOL_NAME_1).unwrap().destroy().await.unwrap();
        })
        .await;
}
