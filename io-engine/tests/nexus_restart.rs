pub mod common;

use io_engine_tests::{
    compose::{
        rpc::v1::{GrpcConnect, SharedRpcHandle},
        Binary,
        Builder,
        ComposeTest,
    },
    file_io::DataSize,
    fio::{spawn_fio_task, FioBuilder, FioJobBuilder},
    nexus::NexusBuilder,
    nvme::{find_mayastor_nvme_device_path, NmveConnectGuard},
    nvmf::NvmfLocation,
    pool::PoolBuilder,
    replica::ReplicaBuilder,
};
use std::{sync::Arc, time::Duration};
use tokio::sync::{
    watch::{Receiver, Sender},
    Mutex,
};

const ETCD_IP: &str = "10.1.0.2";
const ETCD_PORT: &str = "2379";
const ETCD_PORT_2: &str = "2380";

const DISK_SIZE: u64 = 12000;
const REPL_SIZE: u64 = 512;
const NEXUS_SIZE: u64 = REPL_SIZE;

const SLEEP_BEFORE: u64 = 4;
const SLEEP_DOWN: u64 = 5;
const SLEEP_ADD_CHILD: u64 = 1;

struct NodeConfig {
    disk_name: &'static str,
    bdev_name: &'static str,
    pool_name: &'static str,
    pool_uuid: &'static str,
    repl_name: &'static str,
    repl_uuid: &'static str,
}

const NODE_CNT: usize = 3;

const NODE_CONFIG: [NodeConfig; NODE_CNT] = [
    NodeConfig {
        disk_name: "/tmp/disk0.img",
        bdev_name: "aio:///tmp/disk0.img?blk_size=512",
        pool_name: "pool_0",
        pool_uuid: "40baf8b5-6256-4f29-b073-61ebf67d9b91",
        repl_name: "repl_0",
        repl_uuid: "45c23e54-dc86-45f6-b55b-e44d05f154dd",
    },
    NodeConfig {
        disk_name: "/tmp/disk1.img",
        bdev_name: "aio:///tmp/disk1.img?blk_size=512",
        pool_name: "pool_1",
        pool_uuid: "2d7f2e76-c1a8-478f-a0c7-b96eb4072075",
        repl_name: "repl_1",
        repl_uuid: "0b30d5e8-c057-463a-96f4-3591d70120f9",
    },
    NodeConfig {
        disk_name: "/tmp/disk2.img",
        bdev_name: "aio:///tmp/disk2.img?blk_size=512",
        pool_name: "pool_2",
        pool_uuid: "6f9fd854-87e8-4a8a-8a83-92faa6244eea",
        repl_name: "repl_2",
        repl_uuid: "88cf669f-34dc-4390-9ad8-24f996f372b5",
    },
];

const NEXUS_NAME: &str = "nexus_0";
const NEXUS_UUID: &str = "d22796b7-332b-4b43-b929-079744d3ddab";

/// TODO
#[allow(dead_code)]
struct StorageNode {
    pool: PoolBuilder,
    repl: ReplicaBuilder,
}

/// Test cluster.
#[allow(dead_code)]
struct TestCluster {
    test: Box<ComposeTest>,
    etcd_endpoint: String,
    etcd: etcd_client::Client,
    nodes: Vec<StorageNode>,
}

type TestClusterRef = Arc<Mutex<TestCluster>>;

impl TestCluster {
    async fn create() -> TestClusterRef {
        // Create test backing store.
        for i in NODE_CONFIG {
            common::delete_file(&[i.disk_name.to_string()]);
            common::truncate_file_bytes(i.disk_name, DISK_SIZE * 1024 * 1024);
        }

        let etcd_endpoint = format!("http://{ETCD_IP}:{ETCD_PORT}");

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
                    .with_portmap(ETCD_PORT, ETCD_PORT)
                    .with_portmap(ETCD_PORT_2, ETCD_PORT_2),
                )
                .add_container_bin(
                    "ms_0",
                    Binary::from_dbg("io-engine")
                        .with_direct_bind(NODE_CONFIG[0].disk_name)
                        .with_args(vec!["-l", "1,2", "-Fcolor,nodate,host"]),
                )
                .add_container_bin(
                    "ms_1",
                    Binary::from_dbg("io-engine")
                        .with_direct_bind(NODE_CONFIG[1].disk_name)
                        .with_args(vec!["-l", "3,4", "-Fcolor,nodate,host"]),
                )
                .add_container_bin(
                    "ms_2",
                    Binary::from_dbg("io-engine")
                        .with_direct_bind(NODE_CONFIG[2].disk_name)
                        .with_args(vec![
                            "-Fcolor,nodate,host",
                            "-l",
                            "5,6",
                            "-p",
                            &etcd_endpoint,
                            "--enable-channel-dbg",
                        ]),
                )
                .with_clean(true)
                .with_logs(true)
                .build()
                .await
                .unwrap(),
        );

        let etcd = etcd_client::Client::connect([&etcd_endpoint], None)
            .await
            .unwrap();

        // Create pools and replicas.
        let mut nodes = Vec::new();

        for (idx, node) in NODE_CONFIG.iter().enumerate() {
            let ms = GrpcConnect::new(&test)
                .grpc_handle_shared(&format!("ms_{idx}"))
                .await
                .unwrap();

            let mut pool = PoolBuilder::new(ms.clone())
                .with_name(node.pool_name)
                .with_uuid(node.pool_uuid)
                .with_bdev(node.bdev_name);

            let mut repl = ReplicaBuilder::new(ms.clone())
                .with_pool(&pool)
                .with_name(node.repl_name)
                .with_uuid(node.repl_uuid)
                .with_thin(false)
                .with_size_mb(REPL_SIZE);

            pool.create().await.unwrap();
            repl.create().await.unwrap();
            repl.share().await.unwrap();

            nodes.push(StorageNode {
                pool,
                repl,
            });
        }

        Arc::new(Mutex::new(Self {
            test,
            etcd_endpoint,
            etcd,
            nodes,
        }))
    }

    async fn get_ms_nex(&self) -> SharedRpcHandle {
        GrpcConnect::new(&self.test)
            .grpc_handle_shared("ms_2")
            .await
            .unwrap()
    }

    async fn start_ms_nex(&mut self) {
        self.test.start("ms_2").await.unwrap();
    }

    async fn stop_ms_nex(&mut self) {
        self.test.kill("ms_2").await.unwrap();
    }

    async fn create_nexus(&mut self) -> NexusBuilder {
        let ms_nex = self.get_ms_nex().await;

        let mut nex = NexusBuilder::new(ms_nex)
            .with_name(NEXUS_NAME)
            .with_uuid(NEXUS_UUID)
            .with_size_mb(NEXUS_SIZE)
            .with_replica(&self.nodes[0].repl)
            .with_replica(&self.nodes[1].repl)
            .with_replica(&self.nodes[2].repl);

        nex.create().await.unwrap();

        nex
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
/// Tests that a nexus can be properly recreated without I/O failures
/// when its io-engine container restarts.
///
/// 1. Create a nexus with 3 replicas.
/// 2. Start I/O.
/// 3. Restart nexus's io-engine while I/O is running.
/// 4. Recreate the nexus with 1 replica.
/// 5. Add 2 remaining replicas, triggering rebuilds.
/// 6. Verify I/O.
async fn nexus_restart() {
    common::composer_init();

    let cluster = TestCluster::create().await;

    let nex = cluster.lock().await.create_nexus().await;
    nex.publish().await.unwrap();

    // Run tasks in parallel, I/O and nexus management.
    let (s, r) = tokio::sync::watch::channel(());

    let j0 = tokio::spawn({
        let nvmf = nex.nvmf_location();
        async move { run_io_task(s, &nvmf).await }
    });
    tokio::pin!(j0);

    let j1 = tokio::spawn({
        let r = r.clone();
        let c = cluster.clone();
        async move {
            run_manage_task(r, c).await;
        }
    });
    tokio::pin!(j1);

    let (r0, r1) = tokio::join!(j0, j1);
    r0.unwrap();
    r1.unwrap();
}

/// Runs multiple FIO I/O jobs.
async fn run_io_task(s: Sender<()>, nvmf: &NvmfLocation) {
    println!("[I/O task] connecting ...");
    let _cg = NmveConnectGuard::connect_addr(&nvmf.addr, &nvmf.nqn);
    println!("[I/O task] connection okay");

    let path = find_mayastor_nvme_device_path(&nvmf.serial)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    // Notify the nexus management task that connection is complete and I/O
    // starts.
    s.send(()).unwrap();

    let fio_write = FioBuilder::new()
        .with_job(
            FioJobBuilder::default()
                .with_filename(path.clone())
                .with_name("benchtest")
                .with_numjobs(1)
                .with_direct(true)
                .with_rw("randwrite")
                .with_do_verify(false)
                .with_ioengine("libaio")
                .with_bs(DataSize::from_kb(4))
                .with_iodepth(16)
                .with_verify("crc32")
                .build(),
        )
        .with_verbose(true)
        .with_verbose_err(true)
        .build();

    println!("[I/O task] Starting write FIO");
    spawn_fio_task(&fio_write).await.unwrap();
    println!("[I/O task] Write FIO finished");

    let fio_verify = FioBuilder::new()
        .with_job(
            FioJobBuilder::new()
                .with_filename(path.clone())
                .with_name("benchtest")
                .with_numjobs(1)
                .with_direct(true)
                .with_rw("randread")
                .with_ioengine("libaio")
                .with_bs(DataSize::from_kb(4))
                .with_iodepth(16)
                .with_verify("crc32")
                .with_verify_fatal(true)
                .with_verify_async(2)
                .build(),
        )
        .with_verbose(true)
        .with_verbose_err(true)
        .build();

    println!("[I/O task] Starting verify FIO");
    spawn_fio_task(&fio_verify).await.unwrap();
    println!("[I/O task] Verify FIO finished");
}

async fn run_manage_task(mut r: Receiver<()>, cluster: TestClusterRef) {
    // Wait until I/O task connects and signals it is ready.
    r.changed().await.unwrap();
    println!("[Manage task] I/O started");

    tokio::time::sleep(Duration::from_secs(SLEEP_BEFORE)).await;

    // Restart io-engine container.
    println!("[Manage task] Restarting io-engine container");
    cluster.lock().await.stop_ms_nex().await;
    tokio::time::sleep(Duration::from_secs(SLEEP_DOWN)).await;
    cluster.lock().await.start_ms_nex().await;

    let ms_nex = cluster.lock().await.get_ms_nex().await;

    // Recreate the nexus with 1 child.
    let mut nex = NexusBuilder::new(ms_nex.clone())
        .with_name(NEXUS_NAME)
        .with_uuid(NEXUS_UUID)
        .with_size_mb(NEXUS_SIZE)
        .with_replica(&cluster.lock().await.nodes[1].repl);

    println!("[Manage task] Recreating the nexus (1 child)");
    nex.create().await.unwrap();

    println!("[Manage task] Adding 2nd child");
    cluster.lock().await.nodes[2].pool.create().await.unwrap();
    nex.add_replica(&cluster.lock().await.nodes[2].repl, false)
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(SLEEP_ADD_CHILD)).await;

    println!("[Manage task] Adding 3nd child");
    nex.add_replica(&cluster.lock().await.nodes[0].repl, false)
        .await
        .unwrap();

    println!("[Manage task] Publishing the nexus");
    nex.publish().await.unwrap();
    println!("[Manage task] Nexus published");
}
