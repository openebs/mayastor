pub mod common;

use std::{sync::Arc, time::Duration};
use tokio::sync::{
    oneshot,
    oneshot::{Receiver, Sender},
};

use common::{
    compose::{
        rpc::v1::{GrpcConnect, SharedRpcHandle},
        Binary,
        Builder,
    },
    fio::{Fio, FioJob},
    nexus::NexusBuilder,
    nvme::{find_mayastor_nvme_device_path, NmveConnectGuard},
    pool::PoolBuilder,
    replica::ReplicaBuilder,
    test::{add_fault_injection, remove_fault_injection},
};

const POOL_SIZE: u64 = 500;
const REPL_SIZE: u64 = 450;
const NEXUS_NAME: &str = "nexus_0";
const NEXUS_SIZE: u64 = REPL_SIZE;
const NEXUS_UUID: &str = "bbe6cbb6-c508-443a-877a-af5fa690c760";

/// Tests that without CRD enabled, initiator would eventually fail I/Os.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nexus_fail_no_crd() {
    test_nexus_fail("0")
        .await
        .expect_err("I/O expected to fail");
}

/// Tests that CRD properly delays I/O retries on initiator, while the target
/// has a chance to replace a failed nexus.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nexus_fail_crd() {
    test_nexus_fail("20")
        .await
        .expect("I/O expected to succeed");
}

async fn test_nexus_fail(crdt: &str) -> std::io::Result<()> {
    common::composer_init();

    let test = Builder::new()
        .name("cargo-test")
        .network("10.1.0.0/16")
        .unwrap()
        .add_container_bin(
            "ms_0",
            Binary::from_dbg("io-engine").with_args(vec!["-l", "5"]),
        )
        .add_container_bin(
            "ms_nex",
            Binary::from_dbg("io-engine").with_args(vec![
                "-l",
                "1,2,3,4",
                "--tgt-crdt",
                crdt,
            ]),
        )
        .with_clean(true)
        .build()
        .await
        .unwrap();

    let test = Arc::new(test);

    let conn = GrpcConnect::new(&test);

    let ms_0 = conn.grpc_handle_shared("ms_0").await.unwrap();
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
        .with_thin(false);

    pool_0.create().await.unwrap();
    repl_0.create().await.unwrap();
    repl_0.share().await.unwrap();

    let mut nex_0 = NexusBuilder::new(ms_nex.clone())
        .with_name(NEXUS_NAME)
        .with_uuid(NEXUS_UUID)
        .with_size_mb(NEXUS_SIZE)
        .with_replica(&repl_0);

    nex_0.create().await.unwrap();
    nex_0.publish().await.unwrap();

    let children = nex_0.get_nexus().await.unwrap().children;
    let dev_name = children[0].device_name.as_ref().unwrap();

    let inj = "domain=child&op=write&stage=compl&offset=0";
    let inj_w = format!("inject://{dev_name}?{inj}");

    let inj = "domain=child&op=read&stage=compl&offset=0";
    let inj_r = format!("inject://{dev_name}?{inj}");

    let cfg = JobCfg {
        ms_nex: ms_nex.clone(),
        nex_0: nex_0.clone(),
        repl_0: repl_0.clone(),
        inj_w: inj_w.clone(),
        inj_r: inj_r.clone(),
    };

    // Run two tasks in parallel, I/O and nexus management.
    let (s, r) = oneshot::channel();

    let j0 = tokio::spawn({
        let cfg = cfg.clone();
        async move { run_io_task(s, cfg).await }
    });
    tokio::pin!(j0);

    let j1 = tokio::spawn({
        let cfg = cfg.clone();
        async move {
            run_manage_task(r, cfg).await;
        }
    });
    tokio::pin!(j1);

    let (io_res, _) = tokio::join!(j0, j1);
    io_res.unwrap()
}

#[derive(Clone)]
struct JobCfg {
    ms_nex: SharedRpcHandle,
    nex_0: NexusBuilder,
    repl_0: ReplicaBuilder,
    inj_w: String,
    inj_r: String,
}

/// Runs multiple FIO I/O jobs.
async fn run_io_task(s: Sender<()>, cfg: JobCfg) -> std::io::Result<()> {
    let nvmf = cfg.nex_0.nvmf_location();
    let _cg = NmveConnectGuard::connect_addr(&nvmf.addr, &nvmf.nqn);
    let path = find_mayastor_nvme_device_path(&nvmf.serial)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    let jobs = (0 .. 10).map(|_| {
        FioJob::new()
            .with_direct(true)
            .with_ioengine("libaio")
            .with_iodepth(128)
            .with_filename(&path)
            .with_runtime(20)
            .with_rw("randwrite")
    });

    let fio = Fio::new().with_jobs(jobs);

    // Notify the nexus management task that connection is complete and I/O
    // starts.
    s.send(()).unwrap();

    // Start FIO.
    tokio::spawn(async move { fio.run() }).await.unwrap()
}

/// Manages the nexus in parallel to I/O task.
/// [1] Nexus is failed by injecting a fault.
/// [2] I/O running in parallel should freeze or fail, depending on how target's
/// configured.
/// [3] Nexus is recreated.
async fn run_manage_task(r: Receiver<()>, cfg: JobCfg) {
    let JobCfg {
        ms_nex,
        inj_w,
        inj_r,
        mut nex_0,
        repl_0,
        ..
    } = cfg;

    // Wait until I/O task connects and signals it is ready.
    r.await.unwrap();

    // Allow I/O to run for some time.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Inject fault, so the nexus would fail.
    add_fault_injection(ms_nex.clone(), &inj_w).await.unwrap();
    add_fault_injection(ms_nex.clone(), &inj_r).await.unwrap();

    // When nexus fails, I/O should be freezing due to CRD (if enabled).
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Destroy the nexus, remove injectios and re-create and re-publish the
    // nexus with the same ID.
    // I/O would eventually retry and the new nexus would run I/O.
    nex_0.shutdown().await.unwrap();
    nex_0.destroy().await.unwrap();

    remove_fault_injection(ms_nex.clone(), &inj_w)
        .await
        .unwrap();
    remove_fault_injection(ms_nex.clone(), &inj_r)
        .await
        .unwrap();

    let mut nex_0 = NexusBuilder::new(ms_nex.clone())
        .with_name(NEXUS_NAME)
        .with_uuid(NEXUS_UUID)
        .with_size_mb(NEXUS_SIZE)
        .with_replica(&repl_0);

    nex_0.create().await.unwrap();
    nex_0.publish().await.unwrap();
}
