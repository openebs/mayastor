#![cfg(feature = "fault-injection")]

pub mod common;

use std::{sync::Arc, time::Duration};
use tokio::sync::{
    oneshot,
    oneshot::{Receiver, Sender},
};

use common::{
    compose::{
        rpc::v1::{
            nexus::{NexusNvmePreemption, NvmeReservation},
            GrpcConnect,
            SharedRpcHandle,
        },
        Binary,
        Builder,
    },
    file_io::DataSize,
    fio::{spawn_fio_task, FioBuilder, FioJobBuilder, FioJobResult},
    nexus::NexusBuilder,
    nvme::{find_mayastor_nvme_device_path, NmveConnectGuard},
    nvmf::NvmfLocation,
    pool::PoolBuilder,
    replica::ReplicaBuilder,
    test::{add_fault_injection, remove_fault_injection},
};

use io_engine::core::fault_injection::{
    FaultDomain,
    FaultIoOperation,
    InjectionBuilder,
};

const POOL_SIZE: u64 = 500;
const REPL_SIZE: u64 = 450;
const REPL_UUID: &str = "65acdaac-14c4-41d8-a55e-d03bfd7185a4";
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

    let inj_w = InjectionBuilder::default()
        .with_device_name(dev_name.clone())
        .with_domain(FaultDomain::NexusChild)
        .with_io_operation(FaultIoOperation::Write)
        .build_uri()
        .unwrap();

    let inj_r = InjectionBuilder::default()
        .with_device_name(dev_name.clone())
        .with_domain(FaultDomain::NexusChild)
        .with_io_operation(FaultIoOperation::Read)
        .build_uri()
        .unwrap();

    let cfg = NexusManageTask {
        ms_nex: ms_nex.clone(),
        nex_0: nex_0.clone(),
        repl_0: repl_0.clone(),
        inj_w,
        inj_r,
    };

    // Run two tasks in parallel, I/O and nexus management.
    let (s, r) = oneshot::channel();

    let j0 = tokio::spawn({
        let nvmf = nex_0.nvmf_location();
        async move { run_io_task(s, &nvmf, 10, 20).await }
    });
    tokio::pin!(j0);

    let j1 = tokio::spawn({
        let cfg = cfg.clone();
        async move {
            run_nexus_manage_task(r, cfg).await;
        }
    });
    tokio::pin!(j1);

    let (io_res, _) = tokio::join!(j0, j1);
    io_res.unwrap()
}

#[derive(Clone)]
struct NexusManageTask {
    ms_nex: SharedRpcHandle,
    nex_0: NexusBuilder,
    repl_0: ReplicaBuilder,
    inj_w: String,
    inj_r: String,
}

/// Runs multiple FIO I/O jobs.
async fn run_io_task(
    s: Sender<()>,
    nvmf: &NvmfLocation,
    cnt: u32,
    rt: u32,
) -> std::io::Result<()> {
    let _cg = NmveConnectGuard::connect_addr(&nvmf.addr, &nvmf.nqn);
    let path = find_mayastor_nvme_device_path(&nvmf.serial)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    let jobs = (0 .. cnt).map(|_| {
        FioJobBuilder::new()
            .with_direct(true)
            .with_ioengine("libaio")
            .with_iodepth(128)
            .with_filename(&path)
            .with_runtime(rt)
            .with_rw("randwrite")
            .build()
    });

    let fio = FioBuilder::new().with_jobs(jobs).build();

    // Notify the nexus management task that connection is complete and I/O
    // starts.
    s.send(()).unwrap();

    // Start FIO.
    spawn_fio_task(&fio).await
}

/// Manages the nexus in parallel to I/O task.
/// [1] Nexus is failed by injecting a fault.
/// [2] I/O running in parallel should freeze or fail, depending on how target's
/// configured.
/// [3] Nexus is recreated.
async fn run_nexus_manage_task(r: Receiver<()>, cfg: NexusManageTask) {
    let NexusManageTask {
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

#[tokio::test]
async fn nexus_crd_resv() {
    common::composer_init();

    const HOSTID_0: &str = "53b35ce9-8e71-49a9-ab9b-cba7c5670fad";
    const HOSTID_1: &str = "c1affd2d-ef79-4ba4-b5cf-8eb48f9c07d0";
    const HOSTID_2: &str = "3f264cc3-1c95-44ca-bc1f-ed7fb68f3894";
    const PTPL_CONTAINER_DIR: &str = "/host/tmp/ptpl";
    const RESV_KEY_1: u64 = 0xabcd_ef00_1234_5678;
    const RESV_KEY_2: u64 = 0xfeed_f00d_bead_5678;

    // Set 1st, 3nd CRD to non-zero value and 2nd to zero.
    // Nexus reservation must select the second one (zero).
    const CRDT: &str = "0,15,0";
    const TOTAL_DELAY: u64 = 15 * 5 * 100;

    let ptpl_dir = |ms| format!("{PTPL_CONTAINER_DIR}/{ms}");

    let test = Builder::new()
        .name("nexus_crd_resv_test")
        .network("10.1.0.0/16")
        .unwrap()
        .add_container_bin(
            "ms_0",
            Binary::from_dbg("io-engine")
                .with_env("NEXUS_NVMF_RESV_ENABLE", "1")
                .with_env("MAYASTOR_NVMF_HOSTID", HOSTID_0)
                .with_args(vec![
                    "-l",
                    "1",
                    "-F",
                    "compact,color,host",
                    "--tgt-crdt",
                    CRDT,
                    "--ptpl-dir",
                    ptpl_dir("ms_0").as_str(),
                ])
                .with_bind("/tmp", "/host/tmp"),
        )
        .add_container_bin(
            "ms_1",
            Binary::from_dbg("io-engine")
                .with_env("NEXUS_NVMF_RESV_ENABLE", "1")
                .with_env("MAYASTOR_NVMF_HOSTID", HOSTID_1)
                .with_args(vec![
                    "-l",
                    "2",
                    "-F",
                    "compact,color,host",
                    "--tgt-crdt",
                    CRDT,
                    "--ptpl-dir",
                    ptpl_dir("ms_1").as_str(),
                ])
                .with_bind("/tmp", "/host/tmp"),
        )
        .add_container_bin(
            "ms_2",
            Binary::from_dbg("io-engine")
                .with_env("NEXUS_NVMF_RESV_ENABLE", "1")
                .with_env("MAYASTOR_NVMF_HOSTID", HOSTID_2)
                .with_args(vec![
                    "-l",
                    "3",
                    "-F",
                    "compact,color,host",
                    "--tgt-crdt",
                    CRDT,
                    "--ptpl-dir",
                    ptpl_dir("ms_2").as_str(),
                ]),
        )
        .with_clean(true)
        .build()
        .await
        .unwrap();

    let conn = GrpcConnect::new(&test);

    let ms_0 = conn.grpc_handle_shared("ms_0").await.unwrap();
    let ms_1 = conn.grpc_handle_shared("ms_1").await.unwrap();
    let ms_2 = conn.grpc_handle_shared("ms_2").await.unwrap();

    let mut pool_0 = PoolBuilder::new(ms_0.clone())
        .with_name("pool0")
        .with_new_uuid()
        .with_malloc("mem0", POOL_SIZE);

    pool_0.create().await.unwrap();

    let mut repl_0 = ReplicaBuilder::new(ms_0.clone())
        .with_pool(&pool_0)
        .with_name("r0")
        .with_uuid(REPL_UUID)
        .with_size_mb(REPL_SIZE)
        .with_thin(false);

    repl_0.create().await.unwrap();
    repl_0.share().await.unwrap();

    // Create nexus #1.
    let mut nex_1 = NexusBuilder::new(ms_1.clone())
        .with_name(NEXUS_NAME)
        .with_uuid(NEXUS_UUID)
        .with_size_mb(NEXUS_SIZE)
        .with_replica(&repl_0)
        .with_resv_key(RESV_KEY_1)
        .with_resv_type(NvmeReservation::ExclusiveAccess)
        .with_preempt_policy(NexusNvmePreemption::Holder);

    nex_1.create().await.unwrap();
    nex_1.publish().await.unwrap();

    // Create nexus #2.
    let mut nex_2 = NexusBuilder::new(ms_2.clone())
        .with_name(NEXUS_NAME)
        .with_uuid(NEXUS_UUID)
        .with_size_mb(NEXUS_SIZE)
        .with_replica(&repl_0)
        .with_resv_key(RESV_KEY_2)
        .with_resv_type(NvmeReservation::ExclusiveAccess)
        .with_preempt_policy(NexusNvmePreemption::Holder);

    nex_2.create().await.unwrap();
    nex_2.publish().await.unwrap();

    // Run I/O on the first nexus, causing SPDK_NVME_SC_RESERVATION_CONFLICT.
    // io-engine must select 2nd CRD, which is configured to be zero.
    let fio_res = {
        let (_cg, path) = nex_1.nvmf_location().open().unwrap();

        let fio = FioBuilder::new()
            .with_job(
                FioJobBuilder::new()
                    .with_name("j0")
                    .with_filename(path)
                    .with_ioengine("libaio")
                    .with_iodepth(1)
                    .with_direct(true)
                    .with_rw("write")
                    .with_size(DataSize::from_kb(4))
                    .build(),
            )
            .build();

        tokio::spawn(async { fio.run() }).await.unwrap()
    };
    assert!(fio_res.total_time < Duration::from_millis(TOTAL_DELAY));

    // The required errno (EBADE) exists on Linux-like targets only. On other
    // platforms like macos, an IDE would highlight it as an error.
    #[cfg(target_os = "linux")]
    assert_eq!(
        fio_res.find_job("j0").unwrap().result,
        FioJobResult::Error(nix::errno::Errno::EBADE)
    );
}
