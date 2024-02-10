#![cfg(feature = "fault-injection")]

pub mod common;

use nix::errno::Errno;
use std::time::Duration;

use common::{
    compose::{rpc::v1::GrpcConnect, Binary, Builder},
    fio::{FioBuilder, FioJobBuilder, FioJobResult},
    pool::PoolBuilder,
    replica::ReplicaBuilder,
    test::add_fault_injection,
};

use io_engine::core::fault_injection::{
    FaultDomain,
    FaultIoOperation,
    FaultIoStage,
    FaultMethod,
    InjectionBuilder,
};

// Test that the third CRD value is used for a replica target.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replica_no_fail_crd() {
    const POOL_SIZE: u64 = 100;
    const REPL_SIZE: u64 = 80;
    const REPL_NAME: &str = "r0";

    // Set 1st, 2nd CRD to non-zero value and 3rd to zero.
    // Replica must select the third one (zero).
    const CRDT: &str = "15,15,0";

    const TOTAL_DELAY: u64 = 15 * 5 * 100;

    common::composer_init();

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
                "--tgt-crdt",
                CRDT,
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
        .with_malloc("mem0", POOL_SIZE);
    pool_0.create().await.unwrap();

    let mut repl_0 = ReplicaBuilder::new(rpc.clone())
        .with_pool(&pool_0)
        .with_name(REPL_NAME)
        .with_new_uuid()
        .with_size_mb(REPL_SIZE)
        .with_thin(true);

    repl_0.create().await.unwrap();
    repl_0.share().await.unwrap();

    // Injection.
    let inj_uri = InjectionBuilder::default()
        .with_device_name(REPL_NAME.to_string())
        .with_domain(FaultDomain::BdevIo)
        .with_io_operation(FaultIoOperation::Write)
        .with_io_stage(FaultIoStage::Submission)
        .with_method(FaultMethod::DATA_TRANSFER_ERROR)
        .with_offset(1000, 1)
        .build_uri()
        .unwrap();

    add_fault_injection(rpc.clone(), &inj_uri).await.unwrap();

    let (_cg, path) = repl_0.nvmf_location().open().unwrap();

    // FIO jobs.
    let fio = FioBuilder::new()
        .with_job(
            FioJobBuilder::new()
                .with_name("job0")
                .with_direct(true)
                .with_ioengine("libaio")
                .with_iodepth(1)
                .with_filename(&path)
                .with_rw("write")
                .build(),
        )
        .build();

    let fio_res = tokio::spawn(async { fio.run() }).await.unwrap();
    let job_res = fio_res.find_job("job0").unwrap();

    assert_eq!(job_res.result, FioJobResult::Error(Errno::EIO));
    assert!(fio_res.total_time < Duration::from_millis(TOTAL_DELAY));
}
