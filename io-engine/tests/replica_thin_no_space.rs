#![cfg(feature = "fault-injection")]

pub mod common;

use nix::errno::Errno;

use common::{
    compose::{rpc::v1::GrpcConnect, Binary, Builder},
    fio::{FioBuilder, FioJobBuilder, FioJobResult},
    pool::PoolBuilder,
    replica::ReplicaBuilder,
    test::add_fault_injection,
};

use io_engine::core::fault_injection::{
    FaultDomain,
    FaultIoStage,
    InjectionBuilder,
};

use spdk_rs::NvmeStatus;

#[tokio::test]
async fn replica_thin_nospc() {
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

    let mut repl_1 = ReplicaBuilder::new(rpc.clone())
        .with_pool(&pool_0)
        .with_name("r1")
        .with_new_uuid()
        .with_size_mb(80)
        .with_thin(false);

    repl_1.create().await.unwrap();

    let nvmf = repl_0.nvmf_location();
    let (_nvmf_conn, path) = nvmf.open().unwrap();

    let fio = FioBuilder::new()
        .with_job(
            FioJobBuilder::new()
                .with_name("j-0")
                .with_direct(true)
                .with_ioengine("libaio")
                .with_iodepth(1)
                .with_filename(&path)
                .with_rw("write")
                .build(),
        )
        .build();

    let res = tokio::spawn(async move { fio.run() }).await.unwrap();

    assert!(matches!(
        res.find_job("j-0").unwrap().result,
        FioJobResult::Error(Errno::ENOSPC)
    ));
}

#[tokio::test]
async fn replica_nospc_inject() {
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
        .with_io_stage(FaultIoStage::Submission)
        .with_method_nvme_error(NvmeStatus::NO_SPACE)
        .build_uri()
        .unwrap();

    add_fault_injection(rpc.clone(), &inj_uri).await.unwrap();

    let nvmf = repl_0.nvmf_location();
    let (_nvmf_conn, path) = nvmf.open().unwrap();

    let fio = FioBuilder::new()
        .with_job(
            FioJobBuilder::new()
                .with_name("j-0")
                .with_direct(true)
                .with_ioengine("libaio")
                .with_iodepth(1)
                .with_filename(&path)
                .with_rw("write")
                .build(),
        )
        .build();

    let res = tokio::spawn(async move { fio.run() }).await.unwrap();

    assert!(matches!(
        res.find_job("j-0").unwrap().result,
        FioJobResult::Error(Errno::ENOSPC)
    ));
}
