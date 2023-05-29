pub mod common;

use common::{
    compose::{rpc::v1::GrpcConnect, Binary, Builder},
    file_io::DataSize,
    fio::{Fio, FioJob},
    nexus::{test_fio_to_nexus, NexusBuilder},
    pool::PoolBuilder,
    replica::ReplicaBuilder,
};

const POOL_SIZE: u64 = 80;

/// Size of thick filler replica.
const THICK_REPL_SIZE: u64 = 60;

/// Overcomitted thin replica size.
const THIN_REPL_SIZE_OVER: u64 = POOL_SIZE - THICK_REPL_SIZE + 20;

/// Nexus size.
const NEXUS_SIZE: u64 = THIN_REPL_SIZE_OVER;

/// Data size okay to write to nexus.
const DATA_SIZE_OK: u64 = POOL_SIZE - THICK_REPL_SIZE - 10;

/// Data size that too much to write to the overcomitted replica.
const DATA_SIZE_OVER: u64 = NEXUS_SIZE - 5;

/// Create a nexus on single overcomitted replica and run FIO to trigger
/// I/O error (because of ENOSPC).
#[tokio::test]
async fn nexus_fio_single_remote() {
    common::composer_init();

    let test = Builder::new()
        .name("cargo-test")
        .network("10.1.0.0/16")
        .unwrap()
        .add_container_bin(
            "ms_0",
            Binary::from_dbg("io-engine").with_args(vec!["-l", "1,2"]),
        )
        .add_container_bin(
            "ms_nex",
            Binary::from_dbg("io-engine").with_args(vec![
                "-l",
                "3,4",
                "-F",
                "compact,color",
            ]),
        )
        .with_clean(true)
        .build()
        .await
        .unwrap();

    let conn = GrpcConnect::new(&test);

    let ms_0 = conn.grpc_handle_shared("ms_0").await.unwrap();
    let ms_nex = conn.grpc_handle_shared("ms_nex").await.unwrap();

    // Node #0
    let mut pool_0 = PoolBuilder::new(ms_0.clone())
        .with_name("pool0")
        .with_new_uuid()
        .with_malloc("mem0", POOL_SIZE);

    let mut thick_0 = ReplicaBuilder::new(ms_0.clone())
        .with_pool(&pool_0)
        .with_name("f0")
        .with_new_uuid()
        .with_size_mb(THICK_REPL_SIZE)
        .with_thin(false);

    let mut repl_0 = ReplicaBuilder::new(ms_0.clone())
        .with_pool(&pool_0)
        .with_name("r0")
        .with_new_uuid()
        .with_thin(true)
        .with_size_mb(THIN_REPL_SIZE_OVER);

    pool_0.create().await.unwrap();
    thick_0.create().await.unwrap();
    repl_0.create().await.unwrap();
    repl_0.share().await.unwrap();

    // Nexus
    let mut nex_0 = NexusBuilder::new(ms_nex.clone())
        .with_name("nexus0")
        .with_new_uuid()
        .with_size_mb(NEXUS_SIZE)
        .with_replica(&repl_0);

    nex_0.create().await.unwrap();
    nex_0.publish().await.unwrap();

    // Run FIO with okay data size.
    test_fio_to_nexus(
        &nex_0,
        Fio::new()
            .with_job(
                FioJob::new()
                    .with_runtime(10)
                    .with_bs(4096)
                    .with_iodepth(8)
                    .with_size(DataSize::from_mb(DATA_SIZE_OK)),
            )
            .with_verbose_err(true),
    )
    .await
    .unwrap();

    // Run FIO with data size exceeding pool capacity.
    let err = test_fio_to_nexus(
        &nex_0,
        Fio::new().with_job(
            FioJob::new()
                .with_runtime(10)
                .with_bs(4096)
                .with_iodepth(8)
                .with_size(DataSize::from_mb(DATA_SIZE_OVER)),
        ),
    )
    .await
    .unwrap_err();

    assert_eq!(err.kind(), std::io::ErrorKind::Other);
    assert!(err.to_string().contains("SPDK FIO error:"));
}

/// Create a nexus on two replicas: okay one and overcomitted one, and run FIO.
/// FIO must succeed as one replica is not overcommited.
#[tokio::test]
async fn nexus_fio_mixed() {
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
                "-F",
                "compact,color",
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

    let mut thick_0 = ReplicaBuilder::new(ms_0.clone())
        .with_pool(&pool_0)
        .with_name("f0")
        .with_new_uuid()
        .with_size_mb(THICK_REPL_SIZE)
        .with_thin(false);

    let mut repl_0 = ReplicaBuilder::new(ms_0.clone())
        .with_pool(&pool_0)
        .with_name("r0")
        .with_new_uuid()
        .with_thin(true)
        .with_size_mb(THIN_REPL_SIZE_OVER);

    pool_0.create().await.unwrap();
    thick_0.create().await.unwrap();
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
        .with_thin(true)
        .with_size_mb(THIN_REPL_SIZE_OVER);

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

    // Run FIO with okay data size.
    test_fio_to_nexus(
        &nex_0,
        Fio::new()
            .with_job(
                FioJob::new()
                    .with_runtime(10)
                    .with_bs(4096)
                    .with_iodepth(8)
                    .with_size(DataSize::from_mb(DATA_SIZE_OK)),
            )
            .with_verbose_err(true),
    )
    .await
    .unwrap();

    // Run FIO with data size exceeding capacity of one of the pools.
    // The other replica must be fine as it is not over committed, so
    // this run must succeed.
    test_fio_to_nexus(
        &nex_0,
        Fio::new().with_job(
            FioJob::new()
                .with_runtime(10)
                .with_bs(4096)
                .with_iodepth(8)
                .with_size(DataSize::from_mb(DATA_SIZE_OVER)),
        ),
    )
    .await
    .unwrap();
}
