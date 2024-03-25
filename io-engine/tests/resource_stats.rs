pub mod common;
use common::{
    compose::{
        rpc::v1::{stats::*, GrpcConnect},
        Binary,
        Builder,
    },
    file_io::DataSize,
    fio::{FioBuilder, FioJobBuilder},
    nexus::{test_fio_to_nexus, NexusBuilder},
    pool::PoolBuilder,
    replica::ReplicaBuilder,
};
use log::info;

const POOL_SIZE: u64 = 80;

/// Size of thick filler replica.
const REPL_SIZE: u64 = 60;

#[tokio::test]
async fn test_resource_stats() {
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

    let mut repl_0 = ReplicaBuilder::new(ms_0.clone())
        .with_pool(&pool_0)
        .with_name("r0")
        .with_new_uuid()
        .with_thin(true)
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
        .with_thin(true)
        .with_size_mb(REPL_SIZE);

    pool_1.create().await.unwrap();
    repl_1.create().await.unwrap();
    repl_1.share().await.unwrap();

    // Nexus
    let mut nex_0 = NexusBuilder::new(ms_nex.clone())
        .with_name("nexus0")
        .with_new_uuid()
        .with_size_mb(REPL_SIZE)
        .with_replica(&repl_0)
        .with_replica(&repl_1);

    nex_0.create().await.unwrap();
    nex_0.publish().await.unwrap();

    ms_nex.lock().await.stats.reset_io_stats(()).await.unwrap();
    ms_0.lock().await.stats.reset_io_stats(()).await.unwrap();
    ms_1.lock().await.stats.reset_io_stats(()).await.unwrap();

    let pool_0_stat = ms_0
        .lock()
        .await
        .stats
        .get_pool_io_stats(ListStatsOption {
            name: None,
        })
        .await
        .unwrap();

    let pool_1_stat = ms_1
        .lock()
        .await
        .stats
        .get_pool_io_stats(ListStatsOption {
            name: None,
        })
        .await
        .unwrap();

    let nexus_stat = ms_nex
        .lock()
        .await
        .stats
        .get_nexus_io_stats(ListStatsOption {
            name: None,
        })
        .await
        .unwrap();

    let nexus_stat: &Vec<IoStats> = &nexus_stat.get_ref().stats;
    let pool_0_stat: &Vec<IoStats> = &pool_0_stat.get_ref().stats;
    let pool_1_stat: &Vec<IoStats> = &pool_1_stat.get_ref().stats;
    let n_stat = nexus_stat.get(0).unwrap();
    let pool_0_stat = pool_0_stat.get(0).unwrap();
    let pool_1_stat = pool_1_stat.get(0).unwrap();
    assert_eq!(n_stat.num_read_ops, 0);
    assert_eq!(pool_0_stat.num_write_ops, 0);
    assert_eq!(pool_1_stat.num_write_ops, 0);
    info!("Stat reset done!");
    // Run FIO with okay data size.
    test_fio_to_nexus(
        &nex_0,
        FioBuilder::new()
            .with_job(
                FioJobBuilder::new()
                    .with_runtime(10)
                    .with_bs(4096)
                    .with_iodepth(8)
                    .with_size(DataSize::from_mb(REPL_SIZE))
                    .build(),
            )
            .with_verbose_err(true)
            .build(),
    )
    .await
    .unwrap();
    info!("Fio completed!");
    // Checks stats after Fio run.
    // TODO: This is just an initial validtion to see if stats change after FIO
    // run. Add more validation later.

    let pool_0_stat = ms_0
        .lock()
        .await
        .stats
        .get_pool_io_stats(ListStatsOption {
            name: None,
        })
        .await
        .unwrap();

    let pool_1_stat = ms_1
        .lock()
        .await
        .stats
        .get_pool_io_stats(ListStatsOption {
            name: None,
        })
        .await
        .unwrap();

    let nexus_stat = ms_nex
        .lock()
        .await
        .stats
        .get_nexus_io_stats(ListStatsOption {
            name: None,
        })
        .await
        .unwrap();

    let nexus_stat: &Vec<IoStats> = &nexus_stat.get_ref().stats;
    let pool_0_stat: &Vec<IoStats> = &pool_0_stat.get_ref().stats;
    let pool_1_stat: &Vec<IoStats> = &pool_1_stat.get_ref().stats;
    let n_stat = nexus_stat.get(0).unwrap();
    let pool_0_stat = pool_0_stat.get(0).unwrap();
    let pool_1_stat = pool_1_stat.get(0).unwrap();
    assert_eq!(n_stat.num_read_ops, 0);
    assert_ne!(pool_0_stat.num_write_ops, 0);
    assert_ne!(pool_1_stat.num_write_ops, 0);
    info!("Stats validated!");
}
