pub mod common;
use common::{
    compose::{
        rpc::v1::{stats::*, GrpcConnect},
        Binary,
        Builder,
    },
    fio::{FioBuilder, FioJobBuilder},
    nexus::{test_fio_to_nexus, NexusBuilder},
    pool::PoolBuilder,
    replica::ReplicaBuilder,
};

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
            Binary::from_dbg("io-engine").with_args(vec!["-l", "3"]),
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

    // Nexus node
    let mut pool_nex = PoolBuilder::new(ms_nex.clone())
        .with_name("pool2")
        .with_new_uuid()
        .with_malloc("mem0", POOL_SIZE);

    let mut repl_nex = ReplicaBuilder::new(ms_nex.clone())
        .with_pool(&pool_nex)
        .with_name("rn")
        .with_new_uuid()
        .with_thin(false)
        .with_size_mb(REPL_SIZE);

    pool_nex.create().await.unwrap();
    repl_nex.create().await.unwrap();
    repl_nex.share().await.unwrap();

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

    let mut nex_0 = NexusBuilder::new(ms_nex.clone())
        .with_name("nexus0")
        .with_new_uuid()
        .with_size_mb(REPL_SIZE)
        .with_replica(&repl_0)
        .with_replica(&repl_1)
        .with_replica(&repl_nex);

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

    let repl_0_stat = ms_0
        .lock()
        .await
        .stats
        .get_replica_io_stats(ListStatsOption {
            name: None,
        })
        .await
        .unwrap();

    let repl_1_stat = ms_1
        .lock()
        .await
        .stats
        .get_replica_io_stats(ListStatsOption {
            name: None,
        })
        .await
        .unwrap();

    let repl_nex_stat = ms_nex
        .lock()
        .await
        .stats
        .get_replica_io_stats(ListStatsOption {
            name: None,
        })
        .await
        .unwrap();

    let nexus_stat: &Vec<IoStats> = &nexus_stat.get_ref().stats;
    let pool_0_stat: &Vec<IoStats> = &pool_0_stat.get_ref().stats;
    let pool_1_stat: &Vec<IoStats> = &pool_1_stat.get_ref().stats;
    let repl_0_stat: &Vec<ReplicaIoStats> = &repl_0_stat.get_ref().stats;
    let repl_1_stat: &Vec<ReplicaIoStats> = &repl_1_stat.get_ref().stats;
    let repl_nex_stat: &Vec<ReplicaIoStats> = &repl_nex_stat.get_ref().stats;
    let nexus_stat = nexus_stat.get(0).unwrap();
    let pool_0_stat = pool_0_stat.get(0).unwrap();
    let pool_1_stat = pool_1_stat.get(0).unwrap();
    let repl_0_stat = repl_0_stat.get(0).unwrap();
    let repl_0_stat = repl_0_stat.stats.clone().unwrap();
    let repl_1_stat = repl_1_stat.get(0).unwrap();
    let repl_1_stat = repl_1_stat.stats.clone().unwrap();
    let repl_nex_stat = repl_nex_stat.get(0).unwrap();
    let repl_nex_stat = repl_nex_stat.stats.clone().unwrap();

    // Validate num_read/write_ops reset across resource.
    assert_eq!(repl_0_stat.num_read_ops, 0);
    assert_eq!(repl_1_stat.num_read_ops, 0);
    assert_eq!(repl_0_stat.num_write_ops, 0);
    assert_eq!(repl_1_stat.num_write_ops, 0);
    assert_eq!(repl_nex_stat.num_write_ops, 0);
    assert_eq!(repl_nex_stat.num_read_ops, 0);
    assert_eq!(nexus_stat.num_read_ops, 0);
    assert_eq!(nexus_stat.num_write_ops, 0);
    assert_eq!(pool_0_stat.num_write_ops, 0);
    assert_eq!(pool_1_stat.num_write_ops, 0);

    test_fio_to_nexus(
        &nex_0,
        FioBuilder::new()
            .with_job(
                FioJobBuilder::new()
                    .with_runtime(10)
                    .with_bs(4096)
                    .with_iodepth(8)
                    .with_rw("randrw")
                    .with_direct(true)
                    .build(),
            )
            .with_verbose_err(true)
            .build(),
    )
    .await
    .unwrap();

    // Validate stats after Fio run.
    let pool_0_stat = ms_0
        .lock()
        .await
        .stats
        .get_pool_io_stats(ListStatsOption {
            name: None,
        })
        .await
        .unwrap();

    let repl_0_stat = ms_0
        .lock()
        .await
        .stats
        .get_replica_io_stats(ListStatsOption {
            name: None,
        })
        .await
        .unwrap();

    let repl_1_stat = ms_1
        .lock()
        .await
        .stats
        .get_replica_io_stats(ListStatsOption {
            name: None,
        })
        .await
        .unwrap();

    let repl_nex_stat = ms_nex
        .lock()
        .await
        .stats
        .get_replica_io_stats(ListStatsOption {
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

    let pool_nex_stat = ms_nex
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
    let pool_nex_stat: &Vec<IoStats> = &pool_nex_stat.get_ref().stats;
    let repl_0_stat: &Vec<ReplicaIoStats> = &repl_0_stat.get_ref().stats;
    let repl_1_stat: &Vec<ReplicaIoStats> = &repl_1_stat.get_ref().stats;
    let repl_nex_stat: &Vec<ReplicaIoStats> = &repl_nex_stat.get_ref().stats;
    let nexus_stat = nexus_stat.get(0).unwrap();
    let pool_0_stat = pool_0_stat.get(0).unwrap();
    let pool_1_stat = pool_1_stat.get(0).unwrap();
    let pool_nex_stat = pool_nex_stat.get(0).unwrap();
    let repl_0_stat = repl_0_stat.get(0).unwrap();
    let repl_0_stat = repl_0_stat.stats.clone().unwrap();
    let repl_1_stat = repl_1_stat.get(0).unwrap();
    let repl_1_stat = repl_1_stat.stats.clone().unwrap();
    let repl_nex_stat = repl_nex_stat.get(0).unwrap();
    let repl_nex_stat = repl_nex_stat.stats.clone().unwrap();

    // Validate non zero num_write_ops.
    assert_ne!(nexus_stat.num_write_ops, 0);
    assert_ne!(pool_0_stat.num_write_ops, 0);
    assert_ne!(pool_1_stat.num_write_ops, 0);
    assert_ne!(repl_0_stat.num_write_ops, 0);
    assert_ne!(repl_1_stat.num_write_ops, 0);
    assert_ne!(repl_nex_stat.num_write_ops, 0);

    // Validate num_read_ops of nexus and replica.
    let replica_num_read_ops = repl_0_stat.num_read_ops
        + repl_1_stat.num_read_ops
        + repl_nex_stat.num_read_ops;
    assert_eq!(nexus_stat.num_read_ops, replica_num_read_ops);

    // Validate num_read_ops of pool and replica.
    assert_eq!(repl_0_stat.num_read_ops, pool_0_stat.num_read_ops);
    assert_eq!(repl_1_stat.num_read_ops, pool_1_stat.num_read_ops);
    assert_eq!(repl_nex_stat.num_read_ops, pool_nex_stat.num_read_ops);

    // Validate num_write_ops of replica and pool.
    assert_eq!(repl_0_stat.num_write_ops, pool_0_stat.num_write_ops);
    assert_eq!(repl_1_stat.num_write_ops, pool_1_stat.num_write_ops);
    assert_eq!(repl_nex_stat.num_write_ops, pool_nex_stat.num_write_ops);

    // Validate num_write_ops of nexus and replica.
    assert_eq!(nexus_stat.num_write_ops, repl_0_stat.num_write_ops);

    // Validate num_write_ops of replica_0 and replica_1.
    assert_eq!(repl_0_stat.num_write_ops, repl_1_stat.num_write_ops);

    // Validate num_write_ops of pool_0, pool_nex and pool_1.
    assert_eq!(pool_0_stat.num_write_ops, pool_1_stat.num_write_ops);
    assert_eq!(pool_1_stat.num_write_ops, pool_nex_stat.num_write_ops);

    // Validate write_latency_ticks of replica and pool
    assert!(
        pool_0_stat.write_latency_ticks <= repl_0_stat.write_latency_ticks,
        "replica wr latency less then pool wr latency"
    );
    assert!(
        pool_1_stat.write_latency_ticks <= repl_1_stat.write_latency_ticks,
        "replica wr latency less then pool wr latency"
    );
    assert!(
        pool_nex_stat.write_latency_ticks <= repl_nex_stat.write_latency_ticks,
        "replica wr latency less then pool wr latency"
    );

    // Validate read_latency_ticks of replica and pool
    assert!(
        pool_0_stat.read_latency_ticks <= repl_0_stat.read_latency_ticks,
        "replica read latency less then pool read latency"
    );
    assert!(
        pool_1_stat.read_latency_ticks <= repl_1_stat.read_latency_ticks,
        "replica read latency less then pool read latency"
    );
    assert!(
        pool_nex_stat.read_latency_ticks <= repl_nex_stat.read_latency_ticks,
        "replica read latency less then pool read latency"
    );

    // Validate read_latency_ticks of nexus and replica
    let replica_read_latency = repl_0_stat.read_latency_ticks
        + repl_1_stat.read_latency_ticks
        + repl_nex_stat.read_latency_ticks;
    assert!(
        replica_read_latency <= nexus_stat.read_latency_ticks,
        "nexus read latency less then replica read latency"
    );
}
