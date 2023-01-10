pub mod common;

use common::{
    compose::{rpc::v1::GrpcConnect, Binary, Builder},
    pool::PoolBuilder,
    replica::ReplicaBuilder,
};
use io_engine_tests::{file_io::BufferSize, nvmf::test_write_to_nvmf};

#[tokio::test]
async fn replica_thin_used_space() {
    common::composer_init();

    let test = Builder::new()
        .name("cargo-test")
        .network("10.1.0.0/16")
        .unwrap()
        .add_container_bin(
            "ms",
            Binary::from_dbg("io-engine").with_args(vec!["-l", "1,2,3,4"]),
        )
        .with_clean(true)
        .build()
        .await
        .unwrap();

    let conn = GrpcConnect::new(&test);

    let ms = conn.grpc_handle_shared("ms").await.unwrap();

    const POOL_SIZE: u64 = 200;
    const REPL_SIZE: u64 = 40;

    //
    let mut pool = PoolBuilder::new(ms.clone())
        .with_name("pool0")
        .with_new_uuid()
        .with_malloc("mem0", POOL_SIZE);

    let mut repl_0 = ReplicaBuilder::new(ms.clone())
        .with_pool(&pool)
        .with_name("repl0")
        .with_new_uuid()
        .with_size_mb(REPL_SIZE)
        .with_thin(true);

    let mut repl_1 = ReplicaBuilder::new(ms.clone())
        .with_pool(&pool)
        .with_name("repl1")
        .with_new_uuid()
        .with_size_mb(REPL_SIZE)
        .with_thin(false);

    pool.create().await.unwrap();
    repl_0.create().await.unwrap();
    repl_0.share().await.unwrap();
    repl_1.create().await.unwrap();
    repl_1.share().await.unwrap();

    let p_before = pool.get_pool().await.unwrap();
    let u_0_before = repl_0.get_replica().await.unwrap().usage.unwrap();
    let u_1_before = repl_1.get_replica().await.unwrap().usage.unwrap();

    // Number of used clusters of a thin provisioned replica must be less than
    // total number of clusters.
    assert!(u_0_before.num_allocated_clusters < u_0_before.num_clusters);
    assert!(u_0_before.allocated_bytes < u_0_before.capacity_bytes);

    // Number of used cluster of a thick provisioned replica must be equal to
    // total number of clusters.
    assert_eq!(u_1_before.num_allocated_clusters, u_1_before.num_clusters);
    assert_eq!(u_1_before.allocated_bytes, u_1_before.capacity_bytes);

    test_write_to_nvmf(&repl_0.nvmf_location(), 0, 30, BufferSize::Mb(1))
        .await
        .unwrap();

    let p_after = pool.get_pool().await.unwrap();
    let u_0_after = repl_0.get_replica().await.unwrap().usage.unwrap();

    // We've copied some data, so number of used clusters must increase.
    assert!(
        u_0_before.num_allocated_clusters < u_0_after.num_allocated_clusters
    );
    assert!(p_before.used < p_after.used);

    // The replica isn't full, so number of used clusters must be less than
    // total number of clusters.
    assert!(u_0_after.num_allocated_clusters < u_0_after.num_clusters);
}
