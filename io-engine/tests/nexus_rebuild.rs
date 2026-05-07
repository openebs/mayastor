use std::{sync::Mutex, time::Duration};

use crossbeam::channel::unbounded;
use once_cell::sync::{Lazy, OnceCell};
use tracing::error;

use io_engine::{
    bdev::{device_create, device_destroy, device_open, nexus::nexus_lookup_mut},
    core::{MayastorCliArgs, Mthread, Protocol},
    rebuild::{BdevRebuildJob, NexusRebuildJob, RebuildState},
};

pub mod common;
use common::{
    compose::{
        rpc::v1::{nexus::ChildState, GrpcConnect},
        Binary, Builder, MayastorTest,
    },
    nexus::NexusBuilder,
    pool::PoolBuilder,
    reactor_poll,
    replica::ReplicaBuilder,
    wait_for_rebuild,
};

// each test `should` use a different nexus name to prevent clashing with
// one another. This allows the failed tests to `panic gracefully` improving
// the output log and allowing the CI to fail gracefully as well
static NEXUS_NAME: Lazy<Mutex<&str>> = Lazy::new(|| Mutex::new("Default"));
pub fn nexus_name() -> &'static str {
    &NEXUS_NAME.lock().unwrap()
}

static NEXUS_SIZE: u64 = 128 * 1024 * 1024; // 128MiB

static MAYASTOR: OnceCell<MayastorTest> = OnceCell::new();

// approximate on-disk metadata that will be written to the child by the nexus
const META_SIZE: u64 = 128 * 1024 * 1024; // 128MiB
const MAX_CHILDREN: u64 = 16;
const POOL_SIZE: u64 = 200; // 200MiB;
const REPL_SIZE: u64 = 50; // 50MiB;

fn get_ms() -> &'static MayastorTest<'static> {
    MAYASTOR.get_or_init(|| {
        MayastorTest::new(MayastorCliArgs {
            enable_io_all_thrd_nexus_channels: true,
            ..Default::default()
        })
    })
}

fn test_ini(name: &'static str) {
    *NEXUS_NAME.lock().unwrap() = name;
    get_err_bdev().clear();

    for i in 0..MAX_CHILDREN {
        common::delete_file(&[get_disk(i)]);
        common::truncate_file_bytes(&get_disk(i), NEXUS_SIZE + META_SIZE);
    }
}

fn test_fini() {
    for i in 0..MAX_CHILDREN {
        common::delete_file(&[get_disk(i)]);
    }
}
#[allow(static_mut_refs)]
fn get_err_bdev() -> &'static mut Vec<u64> {
    unsafe {
        static mut ERROR_DEVICE_INDEXES: Vec<u64> = Vec::<u64>::new();
        &mut ERROR_DEVICE_INDEXES
    }
}
fn get_disk(number: u64) -> String {
    if get_err_bdev().contains(&number) {
        format!("error_device{number}")
    } else {
        format!("/tmp/{}-disk{}.img", nexus_name(), number)
    }
}
fn get_dev(number: u64) -> String {
    if get_err_bdev().contains(&number) {
        format!("bdev:///EE_error_device{number}")
    } else {
        format!("aio://{}?blk_size=512", get_disk(number))
    }
}

async fn nexus_create(size: u64, children: u64, fill_random: bool) {
    let mut ch = Vec::new();
    for i in 0..children {
        ch.push(get_dev(i));
    }

    io_engine::bdev::nexus::nexus_create(nexus_name(), size, None, &ch)
        .await
        .unwrap();

    if fill_random {
        let device = nexus_share().await;
        let nexus_device = device.clone();
        let (s, r) = unbounded::<i32>();
        Mthread::spawn_unaffinitized(move || s.send(common::dd_urandom_blkdev(&nexus_device)));
        let dd_result: i32;
        reactor_poll!(r, dd_result);
        assert_eq!(dd_result, 0, "Failed to fill nexus with random data");

        let (s, r) = unbounded::<String>();
        Mthread::spawn_unaffinitized(move || {
            s.send(common::compare_nexus_device(&device, &get_disk(0), true))
        });
        reactor_poll!(r);
    }
}

async fn nexus_share() -> String {
    let nexus = nexus_lookup_mut(nexus_name()).unwrap();
    let device = common::device_path_from_uri(&nexus.share(Protocol::Off, None).await.unwrap());
    reactor_poll!(200);
    device
}

#[allow(deprecated)]
async fn wait_for_replica_rebuild(src_replica: &str, new_replica: &str) {
    let ms = get_ms();

    // 1. Wait for rebuild to complete.
    loop {
        let replica_name = new_replica.to_string();
        let complete = ms
            .spawn(async move {
                let nexus = nexus_lookup_mut(nexus_name()).unwrap();
                let state = nexus.rebuild_state(&replica_name);

                match state {
                    Err(_e) => true, /* Rebuild task completed and was
                                       * removed */
                    // discarded.
                    Ok(s) => s == RebuildState::Completed,
                }
            })
            .await;

        if complete {
            break;
        } else {
            tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        }
    }

    // 2. Check data integrity via MD5 checksums.
    let src_replica_name = src_replica.to_string();
    let new_replica_name = new_replica.to_string();
    ms.spawn(async move {
        let src_desc = device_open(&src_replica_name, false).unwrap();
        let dst_desc = device_open(&new_replica_name, false).unwrap();
        // Make sure devices are different.
        assert_ne!(
            src_desc.get_device().device_name(),
            dst_desc.get_device().device_name()
        );

        let src_hdl = src_desc.into_handle().unwrap();
        let dst_hdl = dst_desc.into_handle().unwrap();

        let nexus = nexus_lookup_mut(nexus_name()).unwrap();
        let mut src_buf = src_hdl.dma_malloc(nexus.size_in_bytes()).unwrap();
        let mut dst_buf = dst_hdl.dma_malloc(nexus.size_in_bytes()).unwrap();

        // Skip Mayastor partition and read only disk data at offset 10240
        // sectors.
        let data_offset: u64 = 10240 * 512;

        src_buf.fill(0);
        let mut r = src_hdl
            .read_at(data_offset, &mut src_buf)
            .await
            .expect("Failed to read source replica");
        assert_eq!(
            r,
            nexus.size_in_bytes(),
            "Amount of data read from source replica mismatches"
        );

        dst_buf.fill(0);
        r = dst_hdl
            .read_at(data_offset, &mut dst_buf)
            .await
            .expect("Failed to read new replica");
        assert_eq!(
            r,
            nexus.size_in_bytes(),
            "Amount of data read from new replica mismatches"
        );

        println!(
            "Validating new replica, {} bytes to check using MD5 checksum ...",
            nexus.size_in_bytes()
        );
        // Make sure checksums of all 2 buffers do match.
        assert_eq!(
            md5::compute(src_buf.as_slice()),
            md5::compute(dst_buf.as_slice()),
        );
    })
    .await;
}

#[tokio::test]
async fn rebuild_replica() {
    const NUM_CHILDREN: u64 = 6;

    test_ini("rebuild_replica");

    let ms = get_ms();

    ms.spawn(async move {
        nexus_create(NEXUS_SIZE, NUM_CHILDREN, true).await;
        let mut nexus = nexus_lookup_mut(nexus_name()).unwrap();
        nexus
            .as_mut()
            .add_child(&get_dev(NUM_CHILDREN), true)
            .await
            .unwrap();

        for child in 0..NUM_CHILDREN {
            NexusRebuildJob::lookup(&get_dev(child)).expect_err("Should not exist");

            NexusRebuildJob::lookup_src(&get_dev(child))
                .iter()
                .inspect(|&job| {
                    error!(
                        "Job {:?} should be associated with src child {}",
                        job, child
                    );
                })
                .any(|_| panic!("Should not have found any jobs!"));
        }

        let _ = nexus.start_rebuild(&get_dev(NUM_CHILDREN)).await;

        for child in 0..NUM_CHILDREN {
            NexusRebuildJob::lookup(&get_dev(child)).expect_err("rebuild job not created yet");
        }
        let src = NexusRebuildJob::lookup(&get_dev(NUM_CHILDREN))
            .expect("now the job should exist")
            .src_uri()
            .to_string();

        for child in 0..NUM_CHILDREN {
            if get_dev(child) != src {
                NexusRebuildJob::lookup_src(&get_dev(child))
                    .iter()
                    .filter(|s| s.dst_uri() != get_dev(child))
                    .inspect(|&job| {
                        error!(
                            "Job {:?} should be associated with src child {}",
                            job, child
                        );
                    })
                    .any(|_| panic!("Should not have found any jobs!"));
            }
        }

        assert_eq!(
            NexusRebuildJob::lookup_src(&src)
                .iter()
                .inspect(|&job| {
                    assert_eq!(job.dst_uri(), get_dev(NUM_CHILDREN));
                })
                .count(),
            1
        );

        // wait for the rebuild to start - and then pause it
        wait_for_rebuild(
            get_dev(NUM_CHILDREN),
            RebuildState::Running,
            Duration::from_secs(1),
        )
        .await;

        nexus
            .as_mut()
            .pause_rebuild(&get_dev(NUM_CHILDREN))
            .await
            .unwrap();
        assert_eq!(NexusRebuildJob::lookup_src(&src).len(), 1);

        nexus
            .as_mut()
            .add_child(&get_dev(NUM_CHILDREN + 1), true)
            .await
            .unwrap();
        let _ = nexus.start_rebuild(&get_dev(NUM_CHILDREN + 1)).await;
        assert_eq!(NexusRebuildJob::lookup_src(&src).len(), 2);
    })
    .await;

    // Wait for the replica rebuild to complete.
    wait_for_replica_rebuild(&get_dev(0), &get_dev(NUM_CHILDREN + 1)).await;

    ms.spawn(async move {
        let mut nexus = nexus_lookup_mut(nexus_name()).unwrap();

        let history = nexus.rebuild_history();
        assert!(!history.is_empty());

        nexus
            .as_mut()
            .remove_child(&get_dev(NUM_CHILDREN))
            .await
            .unwrap();
        nexus
            .remove_child(&get_dev(NUM_CHILDREN + 1))
            .await
            .unwrap();
        nexus_lookup_mut(nexus_name())
            .unwrap()
            .destroy()
            .await
            .unwrap();
        test_fini();
    })
    .await;
}

#[tokio::test]
async fn rebuild_bdev() {
    test_ini("rebuild_bdev");

    let ms = get_ms();

    ms.spawn(async move {
        let src_uri = "malloc:///d?size_mb=100";
        let dst_uri = "malloc:///d2?size_mb=100";

        device_create(src_uri).await.unwrap();
        device_create(dst_uri).await.unwrap();

        let job = BdevRebuildJob::builder()
            .build(src_uri, dst_uri)
            .await
            .unwrap();
        let chan = job.start().await.unwrap();
        let state = chan.await.unwrap();
        // todo: use completion channel with stats rather than just state?
        let stats = job.stats().await;

        device_destroy(src_uri).await.unwrap();
        device_destroy(dst_uri).await.unwrap();

        assert_eq!(state, RebuildState::Completed, "Rebuild should succeed");
        assert_eq!(stats.blocks_transferred, 100 * 1024 * 2);
    })
    .await;
}

#[tokio::test]
async fn rebuild_bdev_partial() {
    test_ini("rebuild_bdev_partial");

    let ms = get_ms();

    use io_engine::core::segment_map::SegmentMap;

    struct PartialMap(SegmentMap);
    impl PartialMap {
        fn new() -> Self {
            let size = 100 * 1024 * 1024;
            let seg_size = Self::seg_size();
            let blk_size = Self::blk_size();
            let rebuild_map = SegmentMap::new(size / blk_size, blk_size, seg_size);
            Self(rebuild_map)
        }
        fn blk_size() -> u64 {
            512
        }
        fn seg_size() -> u64 {
            64 * 1024
        }
        fn seg_blks() -> u64 {
            Self::seg_size() / Self::blk_size()
        }
        fn seg(self, seg: u64) -> Self {
            self.seg_n(seg, 1)
        }
        fn blk_n(mut self, blk: u64, cnt: u64) -> Self {
            assert!(cnt > 0, "Must set something!");
            self.0.set(blk, cnt, true);
            self
        }
        fn seg_n(self, seg: u64, cnt: u64) -> Self {
            let seg_size = Self::seg_blks();
            self.blk_n(seg * seg_size, seg_size * cnt)
        }
        fn build(self) -> SegmentMap {
            self.0
        }
    }

    ms.spawn(async move {
        let src_uri = "malloc:///d?size_mb=100";
        let dst_uri = "malloc:///d2?size_mb=100";

        device_create(src_uri).await.unwrap();
        device_create(dst_uri).await.unwrap();

        let rebuild_check = |rebuild_map: SegmentMap, index: usize| async move {
            let dirty_blks = rebuild_map.count_dirty_blks();
            let job = BdevRebuildJob::builder()
                .with_bitmap(rebuild_map)
                .build(src_uri, dst_uri)
                .await
                .unwrap();
            let chan = job.start().await.unwrap();
            let state = chan.await.unwrap();
            assert_eq!(state, RebuildState::Completed, "Rebuild should succeed");
            let stats = job.stats().await;
            assert_eq!(stats.blocks_transferred, dirty_blks, "Test {index} failed");
        };

        let test_table = vec![
            PartialMap::new().seg(1).seg(2),
            PartialMap::new().seg(1).seg(2).seg(1).seg_n(2, 1),
            PartialMap::new().seg(20).seg(3).seg(10),
            PartialMap::new().seg(20).seg(3).seg_n(10, 2),
        ];

        for (i, test) in test_table.into_iter().enumerate() {
            rebuild_check(test.build(), i).await;
        }

        device_destroy(src_uri).await.unwrap();
        device_destroy(dst_uri).await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn rebuild_across_mixed_cluster_sizes() {
    common::composer_init();

    let test = Builder::new()
        .name("cargo-test")
        .network("10.1.0.0/16")
        .unwrap()
        .add_container_bin(
            "ms_0",
            Binary::from_dbg("io-engine").with_args(vec![
                "-l",
                "1,2",
                "-Fcolor,compact,host,nodate",
            ]),
        )
        .add_container_bin(
            "ms_1",
            Binary::from_dbg("io-engine").with_args(vec![
                "-l",
                "3,4",
                "-Fcolor,compact,host,nodate",
            ]),
        )
        .with_clean(true)
        .build()
        .await
        .unwrap();

    let conn = GrpcConnect::new(&test);
    let hdl0 = conn.grpc_handle_shared("ms_0").await.unwrap();
    let hdl1 = conn.grpc_handle_shared("ms_1").await.unwrap();

    // pool0 will have default cluster size of 4MiB
    let mut pool0 = PoolBuilder::new(hdl0.clone())
        .with_name("pool_0")
        .with_new_uuid()
        .with_malloc("mem_0", POOL_SIZE);

    let mut pool1 = PoolBuilder::new(hdl1.clone())
        .with_name("pool_1")
        .with_new_uuid()
        .with_malloc("mem_1", POOL_SIZE)
        .with_cluster_size(33554432); // 32MiB cluster size

    pool0.create().await.unwrap();
    pool1.create().await.unwrap();

    let mut repl0 = ReplicaBuilder::new(hdl0.clone())
        .with_pool(&pool0)
        .with_name("repl_0")
        .with_new_uuid()
        .with_size_mb(REPL_SIZE);

    repl0.create().await.unwrap();

    let mut repl1 = ReplicaBuilder::new(hdl1.clone())
        .with_pool(&pool1)
        .with_name("repl_1")
        .with_new_uuid()
        .with_size_mb(REPL_SIZE)
        .with_thin(true);

    repl1.create().await.unwrap();
    repl1.share().await.unwrap();

    assert_eq!(
        repl0
            .get_replica()
            .await
            .unwrap()
            .usage
            .unwrap()
            .cluster_size,
        4194304
    );
    assert_eq!(
        repl1
            .get_replica()
            .await
            .unwrap()
            .usage
            .unwrap()
            .cluster_size,
        33554432
    );

    let mut nex = NexusBuilder::new(hdl0.clone())
        .with_name("nexus0")
        .with_new_uuid()
        .with_size_mb(REPL_SIZE)
        .with_replicas(&[repl0]);

    nex.create().await.unwrap();
    nex.add_replica(&repl1, false).await.unwrap();
    assert_eq!(nex.get_nexus().await.unwrap().children.len(), 2);
    assert!(nex
        .wait_replica_state(
            &repl1,
            ChildState::Online,
            None,
            std::time::Duration::from_secs(5)
        )
        .await
        .is_ok());
}

/// Verifies that nexus rebuild propagates UNMAP semantics: when a region of the
/// source is unmapped, after a rebuild the destination must also be
/// (de)allocated to match the source — i.e. unmapped clusters must not be
/// re-allocated on the destination during rebuild.
///
/// All replicas live in a single io-engine container to minimise the cost of
/// docker compose setup/teardown.
#[tokio::test]
async fn rebuild_thin_unmap_propagates_to_dst() {
    common::composer_init();

    let test = Builder::new()
        .name("cargo-test")
        .network("10.1.0.0/16")
        .unwrap()
        .add_container_bin(
            "ms_0",
            Binary::from_dbg("io-engine").with_args(vec![
                "-l",
                "1,2,3,4",
                "--bs-cluster-unmap",
                "-Fcolor,compact,host,nodate",
            ]),
        )
        .with_clean(true)
        .build()
        .await
        .unwrap();

    let conn = GrpcConnect::new(&test);
    let hdl = conn.grpc_handle_shared("ms_0").await.unwrap();

    use io_engine_tests::{
        file_io::DataSize,
        nexus::{test_trim_to_nexus, test_write_to_nexus},
    };

    const POOL_SIZE: u64 = 100;
    const REPL_SIZE: u64 = 22;
    // Use an explicit 1 MiB cluster size so that the test can write/unmap
    // exactly one cluster at a time without relying on the pool default.
    const CLUSTER_SIZE: u32 = 1024 * 1024;

    let mut pool_0 = PoolBuilder::new(hdl.clone())
        .with_name("pool0")
        .with_new_uuid()
        .with_malloc("mem0", POOL_SIZE)
        .with_cluster_size(CLUSTER_SIZE);
    pool_0.create().await.unwrap();

    let mut pool_1 = PoolBuilder::new(hdl.clone())
        .with_name("pool1")
        .with_new_uuid()
        .with_malloc("mem1", POOL_SIZE)
        .with_cluster_size(CLUSTER_SIZE);
    pool_1.create().await.unwrap();

    let mut pool_2 = PoolBuilder::new(hdl.clone())
        .with_name("pool2")
        .with_new_uuid()
        .with_malloc("mem2", POOL_SIZE)
        .with_cluster_size(CLUSTER_SIZE);
    pool_2.create().await.unwrap();

    let mut repl_0 = ReplicaBuilder::new(hdl.clone())
        .with_pool(&pool_0)
        .with_name("r0")
        .with_new_uuid()
        .with_size_mb(REPL_SIZE)
        .with_thin(true);
    repl_0.create().await.unwrap();
    repl_0.share().await.unwrap();

    let mut repl_1 = ReplicaBuilder::new(hdl.clone())
        .with_pool(&pool_1)
        .with_name("r1")
        .with_new_uuid()
        .with_size_mb(REPL_SIZE)
        .with_thin(true);
    repl_1.create().await.unwrap();
    repl_1.share().await.unwrap();

    let mut repl_2 = ReplicaBuilder::new(hdl.clone())
        .with_pool(&pool_2)
        .with_name("r2")
        .with_new_uuid()
        .with_size_mb(REPL_SIZE)
        .with_thin(true);
    repl_2.create().await.unwrap();
    repl_2.share().await.unwrap();

    let mut nex = NexusBuilder::new(hdl.clone())
        .with_name("nexus0")
        .with_new_uuid()
        .with_size_mb(REPL_SIZE)
        .with_replica(&repl_0)
        .with_replica(&repl_1);
    nex.create().await.unwrap();
    nex.publish().await.unwrap();

    let cluster_bytes = CLUSTER_SIZE as u64;

    // Write two full clusters worth of data (clusters 2 and 3, skipping
    // cluster 0 which is reserved for the superblock / metadata).
    test_write_to_nexus(
        &nex,
        DataSize::from_bytes(2 * cluster_bytes),
        2,
        DataSize::from_bytes(cluster_bytes),
    )
    .await
    .unwrap();

    // Confirm that each source replica has allocated some clusters.
    let r0_before = repl_0.get_replica().await.unwrap().usage.unwrap();
    let r1_before = repl_1.get_replica().await.unwrap().usage.unwrap();
    assert!(
        r0_before.num_allocated_clusters >= 2,
        "Source replica 0 should have at least 2 allocated clusters after write, got {}",
        r0_before.num_allocated_clusters
    );
    assert!(
        r1_before.num_allocated_clusters >= 2,
        "Source replica 1 should have at least 2 allocated clusters after write, got {}",
        r1_before.num_allocated_clusters
    );

    // Unmap exactly one cluster (cluster 3) through the nexus. The nexus
    // forwards the unmap to both source replicas; with --bs-cluster-unmap
    // enabled the underlying blobstore releases the corresponding cluster.
    test_trim_to_nexus(
        &nex,
        DataSize::from_bytes(3 * cluster_bytes),
        DataSize::from_bytes(cluster_bytes),
    )
    .await
    .unwrap();

    // Wait briefly for the asynchronous cluster-release to settle.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Each source replica must now have one fewer allocated cluster.
    let r0_after_trim = repl_0.get_replica().await.unwrap().usage.unwrap();
    let r1_after_trim = repl_1.get_replica().await.unwrap().usage.unwrap();
    assert_eq!(
        r0_before.num_allocated_clusters,
        r0_after_trim.num_allocated_clusters + 1,
        "Source replica 0: expected one cluster released after unmap"
    );
    assert_eq!(
        r1_before.num_allocated_clusters,
        r1_after_trim.num_allocated_clusters + 1,
        "Source replica 1: expected one cluster released after unmap"
    );

    // Add the destination replica. This triggers a nexus rebuild: the
    // destination is synchronised with the sources. Unmapped clusters in the
    // source must be unmapped (not allocated) in the destination.
    nex.add_replica(&repl_2, false).await.unwrap();

    nex.wait_children_online(Duration::from_secs(60))
        .await
        .unwrap();

    // After rebuild the destination must match source allocation.
    let r0_final = repl_0.get_replica().await.unwrap().usage.unwrap();
    let r2_final = repl_2.get_replica().await.unwrap().usage.unwrap();
    assert_eq!(
        r0_final.num_allocated_clusters,
        r2_final.num_allocated_clusters,
        "After nexus rebuild destination cluster count ({}) must match source ({})",
        r2_final.num_allocated_clusters,
        r0_final.num_allocated_clusters,
    );
}

/// Regression test for SPDK concurrent UNMAP issues.
///
/// Creates a 100 GiB thin volume on a 1 MiB-cluster pool, drives a very large
/// number of concurrent cluster-sized UNMAPs through the nexus using fio
/// (`rw=randtrim`, `bs=1M`, `iodepth=128`, `numjobs=8` => up to 1024 in-flight
/// UNMAP commands sustained over a 20s window), then exports the pool and
/// re-imports it from the same backing disk.
///
/// Concurrent UNMAPs at cluster boundaries used to corrupt blobstore
/// metadata, which made the subsequent pool import fail. After the fix the
/// re-import must succeed.
#[tokio::test]
async fn concurrent_unmap_export_and_pool_reimport() {
    common::composer_init();

    use io_engine_tests::{
        compose::rpc::v1::pool::{ExportPoolRequest, ImportPoolRequest},
        file_io::DataSize,
        fio::{FioBuilder, FioJobBuilder},
        nexus::test_fio_to_nexus_aio,
    };

    const POOL_NAME: &str = "pool_concurrent_unmap";
    // 100 GiB thin volume, exposed over NVMf as the nexus device.
    const REPL_SIZE_MB: u64 = 100 * 1024;
    // 1 MiB cluster size so that bs=1M trims are exactly one cluster each.
    const CLUSTER_SIZE: u32 = 1024 * 1024;
    // Sparse backing file with some headroom over the replica size for pool
    // metadata.
    const POOL_SIZE_MB: u64 = REPL_SIZE_MB + 1024;

    // Range pre-written by the writer fio job and then trimmed by the
    // randtrim job so that UNMAPs actually traverse allocated blobstore
    // clusters (not no-ops on a fully-thin region).
    const TRIM_RANGE_MB: u64 = 4 * 1024;

    let pool_uuid = common::generate_uuid();
    let disk_file = format!("/tmp/concurrent-unmap-reimport-{pool_uuid}.img");
    let pool_bdev = format!("aio://{disk_file}?blk_size=512");

    common::delete_file(&[disk_file.clone()]);
    common::truncate_file_bytes(&disk_file, POOL_SIZE_MB * 1024 * 1024);

    let test = Builder::new()
        .name("cargo-test")
        .network("10.1.0.0/16")
        .unwrap()
        .add_container_bin(
            "ms_0",
            Binary::from_dbg("io-engine").with_args(vec![
                "-l",
                "1,2,3,4",
                "--bs-cluster-unmap",
                "-Fcolor,compact,host,nodate",
            ]),
        )
        .with_clean(true)
        .build()
        .await
        .unwrap();

    let conn = GrpcConnect::new(&test);
    let hdl = conn.grpc_handle_shared("ms_0").await.unwrap();

    let mut pool = PoolBuilder::new(hdl.clone())
        .with_name(POOL_NAME)
        .with_uuid(&pool_uuid)
        .with_bdev(&pool_bdev)
        .with_cluster_size(CLUSTER_SIZE);
    pool.create().await.unwrap();

    let mut repl = ReplicaBuilder::new(hdl.clone())
        .with_pool(&pool)
        .with_name("repl_concurrent_unmap")
        .with_new_uuid()
        .with_size_mb(REPL_SIZE_MB)
        .with_thin(true);
    repl.create().await.unwrap();
    repl.share().await.unwrap();

    let mut nex = NexusBuilder::new(hdl.clone())
        .with_name("nexus_concurrent_unmap")
        .with_new_uuid()
        .with_size_mb(REPL_SIZE_MB)
        .with_replica(&repl);
    nex.create().await.unwrap();
    nex.publish().await.unwrap();

    // Pre-allocate clusters in the trimmed range so that the subsequent
    // randtrim phase exercises real blobstore cluster releases rather than
    // hitting unallocated regions.
    let writer = FioBuilder::new()
        .with_job(
            FioJobBuilder::default()
                .with_name("preallocate")
                .with_rw("write")
                .with_bs(DataSize::from_bytes(CLUSTER_SIZE as u64))
                .with_iodepth(64)
                .with_size(DataSize::from_mb(TRIM_RANGE_MB))
                .build(),
        )
        .with_verbose_err(true)
        .build();
    test_fio_to_nexus_aio(&nex, writer).await.unwrap();

    // Issue a very large number of concurrent cluster-sized UNMAPs.
    // numjobs=8 * iodepth=128 = up to 1024 UNMAPs in flight at any moment,
    // sustained for runtime seconds across the pre-allocated range.
    let trimmer = FioBuilder::new()
        .with_job(
            FioJobBuilder::default()
                .with_name("concurrent_unmap")
                .with_rw("randtrim")
                .with_bs(DataSize::from_bytes(CLUSTER_SIZE as u64))
                .with_iodepth(128)
                .with_numjobs(8)
                .with_size(DataSize::from_mb(TRIM_RANGE_MB))
                .with_runtime(20)
                .build(),
        )
        .with_verbose_err(true)
        .build();
    test_fio_to_nexus_aio(&nex, trimmer).await.unwrap();

    // Tear down the nexus and replica so the pool can be exported.
    nex.shutdown().await.unwrap();
    nex.destroy().await.unwrap();
    repl.destroy().await.unwrap();

    // Export the pool: this flushes and closes the on-disk blobstore.
    hdl.lock()
        .await
        .pool
        .export_pool(ExportPoolRequest {
            name: POOL_NAME.to_string(),
            uuid: Some(pool_uuid.clone()),
        })
        .await
        .expect("Pool export must succeed after concurrent UNMAPs");

    // Re-import the pool from the same backing disk. This is the failure
    // mode the SPDK concurrent UNMAP bugs caused; it must succeed.
    hdl.lock()
        .await
        .pool
        .import_pool(ImportPoolRequest {
            name: POOL_NAME.to_string(),
            uuid: Some(pool_uuid.clone()),
            disks: vec![pool_bdev.clone()],
            pooltype: 0,
            encryption: None,
        })
        .await
        .expect("Pool re-import after concurrent UNMAPs must succeed");
}
