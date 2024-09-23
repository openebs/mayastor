pub mod common;

use async_trait::async_trait;
use once_cell::sync::OnceCell;
use std::future::Future;

use spdk_rs::{
    ffihelper::IntoCString,
    libspdk::resize_malloc_disk,
    UntypedBdev,
};

use io_engine::{
    core::MayastorCliArgs,
    lvs::Lvs,
    pool_backend::{IPoolProps, PoolArgs},
};

use io_engine_tests::{
    bdev::{create_bdev, find_bdev_by_name},
    compose::{
        rpc::v1::{pool::Pool, GrpcConnect, SharedRpcHandle},
        Binary,
        Builder,
        ComposeTest,
    },
    pool::PoolBuilder,
    MayastorTest,
};

static MAYASTOR: OnceCell<MayastorTest> = OnceCell::new();

fn ms() -> &'static MayastorTest<'static> {
    MAYASTOR.get_or_init(|| {
        MayastorTest::new(MayastorCliArgs {
            log_format: Some("nodate,nohost,compact".parse().unwrap()),
            reactor_mask: "0x3".into(),
            enable_io_all_thrd_nexus_channels: true,
            ..Default::default()
        })
    })
}

/// Tests if 'a' is approximately equal to 'b' up to the given tolerance (in
/// percents).
fn approx_eq(a: f64, b: f64, t: f64) -> bool {
    assert!(a > 0.0 && b > 0.0 && (0.0 .. 100.0).contains(&t));
    let d = 100.0 * (a - b).abs() / f64::max(a, b);
    d <= t
}

/// Pool stats.
struct TestPoolStats {
    capacity: u64,
    disk_capacity: u64,
}

impl TestPoolStats {
    fn capacity_approx_matches(&self) -> bool {
        approx_eq(self.disk_capacity as f64, self.capacity as f64, 10.0)
    }
}

impl From<&Lvs> for TestPoolStats {
    fn from(lvs: &Lvs) -> Self {
        Self {
            capacity: lvs.capacity(),
            disk_capacity: lvs.disk_capacity(),
        }
    }
}

impl From<Lvs> for TestPoolStats {
    fn from(lvs: Lvs) -> Self {
        Self::from(&lvs)
    }
}

impl From<Pool> for TestPoolStats {
    fn from(p: Pool) -> Self {
        Self {
            capacity: p.capacity,
            disk_capacity: p.disk_capacity,
        }
    }
}

/// Grow test interface.
#[async_trait(?Send)]
trait GrowTest {
    async fn create_pool(&mut self) -> TestPoolStats;
    async fn pool_stats(&self) -> TestPoolStats;
    async fn grow_pool(&mut self) -> (TestPoolStats, TestPoolStats);
    async fn device_size(&mut self) -> u64;
    async fn grow_device(&mut self) -> u64;
}

/// Implements logic for pool grow test.
async fn test_grow(create: impl Future<Output = Box<dyn GrowTest>>) {
    common::composer_init();

    let mut gt = create.await;

    let initial = gt.create_pool().await;

    assert_eq!(initial.disk_capacity, gt.device_size().await);
    assert!(initial.capacity <= initial.disk_capacity);
    assert!(initial.capacity_approx_matches());

    // Resize the device.
    let new_dev_cap = gt.grow_device().await;

    // Pool capacity must not change, disk capacity must reflect disk size
    // change.
    let after_dev_grow = gt.pool_stats().await;
    assert_eq!(after_dev_grow.capacity, initial.capacity);
    assert_eq!(after_dev_grow.disk_capacity, new_dev_cap);

    // Grow the pool.
    let (before_pool_grow, after_pool_grow) = gt.grow_pool().await;
    assert_eq!(before_pool_grow.capacity, initial.capacity);
    assert_eq!(before_pool_grow.disk_capacity, new_dev_cap);

    // Pool must have grown.
    assert!(after_pool_grow.capacity > before_pool_grow.capacity);

    // New pool capacity must be close to the disk capacity.
    assert!(after_pool_grow.capacity <= after_pool_grow.disk_capacity);
    assert!(after_pool_grow.capacity_approx_matches());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lvs_grow_ms_malloc() {
    const SIZE_AFTER_MB: u64 = 128;
    const BDEV_NAME: &str = "mem0";
    const BDEV_URI: &str = "malloc:///mem0?size_mb=64";
    const POOL_NAME: &str = "pool0";
    const POOL_UUID: &str = "40baf8b5-6256-4f29-b073-61ebf67d9b91";

    /// Pool grow test based on LVS code, malloc bdev.
    struct GrowTestMsMalloc {}

    #[async_trait(?Send)]
    impl GrowTest for GrowTestMsMalloc {
        async fn create_pool(&mut self) -> TestPoolStats {
            ms().spawn(async {
                let lvs_args = PoolArgs {
                    name: POOL_NAME.to_string(),
                    disks: vec![BDEV_URI.to_string()],
                    uuid: Some(POOL_UUID.to_string()),
                    cluster_size: None,
                    md_args: None,
                    backend: Default::default(),
                };

                // Create LVS.
                Lvs::create_or_import(lvs_args.clone())
                    .await
                    .unwrap()
                    .into()
            })
            .await
        }

        async fn pool_stats(&self) -> TestPoolStats {
            ms().spawn(async { Lvs::lookup(POOL_NAME).unwrap().into() })
                .await
        }

        async fn grow_pool(&mut self) -> (TestPoolStats, TestPoolStats) {
            ms().spawn(async {
                let lvs = Lvs::lookup(POOL_NAME).unwrap();
                let before = lvs.clone().into();
                lvs.grow().await.unwrap();
                let after = Lvs::lookup(POOL_NAME).unwrap().into();
                (before, after)
            })
            .await
        }

        async fn device_size(&mut self) -> u64 {
            ms().spawn(async {
                UntypedBdev::lookup_by_name(BDEV_NAME)
                    .unwrap()
                    .size_in_bytes()
            })
            .await
        }

        async fn grow_device(&mut self) -> u64 {
            ms().spawn(async {
                unsafe {
                    // Resize the malloc bdev.
                    let name = BDEV_NAME.to_owned();
                    resize_malloc_disk(
                        name.into_cstring().as_ptr(),
                        SIZE_AFTER_MB,
                    );
                };
            })
            .await;
            self.device_size().await
        }
    }

    test_grow(async { Box::new(GrowTestMsMalloc {}) as Box<dyn GrowTest> })
        .await;
}

/// Pool grow test based on gRPC API and malloc bdev.
#[tokio::test]
async fn lvs_grow_api_malloc() {
    const BDEV_NAME: &str = "mem0";
    const BDEV_URI: &str = "malloc:///mem0?size_mb=64";
    const BDEV_URI_RESIZE: &str = "malloc:///mem0?size_mb=128&resize";
    const POOL_NAME: &str = "pool0";
    const POOL_UUID: &str = "40baf8b5-6256-4f29-b073-61ebf67d9b91";

    struct GrowTestApiMalloc {
        #[allow(dead_code)]
        test: ComposeTest,
        ms: SharedRpcHandle,
        pool: PoolBuilder,
    }

    impl GrowTestApiMalloc {
        async fn new() -> GrowTestApiMalloc {
            let test = Builder::new()
                .name("cargo-test")
                .network("10.1.0.0/16")
                .unwrap()
                .add_container_bin(
                    "ms_0",
                    Binary::from_dbg("io-engine").with_args(vec!["-l", "1,2"]),
                )
                .with_clean(true)
                .build()
                .await
                .unwrap();

            let conn = GrpcConnect::new(&test);
            let ms = conn.grpc_handle_shared("ms_0").await.unwrap();

            let pool = PoolBuilder::new(ms.clone())
                .with_name(POOL_NAME)
                .with_uuid(POOL_UUID)
                .with_bdev(BDEV_URI);

            Self {
                test,
                ms,
                pool,
            }
        }
    }

    #[async_trait(?Send)]
    impl GrowTest for GrowTestApiMalloc {
        async fn create_pool(&mut self) -> TestPoolStats {
            self.pool.create().await.unwrap();
            self.pool_stats().await
        }

        async fn pool_stats(&self) -> TestPoolStats {
            self.pool.get_pool().await.unwrap().into()
        }

        async fn grow_pool(&mut self) -> (TestPoolStats, TestPoolStats) {
            let (a, b) = self.pool.grow().await.unwrap();
            (a.into(), b.into())
        }

        async fn device_size(&mut self) -> u64 {
            let bdev =
                find_bdev_by_name(self.ms.clone(), BDEV_NAME).await.unwrap();
            bdev.num_blocks * bdev.blk_size as u64
        }

        async fn grow_device(&mut self) -> u64 {
            let bdev =
                create_bdev(self.ms.clone(), BDEV_URI_RESIZE).await.unwrap();
            bdev.num_blocks * bdev.blk_size as u64
        }
    }

    test_grow(async {
        Box::new(GrowTestApiMalloc::new().await) as Box<dyn GrowTest>
    })
    .await;
}

/// Pool grow test based on gRPC API and file-based AIO device.
#[tokio::test]
async fn lvs_grow_api_aio() {
    const DISK_NAME: &str = "/tmp/disk1.img";
    const BDEV_NAME: &str = "/host/tmp/disk1.img";
    const BDEV_URI: &str = "aio:///host/tmp/disk1.img?blk_size=512";
    const BDEV_URI_RESCAN: &str =
        "aio:///host/tmp/disk1.img?blk_size=512&rescan";
    const POOL_NAME: &str = "pool0";
    const POOL_UUID: &str = "40baf8b5-6256-4f29-b073-61ebf67d9b91";

    struct GrowTestApiAio {
        #[allow(dead_code)]
        test: ComposeTest,
        ms: SharedRpcHandle,
        pool: PoolBuilder,
    }

    impl GrowTestApiAio {
        async fn new() -> GrowTestApiAio {
            common::delete_file(&[DISK_NAME.into()]);
            common::truncate_file(DISK_NAME, 64 * 1024);

            let test = Builder::new()
                .name("cargo-test")
                .network("10.1.0.0/16")
                .unwrap()
                .add_container_bin(
                    "ms_0",
                    Binary::from_dbg("io-engine")
                        .with_args(vec!["-l", "1,2"])
                        .with_bind("/tmp", "/host/tmp"),
                )
                .with_clean(true)
                .build()
                .await
                .unwrap();

            let conn = GrpcConnect::new(&test);
            let ms = conn.grpc_handle_shared("ms_0").await.unwrap();

            let pool = PoolBuilder::new(ms.clone())
                .with_name(POOL_NAME)
                .with_uuid(POOL_UUID)
                .with_bdev(BDEV_URI);

            Self {
                test,
                ms,
                pool,
            }
        }
    }

    #[async_trait(?Send)]
    impl GrowTest for GrowTestApiAio {
        async fn create_pool(&mut self) -> TestPoolStats {
            self.pool.create().await.unwrap();
            self.pool_stats().await
        }

        async fn pool_stats(&self) -> TestPoolStats {
            self.pool.get_pool().await.unwrap().into()
        }

        async fn grow_pool(&mut self) -> (TestPoolStats, TestPoolStats) {
            let (a, b) = self.pool.grow().await.unwrap();
            (a.into(), b.into())
        }

        async fn device_size(&mut self) -> u64 {
            let bdev =
                find_bdev_by_name(self.ms.clone(), BDEV_NAME).await.unwrap();
            bdev.num_blocks * bdev.blk_size as u64
        }

        async fn grow_device(&mut self) -> u64 {
            // Resize bdev's backing file.
            common::truncate_file(DISK_NAME, 128 * 1024);

            // Rescan AIO bdev (re-read its size from the backing media).
            let bdev =
                create_bdev(self.ms.clone(), BDEV_URI_RESCAN).await.unwrap();
            bdev.num_blocks * bdev.blk_size as u64
        }
    }

    test_grow(async {
        Box::new(GrowTestApiAio::new().await) as Box<dyn GrowTest>
    })
    .await;
}
