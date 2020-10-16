pub use common::error_bdev::{
    create_error_bdev,
    inject_error,
    SPDK_BDEV_IO_TYPE_READ,
    SPDK_BDEV_IO_TYPE_WRITE,
    VBDEV_IO_FAILURE,
};
use common::MayastorTest;
use mayastor::{
    bdev::{nexus_create, nexus_lookup, ActionType, NexusErrStore, QueryType},
    core::{Bdev, MayastorCliArgs},
    subsys::Config,
};

pub mod common;

static ERROR_COUNT_TEST_NEXUS: &str = "error_count_test_nexus";

static DISKNAME1: &str = "/tmp/disk1.img";
static BDEVNAME1: &str = "aio:///tmp/disk1.img?blk_size=512";

static DISKNAME2: &str = "/tmp/disk2.img";

static ERROR_DEVICE: &str = "error_device";
static EE_ERROR_DEVICE: &str = "EE_error_device";
// The prefix is added by the vbdev_error module
static BDEV_EE_ERROR_DEVICE: &str = "bdev:///EE_error_device";

static YAML_CONFIG_FILE: &str = "/tmp/error_count_test_nexus.yaml";

#[tokio::test]
async fn nexus_error_count_test() {
    common::truncate_file(DISKNAME1, 64 * 1024);
    common::truncate_file(DISKNAME2, 64 * 1024);

    let mut config = Config::default();
    config.err_store_opts.enable_err_store = true;
    config.err_store_opts.action = ActionType::Ignore;
    config.err_store_opts.err_store_size = 256;
    config.write(YAML_CONFIG_FILE).unwrap();
    let ms = MayastorTest::new(MayastorCliArgs {
        mayastor_config: Some(YAML_CONFIG_FILE.to_string()),
        reactor_mask: "0x3".to_string(),
        ..Default::default()
    });

    ms.spawn(async {
        create_error_bdev(ERROR_DEVICE, DISKNAME2);
        create_nexus().await;
        err_write_nexus(true).await;
        err_read_nexus_both(true).await;
    })
    .await;

    ms.spawn(nexus_err_query_and_test(
        BDEV_EE_ERROR_DEVICE,
        NexusErrStore::READ_FLAG,
        0,
        Some(1_000_000_000),
    ))
    .await;

    ms.spawn(nexus_err_query_and_test(
        BDEV_EE_ERROR_DEVICE,
        NexusErrStore::WRITE_FLAG,
        0,
        Some(1_000_000_000),
    ))
    .await;

    ms.spawn(nexus_err_query_and_test(
        BDEVNAME1,
        NexusErrStore::READ_FLAG | NexusErrStore::WRITE_FLAG,
        0,
        Some(1_000_000_000),
    ))
    .await;

    ms.spawn(async {
        inject_error(
            EE_ERROR_DEVICE,
            SPDK_BDEV_IO_TYPE_WRITE,
            VBDEV_IO_FAILURE,
            1,
        );
        err_write_nexus(false).await;
        err_read_nexus_both(true).await;
    })
    .await;

    ms.spawn(nexus_err_query_and_test(
        BDEV_EE_ERROR_DEVICE,
        NexusErrStore::READ_FLAG,
        0,
        Some(1_000_000_000),
    ))
    .await;

    ms.spawn(nexus_err_query_and_test(
        BDEV_EE_ERROR_DEVICE,
        NexusErrStore::WRITE_FLAG,
        1,
        Some(1_000_000_000),
    ))
    .await;

    ms.spawn(nexus_err_query_and_test(
        BDEVNAME1,
        NexusErrStore::READ_FLAG | NexusErrStore::WRITE_FLAG,
        0,
        Some(1_000_000_000),
    ))
    .await;

    ms.spawn(async {
        inject_error(
            EE_ERROR_DEVICE,
            SPDK_BDEV_IO_TYPE_READ,
            VBDEV_IO_FAILURE,
            1,
        );
        err_read_nexus_both(false).await;
        err_write_nexus(true).await;
    })
    .await;

    ms.spawn(nexus_err_query_and_test(
        BDEV_EE_ERROR_DEVICE,
        NexusErrStore::READ_FLAG,
        1,
        Some(1_000_000_000),
    ))
    .await;

    ms.spawn(nexus_err_query_and_test(
        BDEV_EE_ERROR_DEVICE,
        NexusErrStore::WRITE_FLAG,
        1,
        Some(1_000_000_000),
    ))
    .await;

    ms.spawn(nexus_err_query_and_test(
        BDEVNAME1,
        NexusErrStore::READ_FLAG | NexusErrStore::WRITE_FLAG,
        0,
        Some(1_000_000_000),
    ))
    .await;

    // overflow the error store with errored reads and writes, assumes default
    // buffer size of 256 records
    ms.spawn(async {
        inject_error(
            EE_ERROR_DEVICE,
            SPDK_BDEV_IO_TYPE_READ,
            VBDEV_IO_FAILURE,
            257,
        );
        inject_error(
            EE_ERROR_DEVICE,
            SPDK_BDEV_IO_TYPE_WRITE,
            VBDEV_IO_FAILURE,
            100,
        );
        for _ in 0 .. 257 {
            err_read_nexus_both(false).await;
        }
        for _ in 0 .. 100 {
            err_write_nexus(false).await;
        }
    })
    .await;

    ms.spawn(nexus_err_query_and_test(
        BDEV_EE_ERROR_DEVICE,
        NexusErrStore::READ_FLAG,
        156,
        Some(10_000_000_000),
    ))
    .await;

    ms.spawn(nexus_err_query_and_test(
        BDEV_EE_ERROR_DEVICE,
        NexusErrStore::WRITE_FLAG,
        100,
        Some(10_000_000_000),
    ))
    .await;

    ms.spawn(nexus_err_query_and_test(
        BDEV_EE_ERROR_DEVICE,
        NexusErrStore::WRITE_FLAG,
        0,
        Some(0), // too recent, so nothing there
    ))
    .await;

    ms.spawn(nexus_err_query_and_test(
        BDEV_EE_ERROR_DEVICE,
        NexusErrStore::WRITE_FLAG,
        100,
        Some(1_000_000_000_000_000_000), // underflow, so assumes any age
    ))
    .await;

    ms.spawn(nexus_err_query_and_test(
        BDEV_EE_ERROR_DEVICE,
        NexusErrStore::WRITE_FLAG,
        100,
        None, // no time specified
    ))
    .await;

    common::delete_file(&[DISKNAME1.to_string()]);
    common::delete_file(&[DISKNAME2.to_string()]);
    common::delete_file(&[YAML_CONFIG_FILE.to_string()]);
}

async fn create_nexus() {
    let ch = vec![BDEVNAME1.to_string(), BDEV_EE_ERROR_DEVICE.to_string()];

    nexus_create(ERROR_COUNT_TEST_NEXUS, 64 * 1024 * 1024, None, &ch)
        .await
        .unwrap();
}

async fn nexus_err_query_and_test(
    child_bdev: &str,
    io_type_flags: u32,
    expected_count: u32,
    age_nano: Option<u64>,
) {
    let nexus = nexus_lookup(ERROR_COUNT_TEST_NEXUS).unwrap();
    let count = nexus
        .error_record_query(
            child_bdev,
            io_type_flags,
            NexusErrStore::IO_FAILED_FLAG,
            age_nano,
            QueryType::Total,
        )
        .expect("failed to query child");
    assert!(count.is_some()); // true if the error_store is enabled
    assert_eq!(count.unwrap(), expected_count);
}

async fn err_write_nexus(succeed: bool) {
    let bdev = Bdev::lookup_by_name(ERROR_COUNT_TEST_NEXUS)
        .expect("failed to lookup nexus");
    let d = bdev
        .open(true)
        .expect("failed open bdev")
        .into_handle()
        .unwrap();
    let buf = d.dma_malloc(512).expect("failed to allocate buffer");

    match d.write_at(0, &buf).await {
        Ok(_) => {
            assert_eq!(succeed, true);
        }
        Err(_) => {
            assert_eq!(succeed, false);
        }
    };
}

async fn err_read_nexus_both(succeed: bool) {
    let res1 = err_read_nexus().await;
    let res2 = err_read_nexus().await;

    if succeed {
        assert!(res1 && res2); // both succeeded
    } else {
        assert_ne!(res1, res2); // one succeeded, one failed
    }
}

async fn err_read_nexus() -> bool {
    let bdev = Bdev::lookup_by_name(ERROR_COUNT_TEST_NEXUS)
        .expect("failed to lookup nexus");
    let d = bdev
        .open(true)
        .expect("failed open bdev")
        .into_handle()
        .unwrap();
    let mut buf = d.dma_malloc(512).expect("failed to allocate buffer");

    d.read_at(0, &mut buf).await.is_ok()
}
