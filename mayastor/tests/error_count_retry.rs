extern crate log;

use std::time::Duration;

use crossbeam::channel::unbounded;

pub use common::error_bdev::{
    create_error_bdev,
    inject_error,
    SPDK_BDEV_IO_TYPE_READ,
    SPDK_BDEV_IO_TYPE_WRITE,
    VBDEV_IO_FAILURE,
};
use mayastor::{
    bdev::{nexus_create, nexus_lookup, ActionType, NexusErrStore, QueryType},
    core::{
        mayastor_env_stop,
        Bdev,
        MayastorCliArgs,
        MayastorEnvironment,
        Reactor,
    },
    subsys::Config,
};

pub mod common;

static ERROR_COUNT_TEST_NEXUS: &str = "error_count_retry_nexus";

static DISKNAME1: &str = "/tmp/disk1.img";

static ERROR_DEVICE: &str = "error_retry_device";
static EE_ERROR_DEVICE: &str = "EE_error_retry_device"; // The prefix is added by the vbdev_error module
static BDEV_EE_ERROR_DEVICE: &str = "bdev:///EE_error_retry_device";

static YAML_CONFIG_FILE: &str = "/tmp/error_count_retry_nexus.yaml";

#[test]
fn nexus_error_count_retry_test() {
    common::truncate_file(DISKNAME1, 64 * 1024);

    let mut config = Config::default();
    config.err_store_opts.enable_err_store = true;
    config.err_store_opts.action = ActionType::Ignore;
    config.err_store_opts.err_store_size = 256;
    config.err_store_opts.max_io_attempts = 2;

    config.write(YAML_CONFIG_FILE).unwrap();
    test_init!(YAML_CONFIG_FILE);

    // baseline test with no errors injected
    Reactor::block_on(async {
        create_error_bdev(ERROR_DEVICE, DISKNAME1);
        create_nexus().await;
        err_write_nexus(true).await;
        err_read_nexus(true).await;
    });

    reactor_run_millis(10); // give time for any errors to be added to the error store

    nexus_err_query_and_test(
        BDEV_EE_ERROR_DEVICE,
        NexusErrStore::READ_FLAG | NexusErrStore::WRITE_FLAG,
        0,
        Some(1_000_000_000),
    );

    // 1 write error injected, 2 attempts allowed, 1 write error should be
    // logged and the IO should succeed
    Reactor::block_on(async {
        inject_error(
            EE_ERROR_DEVICE,
            SPDK_BDEV_IO_TYPE_WRITE,
            VBDEV_IO_FAILURE,
            1,
        );
        err_write_nexus(true).await;
    });

    reactor_run_millis(10); // give time for any errors to be added to the error store

    nexus_err_query_and_test(
        BDEV_EE_ERROR_DEVICE,
        NexusErrStore::WRITE_FLAG,
        1,
        Some(1_000_000_000),
    );

    // 2 errors injected, 2 attempts allowed, 1 read attempt, 2 read errors
    // should be logged and the IO should fail
    Reactor::block_on(async {
        inject_error(
            EE_ERROR_DEVICE,
            SPDK_BDEV_IO_TYPE_READ,
            VBDEV_IO_FAILURE,
            2,
        );
        err_read_nexus(false).await;
    });

    // IO should now succeed
    Reactor::block_on(async {
        err_read_nexus(true).await;
    });

    mayastor_env_stop(0);

    common::delete_file(&[DISKNAME1.to_string()]);
    common::delete_file(&[YAML_CONFIG_FILE.to_string()]);
}

async fn create_nexus() {
    let ch = vec![BDEV_EE_ERROR_DEVICE.to_string()];

    nexus_create(ERROR_COUNT_TEST_NEXUS, 64 * 1024 * 1024, None, &ch)
        .await
        .unwrap();
}

fn nexus_err_query_and_test(
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

async fn err_read_nexus(succeed: bool) {
    let bdev = Bdev::lookup_by_name(ERROR_COUNT_TEST_NEXUS)
        .expect("failed to lookup nexus");
    let d = bdev
        .open(true)
        .expect("failed open bdev")
        .into_handle()
        .unwrap();
    let mut buf = d.dma_malloc(512).expect("failed to allocate buffer");

    match d.read_at(0, &mut buf).await {
        Ok(_) => {
            assert_eq!(succeed, true);
        }
        Err(_) => {
            assert_eq!(succeed, false);
        }
    };
}

fn reactor_run_millis(milliseconds: u64) {
    let (s, r) = unbounded::<()>();
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(milliseconds));
        s.send(())
    });
    reactor_poll!(r);
}
