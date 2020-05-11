extern crate log;

use crossbeam::channel::unbounded;

use std::{ffi::CString, time::Duration};
pub mod common;
use mayastor::{
    bdev::{nexus_create, nexus_lookup, NexusErrStore},
    core::{
        mayastor_env_stop,
        Bdev,
        MayastorCliArgs,
        MayastorEnvironment,
        Reactor,
    },
};

use spdk_sys::{
    create_aio_bdev,
    spdk_vbdev_error_create,
    spdk_vbdev_error_inject_error,
    SPDK_BDEV_IO_TYPE_READ,
    SPDK_BDEV_IO_TYPE_WRITE,
};

static ERROR_COUNT_TEST_NEXUS: &str = "error_count_test_nexus";

static DISKNAME1: &str = "/tmp/disk1.img";
static BDEVNAME1: &str = "aio:///tmp/disk1.img?blk_size=512";

static DISKNAME2: &str = "/tmp/disk2.img";

static ERROR_DEVICE: &str = "error_device";
static EE_ERROR_DEVICE: &str = "EE_error_device"; // The prefix is added by the vbdev_error module
static BDEV_EE_ERROR_DEVICE: &str = "bdev:///EE_error_device";

// constant used by the vbdev_error module but not exported
const VBDEV_IO_FAILURE: u32 = 1;

#[test]
fn nexus_error_count_test() {
    common::truncate_file(DISKNAME1, 64 * 1024);
    common::truncate_file(DISKNAME2, 64 * 1024);

    test_init!();

    Reactor::block_on(async {
        create_error_bdev().await;
        create_nexus().await;
        err_write_nexus().await;
        err_read_nexus().await;
    });

    reactor_pause_millis(1); // give time for any errors to be added to the error store

    nexus_err_query_and_test(BDEV_EE_ERROR_DEVICE, NexusErrStore::READ_FLAG, 0);

    nexus_err_query_and_test(
        BDEV_EE_ERROR_DEVICE,
        NexusErrStore::WRITE_FLAG,
        0,
    );
    nexus_err_query_and_test(
        BDEVNAME1,
        NexusErrStore::READ_FLAG | NexusErrStore::WRITE_FLAG,
        0,
    );

    Reactor::block_on(async {
        inject_error(SPDK_BDEV_IO_TYPE_WRITE, VBDEV_IO_FAILURE, 1).await;
        err_write_nexus().await;
        err_read_nexus().await;
    });

    reactor_pause_millis(1); // give time for any errors to be added to the error store

    nexus_err_query_and_test(BDEV_EE_ERROR_DEVICE, NexusErrStore::READ_FLAG, 0);

    nexus_err_query_and_test(
        BDEV_EE_ERROR_DEVICE,
        NexusErrStore::WRITE_FLAG,
        1,
    );
    nexus_err_query_and_test(
        BDEVNAME1,
        NexusErrStore::READ_FLAG | NexusErrStore::WRITE_FLAG,
        0,
    );

    Reactor::block_on(async {
        inject_error(SPDK_BDEV_IO_TYPE_READ, VBDEV_IO_FAILURE, 1).await;
        err_read_nexus().await; // multiple reads because there are two replicas
        err_read_nexus().await; // and we may get the wrong one
        err_write_nexus().await;
    });

    reactor_pause_millis(1); // give time for any errors to be added to the error store

    nexus_err_query_and_test(BDEV_EE_ERROR_DEVICE, NexusErrStore::READ_FLAG, 1);

    nexus_err_query_and_test(
        BDEV_EE_ERROR_DEVICE,
        NexusErrStore::WRITE_FLAG,
        1,
    );
    nexus_err_query_and_test(
        BDEVNAME1,
        NexusErrStore::READ_FLAG | NexusErrStore::WRITE_FLAG,
        0,
    );

    // overflow the error store with errored reads and writes, assumes default
    // buffer size of 256 records
    Reactor::block_on(async {
        inject_error(SPDK_BDEV_IO_TYPE_READ, VBDEV_IO_FAILURE, 257).await;
        inject_error(SPDK_BDEV_IO_TYPE_WRITE, VBDEV_IO_FAILURE, 100).await;
        for _ in 0 .. 257 {
            err_read_nexus().await; // multiple reads because there are two replicas
            err_read_nexus().await; // and we may get the wrong one
        }
        for _ in 0 .. 100 {
            err_write_nexus().await;
        }
    });

    reactor_pause_millis(1); // give time for any errors to be added to the error store

    nexus_err_query_and_test(
        BDEV_EE_ERROR_DEVICE,
        NexusErrStore::READ_FLAG,
        156,
    );
    nexus_err_query_and_test(
        BDEV_EE_ERROR_DEVICE,
        NexusErrStore::WRITE_FLAG,
        100,
    );

    mayastor_env_stop(0);
}

async fn inject_error(op: u32, mode: u32, count: u32) {
    let retval: i32;
    let err_bdev_name_str =
        CString::new(EE_ERROR_DEVICE).expect("Failed to create name string");
    let raw = err_bdev_name_str.into_raw();

    unsafe {
        retval = spdk_vbdev_error_inject_error(raw, op, mode, count);
    }
    assert_eq!(retval, 0);
}

async fn create_error_bdev() {
    let mut retval: i32;
    let cname = CString::new(ERROR_DEVICE).unwrap();
    let filename = CString::new(DISKNAME2).unwrap();

    unsafe {
        // this allows us to create a bdev without its name being a uri
        retval = create_aio_bdev(cname.as_ptr(), filename.as_ptr(), 512)
    };
    assert_eq!(retval, 0);

    let err_bdev_name_str = CString::new(ERROR_DEVICE.to_string())
        .expect("Failed to create name string");
    unsafe {
        retval = spdk_vbdev_error_create(err_bdev_name_str.as_ptr()); // create the error bdev around it
    }
    assert_eq!(retval, 0);
}

async fn create_nexus() {
    let ch = vec![BDEVNAME1.to_string(), BDEV_EE_ERROR_DEVICE.to_string()];

    nexus_create(ERROR_COUNT_TEST_NEXUS, 64 * 1024 * 1024, None, &ch)
        .await
        .unwrap();
}

fn nexus_err_query_and_test(
    child_bdev: &str,
    io_type_flags: u32,
    expected_count: u32,
) {
    let nexus = nexus_lookup(ERROR_COUNT_TEST_NEXUS).unwrap();
    let count = nexus
        .error_record_query(
            child_bdev,
            io_type_flags,
            NexusErrStore::IO_FAILED_FLAG,
            1_000_000_000, // within the past 1 second
        )
        .expect("failed to query child");
    assert!(count.is_some()); // true if the error_store is enabled
    assert_eq!(count.unwrap(), expected_count);
}

async fn err_write_nexus() {
    let bdev = Bdev::lookup_by_name(ERROR_COUNT_TEST_NEXUS)
        .expect("failed to lookup nexus");
    let d = bdev
        .open(true)
        .expect("failed open bdev")
        .into_handle()
        .unwrap();
    let buf = d.dma_malloc(512).expect("failed to allocate buffer");

    let _ = d.write_at(0, &buf).await;
}

async fn err_read_nexus() {
    let bdev = Bdev::lookup_by_name(ERROR_COUNT_TEST_NEXUS)
        .expect("failed to lookup nexus");
    let d = bdev
        .open(true)
        .expect("failed open bdev")
        .into_handle()
        .unwrap();
    let mut buf = d.dma_malloc(512).expect("failed to allocate buffer");

    let _ = d.read_at(0, &mut buf).await;
}

fn reactor_pause_millis(milliseconds: u64) {
    let (s, r) = unbounded::<()>();
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(milliseconds));
        s.send(())
    });
    reactor_poll!(r);
}
