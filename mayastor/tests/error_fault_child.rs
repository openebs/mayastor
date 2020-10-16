pub use common::error_bdev::{
    create_error_bdev,
    inject_error,
    SPDK_BDEV_IO_TYPE_READ,
    SPDK_BDEV_IO_TYPE_WRITE,
    VBDEV_IO_FAILURE,
};
use mayastor::{
    bdev::{ActionType, nexus_create, nexus_lookup, NexusStatus},
    core::{Bdev, MayastorCliArgs},
    subsys::Config,
};

pub mod common;

static ERROR_COUNT_TEST_NEXUS: &str = "error_fault_child_test_nexus";

static DISKNAME1: &str = "/tmp/disk1.img";
static BDEVNAME1: &str = "aio:///tmp/disk1.img?blk_size=512";

static DISKNAME2: &str = "/tmp/disk2.img";

static ERROR_DEVICE: &str = "error_device";
static EE_ERROR_DEVICE: &str = "EE_error_device";
// The prefix is added by the vbdev_error module
static BDEV_EE_ERROR_DEVICE: &str = "bdev:///EE_error_device";

static YAML_CONFIG_FILE: &str = "/tmp/error_fault_child_test_nexus.yaml";

#[tokio::test]
async fn nexus_fault_child_test() {
    common::truncate_file(DISKNAME1, 64 * 1024);
    common::truncate_file(DISKNAME2, 64 * 1024);

    let mut config = Config::default();
    config.err_store_opts.enable_err_store = true;
    config.err_store_opts.err_store_size = 256;
    config.err_store_opts.action = ActionType::Fault;
    config.err_store_opts.retention_ns = 1_000_000_000;
    config.err_store_opts.max_errors = 4;

    config.write(YAML_CONFIG_FILE).unwrap();
    let ms = common::MayastorTest::new(MayastorCliArgs {
        mayastor_config: Some(YAML_CONFIG_FILE.to_string()),
        reactor_mask: "0x3".to_string(),
        ..Default::default()
    });

    ms.spawn(async {
        create_error_bdev(ERROR_DEVICE, DISKNAME2);
        create_nexus().await;

        check_nexus_state_is(NexusStatus::Online);

        inject_error(
            EE_ERROR_DEVICE,
            SPDK_BDEV_IO_TYPE_READ,
            VBDEV_IO_FAILURE,
            10,
        );
        inject_error(
            EE_ERROR_DEVICE,
            SPDK_BDEV_IO_TYPE_WRITE,
            VBDEV_IO_FAILURE,
            10,
        );

        for _ in 0..3 {
            err_read_nexus_both(false).await;
            common::reactor_run_millis(1);
        }
        for _ in 0..2 {
            // the second iteration causes the error count to exceed the max no
            // of retry errors (4) for the read and causes the child to be
            // removed
            err_read_nexus_both(false).await;
            common::reactor_run_millis(1);
        }
    })
        .await;

    // error child should be removed from the IO path here

    ms.spawn(async { check_nexus_state_is(NexusStatus::Degraded) })
        .await;

    ms.spawn(async {
        err_read_nexus_both(true).await; // should succeed because both IOs go to the remaining child
        err_write_nexus(true).await; // should succeed because the IO goes to
        // the remaining child
    })
        .await;

    ms.spawn(async {
        delete_nexus().await;
    })
        .await;

    common::delete_file(&[DISKNAME1.to_string()]);
    common::delete_file(&[DISKNAME2.to_string()]);
    common::delete_file(&[YAML_CONFIG_FILE.to_string()]);
}

fn check_nexus_state_is(expected_status: NexusStatus) {
    let nexus = nexus_lookup(ERROR_COUNT_TEST_NEXUS).unwrap();
    assert_eq!(nexus.status(), expected_status);
}

async fn create_nexus() {
    let ch = vec![BDEV_EE_ERROR_DEVICE.to_string(), BDEVNAME1.to_string()];

    nexus_create(ERROR_COUNT_TEST_NEXUS, 64 * 1024 * 1024, None, &ch)
        .await
        .unwrap();
}

async fn delete_nexus() {
    let n = nexus_lookup(ERROR_COUNT_TEST_NEXUS).unwrap();
    n.destroy().await.unwrap();
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

async fn err_read_nexus_both(succeed: bool) {
    let res1 = err_read_nexus().await;
    let res2 = err_read_nexus().await;

    if succeed {
        assert!(res1 && res2); // both succeeded
    } else {
        assert_ne!(res1, res2); // one succeeded, one failed
    }
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
