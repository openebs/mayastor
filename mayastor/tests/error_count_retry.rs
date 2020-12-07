pub use common::error_bdev::{
    create_error_bdev,
    inject_error,
    SPDK_BDEV_IO_TYPE_WRITE,
    VBDEV_IO_FAILURE,
};
use mayastor::{
    bdev::{nexus_create, nexus_lookup, NexusStatus},
    core::{Bdev, MayastorCliArgs},
    subsys::Config,
};

use common::MayastorTest;
use once_cell::sync::OnceCell;

pub mod common;

static YAML_CONFIG_FILE: &str = "/tmp/error_retry_test.yaml";
static MS: OnceCell<MayastorTest> = OnceCell::new();

static NON_ERROR_DISK: &str = "/tmp/non_error.img";
static ERROR_DISK: &str = "/tmp/error.img";
static NON_ERROR_BASE_BDEV: &str = "aio:///tmp/non_error.img?blk_size=512";

fn mayastor() -> &'static MayastorTest<'static> {
    let mut config = Config::default();
    config.err_store_opts.max_io_attempts = 2;
    config.write(YAML_CONFIG_FILE).unwrap();

    let ms = MS.get_or_init(|| {
        MayastorTest::new(MayastorCliArgs {
            mayastor_config: Some(YAML_CONFIG_FILE.to_string()),
            reactor_mask: "0x3".to_string(),
            ..Default::default()
        })
    });
    &ms
}

#[tokio::test]
async fn nexus_retry_child_write_succeed_test() {
    let nexus_name = "error_retry_write_succeed";
    let error_device = "error_device_write_succeed";
    let ee_error_device = format!("EE_{}", error_device);
    let bdev_ee_error_device = format!("bdev:///{}", ee_error_device);

    common::truncate_file(ERROR_DISK, 64 * 1024);
    common::truncate_file(NON_ERROR_DISK, 64 * 1024);

    mayastor()
        .spawn(async move {
            create_error_bdev(error_device, ERROR_DISK);
            create_nexus(
                nexus_name,
                &bdev_ee_error_device,
                &NON_ERROR_BASE_BDEV,
            )
            .await;

            check_nexus_state_is(nexus_name, NexusStatus::Online);

            inject_error(
                &ee_error_device,
                SPDK_BDEV_IO_TYPE_WRITE,
                VBDEV_IO_FAILURE,
                1,
            );

            err_write_nexus(nexus_name, true).await; //should succeed, 2 attempts vs 1 error
            check_nexus_state_is(nexus_name, NexusStatus::Degraded);
            delete_nexus(nexus_name).await;
        })
        .await;

    common::delete_file(&[ERROR_DISK.to_string()]);
    common::delete_file(&[NON_ERROR_DISK.to_string()]);
    common::delete_file(&[YAML_CONFIG_FILE.to_string()]);
}

#[tokio::test]
async fn nexus_retry_child_write_fail_test() {
    let nexus_name = "error_retry_write_fail";
    let error_device = "error_device_write_fail";
    let ee_error_device = format!("EE_{}", error_device);
    let bdev_ee_error_device = format!("bdev:///{}", ee_error_device);

    common::truncate_file(ERROR_DISK, 64 * 1024);
    common::truncate_file(NON_ERROR_DISK, 64 * 1024);

    mayastor()
        .spawn(async move {
            create_error_bdev(error_device, ERROR_DISK);
            create_nexus(
                nexus_name,
                &bdev_ee_error_device,
                &NON_ERROR_BASE_BDEV,
            )
            .await;
            check_nexus_state_is(nexus_name, NexusStatus::Online);

            inject_error(
                &ee_error_device,
                SPDK_BDEV_IO_TYPE_WRITE,
                VBDEV_IO_FAILURE,
                2,
            );

            err_write_nexus(nexus_name, false).await; //should fail, 2 attempts vs 2 errors
            check_nexus_state_is(nexus_name, NexusStatus::Degraded);
            delete_nexus(nexus_name).await;
        })
        .await;

    common::delete_file(&[ERROR_DISK.to_string()]);
    common::delete_file(&[NON_ERROR_DISK.to_string()]);
    common::delete_file(&[YAML_CONFIG_FILE.to_string()]);
}

fn check_nexus_state_is(name: &str, expected_status: NexusStatus) {
    let nexus = nexus_lookup(name).unwrap();
    assert_eq!(nexus.status(), expected_status);
}

async fn create_nexus(name: &str, err_dev: &str, dev: &str) {
    let ch = vec![err_dev.to_string(), dev.to_string()];

    nexus_create(&name.to_string(), 64 * 1024 * 1024, None, &ch)
        .await
        .unwrap();
}

async fn delete_nexus(name: &str) {
    let n = nexus_lookup(name).unwrap();
    n.destroy().await.unwrap();
}

async fn err_write_nexus(name: &str, succeed: bool) {
    let bdev = Bdev::lookup_by_name(name).expect("failed to lookup nexus");
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
