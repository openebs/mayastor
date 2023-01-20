use once_cell::sync::OnceCell;
use std::convert::TryFrom;

extern crate libnvme_rs;

use io_engine::{
    bdev::nexus::{nexus_create, nexus_lookup_mut},
    core::{MayastorCliArgs, Protocol},
};

pub mod common;
use common::compose::MayastorTest;

static DISKNAME1: &str = "/tmp/disk1.img";
static BDEVNAME1: &str = "aio:///tmp/disk1.img?blk_size=512";

static DISKNAME2: &str = "/tmp/disk2.img";
static BDEVNAME2: &str = "aio:///tmp/disk2.img?blk_size=512";

static MAYASTOR: OnceCell<MayastorTest> = OnceCell::new();

static DISK_SIZE_MB: u64 = 400;

macro_rules! prepare_storage {
    () => {
        common::delete_file(&[DISKNAME1.into(), DISKNAME2.into()]);
        common::truncate_file(DISKNAME1, DISK_SIZE_MB * 1024);
        common::truncate_file(DISKNAME2, DISK_SIZE_MB * 1024);
    };
}

fn get_ms() -> &'static MayastorTest<'static> {
    MAYASTOR.get_or_init(|| MayastorTest::new(MayastorCliArgs::default()))
}

async fn create_connected_nvmf_nexus(
    ms: &'static MayastorTest<'static>,
) -> (libnvme_rs::NvmeTarget, String) {
    let uri = ms
        .spawn(async {
            create_nexus().await;
            let nexus = nexus_lookup_mut("nexus").unwrap();
            nexus.share(Protocol::Nvmf, None).await.unwrap()
        })
        .await;

    // Create and connect NVMF target.
    let target = libnvme_rs::NvmeTarget::try_from(uri)
        .unwrap()
        .with_rand_hostnqn(true);
    target.connect().unwrap();
    let devices = target.block_devices(2).unwrap();

    assert_eq!(devices.len(), 1);
    (target, devices[0].to_string())
}

async fn mount_test(ms: &'static MayastorTest<'static>, fstype: &str) {
    let (target, nvmf_dev) = create_connected_nvmf_nexus(ms).await;

    // Create a filesystem with test file.
    assert!(common::mkfs(&nvmf_dev, fstype));
    let md5sum = match common::mount_and_write_file(&nvmf_dev) {
        Ok(r) => r,
        Err(e) => panic!("Failed to create test file: {}", e),
    };

    // Disconnect NVMF target, then unshare and destroy nexus.
    target.disconnect().unwrap();

    ms.spawn(async {
        let mut nexus = nexus_lookup_mut("nexus").unwrap();
        nexus.as_mut().unshare_nexus().await.unwrap();
        nexus.destroy().await.unwrap();
    })
    .await;

    /* Create 2 single-disk nexuses for every existing disk (already)
     * populated with test data file, and check overall data consistency
     * by accessing each disk separately via its own nexus.
     */
    ms.spawn(async {
        create_nexus_splitted().await;
    })
    .await;

    for n in ["left", "right"].iter() {
        let uri = ms
            .spawn(async move {
                let nexus = nexus_lookup_mut(n).unwrap();
                nexus.share(Protocol::Nvmf, None).await.unwrap()
            })
            .await;

        // Create and connect NVMF target.
        let target = libnvme_rs::NvmeTarget::try_from(uri)
            .unwrap()
            .with_rand_hostnqn(true);
        target.connect().unwrap();
        let devices = target.block_devices(2).unwrap();

        assert_eq!(devices.len(), 1);
        let nvmf_dev = &devices[0];
        let md5 = common::mount_and_get_md5(nvmf_dev).unwrap();

        assert_eq!(md5, md5sum);

        // Cleanup target.
        target.disconnect().unwrap();
        ms.spawn(async move {
            let mut nexus = nexus_lookup_mut(n).unwrap();
            nexus.as_mut().unshare_nexus().await.unwrap();
            nexus.destroy().await.unwrap();
        })
        .await;
    }
}

#[tokio::test]
async fn mount_fs_mirror() {
    let ms = get_ms();

    prepare_storage!();

    mount_test(ms, "xfs").await;
    mount_test(ms, "ext4").await;
    mount_test(ms, "btrfs").await;
}

#[tokio::test]
async fn mount_fs_multiple() {
    let ms = get_ms();

    prepare_storage!();
    let (target, nvmf_dev) = create_connected_nvmf_nexus(ms).await;

    for _i in 0 .. 10 {
        common::mount_umount(&nvmf_dev).unwrap();
    }

    target.disconnect().unwrap();
    ms.spawn(async move {
        let mut nexus = nexus_lookup_mut("nexus").unwrap();
        nexus.as_mut().unshare_nexus().await.unwrap();
        nexus.destroy().await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn mount_fn_fio() {
    let ms = get_ms();

    prepare_storage!();
    let (target, nvmf_dev) = create_connected_nvmf_nexus(ms).await;

    common::fio_run_verify(&nvmf_dev).unwrap();

    target.disconnect().unwrap();
    ms.spawn(async move {
        let mut nexus = nexus_lookup_mut("nexus").unwrap();
        nexus.as_mut().unshare_nexus().await.unwrap();
        nexus.destroy().await.unwrap();
    })
    .await;
}

async fn create_nexus() {
    let ch = vec![BDEVNAME1.to_string(), BDEVNAME2.to_string()];
    nexus_create("nexus", DISK_SIZE_MB * 1024 * 1024, None, &ch)
        .await
        .unwrap();
}

async fn create_nexus_splitted() {
    let ch = vec![BDEVNAME1.to_string()];
    nexus_create("left", DISK_SIZE_MB * 1024 * 1024, None, &ch)
        .await
        .unwrap();

    let ch = vec![BDEVNAME2.to_string()];
    nexus_create("right", DISK_SIZE_MB * 1024 * 1024, None, &ch)
        .await
        .unwrap();
}
