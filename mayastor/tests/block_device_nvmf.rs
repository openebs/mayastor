use once_cell::sync::OnceCell;

use mayastor::{
    bdev::{device_create, device_destroy, device_lookup, device_open},
    core::MayastorCliArgs,
};

pub mod common;
use common::compose::MayastorTest;
use uuid::Uuid;

static MAYASTOR: OnceCell<MayastorTest> = OnceCell::new();

//const MAYASTOR_CTRLR_TITLE: &str = "Mayastor NVMe controler";
//const MAYASTOR_NQN_PREFIX: &str = "nqn.2019-05.io.openebs:";

fn get_ms() -> &'static MayastorTest<'static> {
    let instance =
        MAYASTOR.get_or_init(|| MayastorTest::new(MayastorCliArgs::default()));
    &instance
}

async fn launch_instance() -> String {
    return "nvmf://172.16.175.130:8420/nqn.2019-05.io.openebs:disk0"
        .to_string();
}

#[tokio::test]
async fn nvmf_device_create_destroy() {
    let ms = get_ms();
    let url = "nvmf://172.16.175.130:8420/nqn.2019-05.io.openebs:disk0";

    ms.spawn(async move {
        let name1 = device_create(url).await.unwrap();

        // Check device properties for sanity.
        let bdev = device_lookup(&name1).unwrap();
        assert_eq!(bdev.product_name(), "NVMe disk");
        assert_eq!(bdev.driver_name(), "nvme");
        assert_eq!(bdev.device_name(), name1);

        assert_ne!(bdev.block_len(), 0);
        assert_ne!(bdev.num_blocks(), 0);
        assert_ne!(bdev.size_in_bytes(), 0);
        assert_eq!(bdev.block_len() * bdev.num_blocks(), bdev.size_in_bytes());

        Uuid::parse_str(&bdev.uuid()).unwrap();

        // Destroy the device the first time - should succeed.
        device_destroy(url).await.unwrap();

        // Destroy the device which is supposed to be already destroyed -
        // should fail.
        assert!(device_destroy(url).await.is_err());

        // Create the same device one more time - should succeed.
        let name2 = device_create(url).await.unwrap();

        // Destroy the device the second time - should succeed.
        device_destroy(url).await.unwrap();

        // Device paths should match.
        assert_eq!(name1, name2);
    })
    .await;
}

#[tokio::test]
async fn nvmf_device_identify_controller() {
    let ms = get_ms();
    let url = launch_instance().await;

    ms.spawn(async move {
        let name = device_create(&url).await.unwrap();
        let descr = device_open(&name, false).unwrap();
        let handle = descr.into_handle().unwrap();

        let _buf = handle.nvme_identify_ctrlr().await.unwrap();
    })
    .await;
}
