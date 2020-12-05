use once_cell::sync::OnceCell;

use mayastor::{
    core::MayastorCliArgs,
    nexus_uri::{bdev_create, bdev_destroy},
};

pub mod common;
use common::compose::MayastorTest;

static MAYASTOR: OnceCell<MayastorTest> = OnceCell::new();

fn get_ms() -> &'static MayastorTest<'static> {
    let instance =
        MAYASTOR.get_or_init(|| MayastorTest::new(MayastorCliArgs::default()));
    &instance
}

#[tokio::test]
async fn nvmf_device_create_destroy() {
    let ms = get_ms();
    let url = "nvmx://172.16.175.130:8420/nqn.2019-05.io.openebs:disk0";

    ms.spawn(async move {
        let d = bdev_create(
            "nvmx://172.16.175.130:8420/nqn.2019-05.io.openebs:disk0",
        )
        .await
        .unwrap();
        println!("++ DEVICE: {:?}", d);

        // Destroy the device the  first time - should succeed.
        bdev_destroy(url).await.unwrap();

        // Destroy the device which is supposed to be already destroyed -
        // should fail.
        assert!(bdev_destroy(url).await.is_err());
    })
    .await;
}
