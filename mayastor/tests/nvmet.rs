use common::compose::MayastorTest;
use mayastor::core::{Bdev, MayastorCliArgs};

use mayastor::bdev::nexus_create;

pub mod common;
async fn create_nexus() {
    nexus_create(
        "nexus0",
        250 * 1024 * 1024 * 1024,
        None,
        &["nvmf://127.0.0.1/replica1".to_string()],
    )
    .await
    .unwrap();
}

async fn bdev_info() {
    let bdev = Bdev::bdev_first().unwrap();
    dbg!(bdev);
}

#[ignore]
#[tokio::test]
async fn nvmet_nexus_test() {
    std::env::set_var("NEXUS_LABEL_IGNORE_ERRORS", "1");
    let ms = MayastorTest::new(MayastorCliArgs {
        reactor_mask: 0x3.to_string(),
        no_pci: true,
        grpc_endpoint: "0.0.0.0".to_string(),
        ..Default::default()
    });

    ms.spawn(create_nexus()).await;
    ms.spawn(bdev_info()).await;
}
