use io_engine::bdev::nexus::nexus_create;

use io_engine::core::{BdevHandle, MayastorCliArgs};
use rpc::mayastor::{BdevShareRequest, BdevUri};

pub mod common;
use common::compose;
#[tokio::test]
async fn nexus_reset_mirror() {
    let test = compose::Builder::new()
        .name("cargo-test")
        .network("10.1.0.0/16")
        .add_container("ms2")
        .add_container("ms1")
        .with_clean(true)
        .build()
        .await
        .unwrap();

    let mut hdls = test.grpc_handles().await.unwrap();

    let mut children: Vec<String> = Vec::new();
    for h in &mut hdls {
        h.bdev
            .create(BdevUri {
                uri: "malloc:///disk0?size_mb=100".into(),
            })
            .await
            .unwrap();
        children.push(
            h.bdev
                .share(BdevShareRequest {
                    name: "disk0".into(),
                    proto: "nvmf".into(),
                })
                .await
                .unwrap()
                .into_inner()
                .uri,
        )
    }
    let mayastor = common::MayastorTest::new(MayastorCliArgs::default());

    // test the reset
    mayastor
        .spawn(async move {
            nexus_create("reset_test", 1024 * 1024 * 50, None, &children)
                .await
                .unwrap();

            let bdev = BdevHandle::open("reset_test", true, true).unwrap();
            bdev.reset().await.unwrap();
        })
        .await
}
