use io_engine::{
    bdev::nexus::{nexus_create, nexus_lookup_mut},
    core::MayastorCliArgs,
};
use rpc::mayastor::{BdevShareRequest, BdevUri, Null};

pub mod common;
use common::{compose::Builder, MayastorTest};

static NEXUS_NAME: &str = "child_location_nexus";

#[tokio::test]
async fn child_location() {
    // create a new composeTest
    let test = Builder::new()
        .name("child_location_test")
        .network("10.1.0.0/16")
        .add_container("ms1")
        .with_clean(true)
        .build()
        .await
        .unwrap();

    // Use GRPC handles to invoke methods on containers
    let mut hdls = test.grpc_handles().await.unwrap();

    // Create and share a bdev over nvmf
    hdls[0].bdev.list(Null {}).await.unwrap();
    hdls[0]
        .bdev
        .create(BdevUri {
            uri: "malloc:///disk0?size_mb=100".into(),
        })
        .await
        .unwrap();
    hdls[0]
        .bdev
        .share(BdevShareRequest {
            name: "disk0".into(),
            proto: "nvmf".into(),
        })
        .await
        .unwrap();

    let mayastor = MayastorTest::new(MayastorCliArgs::default());
    mayastor
        .spawn(async move {
            // Create a nexus with a local child, and one remote child
            // (exported over nvmf).
            nexus_create(
                NEXUS_NAME,
                1024 * 1024 * 50,
                None,
                &[
                    "malloc:///malloc0?blk_size=512&size_mb=100".into(),
                    format!(
                        "nvmf://{}:8420/nqn.2019-05.io.openebs:disk0",
                        hdls[0].endpoint.ip()
                    ),
                ],
            )
            .await
            .unwrap();

            let nexus =
                nexus_lookup_mut(NEXUS_NAME).expect("Failed to find nexus");
            let children = &nexus.children;
            assert_eq!(children.len(), 2);
            assert!(children[0].is_local().unwrap());
            assert!(!children[1].is_local().unwrap());
        })
        .await;
}
