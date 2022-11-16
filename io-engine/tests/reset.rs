use io_engine::{
    bdev::nexus::nexus_create,
    core::{MayastorCliArgs, UntypedBdevHandle},
};

pub mod common;
use common::{
    compose,
    compose::rpc::v0::{
        mayastor::{BdevShareRequest, BdevUri},
        GrpcConnect,
    },
};

#[tokio::test]
async fn nexus_reset_mirror() {
    common::composer_init();

    let test = compose::Builder::new()
        .name("cargo-test")
        .network("10.1.0.0/16")
        .unwrap()
        .add_container_dbg("ms2")
        .add_container_dbg("ms1")
        .with_clean(true)
        .build()
        .await
        .unwrap();

    let grpc = GrpcConnect::new(&test);

    let mut hdls = grpc.grpc_handles().await.unwrap();

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
                    ..Default::default()
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

            let bdev =
                UntypedBdevHandle::open("reset_test", true, true).unwrap();
            bdev.reset().await.unwrap();
        })
        .await
}
