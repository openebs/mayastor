use mayastor::{
    bdev::nexus_create,
    core::{Bdev, MayastorCliArgs},
    nexus_uri::bdev_create,
};
use rpc::mayastor::{BdevShareRequest, BdevUri, Null};

pub mod common;
use common::{compose::Builder, MayastorTest};

#[tokio::test]
async fn compose_up_down() {
    // create a new composeTest and run a basic example
    let test = Builder::new()
        .name("cargo-test")
        .network("10.1.0.0/16")
        .add_container("ms2")
        .add_container("ms1")
        .with_clean(true)
        .build()
        .await
        .unwrap();

    // get the handles if needed, to invoke methods to the containers
    let mut hdls = test.grpc_handles().await.unwrap();

    // create and share a bdev on each container
    for h in &mut hdls {
        h.bdev.list(Null {}).await.unwrap();
        h.bdev
            .create(BdevUri {
                uri: "malloc:///disk0?size_mb=100".into(),
            })
            .await
            .unwrap();
        h.bdev
            .share(BdevShareRequest {
                name: "disk0".into(),
                proto: "nvmf".into(),
            })
            .await
            .unwrap();

        test.logs(&h.name).await.unwrap();
    }

    // start mayastor and do something the container bdev, this will shutdown
    // on drop. The main thread will not block as it used too.
    let mayastor = MayastorTest::new(MayastorCliArgs::default());

    // create a nexus over the bdevs
    mayastor
        .spawn(async move {
            nexus_create(
                "foo",
                1024 * 1024 * 50,
                None,
                &[
                    format!(
                        "nvmf://{}:8420/nqn.2019-05.io.openebs:disk0",
                        hdls[0].endpoint.ip()
                    ),
                    format!(
                        "nvmf://{}:8420/nqn.2019-05.io.openebs:disk0",
                        hdls[1].endpoint.ip()
                    ),
                ],
            )
            .await
        })
        .await
        .unwrap();

    // why not
    mayastor
        .spawn(async {
            bdev_create("malloc:///malloc0?size_mb=100").await.unwrap();
        })
        .await;

    // this will not compile: -- as it should not compile as bdev is not !Send
    // let bdev = mayastor.spawn(async { Bdev::lookup_by_name("foo") }).await;

    let bdevs = mayastor
        .spawn(async {
            Bdev::bdev_first()
                .unwrap()
                .into_iter()
                .map(|b| b.name())
                .collect::<Vec<String>>()
        })
        .await;

    // should return 4 bdevs
    assert_eq!(bdevs.len(), 4);
}
