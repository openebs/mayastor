use io_engine::{
    bdev::{
        device_lookup,
        nexus::{nexus_create, nexus_lookup_mut},
    },
    core::{MayastorCliArgs, UntypedBdev},
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
    let nvmf_devs = mayastor
        .spawn(async move {
            let children = [
                format!(
                    "nvmf://{}:8420/nqn.2019-05.io.openebs:disk0",
                    hdls[0].endpoint.ip()
                ),
                format!(
                    "nvmf://{}:8420/nqn.2019-05.io.openebs:disk0",
                    hdls[1].endpoint.ip()
                ),
            ];

            nexus_create("foo", 1024 * 1024 * 50, None, &children)
                .await
                .unwrap();

            let mut nexus = nexus_lookup_mut("foo").unwrap();

            // Get NVMf device names for all nexus children for further lookup.
            children
                .iter()
                .map(|n| {
                    nexus
                        .as_mut()
                        .get_child_by_name(n)
                        .unwrap()
                        .get_device()
                        .unwrap()
                        .device_name()
                })
                .collect::<Vec<String>>()
        })
        .await;

    // why not
    mayastor
        .spawn(async {
            bdev_create("malloc:///malloc0?size_mb=64").await.unwrap();
        })
        .await;

    let bdevs = mayastor
        .spawn(async {
            UntypedBdev::bdev_first()
                .unwrap()
                .into_iter()
                .map(|b| b.name().to_string())
                .collect::<Vec<String>>()
        })
        .await;

    // In total there should be 4 devices: 2 BDEV-based and 2 NVMF based.
    // However, since NVMF devices can't be enumerated by libspdk, we see only 2
    // such devices here.
    assert_eq!(bdevs.len(), 2);

    // In addition, we should locate 2 NVMF devices.
    for d in nvmf_devs.iter() {
        device_lookup(d).expect("Can't lookup NVMf device");
    }
}
