#![allow(unused_assignments)]

use common::{bdev_io, compose::Builder, MayastorTest};
use mayastor::{
    bdev::{nexus_create, nexus_lookup},
    core::MayastorCliArgs,
};
use rpc::mayastor::{BdevShareRequest, BdevUri, Null};
use tokio::time::Duration;

pub mod common;
static NXNAME: &str = "nexus";

#[ignore]
#[tokio::test]
async fn replica_stop_cont() {
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
    }

    let mayastor = MayastorTest::new(MayastorCliArgs::default());

    mayastor
        .spawn(async move {
            nexus_create(
                NXNAME,
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
            .unwrap();
            bdev_io::write_some(NXNAME, 0, 0xff).await.unwrap();
            bdev_io::read_some(NXNAME, 0, 0xff).await.unwrap();
        })
        .await;

    test.pause("ms1").await.unwrap();
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    for i in 1 .. 6 {
        ticker.tick().await;
        println!("waiting for the container to be fully suspended... {}/5", i);
    }

    mayastor.send(async {
        // we do not determine if the IO completed with an error or not just
        // that it completes.
        let _ = dbg!(bdev_io::read_some(NXNAME, 0, 0xff).await);
        let _ = dbg!(bdev_io::read_some(NXNAME, 0, 0xff).await);
    });

    println!("IO submitted unfreezing container...");

    for i in 1 .. 6 {
        ticker.tick().await;
        println!("unfreeze delay... {}/5", i);
    }
    test.thaw("ms1").await.unwrap();
    println!("container thawed");
    mayastor
        .spawn(async {
            let nexus = nexus_lookup(NXNAME).unwrap();
            nexus.destroy().await.unwrap();
        })
        .await;
}
