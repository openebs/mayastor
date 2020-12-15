use common::{compose::Builder, MayastorTest};
use mayastor::{
    bdev::{nexus_create, nexus_lookup, NexusStatus},
    core::MayastorCliArgs,
    subsys::{Config, NvmeBdevOpts},
};
use rpc::mayastor::{BdevShareRequest, BdevUri, Null, ShareProtocolNexus};
use std::process::{Command, Stdio};
use tokio::time::Duration;

pub mod common;
static NXNAME: &str = "nexus";

#[tokio::test]
async fn replica_stop_cont() {
    // Use a shorter timeouts than the default to reduce test runtime
    Config::get_or_init(|| Config {
        nvme_bdev_opts: NvmeBdevOpts {
            action_on_timeout: 2,
            timeout_us: 5_000_000,
            keep_alive_timeout_ms: 5_000,
            retry_count: 2,
            ..Default::default()
        },
        ..Default::default()
    })
    .apply();
    let test = Builder::new()
        .name("cargo-test")
        .network("10.1.0.0/16")
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

    // create a nexus with the remote replica as its child
    mayastor
        .spawn(async move {
            nexus_create(
                NXNAME,
                1024 * 1024 * 50,
                None,
                &[format!(
                    "nvmf://{}:8420/nqn.2019-05.io.openebs:disk0",
                    hdls[0].endpoint.ip()
                )],
            )
            .await
            .unwrap();
            nexus_lookup(&NXNAME)
                .unwrap()
                .share(ShareProtocolNexus::NexusNvmf, None)
                .await
                .expect("should publish nexus over nvmf");
        })
        .await;

    test.pause("ms1").await.unwrap();
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    for i in 1 .. 6 {
        ticker.tick().await;
        println!("waiting for the container to be fully suspended... {}/5", i);
    }

    // initiate the read and leave it in the background to time out
    let nxuri =
        format!("nvmf://127.0.0.1:8420/nqn.2019-05.io.openebs:{}", NXNAME);
    Command::new("../target/debug/initiator")
        .args(&[&nxuri, "read", "/tmp/tmpread"])
        .stdout(Stdio::piped())
        .spawn()
        .expect("should send read from initiator");

    println!("IO submitted unfreezing container...");

    // KATO is 5s, wait at least that long
    let n = 10;
    for i in 1 ..= n {
        ticker.tick().await;
        println!("unfreeze delay... {}/{}", i, n);
    }
    test.thaw("ms1").await.unwrap();
    println!("container thawed");

    // unshare the nexus while its status is faulted
    mayastor
        .spawn(async move {
            assert_eq!(
                nexus_lookup(&NXNAME).unwrap().status(),
                NexusStatus::Faulted,
            );
            nexus_lookup(&NXNAME)
                .unwrap()
                .unshare_nexus()
                .await
                .expect("should unpublish nexus");
        })
        .await;
}
