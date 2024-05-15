use io_engine::{
    bdev::nexus::{nexus_create, nexus_lookup_mut, NexusStatus},
    bdev_api::bdev_get_name,
    constants::NVME_NQN_PREFIX,
    core::{MayastorCliArgs, Protocol, UntypedBdev},
    subsys::{Config, NvmeBdevOpts},
};
use std::process::{Command, Stdio};
use tokio::time::Duration;

pub mod common;

use common::{
    compose::{
        rpc::v0::{
            mayastor::{BdevShareRequest, BdevUri, Null},
            GrpcConnect,
        },
        Builder,
    },
    MayastorTest,
};

static NXNAME: &str = "nexus";

#[tokio::test]
#[ignore]
async fn replica_stop_cont() {
    common::composer_init();

    // Use shorter timeouts than the defaults to reduce test runtime
    Config::get_or_init(|| Config {
        nvme_bdev_opts: NvmeBdevOpts {
            timeout_us: 5_000_000,
            keep_alive_timeout_ms: 5_000,
            transport_retry_count: 2,
            ..Default::default()
        },
        ..Default::default()
    })
    .apply();
    let test = Builder::new()
        .name("cargo-test")
        .network("10.1.0.0/16")
        .unwrap()
        .add_container_dbg("ms1")
        .with_clean(true)
        .build()
        .await
        .unwrap();

    let grpc = GrpcConnect::new(&test);

    // get the handles if needed, to invoke methods to the containers
    let mut hdls = grpc.grpc_handles().await.unwrap();

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
                ..Default::default()
            })
            .await
            .unwrap();
    }

    let mayastor = MayastorTest::new(MayastorCliArgs::default());

    // create a nexus with the remote replica as its child
    let child_uri = format!(
        "nvmf://{}:8420/{NVME_NQN_PREFIX}:disk0",
        hdls[0].endpoint.ip()
    );
    let c = child_uri.clone();
    mayastor
        .spawn(async move {
            nexus_create(NXNAME, 1024 * 1024 * 50, None, &[c.clone()])
                .await
                .unwrap();
            nexus_lookup_mut(NXNAME)
                .unwrap()
                .share(Protocol::Nvmf, None)
                .await
                .expect("should publish nexus over nvmf");
            assert!(
                UntypedBdev::lookup_by_name(&bdev_get_name(&c).unwrap())
                    .is_some(),
                "child bdev must exist"
            );
        })
        .await;

    test.pause("ms1").await.unwrap();
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    for i in 1 .. 6 {
        ticker.tick().await;
        println!("waiting for the container to be fully suspended... {i}/5");
    }

    // initiate the read and leave it in the background to time out
    let nxuri = format!("nvmf://127.0.0.1:8420/{NVME_NQN_PREFIX}:{NXNAME}");
    Command::new("../target/debug/initiator")
        .args([&nxuri, "read", "/tmp/tmpread"])
        .stdout(Stdio::piped())
        .spawn()
        .expect("should send read from initiator");

    println!("IO submitted unfreezing container...");

    // KATO is 5s, wait at least that long
    let n = 10;
    for i in 1 ..= n {
        ticker.tick().await;
        println!("unfreeze delay... {i}/{n}");
    }
    test.thaw("ms1").await.unwrap();
    println!("container thawed");

    // Wait for faulting to complete first
    ticker.tick().await;

    // with no child to send read to, io should still complete as failed
    let status = Command::new("../target/debug/initiator")
        .args([&nxuri, "read", "/tmp/tmpread"])
        .stdout(Stdio::piped())
        .status()
        .expect("should send read from initiator");
    assert!(!status.success());

    // unshare the nexus while its status is faulted
    let c = child_uri.clone();
    mayastor
        .spawn(async move {
            assert!(
                UntypedBdev::lookup_by_name(&bdev_get_name(&c).unwrap())
                    .is_none(),
                "child bdev must be destroyed"
            );
            let nx = nexus_lookup_mut(NXNAME).unwrap();
            assert_eq!(nx.status(), NexusStatus::Faulted);
            assert_eq!(nx.children().len(), 1, "nexus child must still exist");
            nx.unshare_nexus().await.expect("should unpublish nexus");
        })
        .await;
}
