//!
//! At a high level this is what is tested during
//! this run. For each core we are assigned we will
//! start a job
//!
//!
//! +------------+      +-------------------------+
//! |            |      |                         |
//! |    job     |      |       +--nvmf----> MS1  |
//! |            |      |       |                 |
//! +------------+      +-------------------------+
//!                             |
//! +------------+      +-------------------------+
//! |            |      |       |                 |
//! |    nvmf    |      |       +--nvmf----> MS2  |
//  |            |      |       |                 |
//  +------------+      +-------------------------+
//!       |                     |
//!       |             +-------------------------+
//!       |             |                         |
//!       |             |       |                 |
//!       +-+nvmf------>+ nexus +--loop----> MS3  |
//!                     |                         |
//!                     +-------------------------+
//!
//!
//! The idea is that we then "hot remove" targets while
//! the nexus is still able to process IO.
//!
//!
//! When we encounter an IO problem, we must reconfigure all cores, (unless we
//! use single cores of course) and this multi core reconfiguration is what we
//! are trying to test here, and so we require a certain amount of cores to test
//! this to begin with. Also, typically, no more than one mayastor instance will
//! be bound to a particular core. As such we "spread" out cores as much as
//! possible.
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

use once_cell::sync::OnceCell;

const NEXUS_UUID: &str = "00000000-0000-0000-0000-000000000001";

use common::compose::{Builder, ComposeTest, MayastorTest};
use mayastor::{
    core::{
        io_driver,
        io_driver::JobQueue,
        Bdev,
        Cores,
        MayastorCliArgs,
        SIG_RECEIVED,
    },
    nexus_uri::bdev_create,
};
use rpc::mayastor::{
    BdevShareRequest,
    BdevUri,
    CreateNexusRequest,
    CreateReply,
    ListNexusReply,
    Null,
    PublishNexusRequest,
};

use composer::Binary;
use mayastor::subsys::{Config, NvmeBdevOpts};
use tokio::time::interval;

pub mod common;

static MAYASTOR: OnceCell<MayastorTest> = OnceCell::new();
static DOCKER_COMPOSE: OnceCell<ComposeTest> = OnceCell::new();

/// create a malloc bdev and export them over nvmf, returns the URI of the
/// constructed target.
async fn create_target(container: &str) -> String {
    let mut h = DOCKER_COMPOSE
        .get()
        .unwrap()
        .grpc_handle(container)
        .await
        .unwrap();
    h.bdev
        .create(BdevUri {
            uri: "malloc:///disk0?size_mb=64".into(),
        })
        .await
        .unwrap();
    // share it over nvmf
    let ep = h
        .bdev
        .share(BdevShareRequest {
            name: "disk0".into(),
            proto: "nvmf".into(),
        })
        .await
        .unwrap();

    DOCKER_COMPOSE.get().unwrap().logs_all().await.unwrap();
    ep.into_inner().uri
}

/// create a local malloc bdev, and then use it to create a nexus with the
/// remote targets added. This reflects the current approach where we have
/// children as: bdev:/// and nvmf:// we really should get rid of this
/// asymmetrical composition if we can.
async fn create_nexus(container: &str, mut kiddos: Vec<String>) -> String {
    let mut h = DOCKER_COMPOSE
        .get()
        .unwrap()
        .grpc_handle(container)
        .await
        .unwrap();

    let bdev = h
        .bdev
        .create(BdevUri {
            uri: "malloc:///disk0?size_mb=64".into(),
        })
        .await
        .unwrap();

    kiddos.push(format!("bdev:///{}", bdev.into_inner().name));

    h.mayastor
        .create_nexus(CreateNexusRequest {
            uuid: NEXUS_UUID.to_string(),
            size: 60 * 1024 * 1024,
            children: kiddos,
        })
        .await
        .unwrap();

    let endpoint = h
        .mayastor
        .publish_nexus(PublishNexusRequest {
            uuid: NEXUS_UUID.into(),
            share: 1,
            ..Default::default()
        })
        .await
        .unwrap();

    endpoint.into_inner().device_uri
}

/// create the work -- which means the nexus, replica's and the jobs. on return
/// IO flows through mayastorTest to all 3 containers
async fn create_topology(queue: Arc<JobQueue>) {
    let r1 = create_target("ms1").await;
    // let r2 = create_target("ms2").await;
    let endpoint = create_nexus("ms3", vec![r1]).await;

    // the nexus is running on ms3 we will use a 4th instance of mayastor to
    // create a nvmf bdev and push IO to it.

    let ms = MAYASTOR.get().unwrap();
    let bdev = ms
        .spawn(async move {
            let bdev = bdev_create(&endpoint).await.unwrap();
            bdev
        })
        .await;

    // start the workload by running a job on each core, this simulates the way
    // the targets use multiple cores
    ms.spawn(async move {
        for c in Cores::count() {
            let bdev = Bdev::lookup_by_name(&bdev).unwrap();
            let job = io_driver::Builder::new()
                .core(c)
                .bdev(bdev)
                .qd(8)
                .io_size(512)
                .build()
                .await;

            queue.start(job);
        }
    })
    .await;
}

async fn check_nexus<F: FnOnce(ListNexusReply)>(checker: F) {
    let mut ms3 = DOCKER_COMPOSE
        .get()
        .unwrap()
        .grpc_handle("ms3")
        .await
        .unwrap();
    let list = ms3.mayastor.list_nexus(Null {}).await.unwrap().into_inner();
    checker(list)
}

/// kill replica issues an unshare to the container which more or less amounts
/// to the same thing as killing the container.
async fn kill_replica(container: &str) {
    let t = DOCKER_COMPOSE.get().unwrap();
    let mut hdl = t.grpc_handle(container).await.unwrap();

    hdl.bdev
        .unshare(CreateReply {
            name: "disk0".to_string(),
        })
        .await
        .unwrap();
}

#[allow(dead_code)]
async fn pause_replica(container: &str) {
    let t = DOCKER_COMPOSE.get().unwrap();
    t.pause(container).await.unwrap();
}

#[allow(dead_code)]
async fn unpause_replica(container: &str) {
    let t = DOCKER_COMPOSE.get().unwrap();
    t.thaw(container).await.unwrap();
}

#[allow(dead_code)]
async fn kill_local(container: &str) {
    let t = DOCKER_COMPOSE.get().unwrap();
    let mut hdl = t.grpc_handle(container).await.unwrap();
    hdl.bdev
        .destroy(BdevUri {
            uri: "malloc:///disk0".into(),
        })
        .await
        .unwrap();
}

async fn list_bdevs(container: &str) {
    let mut h = DOCKER_COMPOSE
        .get()
        .unwrap()
        .grpc_handle(container)
        .await
        .unwrap();
    dbg!(h.bdev.list(Null {}).await.unwrap());
}

#[tokio::test]
async fn nvmf_bdev_test() {
    let queue = Arc::new(JobQueue::new());

    Config::get_or_init(|| Config {
        nvme_bdev_opts: NvmeBdevOpts {
            action_on_timeout: 2,
            timeout_us: 10_000_000,
            retry_count: 5,
            ..Default::default()
        },
        ..Default::default()
    })
    .apply();

    // create the docker containers each container started with two adjacent CPU
    // cores. ms1 will have core mask 0x3, ms3 will have core mask 0xc and so
    // on. the justification for this enormous core spreading is we want to
    // test and ensure that things do not interfere with one and other and
    // yet, still have at least more than one core such that we mimic
    // production workloads.
    //

    let compose = Builder::new()
        .name("cargo-test")
        .network("10.1.0.0/16")
        .add_container_bin(
            "ms1",
            Binary::from_dbg("mayastor").with_args(vec!["-l", "1"]),
        )
        // .add_container_bin(
        //     "ms2",
        //     Binary::from_dbg("mayastor").with_args(vec!["-l", "2"]),
        // )
        .add_container_bin(
            "ms3",
            Binary::from_dbg("mayastor").with_args(vec!["-l", "3"]),
        )
        .with_clean(true)
        .with_prune(true)
        .build()
        .await
        .unwrap();

    DOCKER_COMPOSE.set(compose).unwrap();
    // this is based on the number of containers above.
    let mask = format!("{:#01x}", (1 << 1) | (1 << 2));
    let ms = MayastorTest::new(MayastorCliArgs {
        reactor_mask: mask,
        no_pci: true,
        grpc_endpoint: "0.0.0.0".to_string(),
        ..Default::default()
    });

    let ms = MAYASTOR.get_or_init(|| ms);

    let mut ticker = interval(Duration::from_millis(1000));
    create_topology(Arc::clone(&queue)).await;

    list_bdevs("ms3").await;

    for i in 1 .. 10 {
        ticker.tick().await;
        if i == 5 {
            kill_replica("ms1").await;
        }

        ms.spawn(async {
            let bdev = Bdev::bdev_first().unwrap();
            dbg!(bdev.stats().await.unwrap());
        })
        .await;
        // ctrl was hit so exit the loop here
        if SIG_RECEIVED.load(Ordering::Relaxed) {
            break;
        }
    }

    check_nexus(|n| {
        n.nexus_list.iter().for_each(|n| {
            dbg!(n);
        });
    })
    .await;

    list_bdevs("ms3").await;
    DOCKER_COMPOSE.get().unwrap().logs("ms3").await.unwrap();

    queue.stop_all().await;
    ms.stop().await;
    DOCKER_COMPOSE.get().unwrap().down().await;
}
