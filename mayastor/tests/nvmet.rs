use common::compose::MayastorTest;
use mayastor::core::{
    io_driver,
    mayastor_env_stop,
    Bdev,
    Cores,
    MayastorCliArgs,
    Share,
    SIG_RECEIVED,
};
static MAYASTOR: OnceCell<MayastorTest> = OnceCell::new();

use mayastor::{
    bdev::{nexus_create, nexus_lookup},
    core::io_driver::{IoType, JobQueue},
};
use once_cell::sync::OnceCell;
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

pub mod common;
async fn create_nexus() {
    let children = (1 ..= 3)
        .into_iter()
        .map(|i| format!("nvmf://127.0.0.1/replica{}", i))
        .collect::<Vec<String>>();

    nexus_create(
        "e1e27668-fbe1-4c8a-9108-513f6e44d342",
        250 * 1024 * 1024 * 1024,
        None,
        &children,
    )
    .await
    .unwrap();

    let nexus = nexus_lookup("e1e27668-fbe1-4c8a-9108-513f6e44d342").unwrap();
    nexus.share_nvmf().await;
}

async fn remove_child(index: usize) {
    let to_remove = format!("127.0.0.1/replica{}", index);
    let nexus = nexus_lookup("e1e27668-fbe1-4c8a-9108-513f6e44d342").unwrap();
    nexus.remove_child(&to_remove).await.unwrap()
}

async fn bdev_info() {
    let bdev = Bdev::bdev_first().unwrap();
    dbg!(bdev);
}

async fn start_workload(queue: Arc<JobQueue>) {
    let ms = MAYASTOR.get().unwrap();
    ms.spawn(async move {
        for c in Cores::count() {
            let bdev =
                Bdev::lookup_by_name("e1e27668-fbe1-4c8a-9108-513f6e44d342")
                    .unwrap();
            let job = io_driver::Builder::new()
                .core(c)
                .rw(IoType::WRITE)
                .bdev(bdev)
                .qd(64)
                .io_size(512)
                .build()
                .await;
            queue.start(job);
        }
    })
    .await;
}

#[tokio::test]
async fn nvmet_nexus_test() {
    std::env::set_var("NEXUS_LABEL_IGNORE_ERRORS", "1");
    std::env::set_var("MY_POD_IP", "192.168.1.4");
    let ms = MayastorTest::new(MayastorCliArgs {
        reactor_mask: 0xA.to_string(),
        no_pci: true,
        grpc_endpoint: "0.0.0.0".to_string(),
        ..Default::default()
    });
    let ms = MAYASTOR.get_or_init(|| ms);

    ms.spawn(create_nexus()).await;
    ms.spawn(bdev_info()).await;

    let mut ticker = tokio::time::interval(Duration::from_millis(1000));
    // ctrl was hit so exit the loop here
    loop {
        if SIG_RECEIVED.load(Ordering::Relaxed) {
            break;
        }

        ms.spawn(async {
            let bdev = Bdev::bdev_first().unwrap();
            println!("{:?}", bdev.stats().await.unwrap());
        })
        .await;
        ticker.tick().await;
    }

    ms.stop().await;
}
