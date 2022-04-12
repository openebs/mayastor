use std::{pin::Pin, time::Duration};

use io_engine::core::{Cores, MayastorCliArgs, Mthread, Share, UntypedBdev};

pub mod common;
use common::MayastorTest;
use io_engine::nexus_uri::bdev_create;

async fn mayastor_to_runtime() {
    // the future is created on mayastor and send to tokio. So assert we are
    // running on something that is
    assert_eq!(Cores::current(), Cores::first());
    assert!(Mthread::current().is_some());

    // now spawn something
    io_engine::core::runtime::spawn(
        // spawn a future to send something back to mayastor
        runtime_to_mayastor(),
    );
}

async fn runtime_to_mayastor() {
    assert_eq!(Cores::current(), u32::MAX);
    assert_eq!(Mthread::current(), None);

    // we should not have a core here
    assert_eq!(Cores::current(), u32::MAX);
    tokio::time::sleep(Duration::from_micros(400)).await;
    // simulate we perform some work

    let st = Mthread::get_init();
    let rx = st
        .spawn_local(async move {
            let mut bdev = UntypedBdev::lookup_by_name("malloc0").unwrap();
            let bdev = Pin::new(&mut bdev);
            bdev.share_nvmf(None).await.unwrap();
        })
        .unwrap();
    let _ = rx.await;
}

fn running_on_thread() {
    assert_eq!(Cores::current(), u32::MAX);
    assert_eq!(Mthread::current(), None);
    std::thread::sleep(Duration::from_secs(2));
}

#[tokio::test]
async fn thread_tokio() {
    let args = MayastorCliArgs {
        reactor_mask: "0x3".into(),
        ..Default::default()
    };

    let ms = MayastorTest::new(args);

    let st = Mthread::get_init();
    let name = "malloc:///malloc0?size_mb=4";
    let rx = st.spawn_local(async move { bdev_create(name).await });

    ms.send(mayastor_to_runtime());

    rx.unwrap().await.unwrap().unwrap();
    let th = io_engine::core::runtime::spawn_blocking(running_on_thread);
    tokio::time::sleep(Duration::from_secs(1)).await;
    th.await.unwrap();
}
