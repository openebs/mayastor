use crossbeam::channel::unbounded;

pub mod common;

use mayastor::{
    bdev::nexus_lookup,
    core::{MayastorCliArgs, MayastorEnvironment, Reactor},
    replicas::rebuild::RebuildState,
};

use rpc::mayastor::ShareProtocolNexus;

const NEXUS_NAME: &str = "rebuild_test_nexus";
const NEXUS_SIZE: u64 = 10 * 1024 * 1024; // 10MiB
const MAX_CHILDREN: u64 = 16;

fn test_ini() {
    test_init!();
    for i in 0 .. MAX_CHILDREN {
        common::delete_file(&[get_disk(i)]);
        common::truncate_file_bytes(&get_disk(i), NEXUS_SIZE);
    }
}
fn test_fini() {
    //mayastor_env_stop(0);
    for i in 0 .. MAX_CHILDREN {
        common::delete_file(&[get_disk(i)]);
    }
}

fn get_disk(number: u64) -> String {
    format!("/tmp/disk{}.img", number)
}
fn get_dev(number: u64) -> String {
    format!("aio://{}?blk_size=512", get_disk(number))
}

#[test]
fn rebuild_test() {
    test_ini();

    Reactor::block_on(async {
        nexus_create(1).await;
        nexus_add_child(1, true).await;
        nexus_lookup(NEXUS_NAME).unwrap().destroy().await.unwrap();
    });

    test_fini();
}

#[test]
// test the rebuild flag of the add_child operation
fn rebuild_test_add() {
    test_ini();

    Reactor::block_on(async {
        nexus_create(1).await;
        let nexus = nexus_lookup(NEXUS_NAME).unwrap();

        nexus.add_child(&get_dev(1), true).await.unwrap();
        nexus
            .start_rebuild(&get_dev(1))
            .expect_err("rebuild expected to be present");
        nexus_test_child(1).await;

        nexus.add_child(&get_dev(2), false).await.unwrap();
        let _ = nexus
            .start_rebuild(&get_dev(2))
            .expect("rebuild not expected to be present");

        nexus_lookup(NEXUS_NAME).unwrap().destroy().await.unwrap();
    });

    test_fini();
}

#[test]
fn rebuild_progress() {
    test_ini();

    async fn test_progress(polls: u64, progress: u64) -> u64 {
        let nexus = nexus_lookup(NEXUS_NAME).unwrap();
        nexus.resume_rebuild(&get_dev(1)).await.unwrap();
        // { polls } to poll with an expr rather than an ident
        reactor_poll!({ polls });
        nexus.pause_rebuild(&get_dev(1)).await.unwrap();
        common::wait_for_rebuild(
            get_dev(1),
            RebuildState::Paused,
            std::time::Duration::from_millis(100),
        );
        let p = nexus.get_rebuild_progress(&get_dev(1)).unwrap();
        assert!(p.progress >= progress);
        p.progress
    };

    Reactor::block_on(async {
        nexus_create(1).await;
        nexus_add_child(1, false).await;
        // naive check to see if progress is being made
        let mut progress = 0;
        for _ in 0 .. 10 {
            progress = test_progress(50, progress).await;
        }
        nexus_lookup(NEXUS_NAME).unwrap().destroy().await.unwrap();
    });

    test_fini();
}

#[test]
fn rebuild_child_faulted() {
    test_ini();

    Reactor::block_on(async move {
        nexus_create(2).await;

        let nexus = nexus_lookup(NEXUS_NAME).unwrap();
        nexus
            .start_rebuild(&get_dev(1))
            .expect_err("Rebuild only degraded children!");

        nexus.remove_child(&get_dev(1)).await.unwrap();
        assert_eq!(nexus.children.len(), 1);
        nexus
            .start_rebuild(&get_dev(0))
            .expect_err("Cannot rebuild from the same child");

        nexus.destroy().await.unwrap();
    });

    test_fini();
}

#[test]
fn rebuild_dst_removal() {
    test_ini();

    Reactor::block_on(async move {
        let new_child = 2;
        nexus_create(new_child).await;
        nexus_add_child(new_child, false).await;

        let nexus = nexus_lookup(NEXUS_NAME).unwrap();
        nexus.pause_rebuild(&get_dev(new_child)).await.unwrap();
        nexus.remove_child(&get_dev(new_child)).await.unwrap();

        nexus.destroy().await.unwrap();
    });

    test_fini();
}

#[test]
fn rebuild_src_removal() {
    test_ini();

    Reactor::block_on(async move {
        let new_child = 2;
        assert!(new_child > 1);
        nexus_create(new_child).await;
        nexus_add_child(new_child, false).await;

        let nexus = nexus_lookup(NEXUS_NAME).unwrap();
        nexus.pause_rebuild(&get_dev(new_child)).await.unwrap();
        nexus.remove_child(&get_dev(0)).await.unwrap();

        // tests if new_child which had it's original rebuild src removed
        // ended up being rebuilt successfully
        nexus_test_child(new_child).await;

        nexus.destroy().await.unwrap();
    });

    test_fini();
}

async fn nexus_create(children: u64) {
    let mut ch = Vec::new();
    for i in 0 .. children {
        ch.push(get_dev(i));
    }

    mayastor::bdev::nexus_create(NEXUS_NAME, NEXUS_SIZE, None, &ch)
        .await
        .unwrap();

    let nexus = nexus_lookup(NEXUS_NAME).unwrap();
    let device = common::device_path_from_uri(
        nexus
            .share(ShareProtocolNexus::NexusNbd, None)
            .await
            .unwrap(),
    );

    let nexus_device = device.clone();
    let (s, r) = unbounded::<String>();
    std::thread::spawn(move || {
        s.send(common::dd_urandom_blkdev(&nexus_device))
    });
    reactor_poll!(r);

    let (s, r) = unbounded::<String>();
    std::thread::spawn(move || {
        s.send(common::compare_nexus_device(&device, &get_disk(0), true))
    });
    reactor_poll!(r);
}

async fn nexus_add_child(new_child: u64, wait: bool) {
    let nexus = nexus_lookup(NEXUS_NAME).unwrap();

    nexus.add_child(&get_dev(new_child), true).await.unwrap();

    if wait {
        common::wait_for_rebuild(
            get_dev(new_child),
            RebuildState::Completed,
            std::time::Duration::from_secs(10),
        );

        nexus_test_child(new_child).await;
    } else {
        // allows for the rebuild to start running (future run by the reactor)
        reactor_poll!(2);
    }
}

async fn nexus_test_child(child: u64) {
    common::wait_for_rebuild(
        get_dev(child),
        RebuildState::Completed,
        std::time::Duration::from_secs(10),
    );

    let (s, r) = unbounded::<String>();
    std::thread::spawn(move || {
        s.send(common::compare_devices(
            &get_disk(0),
            &get_disk(child),
            true,
        ))
    });
    reactor_poll!(r);
}
