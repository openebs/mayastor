use std::{sync::Mutex, time::Duration};

use crossbeam::channel::unbounded;
use once_cell::sync::Lazy;
use tracing::error;

use mayastor::{
    bdev::nexus_lookup,
    core::{MayastorCliArgs, MayastorEnvironment, Mthread, Reactor},
    rebuild::{RebuildJob, RebuildState},
};
use rpc::mayastor::ShareProtocolNexus;

pub mod common;
use common::wait_for_rebuild;

// each test `should` use a different nexus name to prevent clashing with
// one another. This allows the failed tests to `panic gracefully` improving
// the output log and allowing the CI to fail gracefully as well
static NEXUS_NAME: Lazy<Mutex<&str>> = Lazy::new(|| Mutex::new("Default"));
pub fn nexus_name() -> &'static str {
    &NEXUS_NAME.lock().unwrap()
}

static NEXUS_SIZE: u64 = 5 * 1024 * 1024; // 5MiB

// approximate on-disk metadata that will be written to the child by the nexus
const META_SIZE: u64 = 5 * 1024 * 1024; // 5MiB
const MAX_CHILDREN: u64 = 16;

fn test_ini(name: &'static str) {
    *NEXUS_NAME.lock().unwrap() = name;
    get_err_bdev().clear();

    test_init!();
    for i in 0 .. MAX_CHILDREN {
        common::delete_file(&[get_disk(i)]);
        common::truncate_file_bytes(&get_disk(i), NEXUS_SIZE + META_SIZE);
    }
}

fn test_fini() {
    //mayastor_env_stop(0);
    for i in 0 .. MAX_CHILDREN {
        common::delete_file(&[get_disk(i)]);
    }
}

fn get_err_bdev() -> &'static mut Vec<u64> {
    unsafe {
        static mut ERROR_DEVICE_INDEXES: Vec<u64> = Vec::<u64>::new();
        &mut ERROR_DEVICE_INDEXES
    }
}
fn get_disk(number: u64) -> String {
    if get_err_bdev().contains(&number) {
        format!("error_device{}", number)
    } else {
        format!("/tmp/{}-disk{}.img", nexus_name(), number)
    }
}
fn get_dev(number: u64) -> String {
    if get_err_bdev().contains(&number) {
        format!("bdev:///EE_error_device{}", number)
    } else {
        format!("aio://{}?blk_size=512", get_disk(number))
    }
}

async fn nexus_create(size: u64, children: u64, fill_random: bool) {
    let mut ch = Vec::new();
    for i in 0 .. children {
        ch.push(get_dev(i));
    }

    mayastor::bdev::nexus_create(nexus_name(), size, None, &ch)
        .await
        .unwrap();

    if fill_random {
        let device = nexus_share().await;
        let nexus_device = device.clone();
        let (s, r) = unbounded::<i32>();
        Mthread::spawn_unaffinitized(move || {
            s.send(common::dd_urandom_blkdev(&nexus_device))
        });
        let dd_result: i32;
        reactor_poll!(r, dd_result);
        assert_eq!(dd_result, 0, "Failed to fill nexus with random data");

        let (s, r) = unbounded::<String>();
        Mthread::spawn_unaffinitized(move || {
            s.send(common::compare_nexus_device(&device, &get_disk(0), true))
        });
        reactor_poll!(r);
    }
}

async fn nexus_share() -> String {
    let nexus = nexus_lookup(nexus_name()).unwrap();
    let device = common::device_path_from_uri(
        nexus
            .share(ShareProtocolNexus::NexusNbd, None)
            .await
            .unwrap(),
    );
    reactor_poll!(200);
    device
}

#[test]
fn rebuild_lookup() {
    test_ini("rebuild_lookup");

    Reactor::block_on(async move {
        let children = 6;
        nexus_create(NEXUS_SIZE, children, false).await;
        let nexus = nexus_lookup(nexus_name()).unwrap();
        nexus.add_child(&get_dev(children), true).await.unwrap();

        for child in 0 .. children {
            RebuildJob::lookup(&get_dev(child)).expect_err("Should not exist");

            RebuildJob::lookup_src(&get_dev(child))
                .iter()
                .inspect(|&job| {
                    error!(
                        "Job {:?} should be associated with src child {}",
                        job, child
                    );
                })
                .any(|_| panic!("Should not have found any jobs!"));
        }

        let _ = nexus.start_rebuild(&get_dev(children)).await.unwrap();
        for child in 0 .. children {
            RebuildJob::lookup(&get_dev(child))
                .expect_err("rebuild job not created yet");
        }
        let src = RebuildJob::lookup(&get_dev(children))
            .expect("now the job should exist")
            .source
            .clone();

        for child in 0 .. children {
            if get_dev(child) != src {
                RebuildJob::lookup_src(&get_dev(child))
                    .iter()
                    .filter(|s| s.destination != get_dev(child))
                    .inspect(|&job| {
                        error!(
                            "Job {:?} should be associated with src child {}",
                            job, child
                        );
                    })
                    .any(|_| panic!("Should not have found any jobs!"));
            }
        }

        assert_eq!(
            RebuildJob::lookup_src(&src)
                .iter()
                .inspect(|&job| {
                    assert_eq!(job.destination, get_dev(children));
                })
                .count(),
            1
        );

        // wait for the rebuild to start - and then pause it
        wait_for_rebuild(
            get_dev(children),
            RebuildState::Running,
            Duration::from_secs(1),
        );
        nexus.pause_rebuild(&get_dev(children)).await.unwrap();
        assert_eq!(RebuildJob::lookup_src(&src).len(), 1);

        nexus.add_child(&get_dev(children + 1), true).await.unwrap();
        let _ = nexus.start_rebuild(&get_dev(children + 1)).await.unwrap();
        assert_eq!(RebuildJob::lookup_src(&src).len(), 2);

        nexus.remove_child(&get_dev(children)).await.unwrap();
        nexus.remove_child(&get_dev(children + 1)).await.unwrap();
        nexus_lookup(nexus_name()).unwrap().destroy().await.unwrap();
    });

    test_fini();
}
