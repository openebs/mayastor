use std::sync::Mutex;

use crossbeam::channel::unbounded;
use once_cell::sync::Lazy;

use common::error_bdev;
use mayastor::{
    bdev::{nexus_lookup, ChildStatus, VerboseError},
    core::{MayastorCliArgs, MayastorEnvironment, Mthread, Reactor},
    replicas::rebuild::{RebuildJob, RebuildState, SEGMENT_SIZE},
};
use rpc::mayastor::ShareProtocolNexus;

pub mod common;

// each test `should` use a different nexus name to prevent clashing with
// one another. This allows the failed tests to `panic gracefully` improving
// the output log and allowing the CI to fail gracefully as well
static NEXUS_NAME: Lazy<Mutex<&str>> = Lazy::new(|| Mutex::new("Default"));
pub fn nexus_name() -> &'static str {
    &NEXUS_NAME.lock().unwrap()
}

static NEXUS_SIZE: u64 = 5 * 1024 * 1024; // 10MiB

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
fn get_err_dev(index: u64) -> String {
    format!("EE_error_device{}", index)
}
fn set_err_dev(index: u64) {
    if !get_err_bdev().contains(&index) {
        let backing = get_disk(index);
        get_err_bdev().push(index);
        error_bdev::create_error_bdev(&get_disk(index), &backing);
    }
}
fn get_disk(number: u64) -> String {
    if get_err_bdev().contains(&number) {
        format!("error_device{}", number)
    } else {
        format!("/tmp/disk{}.img", number)
    }
}
fn get_dev(number: u64) -> String {
    if get_err_bdev().contains(&number) {
        format!("bdev:///EE_error_device{}", number)
    } else {
        format!("aio://{}-{}?blk_size=512", nexus_name(), get_disk(number))
    }
}

#[test]
fn rebuild_test_basic() {
    test_ini("rebuild_test_basic");

    Reactor::block_on(async {
        nexus_create(NEXUS_SIZE, 1, false).await;
        nexus_add_child(1, true).await;
        nexus_lookup(nexus_name()).unwrap().destroy().await.unwrap();
    });

    test_fini();
}

#[test]
// test the rebuild flag of the add_child operation
fn rebuild_test_add() {
    test_ini("rebuild_test_add");

    Reactor::block_on(async {
        nexus_create(NEXUS_SIZE, 1, true).await;
        let nexus = nexus_lookup(nexus_name()).unwrap();

        nexus.add_child(&get_dev(1), false).await.unwrap();
        nexus
            .start_rebuild(&get_dev(1))
            .await
            .expect_err("rebuild expected to be present");
        nexus_test_child(1).await;

        nexus.add_child(&get_dev(2), true).await.unwrap();
        let _ = nexus
            .start_rebuild(&get_dev(2))
            .await
            .expect("rebuild not expected to be present");

        nexus_lookup(nexus_name()).unwrap().destroy().await.unwrap();
    });

    test_fini();
}

#[test]
fn rebuild_progress() {
    test_ini("rebuild_progress");

    async fn test_progress(polls: u64, progress: u32) -> u32 {
        let nexus = nexus_lookup(nexus_name()).unwrap();
        nexus.resume_rebuild(&get_dev(1)).await.unwrap();
        // { polls } to poll with an expr rather than an ident
        reactor_poll!({ polls });
        nexus.pause_rebuild(&get_dev(1)).await.unwrap();
        common::wait_for_rebuild(
            get_dev(1),
            RebuildState::Paused,
            std::time::Duration::from_millis(100),
        )
        .unwrap();
        let p = nexus.get_rebuild_progress(&get_dev(1)).unwrap();
        assert!(p.progress >= progress);
        p.progress
    };

    Reactor::block_on(async {
        nexus_create(NEXUS_SIZE, 1, false).await;
        nexus_add_child(1, false).await;
        // naive check to see if progress is being made
        let mut progress = 0;
        for _ in 0 .. 10 {
            progress = test_progress(50, progress).await;
        }
        nexus_lookup(nexus_name()).unwrap().destroy().await.unwrap();
    });

    test_fini();
}

#[test]
fn rebuild_child_faulted() {
    test_ini("rebuild_child_faulted");

    Reactor::block_on(async move {
        nexus_create(NEXUS_SIZE, 2, false).await;

        let nexus = nexus_lookup(nexus_name()).unwrap();
        nexus
            .start_rebuild(&get_dev(1))
            .await
            .expect_err("Rebuild only degraded children!");

        nexus.remove_child(&get_dev(1)).await.unwrap();
        assert_eq!(nexus.children.len(), 1);
        nexus
            .start_rebuild(&get_dev(0))
            .await
            .expect_err("Cannot rebuild from the same child");

        nexus.destroy().await.unwrap();
    });

    test_fini();
}

#[test]
fn rebuild_dst_removal() {
    test_ini("rebuild_dst_removal");

    Reactor::block_on(async move {
        let new_child = 2;
        nexus_create(NEXUS_SIZE, new_child, false).await;
        nexus_add_child(new_child, false).await;

        let nexus = nexus_lookup(nexus_name()).unwrap();
        nexus.pause_rebuild(&get_dev(new_child)).await.unwrap();
        nexus.remove_child(&get_dev(new_child)).await.unwrap();

        nexus.destroy().await.unwrap();
    });

    test_fini();
}

#[test]
fn rebuild_src_removal() {
    test_ini("rebuild_src_removal");

    Reactor::block_on(async move {
        let new_child = 2;
        assert!(new_child > 1);
        nexus_create(NEXUS_SIZE, new_child, true).await;
        nexus_add_child(new_child, false).await;

        let nexus = nexus_lookup(nexus_name()).unwrap();
        nexus.pause_rebuild(&get_dev(new_child)).await.unwrap();
        nexus.remove_child(&get_dev(0)).await.unwrap();

        // tests if new_child which had its original rebuild src removed
        // ended up being rebuilt successfully
        nexus_test_child(new_child).await;

        nexus.destroy().await.unwrap();
    });

    test_fini();
}

#[test]
fn rebuild_with_load() {
    test_ini("rebuild_with_load");

    Reactor::block_on(async {
        nexus_create(NEXUS_SIZE, 1, false).await;
        let nexus = nexus_lookup(nexus_name()).unwrap();
        let nexus_device =
            common::device_path_from_uri(nexus.get_share_path().unwrap());

        let (s, r1) = unbounded::<i32>();
        std::thread::spawn(move || {
            Mthread::unaffinitize();
            s.send(common::fio_verify_size(&nexus_device, NEXUS_SIZE * 2))
        });
        let (s, r2) = unbounded::<()>();
        std::thread::spawn(move || {
            Mthread::unaffinitize();
            std::thread::sleep(std::time::Duration::from_millis(1500));
            s.send(())
        });
        // warm up fio with a single child first
        reactor_poll!(r2);
        nexus_add_child(1, false).await;
        let fio_result: i32;
        reactor_poll!(r1, fio_result);
        assert_eq!(fio_result, 0, "Failed to run fio_verify_size");

        nexus_test_child(1).await;

        nexus.destroy().await.unwrap();
    });

    test_fini();
}

async fn nexus_create(size: u64, children: u64, fill_random: bool) {
    let mut ch = Vec::new();
    for i in 0 .. children {
        ch.push(get_dev(i));
    }

    mayastor::bdev::nexus_create(nexus_name(), size, None, &ch)
        .await
        .unwrap();

    let nexus = nexus_lookup(nexus_name()).unwrap();
    let device = common::device_path_from_uri(
        nexus
            .share(ShareProtocolNexus::NexusNbd, None)
            .await
            .unwrap(),
    );
    reactor_poll!(100);

    if fill_random {
        let nexus_device = device.clone();
        let (s, r) = unbounded::<i32>();
        std::thread::spawn(move || {
            s.send(common::dd_urandom_blkdev(&nexus_device))
        });
        let dd_result: i32;
        reactor_poll!(r, dd_result);
        assert_eq!(dd_result, 0, "Failed to fill nexus with random data");

        let (s, r) = unbounded::<String>();
        std::thread::spawn(move || {
            s.send(common::compare_nexus_device(&device, &get_disk(0), true))
        });
        reactor_poll!(r);
    }
}

async fn nexus_add_child(new_child: u64, wait: bool) {
    let nexus = nexus_lookup(nexus_name()).unwrap();

    nexus.add_child(&get_dev(new_child), false).await.unwrap();

    if wait {
        common::wait_for_rebuild(
            get_dev(new_child),
            RebuildState::Completed,
            std::time::Duration::from_secs(10),
        )
        .unwrap();

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
    )
    .unwrap();

    let nexus = nexus_lookup(nexus_name()).unwrap();

    let (s, r) = unbounded::<String>();
    std::thread::spawn(move || {
        s.send(common::compare_devices(
            &get_disk(0),
            &get_disk(child),
            nexus.size(),
            true,
        ))
    });
    reactor_poll!(r);
}

#[test]
// test rebuild with different combinations of sizes for src and dst children
fn rebuild_sizes() {
    test_ini("rebuild_sizes");

    let nexus_size = 10 * 1024 * 1024; // 10MiB
    let child_size = nexus_size + META_SIZE;
    let mut test_cases = vec![
        // size of (first child, second, third)
        // first child size is same as the nexus size to set it as the minimum
        // otherwise a child bigger than the nexus but smaller than the
        // smallest child would not be allowed
        (nexus_size, child_size, child_size),
        (nexus_size, child_size * 2, child_size),
        (nexus_size, child_size, child_size * 2),
        (nexus_size, child_size * 2, child_size * 2),
    ];
    // now for completeness sake we also test the cases where the actual
    // nexus_size will be lower due to the on-disk metadata
    let child_size = nexus_size;
    test_cases.extend(vec![
        (nexus_size, child_size, child_size),
        (nexus_size, child_size * 2, child_size),
        (nexus_size, child_size, child_size * 2),
        (nexus_size, child_size * 2, child_size * 2),
    ]);

    for (test_case_index, test_case) in test_cases.iter().enumerate() {
        common::delete_file(&[get_disk(0), get_disk(1), get_disk(1)]);
        // first healthy child in the list is used as the rebuild source
        common::truncate_file_bytes(&get_disk(0), test_case.1);
        common::truncate_file_bytes(&get_disk(1), test_case.0);
        common::truncate_file_bytes(&get_disk(2), test_case.2);

        let nexus_size = test_case.0;
        Reactor::block_on(async move {
            // add an extra child so that the minimum size is set to
            // match the nexus size
            nexus_create(nexus_size, 2, false).await;
            let nexus = nexus_lookup(nexus_name()).unwrap();
            nexus.add_child(&get_dev(2), true).await.unwrap();
            // within start_rebuild the size should be validated
            let _ = nexus.start_rebuild(&get_dev(2)).await.unwrap_or_else(|e| {
                log::error!( "Case {} - Child should have started to rebuild but got error:\n {:}",
                    test_case_index, e.verbose());
                panic!(
                    "Case {} - Child should have started to rebuild but got error:\n {}",
                    test_case_index, e.verbose()
                )
            });
            // sanity check that the rebuild does succeed
            common::wait_for_rebuild(
                get_dev(2),
                RebuildState::Completed,
                std::time::Duration::from_secs(20),
            )
            .unwrap();

            nexus.destroy().await.unwrap();
        });
    }

    test_fini();
}

#[test]
// tests the rebuild with multiple size and a non-multiple size of the segment
fn rebuild_segment_sizes() {
    test_ini("rebuild_segment_sizes");

    assert!(SEGMENT_SIZE > 512 && SEGMENT_SIZE < NEXUS_SIZE);

    let test_cases = vec![
        // multiple of SEGMENT_SIZE
        SEGMENT_SIZE * 10,
        // not multiple of SEGMENT_SIZE
        (SEGMENT_SIZE * 10) + 512,
    ];

    for test_case in test_cases.iter() {
        let nexus_size = *test_case;
        Reactor::block_on(async move {
            nexus_create(nexus_size, 1, false).await;
            nexus_add_child(1, true).await;
            nexus_lookup(nexus_name()).unwrap().destroy().await.unwrap();
        });
    }

    test_fini();
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
                    log::error!(
                        "Job {:?} should be associated with src child {}",
                        job,
                        child
                    );
                })
                .any(|_| panic!("Should not have found any jobs!"));
        }

        let _ = nexus.start_rebuild(&get_dev(children)).await.unwrap();
        for child in 0 .. children - 1 {
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
                        log::error!(
                            "Job {:?} should be associated with src child {}",
                            job,
                            child
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
        nexus.add_child(&get_dev(children + 1), true).await.unwrap();
        let _ = nexus.start_rebuild(&get_dev(children + 1)).await.unwrap();
        assert_eq!(RebuildJob::lookup_src(&src).len(), 2);

        nexus.remove_child(&get_dev(children)).await.unwrap();
        nexus.remove_child(&get_dev(children + 1)).await.unwrap();
        nexus_lookup(nexus_name()).unwrap().destroy().await.unwrap();
    });

    test_fini();
}

#[test]
// todo: decide whether to keep the idempotence on the operations or to
// create a RPC version which achieves the idempotence
fn rebuild_operations() {
    test_ini("rebuild_operations");

    Reactor::block_on(async {
        nexus_create(NEXUS_SIZE, 1, false).await;
        let nexus = nexus_lookup(nexus_name()).unwrap();

        nexus
            .resume_rebuild(&get_dev(1))
            .await
            .expect_err("no rebuild to resume");

        nexus_add_child(1, false).await;

        nexus
            .resume_rebuild(&get_dev(1))
            .await
            .expect("already running");

        nexus.pause_rebuild(&get_dev(1)).await.unwrap();
        reactor_poll!(10);
        // already pausing so no problem
        nexus.pause_rebuild(&get_dev(1)).await.unwrap();
        reactor_poll!(10);

        let _ = nexus
            .start_rebuild(&get_dev(1))
            .await
            .expect_err("a rebuild already exists");

        nexus.stop_rebuild(&get_dev(1)).await.unwrap();
        common::wait_for_rebuild(
            get_dev(1),
            RebuildState::Stopped,
            // already stopping, should be enough
            std::time::Duration::from_millis(250),
        )
        .unwrap();
        // already stopped
        nexus.stop_rebuild(&get_dev(1)).await.unwrap();

        nexus_lookup(nexus_name()).unwrap().destroy().await.unwrap();
    });

    test_fini();
}

#[test]
#[ignore]
// rebuilds N children at the same time
// creates the nexus with 1 healthy and then adds N children which
// have to be rebuilt - this means we have N active rebuilds jobs
fn rebuild_multiple() {
    test_ini("rebuild_multiple");

    let active_rebuilds = 4;
    Reactor::block_on(async move {
        nexus_create(NEXUS_SIZE, 1, false).await;
        let nexus = nexus_lookup(nexus_name()).unwrap();

        for child in 1 ..= active_rebuilds {
            nexus_add_child(child, false).await;
        }

        assert_eq!(RebuildJob::count(), active_rebuilds as usize);

        for child in 1 ..= active_rebuilds {
            common::wait_for_rebuild(
                get_dev(child),
                RebuildState::Completed,
                std::time::Duration::from_secs(20),
            )
            .unwrap();
            nexus.remove_child(&get_dev(child)).await.unwrap();
        }

        // make sure we can recreate the jobs again (as they
        // will have the same URI)

        for child in 1 ..= active_rebuilds {
            nexus_add_child(child, false).await;
        }

        for child in 1 ..= active_rebuilds {
            common::wait_for_rebuild(
                get_dev(child),
                RebuildState::Running,
                std::time::Duration::from_millis(100),
            )
            .unwrap();
            nexus.remove_child(&get_dev(child)).await.unwrap();
        }

        nexus.destroy().await.unwrap();
    });

    test_fini();
}

#[test]
fn rebuild_fault_src() {
    test_ini("rebuild_fault_src");
    set_err_dev(0);

    Reactor::block_on(async {
        nexus_create(NEXUS_SIZE, 1, false).await;

        let nexus = nexus_lookup(nexus_name()).unwrap();
        nexus.add_child(&get_dev(1), false).await.unwrap();

        error_bdev::inject_error(
            &get_err_dev(0),
            error_bdev::SPDK_BDEV_IO_TYPE_READ,
            error_bdev::VBDEV_IO_FAILURE,
            88,
        );

        common::wait_for_rebuild(
            get_dev(1),
            RebuildState::Failed,
            std::time::Duration::from_secs(20),
        )
        .unwrap();
        // allow the nexus futures to run
        reactor_poll!(10);
        assert_eq!(nexus.children[1].status(), ChildStatus::Faulted);

        nexus_lookup(nexus_name()).unwrap().destroy().await.unwrap();
    });

    test_fini();
}

#[test]
fn rebuild_fault_dst() {
    test_ini("rebuild_fault_dst");
    set_err_dev(1);

    Reactor::block_on(async {
        nexus_create(NEXUS_SIZE, 1, false).await;

        let nexus = nexus_lookup(nexus_name()).unwrap();
        nexus.add_child(&get_dev(1), false).await.unwrap();

        error_bdev::inject_error(
            &get_err_dev(1),
            error_bdev::SPDK_BDEV_IO_TYPE_WRITE,
            error_bdev::VBDEV_IO_FAILURE,
            88,
        );

        common::wait_for_rebuild(
            get_dev(1),
            RebuildState::Failed,
            std::time::Duration::from_secs(20),
        )
        .unwrap();
        // allow the nexus futures to run
        reactor_poll!(10);
        assert_eq!(nexus.children[1].status(), ChildStatus::Faulted);

        nexus_lookup(nexus_name()).unwrap().destroy().await.unwrap();
    });

    test_fini();
}
