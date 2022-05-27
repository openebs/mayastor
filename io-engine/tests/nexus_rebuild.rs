use std::{sync::Mutex, time::Duration};

use crossbeam::channel::unbounded;
use once_cell::sync::{Lazy, OnceCell};
use tracing::error;

use io_engine::{
    bdev::{device_open, nexus::nexus_lookup_mut},
    core::{MayastorCliArgs, Mthread, Protocol},
    rebuild::{RebuildJob, RebuildState, RebuildState::Completed},
};

pub mod common;
use common::{compose::MayastorTest, wait_for_rebuild};

// each test `should` use a different nexus name to prevent clashing with
// one another. This allows the failed tests to `panic gracefully` improving
// the output log and allowing the CI to fail gracefully as well
static NEXUS_NAME: Lazy<Mutex<&str>> = Lazy::new(|| Mutex::new("Default"));
pub fn nexus_name() -> &'static str {
    &NEXUS_NAME.lock().unwrap()
}

static NEXUS_SIZE: u64 = 128 * 1024 * 1024; // 128MiB

static MAYASTOR: OnceCell<MayastorTest> = OnceCell::new();

// approximate on-disk metadata that will be written to the child by the nexus
const META_SIZE: u64 = 128 * 1024 * 1024; // 128MiB
const MAX_CHILDREN: u64 = 16;

fn get_ms() -> &'static MayastorTest<'static> {
    MAYASTOR.get_or_init(|| MayastorTest::new(MayastorCliArgs::default()))
}

fn test_ini(name: &'static str) {
    *NEXUS_NAME.lock().unwrap() = name;
    get_err_bdev().clear();

    for i in 0 .. MAX_CHILDREN {
        common::delete_file(&[get_disk(i)]);
        common::truncate_file_bytes(&get_disk(i), NEXUS_SIZE + META_SIZE);
    }
}

fn test_fini() {
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

    io_engine::bdev::nexus::nexus_create(nexus_name(), size, None, &ch)
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
    let nexus = nexus_lookup_mut(nexus_name()).unwrap();
    let device = common::device_path_from_uri(
        &nexus.share(Protocol::Off, None).await.unwrap(),
    );
    reactor_poll!(200);
    device
}

async fn wait_for_replica_rebuild(src_replica: &str, new_replica: &str) {
    let ms = get_ms();

    // 1. Wait for rebuild to complete.
    loop {
        let replica_name = new_replica.to_string();
        let complete = ms
            .spawn(async move {
                let nexus = nexus_lookup_mut(nexus_name()).unwrap();
                let state = nexus.get_rebuild_state(&replica_name).await;

                match state {
                    Err(_e) => true, /* Rebuild task completed and was
                                       * removed */
                    // discarded.
                    Ok(s) => s == Completed,
                }
            })
            .await;

        if complete {
            break;
        } else {
            tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        }
    }

    // 2. Check data integrity via MD5 checksums.
    let src_replica_name = src_replica.to_string();
    let new_replica_name = new_replica.to_string();
    ms.spawn(async move {
        let src_desc = device_open(&src_replica_name, false).unwrap();
        let dst_desc = device_open(&new_replica_name, false).unwrap();
        // Make sure devices are different.
        assert_ne!(
            src_desc.get_device().device_name(),
            dst_desc.get_device().device_name()
        );

        let src_hdl = src_desc.into_handle().unwrap();
        let dst_hdl = dst_desc.into_handle().unwrap();

        let nexus = nexus_lookup_mut(nexus_name()).unwrap();
        let mut src_buf = src_hdl.dma_malloc(nexus.size_in_bytes()).unwrap();
        let mut dst_buf = dst_hdl.dma_malloc(nexus.size_in_bytes()).unwrap();

        // Skip Mayastor partition and read only disk data at offset 10240
        // sectors.
        let data_offset: u64 = 10240 * 512;

        src_buf.fill(0);
        let mut r = src_hdl
            .read_at(data_offset, &mut src_buf)
            .await
            .expect("Failed to read source replica");
        assert_eq!(
            r,
            nexus.size_in_bytes(),
            "Amount of data read from source replica mismatches"
        );

        dst_buf.fill(0);
        r = dst_hdl
            .read_at(data_offset, &mut dst_buf)
            .await
            .expect("Failed to read new replica");
        assert_eq!(
            r,
            nexus.size_in_bytes(),
            "Amount of data read from new replica mismatches"
        );

        println!(
            "Validating new replica, {} bytes to check using MD5 checksum ...",
            nexus.size_in_bytes()
        );
        // Make sure checksums of all 2 buffers do match.
        assert_eq!(
            md5::compute(src_buf.as_slice()),
            md5::compute(dst_buf.as_slice()),
        );
    })
    .await;
}

#[tokio::test]
async fn rebuild_replica() {
    const NUM_CHILDREN: u64 = 6;

    test_ini("rebuild_replica");

    let ms = get_ms();

    ms.spawn(async move {
        nexus_create(NEXUS_SIZE, NUM_CHILDREN, true).await;
        let mut nexus = nexus_lookup_mut(nexus_name()).unwrap();
        nexus
            .as_mut()
            .add_child(&get_dev(NUM_CHILDREN), true)
            .await
            .unwrap();

        for child in 0 .. NUM_CHILDREN {
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

        let _ = nexus
            .as_mut()
            .start_rebuild(&get_dev(NUM_CHILDREN))
            .await
            .unwrap();

        for child in 0 .. NUM_CHILDREN {
            RebuildJob::lookup(&get_dev(child))
                .expect_err("rebuild job not created yet");
        }
        let src = RebuildJob::lookup(&get_dev(NUM_CHILDREN))
            .expect("now the job should exist")
            .source
            .clone();

        for child in 0 .. NUM_CHILDREN {
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
                    assert_eq!(job.destination, get_dev(NUM_CHILDREN));
                })
                .count(),
            1
        );

        // wait for the rebuild to start - and then pause it
        wait_for_rebuild(
            get_dev(NUM_CHILDREN),
            RebuildState::Running,
            Duration::from_secs(1),
        );

        nexus
            .as_mut()
            .pause_rebuild(&get_dev(NUM_CHILDREN))
            .await
            .unwrap();
        assert_eq!(RebuildJob::lookup_src(&src).len(), 1);

        nexus
            .as_mut()
            .add_child(&get_dev(NUM_CHILDREN + 1), true)
            .await
            .unwrap();
        let _ = nexus
            .start_rebuild(&get_dev(NUM_CHILDREN + 1))
            .await
            .unwrap();
        assert_eq!(RebuildJob::lookup_src(&src).len(), 2);
    })
    .await;

    // Wait for the replica rebuild to complete.
    wait_for_replica_rebuild(&get_dev(0), &get_dev(NUM_CHILDREN + 1)).await;

    ms.spawn(async move {
        let mut nexus = nexus_lookup_mut(nexus_name()).unwrap();

        nexus
            .as_mut()
            .remove_child(&get_dev(NUM_CHILDREN))
            .await
            .unwrap();
        nexus
            .remove_child(&get_dev(NUM_CHILDREN + 1))
            .await
            .unwrap();
        nexus_lookup_mut(nexus_name())
            .unwrap()
            .destroy()
            .await
            .unwrap();
        test_fini();
    })
    .await;
}
