use mayastor::{
    bdev::{nexus_create, nexus_lookup, uring_util},
    core::{
        mayastor_env_stop,
        Bdev,
        BdevHandle,
        MayastorCliArgs,
        MayastorEnvironment,
        Reactor,
    },
    nexus_uri::{bdev_create, bdev_destroy},
};
use rpc::mayastor::ShareProtocolNexus;
use std::sync::Once;

static DISKNAME1: &str = "/tmp/disk1.img";
static BDEVNAME1: &str = "aio:///tmp/disk1.img?blk_size=512";

static DISKNAME2: &str = "/tmp/disk2.img";
static BDEVNAME2: &str = "aio:///tmp/disk2.img?blk_size=512";

static DISKNAME3: &str = "/tmp/disk3.img";
static BDEVNAME3: &str = "uring:///tmp/disk3.img?blk_size=512";

static mut DO_URING: bool = false;
static INIT: Once = Once::new();

pub mod common;

fn do_uring() -> bool {
    unsafe {
        INIT.call_once(|| {
            DO_URING = uring_util::fs_supports_direct_io(DISKNAME3)
                && uring_util::fs_type_supported(DISKNAME3)
                && uring_util::kernel_support();
        });
        DO_URING
    }
}

async fn create_nexus() {
    let ch = if do_uring() {
        vec![
            BDEVNAME1.to_string(),
            BDEVNAME2.to_string(),
            BDEVNAME3.to_string(),
        ]
    } else {
        vec![BDEVNAME1.to_string(), BDEVNAME2.to_string()]
    };
    nexus_create("core_nexus", 64 * 1024 * 1024, None, &ch)
        .await
        .unwrap();
}

#[test]
fn core() {
    test_init!();
    common::truncate_file(DISKNAME1, 64 * 1024);
    common::truncate_file(DISKNAME2, 64 * 1024);
    common::truncate_file(DISKNAME3, 64 * 1024);

    Reactor::block_on(async {
        works().await;
    });
}

async fn works() {
    assert_eq!(Bdev::lookup_by_name("core_nexus").is_none(), true);
    create_nexus().await;
    let b = Bdev::lookup_by_name("core_nexus").unwrap();
    assert_eq!(b.name(), "core_nexus");

    let desc = Bdev::open_by_name("core_nexus", false).unwrap();
    let channel = desc.get_channel().expect("failed to get IO channel");
    drop(channel);
    drop(desc);

    let n = nexus_lookup("core_nexus").expect("nexus not found");
    n.destroy().await.unwrap();
}

#[test]
fn core_2() {
    test_init!();
    Reactor::block_on(async {
        create_nexus().await;

        let n = nexus_lookup("core_nexus").expect("failed to lookup nexus");

        let d1 = Bdev::open_by_name("core_nexus", true)
            .expect("failed to open first desc to nexus");
        let d2 = Bdev::open_by_name("core_nexus", true)
            .expect("failed to open second desc to nexus");

        let ch1 = d1.get_channel().expect("failed to get channel!");
        let ch2 = d2.get_channel().expect("failed to get channel!");
        drop(ch1);
        drop(ch2);

        // we must drop the descriptors before we destroy the nexus
        drop(dbg!(d1));
        drop(dbg!(d2));
        n.destroy().await.unwrap();
    });
}

#[test]
fn core_3() {
    test_init!();
    Reactor::block_on(async {
        bdev_create(BDEVNAME1).await.expect("failed to create bdev");
        let hdl2 = BdevHandle::open(BDEVNAME1, true, true)
            .expect("failed to create the handle!");
        let hdl3 = BdevHandle::open(BDEVNAME1, true, true);
        assert_eq!(hdl3.is_err(), true);

        // we must drop the descriptors before we destroy the nexus
        drop(hdl2);
        drop(hdl3);

        bdev_destroy(BDEVNAME1).await.unwrap();
    });
}

#[test]
// Test nexus with different combinations of sizes for src and dst children
fn core_4() {
    test_init!();

    common::delete_file(&[DISKNAME1.to_string()]);
    common::delete_file(&[DISKNAME2.to_string()]);

    let nexus_size: u64 = 10 * 1024 * 1024; // 10MiB
    let nexus_name: &str = "nexus_sizes";

    // nexus size is always NEXUS_SIZE
    // (size of child1, create success, size of child2, add child2 success)
    let test_cases = vec![
        (nexus_size, true, nexus_size * 2, true),
        (nexus_size, true, nexus_size / 2, false),
        (nexus_size * 2, true, nexus_size, false),
        (nexus_size * 2, true, nexus_size * 2, true),
        (nexus_size / 2, false, nexus_size / 2, false),
    ];

    for (test_case_index, test_case) in test_cases.iter().enumerate() {
        common::truncate_file(DISKNAME1, test_case.0 / 1024);
        common::truncate_file(DISKNAME2, test_case.2 / 1024);

        let nexus_ok = test_case.1;
        let child_ok = test_case.3;

        Reactor::block_on(async move {
            let create = nexus_create(
                nexus_name,
                nexus_size,
                None,
                &[BDEVNAME1.to_string()],
            )
            .await;
            if nexus_ok {
                create.unwrap_or_else(|_| {
                    panic!(
                        "Case {} - Nexus should have have been created",
                        test_case_index
                    )
                });
                let nexus = nexus_lookup(nexus_name).unwrap();

                if child_ok {
                    nexus.add_child(&BDEVNAME2).await.unwrap_or_else(|_| {
                        panic!(
                            "Case {} - Child should have been added",
                            test_case_index
                        )
                    });
                } else {
                    nexus.add_child(&BDEVNAME2).await.expect_err(&format!(
                        "Case {} - Child should have been added",
                        test_case_index
                    ));
                }

                nexus.destroy().await.unwrap();
            } else {
                create.expect_err(&format!(
                    "Case {} - Nexus should not have been created",
                    test_case_index
                ));
            }
        });

        common::delete_file(&[DISKNAME1.to_string()]);
        common::delete_file(&[DISKNAME2.to_string()]);
    }
}

#[test]
// Test nexus bdev size when created with children of the same size and larger
fn core_5() {
    test_init!();

    common::delete_file(&[DISKNAME1.to_string()]);
    let nexus_size: u64 = 100 * 1024 * 1024; // 100MiB
    let nexus_name: &str = "nexus_size";

    let test_cases =
        vec![(nexus_size, nexus_size * 2), (nexus_size, nexus_size)];

    for test_case in test_cases.iter() {
        let nexus_size = test_case.0;
        let child_size = test_case.1;

        common::truncate_file(DISKNAME1, child_size / 1024);

        Reactor::block_on(async move {
            nexus_create(
                nexus_name,
                nexus_size,
                None,
                &[BDEVNAME1.to_string()],
            )
            .await
            .unwrap();
            let nexus = nexus_lookup(nexus_name).unwrap();
            let device = common::device_path_from_uri(
                nexus
                    .share(ShareProtocolNexus::NexusNbd, None)
                    .await
                    .unwrap(),
            );

            let size = common::get_device_size(&device);
            // size of the shared device:
            // if the child is sufficiently large it should match the requested
            // nexus_size or a little less (smallest child size
            // minus partition metadata)
            assert!(size <= nexus_size);

            nexus.destroy().await.unwrap();
        });

        common::delete_file(&[DISKNAME1.to_string()]);
    }

    mayastor_env_stop(1);
}
