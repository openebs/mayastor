use std::sync::Once;

use once_cell::sync::OnceCell;
use uuid::Uuid;

use common::MayastorTest;
use io_engine::{
    bdev::{
        nexus::{nexus_create, nexus_lookup_mut},
        util::uring,
    },
    core::{BdevHandle, MayastorCliArgs, Protocol, UntypedBdev},
    nexus_uri::{bdev_create, bdev_destroy},
};

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
            DO_URING = uring::kernel_support();
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

static MS: OnceCell<MayastorTest> = OnceCell::new();

fn mayastor() -> &'static MayastorTest<'static> {
    MS.get_or_init(|| MayastorTest::new(MayastorCliArgs::default()))
}

#[tokio::test]
async fn core() {
    common::truncate_file(DISKNAME1, 64 * 1024);
    common::truncate_file(DISKNAME2, 64 * 1024);
    common::truncate_file(DISKNAME3, 64 * 1024);

    mayastor()
        .spawn(async {
            works().await;
        })
        .await;
}

async fn works() {
    assert!(UntypedBdev::lookup_by_name("core_nexus").is_none());
    create_nexus().await;
    let b = UntypedBdev::lookup_by_name("core_nexus").unwrap();
    assert_eq!(b.name(), "core_nexus");

    let desc = UntypedBdev::open_by_name("core_nexus", false).unwrap();
    let channel = desc.get_channel().expect("failed to get IO channel");
    drop(channel);
    drop(desc);
    let n = nexus_lookup_mut("core_nexus").expect("nexus not found");
    n.destroy().await.unwrap();
}

#[tokio::test]
async fn core_2() {
    mayastor()
        .spawn(async {
            create_nexus().await;

            let n =
                nexus_lookup_mut("core_nexus").expect("failed to lookup nexus");

            let d1 = UntypedBdev::open_by_name("core_nexus", true)
                .expect("failed to open first desc to nexus");
            let d2 = UntypedBdev::open_by_name("core_nexus", true)
                .expect("failed to open second desc to nexus");

            let ch1 = d1.get_channel().expect("failed to get channel!");
            let ch2 = d2.get_channel().expect("failed to get channel!");
            drop(ch1);
            drop(ch2);

            // we must drop the descriptors before we destroy the nexus
            drop(dbg!(d1));
            drop(dbg!(d2));
            n.destroy().await.unwrap();
        })
        .await;
}

#[tokio::test]
async fn core_3() {
    mayastor()
        .spawn(async {
            bdev_create(BDEVNAME1).await.expect("failed to create bdev");
            let hdl2 = BdevHandle::open(BDEVNAME1, true, true)
                .expect("failed to create the handle!");
            let hdl3 = BdevHandle::open(BDEVNAME1, true, true);
            assert!(hdl3.is_err());

            // we must drop the descriptors before we destroy the nexus
            drop(hdl2);
            drop(hdl3);

            bdev_destroy(BDEVNAME1).await.unwrap();
        })
        .await;
}

#[tokio::test]
async fn core_4() {
    common::delete_file(&[DISKNAME1.to_string()]);
    common::delete_file(&[DISKNAME2.to_string()]);

    let nexus_size: u64 = 10 * 1024 * 1024; // 10MiB
    let nexus_name: &str = "nexus_sizes";

    // nexus size is always "nexus_size"
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

        mayastor()
            .spawn(async move {
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
                    let mut nexus = nexus_lookup_mut(nexus_name).unwrap();

                    if child_ok {
                        nexus
                            .as_mut()
                            .add_child(BDEVNAME2, true)
                            .await
                            .unwrap_or_else(|_| {
                                panic!(
                                    "Case {} - Child should have been added",
                                    test_case_index
                                )
                            });
                    } else {
                        nexus
                            .as_mut()
                            .add_child(BDEVNAME2, true)
                            .await
                            .expect_err(&format!(
                                "Case {} - Child should not have been added",
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
            })
            .await;

        common::delete_file(&[DISKNAME1.to_string()]);
        common::delete_file(&[DISKNAME2.to_string()]);
    }
}

#[tokio::test]
async fn core_5() {
    common::delete_file(&[DISKNAME1.to_string()]);
    let nexus_size: u64 = 100 * 1024 * 1024; // 100MiB
    let nexus_name: &str = "nexus_size";

    let test_cases =
        vec![(nexus_size, nexus_size * 2), (nexus_size, nexus_size)];

    for test_case in test_cases.iter() {
        let nexus_size = test_case.0;
        let child_size = test_case.1;

        common::truncate_file(DISKNAME1, child_size / 1024);

        mayastor()
            .spawn(async move {
                nexus_create(
                    nexus_name,
                    nexus_size,
                    None,
                    &[BDEVNAME1.to_string()],
                )
                .await
                .unwrap();
                let mut nexus = nexus_lookup_mut(nexus_name).unwrap();
                // need to refactor this test to use nvmf instead of nbd
                // once the libnvme-rs refactoring is done
                let device = common::device_path_from_uri(
                    &nexus.as_mut().share(Protocol::Off, None).await.unwrap(),
                );

                let size = common::get_device_size(&device);
                // size of the shared device:
                // if the child is sufficiently large it should match the
                // requested nexus_size or a little less
                // (smallest child size minus partition
                // metadata)
                assert!(size <= nexus_size);

                nexus.destroy().await.unwrap();
            })
            .await;

        common::delete_file(&[DISKNAME1.to_string()]);
    }
}

#[tokio::test]
// Test nexus with inaccessible bdev for 2nd child
async fn core_6() {
    common::truncate_file(DISKNAME1, 64 * 1024);

    let file_uuid = Uuid::new_v4();
    let ch = vec![
        BDEVNAME1.to_string(),
        "aio:///tmp/disk2".to_string() + &file_uuid.to_simple().to_string(),
    ];
    mayastor()
        .spawn(async move {
            nexus_create("nexus_child_2_missing", 64 * 1024 * 1024, None, &ch)
                .await
                .expect_err("Nexus should not be created");
        })
        .await;
}
