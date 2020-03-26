use mayastor::{
    bdev::{nexus_create, nexus_lookup, uring_util},
    core::{
        mayastor_env_stop, Bdev, BdevHandle, MayastorCliArgs,
        MayastorEnvironment, Reactor,
    },
    nexus_uri::{bdev_create, bdev_destroy},
};
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
                && uring_util::kernel_supports_io_uring();
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
        mayastor_env_stop(1);
    });
}
