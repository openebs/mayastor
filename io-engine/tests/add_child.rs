#[macro_use]
extern crate assert_matches;

use io_engine::{
    bdev::nexus::{nexus_create, nexus_lookup_mut, ChildState, Reason},
    core::{MayastorCliArgs, Protocol},
};

static NEXUS_NAME: &str = "nexus";

static FILE_SIZE: u64 = 64 * 1024 * 1024; // 64MiB

static DISKNAME1: &str = "/tmp/disk1.img";
static BDEVNAME1: &str = "aio:///tmp/disk1.img?blk_size=512";

static DISKNAME2: &str = "/tmp/disk2.img";
static BDEVNAME2: &str = "aio:///tmp/disk2.img?blk_size=512";

pub mod common;
use common::MayastorTest;

fn test_start() {
    common::delete_file(&[DISKNAME1.into(), DISKNAME2.into()]);
    common::truncate_file(DISKNAME1, FILE_SIZE);
    common::truncate_file(DISKNAME2, FILE_SIZE);
}

fn test_finish() {
    let disks = [DISKNAME1.into(), DISKNAME2.into()];
    common::delete_file(&disks);
}

#[tokio::test]
async fn add_child() {
    test_start();
    let ms = MayastorTest::new(MayastorCliArgs::default());
    // Create a nexus with a single child
    ms.spawn(async {
        let children = vec![BDEVNAME1.to_string()];
        nexus_create(NEXUS_NAME, 512 * 131_072, None, &children)
            .await
            .expect("Failed to create nexus");
    })
    .await;

    // Test adding a child to an unshared nexus
    ms.spawn(async {
        let mut nexus = nexus_lookup_mut(NEXUS_NAME).unwrap();
        nexus
            .as_mut()
            .add_child(BDEVNAME2, false)
            .await
            .expect("Failed to add child");
        assert_eq!(nexus.children.len(), 2);

        // Expect the added child to be in the out-of-sync state
        assert_matches!(
            nexus.children[1].state(),
            ChildState::Faulted(Reason::OutOfSync)
        );
    })
    .await;

    // Test removing a child from an unshared nexus
    ms.spawn(async {
        let mut nexus = nexus_lookup_mut(NEXUS_NAME).unwrap();
        nexus
            .as_mut()
            .remove_child(BDEVNAME2)
            .await
            .expect("Failed to remove child");
        assert_eq!(nexus.children.len(), 1);
    })
    .await;

    // Share nexus
    ms.spawn(async {
        let nexus = nexus_lookup_mut(NEXUS_NAME).unwrap();
        nexus
            .share(Protocol::Nvmf, None)
            .await
            .expect("Failed to share nexus");
    })
    .await;

    // Test adding a child to a shared nexus
    ms.spawn(async {
        let mut nexus = nexus_lookup_mut(NEXUS_NAME).unwrap();
        nexus
            .as_mut()
            .add_child(BDEVNAME2, false)
            .await
            .expect("Failed to add child");
        assert_eq!(nexus.children.len(), 2);

        // Expect the added child to be in the out-of-sync state
        assert_matches!(
            nexus.children[1].state(),
            ChildState::Faulted(Reason::OutOfSync)
        );
    })
    .await;

    // Test removing a child from a shared nexus
    ms.spawn(async {
        let mut nexus = nexus_lookup_mut(NEXUS_NAME).unwrap();
        nexus
            .as_mut()
            .remove_child(BDEVNAME2)
            .await
            .expect("Failed to remove child");
        assert_eq!(nexus.children.len(), 1);
    })
    .await;

    // Unshare nexus
    ms.spawn(async {
        let nexus = nexus_lookup_mut(NEXUS_NAME).unwrap();
        nexus
            .unshare_nexus()
            .await
            .expect("Failed to unshare nexus");
    })
    .await;

    test_finish();
}
