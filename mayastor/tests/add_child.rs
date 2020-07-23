use mayastor::{
    bdev::{nexus_create, nexus_lookup, ChildStatus},
    core::{mayastor_env_stop, MayastorCliArgs, MayastorEnvironment, Reactor},
};
use std::{ops::Deref, process::Command};

static NEXUS_NAME: &str = "nexus";

static DISKNAME1: &str = "/tmp/disk1.img";
static BDEVNAME1: &str = "aio:///tmp/disk1.img?blk_size=512";

static DISKNAME2: &str = "/tmp/disk2.img";
static BDEVNAME2: &str = "aio:///tmp/disk2.img?blk_size=512";

pub mod common;

fn test_start() {
    common::mayastor_test_init();
    // Create disk images
    let disks = [DISKNAME1, DISKNAME2];
    disks
        .iter()
        .map(|disk| {
            let output = Command::new("truncate")
                .args(&["-s", "64m", disk])
                .output()
                .expect("failed to create disk image");

            assert_eq!(output.status.success(), true);
        })
        .for_each(drop);
}

fn test_finish() {
    // Destroy disk images
    let disks = [DISKNAME1, DISKNAME2];
    disks
        .iter()
        .map(|disk| {
            let output = Command::new("rm")
                .args(&[disk.deref()])
                .output()
                .expect("failed to delete disk image");

            assert_eq!(output.status.success(), true);
        })
        .for_each(drop);
}

#[test]
fn add_child() {
    test_start();
    let rc = MayastorEnvironment::new(MayastorCliArgs::default())
        .start(|| {
            // Create a nexus with a single child
            Reactor::block_on(async {
                let children = vec![BDEVNAME1.to_string()];
                nexus_create(NEXUS_NAME, 512 * 131_072, None, &children)
                    .await
                    .expect("Failed to create nexus");
            });

            // Test adding a child to an unshared nexus
            Reactor::block_on(async {
                let nexus = nexus_lookup(NEXUS_NAME).unwrap();
                nexus
                    .add_child(BDEVNAME2, false)
                    .await
                    .expect("Failed to add child");
                assert_eq!(nexus.children.len(), 2);
                // A faulted state indicates the child was added but something
                // went wrong i.e. the rebuild failed to start
                assert_ne!(nexus.children[1].status(), ChildStatus::Faulted);
            });

            // Test removing a child from an unshared nexus
            Reactor::block_on(async {
                let nexus = nexus_lookup(NEXUS_NAME).unwrap();
                nexus
                    .remove_child(BDEVNAME2)
                    .await
                    .expect("Failed to remove child");
                assert_eq!(nexus.children.len(), 1);
            });

            // Share nexus
            Reactor::block_on(async {
                let nexus = nexus_lookup(NEXUS_NAME).unwrap();
                nexus
                    .share(rpc::mayastor::ShareProtocolNexus::NexusIscsi, None)
                    .await
                    .expect("Failed to share nexus");
            });

            // Test adding a child to a shared nexus
            Reactor::block_on(async {
                let nexus = nexus_lookup(NEXUS_NAME).unwrap();
                nexus
                    .add_child(BDEVNAME2, false)
                    .await
                    .expect("Failed to add child");
                assert_eq!(nexus.children.len(), 2);
                // A faulted state indicates the child was added but something
                // went wrong i.e. the rebuild failed to start
                assert_ne!(nexus.children[1].status(), ChildStatus::Faulted);
            });

            // Test removing a child from a shared nexus
            Reactor::block_on(async {
                let nexus = nexus_lookup(NEXUS_NAME).unwrap();
                nexus
                    .remove_child(BDEVNAME2)
                    .await
                    .expect("Failed to remove child");
                assert_eq!(nexus.children.len(), 1);
            });

            // Unshare nexus
            Reactor::block_on(async {
                let nexus = nexus_lookup(NEXUS_NAME).unwrap();
                nexus.unshare().await.expect("Failed to unshare nexus");
            });

            mayastor_env_stop(0);
        })
        .unwrap();
    assert_eq!(rc, 0);
    test_finish();
}
