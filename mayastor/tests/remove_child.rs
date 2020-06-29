use std::process::Command;

use mayastor::{
    bdev::{nexus_create, nexus_lookup, NexusStatus},
    core::{
        mayastor_env_stop,
        Bdev,
        MayastorCliArgs,
        MayastorEnvironment,
        Reactor,
    },
};

static DISKNAME1: &str = "/tmp/disk1.img";
static BDEVNAME1: &str = "aio:///tmp/disk1.img?blk_size=512";

static DISKNAME2: &str = "/tmp/disk2.img";
static BDEVNAME2: &str = "aio:///tmp/disk2.img?blk_size=512";
pub mod common;
#[test]
fn remove_child() {
    common::mayastor_test_init();

    // setup our test files

    let output = Command::new("truncate")
        .args(&["-s", "64m", DISKNAME1])
        .output()
        .expect("failed exec truncate");

    assert_eq!(output.status.success(), true);

    let output = Command::new("truncate")
        .args(&["-s", "64m", DISKNAME2])
        .output()
        .expect("failed exec truncate");

    assert_eq!(output.status.success(), true);
    let mut args = MayastorCliArgs::default();
    args.log_components = vec!["all".into()];
    let rc = MayastorEnvironment::new(MayastorCliArgs::default())
        .start(|| Reactor::block_on(works()).unwrap())
        .unwrap();

    assert_eq!(rc, 0);
}

async fn works() {
    let child1 = BDEVNAME1;

    //"nvmf://192.168.1.4:8420/nqn.2019-05.io.openebs:disk1".to_string();
    let child2 = BDEVNAME2;
    //"nvmf://192.168.1.4:8420/nqn.2019-05.io.openebs:disk2".to_string();

    let children = vec![child1.into(), child2.into()];

    nexus_create("hello", 512 * 131_072, None, &children)
        .await
        .unwrap();

    let nexus = nexus_lookup("hello").unwrap();

    // open the nexus in read write
    let nd_bdev = Bdev::lookup_by_name("hello").expect("failed to lookup bdev");
    let _nd = nd_bdev
        .open(true)
        .expect("failed open bdev")
        .into_handle()
        .unwrap();
    assert_eq!(nexus.status(), NexusStatus::Online);

    nexus.remove_child(BDEVNAME1).await.unwrap();
    mayastor_env_stop(0);
}
