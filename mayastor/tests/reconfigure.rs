#![allow(clippy::cognitive_complexity)]

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
    replicas::rebuild::RebuildState,
};

static DISKNAME1: &str = "/tmp/disk1.img";
static BDEVNAME1: &str = "aio:///tmp/disk1.img?blk_size=512";

static DISKNAME2: &str = "/tmp/disk2.img";
static BDEVNAME2: &str = "aio:///tmp/disk2.img?blk_size=512";
pub mod common;
#[test]
fn reconfigure() {
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

    let rc = MayastorEnvironment::new(MayastorCliArgs::default())
        .start(|| Reactor::block_on(works()).unwrap())
        .unwrap();

    assert_eq!(rc, 0);

    let output = Command::new("rm")
        .args(&["-rf", DISKNAME1, DISKNAME2])
        .output()
        .expect("failed delete test file");

    assert_eq!(output.status.success(), true);
}

fn buf_compare(first: &[u8], second: &[u8]) {
    for i in 0 .. first.len() {
        assert_eq!(first[i], second[i]);
    }
}

async fn stats_compare(first: &Bdev, second: &Bdev) {
    let stats1 = first.stats().await.unwrap();
    let stats2 = second.stats().await.unwrap();

    assert_eq!(stats1.num_write_ops, stats2.num_write_ops);
}

async fn works() {
    let child1 = BDEVNAME1.to_string();
    let child2 = BDEVNAME2.to_string();

    let children = vec![child1.clone(), child2.clone()];

    nexus_create("hello", 512 * 131_072, None, &children)
        .await
        .unwrap();

    let nexus = nexus_lookup("hello").unwrap();

    // open the nexus in read write
    let nd_bdev = Bdev::lookup_by_name("hello").expect("failed to lookup bdev");
    let nd = nd_bdev
        .open(true)
        .expect("failed open bdev")
        .into_handle()
        .unwrap();
    assert_eq!(nexus.status(), NexusStatus::Online);
    // open the children in RO

    let cd1_bdev =
        Bdev::lookup_by_name(BDEVNAME1).expect("failed to lookup bdev");
    let cd2_bdev =
        Bdev::lookup_by_name(BDEVNAME2).expect("failed to lookup bdev");
    let cd1 = cd1_bdev
        .open(false)
        .expect("failed open bdev")
        .into_handle()
        .unwrap();
    let cd2 = cd2_bdev
        .open(false)
        .expect("failed open bdev")
        .into_handle()
        .unwrap();

    let bdev1 = cd1.get_bdev();
    let bdev2 = cd2.get_bdev();

    // write out a region of blocks to ensure a specific data pattern
    let mut buf = nd.dma_malloc(4096).expect("failed to allocate buffer");
    buf.fill(0xff);

    // allocate buffer for child to read
    let mut buf1 = cd1.dma_malloc(4096).unwrap();
    let mut buf2 = cd2.dma_malloc(4096).unwrap();

    // write out 0xff to the nexus, all children should have the same
    for i in 0 .. 10 {
        nd.write_at(i * 4096, &buf).await.unwrap();
    }

    // verify that both children have the same write count
    stats_compare(&bdev1, &bdev2).await;

    // compare all buffers byte for byte
    for i in 0 .. 10 {
        // account for the offset (in number of blocks)
        cd1.read_at((i * 4096) + (10240 * 512), &mut buf1)
            .await
            .unwrap();
        cd2.read_at((i * 4096) + (10240 * 512), &mut buf2)
            .await
            .unwrap();
        buf_compare(buf1.as_slice(), buf2.as_slice());
    }

    // fill the nexus buffer with 0xF
    buf.fill(0xF0);

    // turn one child offline
    nexus.offline_child(&child2).await.unwrap();
    assert_eq!(nexus.status(), NexusStatus::Degraded);

    // write 0xF0 to the nexus
    for i in 0 .. 10 {
        nd.write_at(i * 4096, &buf).await.unwrap();
    }

    // verify that only child2 has the 0xF0 pattern set, child2 still has 0xff
    for i in 0 .. 10 {
        buf1.fill(0x0);
        buf2.fill(0x0);

        cd1.read_at((i * 4096) + (10240 * 512), &mut buf1)
            .await
            .unwrap();
        cd2.read_at((i * 4096) + (10240 * 512), &mut buf2)
            .await
            .unwrap();

        buf1.as_slice()
            .iter()
            .map(|b| assert_eq!(*b, 0xf0))
            .for_each(drop);
        buf2.as_slice()
            .iter()
            .map(|b| assert_eq!(*b, 0xff))
            .for_each(drop);
    }

    // bring back the offlined child
    nexus.online_child(&child2).await.unwrap();
    assert_eq!(nexus.status(), NexusStatus::Degraded);

    common::wait_for_rebuild(
        child2.to_string(),
        RebuildState::Completed,
        std::time::Duration::from_secs(20),
    );

    assert_eq!(nexus.status(), NexusStatus::Online);

    buf.fill(0xAA);
    // write 0xAA to the nexus
    for i in 0 .. 10 {
        nd.write_at(i * 4096, &buf).await.unwrap();
    }

    // both children should have 0xAA set

    for i in 0 .. 10 {
        buf1.fill(0x0);
        buf2.fill(0x0);
        cd1.read_at((i * 4096) + (10240 * 512), &mut buf1)
            .await
            .unwrap();
        cd2.read_at((i * 4096) + (10240 * 512), &mut buf2)
            .await
            .unwrap();
        buf1.as_slice()
            .iter()
            .map(|b| assert_eq!(*b, 0xAA))
            .for_each(drop);
        buf2.as_slice()
            .iter()
            .map(|b| assert_eq!(*b, 0xAA))
            .for_each(drop);
    }

    drop(cd1);
    drop(cd2);
    drop(nd);

    mayastor_env_stop(0);
}
