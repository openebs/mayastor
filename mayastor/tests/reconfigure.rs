#![allow(clippy::cognitive_complexity)]
use mayastor::{
    bdev::{
        nexus::nexus_bdev::{nexus_create, nexus_lookup},
        Bdev,
    },
    descriptor::Descriptor,
    mayastor_start, spdk_stop,
};

use std::process::Command;

static DISKNAME1: &str = "/tmp/disk1.img";
static BDEVNAME1: &str = "aio:///tmp/disk1.img?blk_size=512";

static DISKNAME2: &str = "/tmp/disk2.img";
static BDEVNAME2: &str = "aio:///tmp/disk2.img?blk_size=512";

#[test]
fn reconfigure() {
    let log = mayastor::spdklog::SpdkLog::new();
    let _ = log.init();
    mayastor::CPS_INIT!();
    let args = vec!["-c", "../etc/test.conf"];

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

    let rc = mayastor_start("test", args, || {
        mayastor::executor::spawn(works());
    });

    assert_eq!(rc, 0);

    let output = Command::new("rm")
        .args(&["-rf", DISKNAME1, DISKNAME2])
        .output()
        .expect("failed delete test file");

    assert_eq!(output.status.success(), true);
}

fn buf_compare(first: &[u8], second: &[u8]) {
    for i in 0..first.len() {
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
    let nd = Descriptor::open("hello", true).expect("failed open bdev");
    // open the children in RO

    let cd1 = Descriptor::open(&child1, false).expect("failed open bdev");
    let cd2 = Descriptor::open(&child2, false).expect("failed open bdev");

    let bdev1 = cd1.get_bdev();
    let bdev2 = cd2.get_bdev();

    // write out a region of blocks to ensure a specific data pattern
    let mut buf = nd.dma_zmalloc(4096).expect("failed to allocate buffer");
    buf.fill(0xff);

    // allocate buffer for child to read
    let mut buf1 = cd1.dma_zmalloc(4096).unwrap();
    let mut buf2 = cd2.dma_zmalloc(4096).unwrap();

    // write out 0xff to the nexus, all children should have the same
    for i in 0..10 {
        nd.write_at(i * 4096, &buf).await.unwrap();
    }

    // verify that both children have the same write count
    stats_compare(&bdev1, &bdev2).await;

    // compare all buffers byte for byte
    for i in 0..10 {
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

    // write 0xF0 to the nexus
    for i in 0..10 {
        nd.write_at(i * 4096, &buf).await.unwrap();
    }

    // verify that only child2 has the 0xF0 pattern set, child2 still has 0xff
    for i in 0..10 {
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

    buf.fill(0xAA);
    // write 0xAA to the nexus
    for i in 0..10 {
        nd.write_at(i * 4096, &buf).await.unwrap();
    }

    // both children should have 0xAA set

    for i in 0..10 {
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

    cd1.close();
    cd2.close();
    nd.close();

    spdk_stop(0);
}
