use std::process::Command;

use mayastor::{
    bdev::bdev_lookup_by_name,
    descriptor::Descriptor,
    environment::{
        args::MayastorCliArgs,
        env::{mayastor_env_stop, MayastorEnvironment},
    },
    nexus_uri::bdev_create,
};

static DISKNAME: &str = "/tmp/disk.img";
static BDEVNAME: &str = "aio:///tmp/disk.img?blk_size=512";
pub mod common;

#[test]
fn io_test() {
    common::mayastor_test_init();
    let output = Command::new("truncate")
        .args(&["-s", "64m", DISKNAME])
        .output()
        .expect("failed exec truncate");

    assert_eq!(output.status.success(), true);

    let rc = MayastorEnvironment::new(MayastorCliArgs::default())
        .start(|| mayastor::executor::spawn(start()))
        .unwrap();

    assert_eq!(rc, 0);
    let output = Command::new("rm")
        .args(&["-rf", DISKNAME])
        .output()
        .expect("failed delete test file");

    assert_eq!(output.status.success(), true);
}

// The actual work here is completely driven by the futures. We
// only execute one future per reactor loop.
async fn start() {
    bdev_create(BDEVNAME).await.expect("failed to create bdev");
    write_some().await;
    read_some().await;
    mayastor_env_stop(0);
}

async fn write_some() {
    let bdev = bdev_lookup_by_name(BDEVNAME).expect("failed to lookup bdev");
    let d = Descriptor::open(&bdev, true).expect("failed open bdev");
    let mut buf = d.dma_malloc(512).expect("failed to allocate buffer");
    buf.fill(0xff);

    let s = buf.as_slice();
    assert_eq!(s[0], 0xff);

    d.write_at(0, &buf).await.unwrap();
}

async fn read_some() {
    let bdev = bdev_lookup_by_name(BDEVNAME).expect("failed to lookup bdev");
    let d = Descriptor::open(&bdev, false);
    let d = d.unwrap();
    let mut buf = d.dma_malloc(1024).expect("failed to allocate buffer");
    let slice = buf.as_mut_slice();

    assert_eq!(slice[0], 0);
    slice[513] = 0xff;
    assert_eq!(slice[513], 0xff);

    d.read_at(0, &mut buf).await.unwrap();

    let slice = buf.as_slice();

    assert_eq!(slice[0], 0xff);
    assert_eq!(slice[513], 0);
}
