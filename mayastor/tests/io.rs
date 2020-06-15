use std::process::Command;

use mayastor::{
    core::{
        mayastor_env_stop,
        Bdev,
        MayastorCliArgs,
        MayastorEnvironment,
        Reactor,
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
        .start(|| {
            Reactor::block_on(async {
                start().await;
            });
        })
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
    let bdev = Bdev::lookup_by_name(BDEVNAME).expect("failed to lookup bdev");
    let d = bdev
        .open(true)
        .expect("failed open bdev")
        .into_handle()
        .unwrap();
    let mut buf = d.dma_malloc(512).expect("failed to allocate buffer");
    buf.fill(0xff);

    let s = buf.as_slice();
    assert_eq!(s[0], 0xff);

    d.write_at(0, &buf).await.unwrap();
}

async fn read_some() {
    let bdev = Bdev::lookup_by_name(BDEVNAME).expect("failed to lookup bdev");
    let d = bdev.open(false).unwrap().into_handle().unwrap();
    let mut buf = d.dma_malloc(1024).expect("failed to allocate buffer");
    let slice = buf.as_mut_slice();

    assert_eq!(slice[0], 0);
    slice[512] = 0xff;
    assert_eq!(slice[512], 0xff);

    d.read_at(0, &mut buf).await.unwrap();

    let slice = buf.as_slice();

    assert_eq!(slice[0], 0xff);
    assert_eq!(slice[512], 0);
}
