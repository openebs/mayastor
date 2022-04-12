use std::process::Command;

use common::bdev_io;
use io_engine::{core::MayastorCliArgs, nexus_uri::bdev_create};

static DISKNAME: &str = "/tmp/disk.img";
static BDEVNAME: &str = "aio:///tmp/disk.img?blk_size=512";
pub mod common;

#[tokio::test]
async fn io_test() {
    let ms = common::MayastorTest::new(MayastorCliArgs::default());

    let output = Command::new("truncate")
        .args(&["-s", "64m", DISKNAME])
        .output()
        .expect("failed exec truncate");

    assert!(output.status.success());
    ms.spawn(async { start().await }).await;

    let output = Command::new("rm")
        .args(&["-rf", DISKNAME])
        .output()
        .expect("failed delete test file");

    assert!(output.status.success());
}

// The actual work here is completely driven by the futures. We
// only execute one future per reactor loop.
async fn start() {
    bdev_create(BDEVNAME).await.expect("failed to create bdev");
    bdev_io::write_some(BDEVNAME, 0, 0xff).await.unwrap();
    bdev_io::read_some(BDEVNAME, 0, 0xff).await.unwrap();
}
