use std::process::Command;

use common::bdev_io;
use mayastor::{
    core::{mayastor_env_stop, MayastorCliArgs, MayastorEnvironment, Reactor},
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
    bdev_io::write_some(BDEVNAME).await.unwrap();
    bdev_io::read_some(BDEVNAME).await.unwrap();
    mayastor_env_stop(0);
}
