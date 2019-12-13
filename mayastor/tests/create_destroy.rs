use mayastor::{
    bdev::nexus::nexus_bdev::{nexus_create, nexus_lookup},
    mayastor_start,
    mayastor_stop,
};

static DISKNAME1: &str = "/tmp/disk1.img";
static BDEVNAME1: &str = "aio:///tmp/disk1.img?blk_size=512";

static DISKNAME2: &str = "/tmp/disk2.img";
static BDEVNAME2: &str = "aio:///tmp/disk2.img?blk_size=512";

pub mod common;
#[test]
fn create_destroy() {
    common::mayastor_test_init();
    let args = vec!["rebuild_task", "-m", "0x3", "-L", "bdev", "-L", "aio"];

    common::truncate_file(DISKNAME1, 64 * 1024);
    common::truncate_file(DISKNAME2, 64 * 1024);

    let rc: i32 = mayastor_start("test", args, || {
        mayastor::executor::spawn(works());
    });

    assert_eq!(rc, 0);
}

async fn works() {
    for _i in 0 .. 100 {
        nexus_create(
            "create",
            64 * 1024 * 1024,
            None,
            &[BDEVNAME1.into(), BDEVNAME2.into()],
        )
        .await
        .unwrap();

        let n = nexus_lookup("create").unwrap();
        n.share(None).await.unwrap();
        n.destroy().await;
    }
    mayastor_stop(0);
}
