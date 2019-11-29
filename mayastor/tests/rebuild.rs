use mayastor::{
    bdev::nexus::nexus_bdev::{nexus_create, nexus_lookup},
    mayastor_start,
    mayastor_stop,
};

static DISKNAME1: &str = "/tmp/disk1.img";
static BDEVNAME1: &str = "aio:///tmp/disk1.img?blk_size=512";

static DISKNAME2: &str = "/tmp/disk2.img";
static BDEVNAME2: &str = "aio:///tmp/disk2.img?blk_size=512";

mod common;
#[test]
fn copy_task() {
    common::mayastor_test_init();
    let args = vec!["rebuild_task", "-m", "0x3"];

    common::dd_random_file(DISKNAME1, 4096, 64 * 1024);
    common::truncate_file(DISKNAME2, 64 * 1024);

    let rc: i32 = mayastor_start("test", args, || {
        mayastor::executor::spawn(works());
    });

    assert_eq!(rc, 0);

    common::compare_files(DISKNAME1, DISKNAME2);
    common::delete_file(&[DISKNAME1.into(), DISKNAME2.into()]);
}

async fn create_nexus() {
    let ch = vec![BDEVNAME1.to_string(), BDEVNAME2.to_string()];
    nexus_create("rebuild_nexus", 64 * 1024 * 1024, None, &ch)
        .await
        .unwrap();
}

async fn rebuild_nexus_online() {
    let nexus = nexus_lookup("rebuild_nexus").unwrap();
    // fault a child which will allow us to rebuild it
    nexus.fault_child(BDEVNAME1).await.unwrap();
    nexus.start_rebuild(0).unwrap();
    nexus.rebuild_completion().await.unwrap();
    nexus.close().unwrap();
}

async fn works() {
    create_nexus().await;
    rebuild_nexus_online().await;
    mayastor_stop(0);
}
