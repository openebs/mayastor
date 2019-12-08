use mayastor::{
    bdev::nexus::nexus_bdev::{nexus_create, nexus_lookup, NexusState},
    environment::{args::MayastorCliArgs, env::MayastorEnvironment},
    mayastor_stop,
    poller::SetTimeout,
};

static DISKNAME1: &str = "/tmp/disk1.img";
static BDEVNAME1: &str = "aio:///tmp/disk1.img?blk_size=512";

static DISKNAME2: &str = "/tmp/disk2.img";
static BDEVNAME2: &str = "aio:///tmp/disk2.img?blk_size=512";

pub mod common;
#[test]
fn copy_task() {
    common::mayastor_test_init();

    common::dd_random_file(DISKNAME1, 4096, 64 * 1024);
    common::truncate_file(DISKNAME2, 64 * 1024);

    let rc = MayastorEnvironment::new(MayastorCliArgs::default())
        .start(|| mayastor::executor::spawn(works()))
        .unwrap();

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
}

async fn works() {
    // mostly to test the timeout function itself here the nexus does not even
    // exist yet
    SetTimeout::usec(
        "rebuild_nexus".into(),
        |nexus: String| {
            let n = nexus_lookup(&nexus).unwrap();
            assert_eq!(n.status(), NexusState::Online);
            mayastor_stop(0);
        },
        5_000_000,
    );
    create_nexus().await;
    rebuild_nexus_online().await;
}
