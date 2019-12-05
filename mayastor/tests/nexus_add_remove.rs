use mayastor::{
    bdev::nexus::{
        instances,
        nexus_bdev::{nexus_create, nexus_lookup, NexusState},
        Error,
        Error::{ChildExists, CreateFailed, Invalid},
    },
    mayastor_start,
    mayastor_stop,
    rebuild::RebuildState,
};

static DISKNAME1: &str = "/tmp/disk1.img";
static BDEVNAME1: &str = "aio:///tmp/disk1.img?blk_size=512";

static DISKNAME2: &str = "/tmp/disk2.img";
static BDEVNAME2: &str = "aio:///tmp/disk2.img?blk_size=512";

static DISKNAME3: &str = "/tmp/disk3.img";
//static BDEVNAME3: &str = "aio:///tmp/disk3.img?blk_size=512";
pub mod common;
#[test]

/// main test
fn nexus_add_remove() {
    common::mayastor_test_init();
    let args = vec!["rebuild_task", "-m", "0x3"];

    //    common::dd_random_file(DISKNAME1, 4096, 64 * 1024);
    common::truncate_file(DISKNAME1, 64 * 1024);
    common::truncate_file(DISKNAME2, 64 * 1024);
    common::truncate_file(DISKNAME3, 64 * 1024);

    let rc: i32 = mayastor_start("test", args, || {
        mayastor::executor::spawn(works());
    });

    assert_eq!(rc, 0);

    //common::compare_files(DISKNAME1, DISKNAME2);
    //common::delete_file(&[DISKNAME1.into(), DISKNAME2.into()]);
}

/// test creation of a nexus with no children
async fn create_nexus() {
    let ch = vec![];
    let r = nexus_create("add_remove", 64 * 1024 * 1024, None, &ch).await;
    assert_eq!(r, Err(Error::NexusIncomplete));
    assert!(instances().is_empty());

    let nexus = nexus_lookup("add_remove");
    assert_eq!(true, nexus.is_none());
}

/// test creation of a nexus using an invalid scheme
async fn nexus_add_invalid_schema() {
    let ch = vec!["/does/not/exist.img".to_string()];
    let r = nexus_create("add_remove", 64 * 1024 * 1024, None, &ch).await;

    assert_eq!(true, r.is_err());
    assert_eq!(r, Err(Invalid("InvalidScheme".into())));

    let nexus = nexus_lookup("add_remove");
    assert_eq!(true, nexus.is_none());
}

/// test creation with an invalid disks
async fn nexus_add_invalid_disk() {
    let ch = vec!["aio:///does/not/exist.img".to_string()];
    let r = nexus_create("add_remove", 64 * 1024 * 1024, None, &ch).await;

    assert_eq!(r, Err(CreateFailed));

    let nexus = nexus_lookup("add_remove");
    assert_eq!(true, nexus.is_none());
}

/// create a nexus with one disk
async fn nexus_add_step1() {
    let ch = vec![BDEVNAME1.to_string()];
    let r = nexus_create("add_remove", 64 * 1024 * 1024, None, &ch).await;

    assert_eq!(r, Ok(()));

    let nexus = nexus_lookup("add_remove").unwrap();
    assert_eq!(NexusState::Online, nexus.status());
}

/// add a second disk to the already created nexus
async fn nexus_add_step2() {
    let nexus = nexus_lookup("add_remove").unwrap();
    assert_eq!(NexusState::Online, nexus.status());

    let result = nexus.add_child(BDEVNAME2).await;
    assert_eq!(NexusState::Degraded, result.unwrap());
}

/// adding the same disk to the nexus
async fn nexus_add_step3() {
    let nexus = nexus_lookup("add_remove").unwrap();
    assert_eq!(NexusState::Degraded, nexus.status());

    let result = nexus.add_child(BDEVNAME2).await;
    assert_eq!(result, Err(ChildExists));
    assert_eq!(NexusState::Degraded, nexus.status());
}

async fn nexus_rebuild_1() {
    let nexus = nexus_lookup("add_remove").unwrap();
    assert_eq!(NexusState::Degraded, nexus.status());

    let result = nexus.start_rebuild(0).unwrap();

    assert_eq!(NexusState::Remuling, nexus.status());
    assert_eq!(NexusState::Remuling, result);

    let result = nexus.rebuild_completion().await.unwrap();

    assert_eq!(result, RebuildState::Completed);
}

async fn works() {
    create_nexus().await;
    nexus_add_invalid_schema().await;
    nexus_add_invalid_disk().await;

    nexus_add_step1().await;
    nexus_add_step2().await;
    nexus_add_step3().await;

    nexus_rebuild_1().await;

    mayastor_stop(0)
}
