extern crate assert_matches;

use assert_matches::assert_matches;
use mayastor::{
    bdev::nexus::{
        instances,
        nexus_bdev::{nexus_create, nexus_lookup, Error, NexusState},
    },
    environment::{args::MayastorCliArgs, env::MayastorEnvironment},
    mayastor_stop,
    rebuild::RebuildState,
};

static DISKNAME1: &str = "/tmp/disk1.img";
static BDEVNAME1: &str = "aio:///tmp/disk1.img?blk_size=512";

static DISKNAME2: &str = "/tmp/disk2.img";
static BDEVNAME2: &str = "aio:///tmp/disk2.img?blk_size=512";

static DISKNAME3: &str = "/tmp/disk3.img";
static BDEVNAME3: &str = "aio:///tmp/disk3.img?blk_size=512";
pub mod common;
#[test]
/// main test
fn nexus_add_remove() {
    common::mayastor_test_init();

    common::dd_random_file(DISKNAME1, 4096, 64 * 1024);
    common::truncate_file(DISKNAME2, 64 * 1024);
    common::truncate_file(DISKNAME3, 64 * 1024);

    let rc = MayastorEnvironment::new(MayastorCliArgs {
        reactor_mask: "0x3".into(),
        mem_size: 0,
        rpc_address: "".to_string(),
        no_pci: true,
        config: None,
        log_components: vec!["thread".into()],
    })
    .start(|| mayastor::executor::spawn(works()))
    .unwrap();

    assert_eq!(rc, 0);
    //common::compare_files(DISKNAME1, DISKNAME2);
    //common::delete_file(&[DISKNAME1.into(), DISKNAME2.into()]);
}

/// test creation of a nexus with no children
async fn create_nexus() {
    let ch = vec![];
    let r = nexus_create("add_remove", 64 * 1024 * 1024, None, &ch).await;
    assert_matches!(
        r,
        Err(Error::NexusIncomplete {
            ..
        })
    );
    assert!(instances().is_empty());

    let nexus = nexus_lookup("add_remove");
    assert_eq!(true, nexus.is_none());
}

/// test creation of a nexus using an invalid scheme
async fn nexus_add_invalid_schema() {
    let ch = vec!["/does/not/exist.img".to_string()];
    let r = nexus_create("add_remove", 64 * 1024 * 1024, None, &ch).await;

    assert_eq!(true, r.is_err());
    assert_matches!(
        r,
        Err(Error::CreateChild {
            ..
        })
    );

    let nexus = nexus_lookup("add_remove");
    assert_eq!(true, nexus.is_none());
}

/// test creation with an invalid disks
async fn nexus_add_invalid_disk() {
    let ch = vec!["aio:///does/not/exist.img".to_string()];
    let r = nexus_create("add_remove", 64 * 1024 * 1024, None, &ch).await;

    assert_matches!(
        r,
        Err(Error::CreateChild {
            ..
        })
    );

    let nexus = nexus_lookup("add_remove");
    assert_eq!(true, nexus.is_none());
}

/// create a nexus with one disk
async fn nexus_add_step1() {
    let ch = vec![BDEVNAME1.to_string()];
    nexus_create("add_remove", 64 * 1024 * 1024, None, &ch)
        .await
        .unwrap();

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
    assert_matches!(
        result,
        Err(Error::CreateChild {
            ..
        })
    );
    assert_eq!(NexusState::Degraded, nexus.status());
}

/// add a replica and rebuild
async fn nexus_rebuild_1() {
    let nexus = nexus_lookup("add_remove").unwrap();
    assert_eq!(NexusState::Degraded, nexus.status());

    let result = nexus.start_rebuild(0).unwrap();

    assert_eq!(NexusState::Remuling, nexus.status());
    assert_eq!(NexusState::Remuling, result);

    let state = nexus.rebuild_completion().await.unwrap();
    assert_eq!(state, RebuildState::Completed);
}

/// once completed the nexus should be online
async fn nexus_remove_1() {
    let nexus = nexus_lookup("add_remove").unwrap();
    assert_eq!(NexusState::Online, nexus.status());

    // removing a child does not degrade a nexus
    nexus.remove_child(BDEVNAME1).await.unwrap();
    assert_eq!(nexus.status(), NexusState::Online);
}

/// add back the removed child and add a third replica
async fn nexus_remove_2() {
    let nexus = nexus_lookup("add_remove").unwrap();
    assert_eq!(NexusState::Online, nexus.status());

    let state = nexus.add_child(BDEVNAME1).await.unwrap();
    assert_eq!(state, NexusState::Degraded);
    assert_eq!(NexusState::Degraded, nexus.status());

    let state = nexus.add_child(BDEVNAME3).await.unwrap();
    assert_eq!(state, NexusState::Degraded);
    assert_eq!(NexusState::Degraded, nexus.status());

    assert_eq!(nexus.child_count(), 3);
    assert_eq!(nexus.is_healthy(), false);
    assert_eq!(nexus.is_online(), false);
}
/// we now have 3 replicas where two of them are degraded. we require two
/// rebuilds
async fn nexus_remove_3() {
    let nexus = nexus_lookup("add_remove").unwrap();
    assert_eq!(NexusState::Degraded, nexus.status());

    let state = nexus.start_rebuild(1).unwrap();
    assert_eq!(state, NexusState::Remuling);
    assert_eq!(nexus.status(), NexusState::Remuling);

    let state = nexus.rebuild_completion().await.unwrap();
    assert_eq!(state, RebuildState::Completed);
    assert_eq!(nexus.status(), NexusState::Degraded);
    assert_eq!(nexus.is_online(), false);

    let state = nexus.start_rebuild(0).unwrap();
    assert_eq!(state, NexusState::Remuling);
    assert_eq!(nexus.status(), NexusState::Remuling);

    let state = nexus.rebuild_completion().await.unwrap();
    assert_eq!(state, RebuildState::Completed);
    assert_eq!(nexus.status(), NexusState::Online);
}
/// removing two replicas
async fn nexus_remove_4() {
    let nexus = nexus_lookup("add_remove").unwrap();
    assert_eq!(NexusState::Online, nexus.status());

    // removing a child does not degrade a nexus
    nexus.remove_child(BDEVNAME1).await.unwrap();
    assert_eq!(nexus.status(), NexusState::Online);

    nexus.remove_child(BDEVNAME2).await.unwrap();
    assert_eq!(nexus.status(), NexusState::Online);
}

/// removing the last replica should error out
async fn nexus_remove_5() {
    let nexus = nexus_lookup("add_remove").unwrap();
    assert_eq!(NexusState::Online, nexus.status());

    // removing a child does not degrade a nexus
    let result = nexus.remove_child(BDEVNAME3).await;

    assert_eq!(result.is_err(), true);
    assert_eq!(nexus.status(), NexusState::Online);
}

/// rebuilding with only one replica should fail
async fn rebuild_should_error() {
    let nexus = nexus_lookup("add_remove").unwrap();
    assert_eq!(NexusState::Online, nexus.status());

    let result = nexus.start_rebuild(0);
    assert_eq!(result.is_err(), true);
    nexus.destroy().await;
}

async fn works() {
    create_nexus().await;
    nexus_add_invalid_schema().await;
    nexus_add_invalid_disk().await;

    nexus_add_step1().await;
    nexus_add_step2().await;
    nexus_add_step3().await;

    nexus_rebuild_1().await;

    nexus_remove_1().await;
    nexus_remove_2().await;
    nexus_remove_3().await;
    nexus_remove_4().await;
    nexus_remove_5().await;

    rebuild_should_error().await;
    //mayastor_env_stop(0);
    mayastor_stop(0);
}
