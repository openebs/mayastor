use std::{collections::HashMap, pin::Pin};

use futures::Future;

use mayastor::{
    bdev::{nexus_create, nexus_lookup},
    core::{
        mayastor_env_stop,
        Bdev,
        BdevHandle,
        MayastorCliArgs,
        MayastorEnvironment,
    },
    executor,
};

use mayastor::nexus_uri::{bdev_create, bdev_destroy};

static DISKNAME1: &str = "/tmp/disk1.img";
static BDEVNAME1: &str = "aio:///tmp/disk1.img?blk_size=512";

static DISKNAME2: &str = "/tmp/disk2.img";
static BDEVNAME2: &str = "aio:///tmp/disk2.img?blk_size=512";

pub mod common;

#[derive(Debug, PartialEq)]
pub enum Test {
    Pass,
    Fail,
}

#[test]
fn core() {
    common::mayastor_test_init();
    common::truncate_file(DISKNAME1, 64 * 1024);
    common::truncate_file(DISKNAME2, 64 * 1024);

    let mut test_cases: HashMap<&str, TestCase> = HashMap::new();
    test_cases.insert("open/close", Box::pin(works()));
    test_cases.insert("multiple open/close", Box::pin(multiple_open()));
    test_cases.insert("handle tests", Box::pin(handle_test()));

    let rc = MayastorEnvironment::new(MayastorCliArgs::default())
        .start(move || {
            executor::spawn(async move {
                for (name, f) in test_cases {
                    println!("\n\nRunning test: {}", name);
                    match f.await {
                        r => println!("\n\n{}.... [{:?}]\n", name, r),
                    }
                }
                mayastor_env_stop(0);
            })
        })
        .unwrap();

    assert_eq!(rc, 0);

    common::compare_files(DISKNAME1, DISKNAME2);
    common::delete_file(&[DISKNAME1.into(), DISKNAME2.into()]);
}

type TestResult = Result<(), ()>;
type TestCase = Pin<Box<dyn Future<Output = TestResult>>>;

async fn create_nexus() -> TestResult {
    let ch = vec![BDEVNAME1.to_string(), BDEVNAME2.to_string()];
    nexus_create("core_nexus", 64 * 1024 * 1024, None, &ch)
        .await
        .expect("failed to crate nexus");
    Ok(())
}

async fn works() -> TestResult {
    assert_eq!(Bdev::lookup_by_name("core_nexus").is_none(), true);
    create_nexus().await.expect("failed to create nexus");
    let b = Bdev::lookup_by_name("core_nexus").unwrap();
    assert_eq!(b.name(), "core_nexus");

    let desc = Bdev::open_by_name("core_nexus", false).unwrap();
    let channel = desc.get_channel().expect("failed to get IO channel");
    drop(channel);
    drop(desc);

    let n = nexus_lookup("core_nexus").expect("nexus not found");
    n.destroy().await;
    Ok(())
}

async fn multiple_open() -> TestResult {
    create_nexus().await.expect("failed to create nexus");

    let n = nexus_lookup("core_nexus").expect("failed to lookup nexus");

    let d1 = Bdev::open_by_name("core_nexus", true)
        .expect("failed to open first desc to nexus");
    let d2 = Bdev::open_by_name("core_nexus", true)
        .expect("failed to open second desc to nexus");

    let ch1 = d1.get_channel().expect("failed to get channel!");
    let ch2 = d2.get_channel().expect("failed to get channel!");
    drop(ch1);
    drop(ch2);

    // we must drop the descriptors before we destroy the nexus
    drop(dbg!(d1));
    drop(dbg!(d2));
    n.destroy().await;

    Ok(())
}

async fn handle_test() -> TestResult {
    bdev_create(BDEVNAME1).await.expect("failed to create bdev");
    let hdl2 = BdevHandle::open(BDEVNAME1, true, true)
        .expect("failed to create the handle!");
    let hdl3 = BdevHandle::open(BDEVNAME1, true, true);
    assert_eq!(hdl3.is_err(), true);

    // we must drop the descriptors before we destroy the nexus
    drop(hdl2);
    drop(hdl3);

    bdev_destroy(BDEVNAME1)
        .await
        .expect("failed to destroy bdev");

    Ok(())
}
