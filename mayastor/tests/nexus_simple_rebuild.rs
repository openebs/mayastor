use crossbeam::channel::unbounded;
pub mod common;

use mayastor::{
    bdev::{nexus_create, nexus_lookup},
    core::{
        mayastor_env_stop,
        MayastorCliArgs,
        MayastorEnvironment,
        Reactor,
    },
};

static DISKNAME1: &str = "/tmp/disk1.img";
static BDEVNAME1: &str = "aio:///tmp/disk1.img?blk_size=512";

static DISKNAME2: &str = "/tmp/disk2.img";
static BDEVNAME2: &str = "aio:///tmp/disk2.img?blk_size=512";

static NEXUS_NAME: &str = "rebuild_test";
static NEXUS_SIZE: u64 = 10 * 1024 * 1024;  // 10MiB

#[test]
fn rebuild_test() {

    common::delete_file(&[DISKNAME1.into(), DISKNAME2.into()]);
    common::truncate_file(DISKNAME1, NEXUS_SIZE / 1024);
    common::truncate_file(DISKNAME2, NEXUS_SIZE / 1024);

    test_init!();

    Reactor::block_on(rebuild_test_start());

    common::delete_file(&[DISKNAME1.into(), DISKNAME2.into()]);
}

async fn rebuild_test_start() {
    create_nexus().await;

    let nexus = nexus_lookup(NEXUS_NAME).unwrap();
    let device = nexus.share(None).await.unwrap();

    let nexus_device = device.clone();
    let (s, r) = unbounded::<String>();
    std::thread::spawn(move || s.send(dd_urandom(&nexus_device)));
    reactor_poll!(r);

    let nexus_device = device.clone();
    let (s, r) = unbounded::<String>();
    std::thread::spawn(move || s.send(compare_nexus_device(&nexus_device, DISKNAME1, true)));
    reactor_poll!(r);
    
    let nexus_device = device.clone();
    let (s, r) = unbounded::<String>();
    std::thread::spawn(move || s.send(compare_nexus_device(&nexus_device, DISKNAME2, false)));
    reactor_poll!(r);

    // add the second child -> atm it's where we rebuild as well
    nexus.add_child(BDEVNAME2).await.unwrap();

    let (s, r) = unbounded::<String>();
    std::thread::spawn(move || s.send(compare_devices(DISKNAME1, DISKNAME2, true)));
    reactor_poll!(r);

    mayastor_env_stop(0);
}

async fn create_nexus() {
    let ch = vec![BDEVNAME1.to_string()];
    nexus_create(NEXUS_NAME, NEXUS_SIZE, None, &ch)
        .await
        .unwrap();
}

pub fn dd_urandom(device: &str) -> String {
    let (_, stdout, _stderr) = run_script::run(
        r#"
        dd if=/dev/urandom of=$1 conv=fsync,nocreat,notrunc iflag=count_bytes count=`blockdev --getsize64 $1`
    "#,
    &vec![device.into()],
    &run_script::ScriptOptions::new(),
    )
    .unwrap();
    stdout
}

pub fn compare_nexus_device(nexus_device: &str, device: &str, expected_pass: bool) -> String {
    let (exit, stdout, _stderr) = run_script::run(
        r#"
        cmp -n `blockdev --getsize64 $1` $1 $2 0 5M
        test $? -eq $3
    "#,
    &vec![nexus_device.into(), device.into(), (!expected_pass as i32).to_string()],
    &run_script::ScriptOptions::new(),
    )
    .unwrap();
    assert_eq!(exit, 0);
    stdout
}

pub fn compare_devices(first_device: &str, second_device: &str, expected_pass: bool) -> String {
    let (exit, stdout, _stderr) = run_script::run(
        r#"
        cmp -b $1 $2 5M 5M
        test $? -eq $3
    "#,
    &vec![first_device.into(), second_device.into(), (!expected_pass as i32).to_string()],
    &run_script::ScriptOptions::new(),
    )
    .unwrap();
    assert_eq!(exit, 0);
    stdout
}
