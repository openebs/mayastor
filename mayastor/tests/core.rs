use mayastor::{
    bdev::nexus::nexus_bdev::nexus_create,
    core::Bdev,
    environment::{
        args::MayastorCliArgs,
        env::{mayastor_env_stop, MayastorEnvironment},
    },
};

static DISKNAME1: &str = "/tmp/disk1.img";
static BDEVNAME1: &str = "aio:///tmp/disk1.img?blk_size=512";

static DISKNAME2: &str = "/tmp/disk2.img";
static BDEVNAME2: &str = "aio:///tmp/disk2.img?blk_size=512";

pub mod common;
#[test]
fn core() {
    common::mayastor_test_init();

    common::truncate_file(DISKNAME1, 64 * 1024);
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
    nexus_create("core_nexus", 64 * 1024 * 1024, None, &ch)
        .await
        .unwrap();
}

async fn works() {
    assert_eq!(Bdev::lookup_by_name("core_nexus").is_none(), true);
    create_nexus().await;
    let b = Bdev::lookup_by_name("core_nexus").unwrap();
    assert_eq!(b.name(), "core_nexus");

    let desc = Bdev::open("core_nexus", false).unwrap();
    let bdev = desc.get_bdev().unwrap();
    println!("{:?}", bdev);
    println!("{:?}", bdev.uuid_as_string());
    assert_eq!(b.name(), bdev.name());
    mayastor_env_stop(0);
}
