use futures::channel::oneshot;
use mayastor::{
    bdev::nexus::{
        nexus_bdev::{nexus_create, nexus_lookup},
        nexus_io,
    },
    mayastor_start,
    mayastor_stop,
};

static DISKNAME1: &str = "/tmp/disk1.img";
static BDEVNAME1: &str = "aio:///tmp/disk1.img?blk_size=512";

static DISKNAME2: &str = "/tmp/disk2.img";
static BDEVNAME2: &str = "aio:///tmp/disk2.img?blk_size=512";

pub mod common;
#[test]
fn mount_fs() {
    common::mayastor_test_init();
    let args = vec!["io_type", "-m", "0x3"];

    common::dd_random_file(DISKNAME1, 4096, 64 * 1024);
    common::truncate_file(DISKNAME2, 64 * 1024);

    let rc: i32 = mayastor_start("test", args, || {
        mayastor::executor::spawn(works());
    });

    assert_eq!(rc, 0);
    common::delete_file(&[DISKNAME1.into(), DISKNAME2.into()]);
}

async fn create_nexus() {
    let ch = vec![BDEVNAME1.to_string(), BDEVNAME2.to_string()];
    nexus_create("nexus", 64 * 1024 * 1024, None, &ch)
        .await
        .unwrap();
}

async fn works() {
    create_nexus().await;
    let nexus = nexus_lookup("nexus").unwrap();

    assert_eq!(true, nexus.io_is_supported(nexus_io::io_type::READ));
    assert_eq!(true, nexus.io_is_supported(nexus_io::io_type::WRITE));
    assert_eq!(true, nexus.io_is_supported(nexus_io::io_type::FLUSH));
    assert_eq!(true, nexus.io_is_supported(nexus_io::io_type::RESET));

    // for aio bdevs this is set to false;
    assert_eq!(false, nexus.io_is_supported(nexus_io::io_type::UNMAP));

    let device = nexus.share(None).await.unwrap();
    let (s, r) = oneshot::channel::<bool>();

    // we cannot block the reactor
    std::thread::spawn(move || {
        common::mkfs(&device, "xfs");
        common::mkfs(&device, "ext4");
        s.send(true)
    });

    r.await.unwrap();
    mayastor_stop(0);
}
