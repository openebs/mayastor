use futures::channel::oneshot;
use log::*;

use mayastor::{
    bdev::{nexus_create, nexus_lookup},
    core::{mayastor_env_stop, MayastorCliArgs, MayastorEnvironment},
};

static DISKNAME1: &str = "/tmp/disk1.img";
static BDEVNAME1: &str = "aio:///tmp/disk1.img?blk_size=512";

static DISKNAME2: &str = "/tmp/disk2.img";
static BDEVNAME2: &str = "aio:///tmp/disk2.img?blk_size=512";

pub mod common;
#[test]
fn mount_fs() {
    common::mayastor_test_init();

    common::truncate_file(DISKNAME1, 64 * 1024);
    common::truncate_file(DISKNAME2, 64 * 1024);

    let rc = MayastorEnvironment::new(MayastorCliArgs::default())
        .start(|| mayastor::executor::spawn(works()))
        .unwrap();

    assert_eq!(rc, 0);

    common::delete_file(&[DISKNAME1.into(), DISKNAME2.into()]);
    common::clean_up_temp();
}

async fn create_nexus() {
    let ch = vec![BDEVNAME1.to_string(), BDEVNAME2.to_string()];
    nexus_create("nexus", 64 * 1024 * 1024, None, &ch)
        .await
        .unwrap();
}

async fn create_nexus_splitted() {
    let ch = vec![BDEVNAME1.to_string()];
    nexus_create("left", 64 * 1024 * 1024, None, &ch)
        .await
        .unwrap();

    let ch = vec![BDEVNAME2.to_string()];
    nexus_create("right", 64 * 1024 * 1024, None, &ch)
        .await
        .unwrap();
}

async fn mirror_fs_test<'a>(fstype: String) {
    info!("running mirror test: {}", fstype);
    create_nexus().await;
    let nexus = nexus_lookup("nexus").unwrap();

    let device = nexus.share(None).await.unwrap();
    let (s, r) = oneshot::channel::<String>();

    // create an XFS filesystem on the nexus device, mount it, create a file and
    // return the md5 of that file

    std::thread::spawn(move || {
        common::mkfs(&device, &fstype);
        let md5 = common::mount_and_write_file(&device);
        s.send(md5)
    });

    r.await.unwrap();
    // destroy the share and the nexus
    nexus.unshare().await.unwrap();
    nexus.destroy().await;

    // create a split nexus, i.e two nexus devices which each one leg of the
    // mirror
    create_nexus_splitted().await;

    let left = nexus_lookup("left").unwrap();
    let right = nexus_lookup("right").unwrap();

    // share both nexuses
    let left_device = left.share(None).await.unwrap();
    let right_device = right.share(None).await.unwrap();

    let (s, r) = oneshot::channel::<String>();

    // read back the md5 from the left leg
    //
    // XXX note -- as the filesystems are mirrors of one and other, you cannot
    // mount them both at the same time. This will be rejected by XFS
    // because they have the same exact UUID
    //

    std::thread::spawn(move || s.send(common::mount_and_get_md5(&left_device)));

    let md5_left = r.await.unwrap();
    left.unshare().await.unwrap();
    left.destroy().await;

    // read the md5 of the right side of the mirror
    let (s, r) = oneshot::channel::<String>();
    std::thread::spawn(move || {
        s.send(common::mount_and_get_md5(&right_device))
    });

    let md5_right = r.await.unwrap();

    right.unshare().await.unwrap();
    right.destroy().await;
    assert_eq!(md5_left, md5_right);
}

async fn mount_unmount() {
    create_nexus().await;
    let nexus = nexus_lookup("nexus").unwrap();

    let device = nexus.share(None).await.unwrap();
    let (s, r) = oneshot::channel::<String>();

    std::thread::spawn(move || {
        for _i in 0 .. 10 {
            common::mount_umount(&device);
        }
        s.send("".into())
    });

    r.await.unwrap();
    nexus.destroy().await;
}

async fn run_fio_on_nexus() {
    println!("Starting FIO for one minute");
    create_nexus().await;
    let nexus = nexus_lookup("nexus").unwrap();

    let device = nexus.share(None).await.unwrap();
    let (s, r) = oneshot::channel::<String>();

    std::thread::spawn(move || s.send(common::fio_run_verify(&device)));

    r.await.unwrap();
    nexus.destroy().await;
}

async fn works() {
    mirror_fs_test("xfs".into()).await;
    mirror_fs_test("ext4".into()).await;
    mount_unmount().await;
    run_fio_on_nexus().await;

    mayastor_env_stop(0);
}
