use crossbeam::channel::unbounded;

use mayastor::{
    bdev::{nexus_create, nexus_lookup},
    core::{mayastor_env_stop, MayastorCliArgs, MayastorEnvironment, Reactor},
};

use rpc::mayastor::ShareProtocolNexus;

static DISKNAME1: &str = "/tmp/disk1.img";
static BDEVNAME1: &str = "aio:///tmp/disk1.img?blk_size=512";

static DISKNAME2: &str = "/tmp/disk2.img";
static BDEVNAME2: &str = "aio:///tmp/disk2.img?blk_size=512";

pub mod common;

#[test]
fn mount_fs() {
    // test xfs as well as ext4
    async fn mirror_fs_test<'a>(fstype: String) {
        create_nexus().await;
        let nexus = nexus_lookup("nexus").unwrap();

        //TODO: repeat this test for NVMF and ISCSI
        let device = nexus
            .share(ShareProtocolNexus::NexusNbd, None)
            .await
            .unwrap();
        let (s, r) = unbounded();

        // create an XFS filesystem on the nexus device, mount it, create a file
        // and return the md5 of that file

        let s1 = s.clone();
        std::thread::spawn(move || {
            common::mkfs(&device, &fstype);
            let md5 = common::mount_and_write_file(&device);
            s1.send(md5).unwrap();
        });

        reactor_poll!(r);
        // destroy the share and the nexus
        nexus.unshare().await.unwrap();
        nexus.destroy().await;

        // create a split nexus, i.e two nexus devices which each one leg of the
        // mirror
        create_nexus_splitted().await;

        let left = nexus_lookup("left").unwrap();
        let right = nexus_lookup("right").unwrap();

        // share both nexuses
        //TODO: repeat this test for NVMF and ISCSI, and permutations?
        let left_device = left
            .share(ShareProtocolNexus::NexusNbd, None)
            .await
            .unwrap();
        let right_device = right
            .share(ShareProtocolNexus::NexusNbd, None)
            .await
            .unwrap();

        let s1 = s.clone();
        std::thread::spawn(move || {
            s1.send(common::mount_and_get_md5(&left_device))
        });
        let md5_left: String;
        reactor_poll!(r, md5_left);

        left.unshare().await.unwrap();
        left.destroy().await;

        let s1 = s.clone();
        // read the md5 of the right side of the mirror
        std::thread::spawn(move || {
            s1.send(common::mount_and_get_md5(&right_device))
        });

        let md5_right;
        reactor_poll!(r, md5_right);
        right.unshare().await.unwrap();
        right.destroy().await;
        assert_eq!(md5_left, md5_right);
    }

    test_init!();

    common::delete_file(&[DISKNAME1.into(), DISKNAME2.into()]);
    common::truncate_file(DISKNAME1, 64 * 1024);
    common::truncate_file(DISKNAME2, 64 * 1024);

    Reactor::block_on(async {
        mirror_fs_test("xfs".into()).await;
        mirror_fs_test("ext4".into()).await;
    });
}

#[test]
fn mount_fs_1() {
    test_init!();
    Reactor::block_on(async {
        let (s, r) = unbounded::<String>();
        create_nexus().await;
        let nexus = nexus_lookup("nexus").unwrap();

        //TODO: repeat this test for NVMF and ISCSI
        let device = nexus
            .share(ShareProtocolNexus::NexusNbd, None)
            .await
            .unwrap();

        std::thread::spawn(move || {
            for _i in 0 .. 10 {
                common::mount_umount(&device);
            }
            s.send("".into())
        });

        reactor_poll!(r);
        nexus.destroy().await;
    });
}

#[test]
fn mount_fs_2() {
    test_init!();
    Reactor::block_on(async {
        create_nexus().await;
        let nexus = nexus_lookup("nexus").unwrap();

        //TODO: repeat this test for NVMF and ISCSI
        let device = nexus
            .share(ShareProtocolNexus::NexusNbd, None)
            .await
            .unwrap();
        let (s, r) = unbounded::<String>();

        std::thread::spawn(move || s.send(common::fio_run_verify(&device)));
        reactor_poll!(r);
        nexus.destroy().await;
    });

    mayastor_env_stop(0);
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
