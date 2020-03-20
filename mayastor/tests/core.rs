use mayastor::{
    bdev::{nexus_create, nexus_lookup},
    core::{
        mayastor_env_stop,
        Bdev,
        BdevHandle,
        MayastorCliArgs,
        MayastorEnvironment,
        Reactor,
    },
    nexus_uri::{bdev_create, bdev_destroy},
};
use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, ErrorKind},
    os::unix::fs::OpenOptionsExt,
    sync::Once,
};

static DISKNAME1: &str = "/tmp/disk1.img";
static BDEVNAME1: &str = "aio:///tmp/disk1.img?blk_size=512";

static DISKNAME2: &str = "/tmp/disk2.img";
static BDEVNAME2: &str = "aio:///tmp/disk2.img?blk_size=512";

static DISKNAME3: &str = "/tmp/disk3.img";
static BDEVNAME3: &str = "uring:///tmp/disk3.img?blk_size=512";

static mut DO_URING: bool = false;
static INIT: Once = Once::new();

pub mod common;

fn fs_supports_direct_io() -> bool {
    // SPDK uring bdev uses IORING_SETUP_IOPOLL which requires O_DIRECT
    // which works on at least XFS filesystems
    match OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_DIRECT)
        .open(DISKNAME3)
    {
        Ok(_f) => true,
        Err(e) => {
            assert_eq!(e.kind(), ErrorKind::InvalidInput);
            println!("Skipping uring bdev, open: {:?}", e);
            false
        }
    }
}

fn get_mount_filesystem() -> Option<String> {
    let mut path = std::path::Path::new(DISKNAME3);
    loop {
        let f = match File::open("/etc/mtab") {
            Ok(f) => f,
            Err(e) => {
                eprintln!("open: {}", e);
                return None;
            }
        };
        let reader = BufReader::new(f);

        let d = path.to_str().unwrap();
        for line in reader.lines() {
            let l = match line {
                Ok(l) => l,
                Err(e) => {
                    eprintln!("line: {}", e);
                    return None;
                }
            };
            let parts: Vec<&str> = l.split_whitespace().collect();
            if !parts.is_empty() && parts[1] == d {
                return Some(parts[2].to_string());
            }
        }

        path = match path.parent() {
            None => return None,
            Some(p) => p,
        }
    }
}

fn fs_type_supported() -> bool {
    match get_mount_filesystem() {
        None => {
            println!("Skipping uring bdev, unknown fs");
            false
        }
        Some(d) => match d.as_str() {
            "xfs" => true,
            _ => {
                println!("Skipping uring bdev, fs: {}", d);
                false
            }
        },
    }
}

fn kernel_supports_io_uring() -> bool {
    // Match SPDK_URING_QUEUE_DEPTH
    let queue_depth = 512;
    match io_uring::IoUring::new(queue_depth) {
        Ok(_ring) => true,
        Err(e) => {
            assert_eq!(e.kind(), ErrorKind::Other);
            assert_eq!(e.raw_os_error().unwrap(), libc::ENOSYS);
            println!("Skipping uring bdev, IoUring::new: {:?}", e);
            false
        }
    }
}

fn do_uring() -> bool {
    unsafe {
        INIT.call_once(|| {
            DO_URING = fs_supports_direct_io()
                && fs_type_supported()
                && kernel_supports_io_uring();
        });
        DO_URING
    }
}

async fn create_nexus() {
    let ch = if do_uring() {
        vec![
            BDEVNAME1.to_string(),
            BDEVNAME2.to_string(),
            BDEVNAME3.to_string(),
        ]
    } else {
        vec![BDEVNAME1.to_string(), BDEVNAME2.to_string()]
    };
    nexus_create("core_nexus", 64 * 1024 * 1024, None, &ch)
        .await
        .unwrap();
}

#[test]
fn core() {
    test_init!();
    common::truncate_file(DISKNAME1, 64 * 1024);
    common::truncate_file(DISKNAME2, 64 * 1024);
    common::truncate_file(DISKNAME3, 64 * 1024);

    Reactor::block_on(async {
        works().await;
    });
}

async fn works() {
    assert_eq!(Bdev::lookup_by_name("core_nexus").is_none(), true);
    create_nexus().await;
    let b = Bdev::lookup_by_name("core_nexus").unwrap();
    assert_eq!(b.name(), "core_nexus");

    let desc = Bdev::open_by_name("core_nexus", false).unwrap();
    let channel = desc.get_channel().expect("failed to get IO channel");
    drop(channel);
    drop(desc);

    let n = nexus_lookup("core_nexus").expect("nexus not found");
    n.destroy().await;
}

#[test]
fn core_2() {
    test_init!();
    Reactor::block_on(async {
        create_nexus().await;

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
    });
}

#[test]
fn core_3() {
    test_init!();
    Reactor::block_on(async {
        bdev_create(BDEVNAME1).await.expect("failed to create bdev");
        let hdl2 = BdevHandle::open(BDEVNAME1, true, true)
            .expect("failed to create the handle!");
        let hdl3 = BdevHandle::open(BDEVNAME1, true, true);
        assert_eq!(hdl3.is_err(), true);

        // we must drop the descriptors before we destroy the nexus
        drop(hdl2);
        drop(hdl3);

        bdev_destroy(BDEVNAME1).await.unwrap();
        mayastor_env_stop(1);
    });
}
