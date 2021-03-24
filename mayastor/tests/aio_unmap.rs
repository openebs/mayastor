use common::MayastorTest;

use mayastor::{
    core::{Bdev, BdevHandle, MayastorCliArgs, Share},
    ffihelper::cb_arg,
    lvs::{Lvol, Lvs},
    nexus_uri::{bdev_create, bdev_destroy},
};

use rpc::mayastor::CreatePoolRequest;

use futures::channel::oneshot;
use once_cell::sync::Lazy;
use std::{
    ffi::{c_void, CString},
    io::{Error, ErrorKind},
    mem::MaybeUninit,
};

use spdk_sys::{
    spdk_bdev_free_io,
    spdk_bdev_io,
    spdk_bdev_unmap,
    spdk_bdev_write_zeroes,
};

use nvmeadm::NvmeTarget;
use std::convert::TryFrom;

use tracing::info;

pub mod common;

static BDEVNAME1: &str = "aio:///tmp/disk1.img";
static DISKNAME1: &str = "/tmp/disk1.img";

// Get the I/O blocksize for the filesystem upon which the given file resides.
fn get_fs_blocksize(path: &str) -> Result<u64, Error> {
    Ok(stat(path)?.st_blksize as u64)
}

// Return the number of (512-byte) blocks allocated for a given file.
// Note that the result is ALWAYS in terms of 512-byte blocks,
// regardless of the underlying filesystem I/O blocksize.
fn get_allocated_blocks(path: &str) -> Result<u64, Error> {
    Ok(stat(path)?.st_blocks as u64)
}

// Obtain stat information for given file
fn stat(path: &str) -> Result<libc::stat64, Error> {
    let mut data: MaybeUninit<libc::stat64> = MaybeUninit::uninit();
    let cpath = CString::new(path).unwrap();

    if unsafe { libc::stat64(cpath.as_ptr(), data.as_mut_ptr()) } < 0 {
        return Err(Error::last_os_error());
    }

    Ok(unsafe { data.assume_init() })
}

extern "C" fn io_completion_cb(
    io: *mut spdk_bdev_io,
    success: bool,
    arg: *mut c_void,
) {
    let sender =
        unsafe { Box::from_raw(arg as *const _ as *mut oneshot::Sender<bool>) };

    unsafe {
        spdk_bdev_free_io(io);
    }

    sender
        .send(success)
        .expect("io completion callback - receiver side disappeared");
}

// Unmap a specified region.
// Partial blocks are zeroed rather than deallocated.
// This means that no blocks will be deallocated if the
// region is smaller than the filesystem I/O blocksize.
async fn unmap(
    handle: &BdevHandle,
    offset: u64,
    nbytes: u64,
) -> Result<(), Error> {
    let (sender, receiver) = oneshot::channel::<bool>();

    let (desc, ch) = handle.io_tuple();
    let errno = unsafe {
        spdk_bdev_unmap(
            desc,
            ch,
            offset,
            nbytes,
            Some(io_completion_cb),
            cb_arg(sender),
        )
    };

    if errno != 0 {
        return Err(Error::from_raw_os_error(errno.abs()));
    }

    if receiver.await.expect("failed awaiting unmap completion") {
        return Ok(());
    }

    Err(Error::new(ErrorKind::Other, "unmap failed"))
}

// Zero a specified region.
// This is performed as efficiently as possible according
// to the capabilities of the underlying bdev.
async fn write_zeroes(
    handle: &BdevHandle,
    offset: u64,
    nbytes: u64,
) -> Result<(), Error> {
    let (sender, receiver) = oneshot::channel::<bool>();
    let (desc, ch) = handle.io_tuple();

    let errno = unsafe {
        spdk_bdev_write_zeroes(
            desc,
            ch,
            offset,
            nbytes,
            Some(io_completion_cb),
            cb_arg(sender),
        )
    };

    if errno != 0 {
        return Err(Error::from_raw_os_error(errno.abs()));
    }

    if receiver
        .await
        .expect("failed awaiting write_zeroes completion")
    {
        return Ok(());
    }

    Err(Error::new(ErrorKind::Other, "write_zeroes failed"))
}

fn setup() -> &'static MayastorTest<'static> {
    static MAYASTOR: Lazy<MayastorTest<'static>> =
        Lazy::new(|| MayastorTest::new(MayastorCliArgs::default()));
    &MAYASTOR
}

#[tokio::test]
async fn zero_bdev_test() {
    const FILE_SIZE: u64 = 64 * 1024;

    common::delete_file(&[DISKNAME1.into()]);
    common::truncate_file(DISKNAME1, FILE_SIZE);

    let ms = setup();

    // Create a bdev.
    ms.spawn(async {
        bdev_create(BDEVNAME1).await.expect("failed to create bdev");
    })
    .await;

    // Write some data to the bdev.
    ms.spawn(async {
        let handle = BdevHandle::open(BDEVNAME1, true, false)
            .expect("failed to obtain bdev handle");
        let mut buf = handle.dma_malloc(4096).unwrap();
        buf.fill(42);
        handle.write_at(2048, &buf).await.unwrap();
        handle.close();
    })
    .await;

    // Read data back from the bdev.
    ms.spawn(async {
        let handle = BdevHandle::open(BDEVNAME1, false, false)
            .expect("failed to obtain bdev handle");
        let mut buf = handle.dma_malloc(4096).unwrap();
        handle.read_at(2048, &mut buf).await.unwrap();
        handle.close();

        for &c in buf.as_slice() {
            assert_eq!(c, 42, "each byte should have value 42");
        }
    })
    .await;

    // Zero the region of the lvol.
    ms.spawn(async {
        let handle = BdevHandle::open(BDEVNAME1, true, false)
            .expect("failed to obtain bdev handle");
        write_zeroes(&handle, 2048, 4096).await.unwrap();
        handle.close();
    })
    .await;

    // Re-read data from the bdev and verify that it has been zeroed.
    ms.spawn(async {
        let handle = BdevHandle::open(BDEVNAME1, false, false)
            .expect("failed to obtain bdev handle");
        let mut buf = handle.dma_malloc(4096).unwrap();
        handle.read_at(2048, &mut buf).await.unwrap();
        handle.close();

        for &c in buf.as_slice() {
            assert_eq!(c, 0, "each byte should have value 0");
        }
    })
    .await;

    // Destroy the bdev.
    ms.spawn(async {
        bdev_destroy(BDEVNAME1).await.unwrap();
    })
    .await;

    // Validate the state of mayastor.
    ms.spawn(async {
        // no bdevs
        assert_eq!(Bdev::bdev_first().into_iter().count(), 0);
    })
    .await;

    common::delete_file(&[DISKNAME1.into()]);
}

#[tokio::test]
async fn unmap_bdev_test() {
    const FILE_SIZE: u64 = 64 * 1024;

    common::delete_file(&[DISKNAME1.into()]);
    common::truncate_file(DISKNAME1, FILE_SIZE);

    let ms = setup();

    // Get underlying filesystem I/O blocksize.
    let fs_blocksize = get_fs_blocksize(DISKNAME1).unwrap();
    info!("filesystem blocksize is {}", fs_blocksize);

    // Check that size of the file is sufficiently large to contain at least 16
    // blocks
    assert!(
        16 * fs_blocksize <= FILE_SIZE * 1024,
        "file {} is too small to contain 16 blocks ({} bytes)",
        DISKNAME1,
        16 * fs_blocksize
    );

    // Verify that there are currently no blocks allocated
    // for the sparse file that is our backing store.
    assert_eq!(
        get_allocated_blocks(DISKNAME1).unwrap(),
        0,
        "expected 0 blocks"
    );

    // Create bdev but do not perform any I/O.
    ms.spawn(async {
        bdev_create(BDEVNAME1).await.expect("failed to create bdev");
    })
    .await;

    // Verify that number of allocated blocks is still 0.
    assert_eq!(
        get_allocated_blocks(DISKNAME1).unwrap(),
        0,
        "expected 0 blocks"
    );

    // Write 10 blocks at an offset of 4 blocks.
    let blocksize = fs_blocksize;
    ms.spawn(async move {
        let handle = BdevHandle::open(BDEVNAME1, true, false)
            .expect("failed to obtain bdev handle");

        let mut buf = handle.dma_malloc(10 * blocksize).unwrap();
        buf.fill(0xff);

        info!("writing 10 blocks");
        handle.write_at(4 * blocksize, &buf).await.unwrap();

        handle.close();
    })
    .await;

    // Verify that 10 blocks have been allocated.
    assert_eq!(
        get_allocated_blocks(DISKNAME1).unwrap() * 512,
        10 * fs_blocksize,
        "expected 10 blocks ({} bytes)",
        10 * fs_blocksize
    );

    // Unmap 4 blocks at an offset of 6 blocks.
    let blocksize = fs_blocksize;
    ms.spawn(async move {
        let handle = BdevHandle::open(BDEVNAME1, true, false)
            .expect("failed to obtain bdev handle");

        info!("unmapping 4 blocks");
        unmap(&handle, 6 * blocksize, 4 * blocksize).await.unwrap();

        handle.close();
    })
    .await;

    // Verify that number of allocated blocks has been reduced to 6.
    assert_eq!(
        get_allocated_blocks(DISKNAME1).unwrap() * 512,
        6 * fs_blocksize,
        "expected 6 blocks ({} bytes)",
        6 * fs_blocksize
    );

    // Unmap 4 blocks at an offset of 10 blocks.
    let blocksize = fs_blocksize;
    ms.spawn(async move {
        let handle = BdevHandle::open(BDEVNAME1, true, false)
            .expect("failed to obtain bdev handle");

        info!("unmapping 4 blocks");
        unmap(&handle, (6 + 4) * blocksize, 4 * blocksize)
            .await
            .unwrap();

        handle.close();
    })
    .await;

    // Verify that number of allocated blocks has been reduced to 2.
    assert_eq!(
        get_allocated_blocks(DISKNAME1).unwrap() * 512,
        2 * fs_blocksize,
        "expected 2 blocks ({} bytes)",
        2 * fs_blocksize
    );

    if fs_blocksize > 1024 {
        // Unmap 1024 BYTES at an offset of 4 blocks.
        // This is less than the underlying filesystem allocation unit size
        // (fs_blocksize), and is too small for any deallocation to occur.
        // The specified region is zeroed but the number of allocated
        // blocks should not change.
        let blocksize = fs_blocksize;
        ms.spawn(async move {
            let handle = BdevHandle::open(BDEVNAME1, true, false)
                .expect("failed to obtain bdev handle");

            info!("unmapping 1024 BYTES");
            unmap(&handle, 4 * blocksize, 1024).await.unwrap();

            handle.close();
        })
        .await;

        // Verify that "unmapped" region has been zeroed.
        let blocksize = fs_blocksize;
        ms.spawn(async move {
            let handle = BdevHandle::open(BDEVNAME1, false, false)
                .expect("failed to obtain bdev handle");
            let mut buf = handle.dma_malloc(1024).unwrap();

            info!("reading 1024 BYTES");
            handle.read_at(4 * blocksize, &mut buf).await.unwrap();
            handle.close();

            for &c in buf.as_slice() {
                assert_eq!(c, 0, "each byte should have value 0");
            }
        })
        .await;

        // Verify that number of allocated blocks has not changed.
        assert_eq!(
            get_allocated_blocks(DISKNAME1).unwrap() * 512,
            2 * fs_blocksize,
            "expected 2 blocks ({} bytes)",
            2 * fs_blocksize
        );
    }

    // Create bdev and unmap 2 blocks at an offset of 4 blocks.
    let blocksize = fs_blocksize;
    ms.spawn(async move {
        let handle = BdevHandle::open(BDEVNAME1, true, false)
            .expect("failed to obtain bdev handle");

        info!("unmapping 2 blocks");
        unmap(&handle, 4 * blocksize, 2 * blocksize).await.unwrap();

        handle.close();
    })
    .await;

    // Destroy the bdev.
    ms.spawn(async move {
        bdev_destroy(BDEVNAME1).await.unwrap();
    })
    .await;

    // Verify that number of allocated blocks has been reduced to 0.
    assert_eq!(
        get_allocated_blocks(DISKNAME1).unwrap(),
        0,
        "expected 0 blocks"
    );

    // Validate the state of mayastor.
    ms.spawn(async {
        // no bdevs
        assert_eq!(Bdev::bdev_first().into_iter().count(), 0);
    })
    .await;

    common::delete_file(&[DISKNAME1.into()]);
}

#[tokio::test]
async fn unmap_share_test() {
    const NUM_VOLS: usize = 2;
    const FILE_SIZE: u64 = 64 * 1024;
    const VOL_SIZE: u64 = 24 * 1024;

    common::delete_file(&[DISKNAME1.into()]);
    common::truncate_file(DISKNAME1, FILE_SIZE);

    let ms = setup();

    // Create a pool.
    ms.spawn(async {
        Lvs::create_or_import(CreatePoolRequest {
            name: "tpool".into(),
            disks: vec![BDEVNAME1.into()],
        })
        .await
        .unwrap();
    })
    .await;

    // Check that we can find the LVS.
    ms.spawn(async {
        assert_eq!(Lvs::iter().count(), 1);
        let pool = Lvs::lookup("tpool").unwrap();
        info!("created pool: name={} UUID={}", pool.name(), pool.uuid());
        assert_eq!(pool.name(), "tpool");
        assert_eq!(pool.used(), 0);
        assert_eq!(pool.base_bdev().name(), DISKNAME1);
    })
    .await;

    // Create lvols on this pool.
    ms.spawn(async {
        let pool = Lvs::lookup("tpool").unwrap();
        for i in 0 .. NUM_VOLS {
            pool.create_lvol(&format!("vol-{}", i), VOL_SIZE * 1024, false)
                .await
                .unwrap();
        }
    })
    .await;

    info!(
        "{} blocks allocated for {} after creating lvols",
        get_allocated_blocks(DISKNAME1).unwrap(),
        DISKNAME1
    );

    // Share all replicas.
    let targets = ms
        .spawn(async {
            let pool = Lvs::lookup("tpool").unwrap();
            assert_eq!(pool.lvols().unwrap().count(), NUM_VOLS);

            let mut targets: Vec<NvmeTarget> = Vec::new();

            for vol in pool.lvols().unwrap() {
                vol.share_nvmf(None).await.unwrap();
                let uri = vol.share_uri().unwrap();
                info!("lvol {} shared as: {}", vol.name(), uri);
                targets.push(NvmeTarget::try_from(uri).unwrap());
            }

            targets
        })
        .await;

    let mut devlist: Vec<String> = Vec::new();

    // Attach all targets.
    for target in &targets {
        let devices = target.connect().unwrap();
        let dev = devices[0].path.to_string();
        info!("nvmf target attached to device: {}", dev);
        devlist.push(dev);
    }

    // Write to all devices
    for dev in &devlist {
        info!("writing to {} with dd ...", dev);
        common::dd_urandom_blkdev(&dev);
    }

    // Disconnect all targets.
    for target in &targets {
        info!("disconnecting target");
        target.disconnect().unwrap();
    }

    info!(
        "{} blocks allocated for {} after finished writing",
        get_allocated_blocks(DISKNAME1).unwrap(),
        DISKNAME1
    );

    assert!(
        get_allocated_blocks(DISKNAME1).unwrap() > 0,
        "number of allocated blocks should be non-zero"
    );

    // Destroy the lvols.
    ms.spawn(async {
        let pool = Lvs::lookup("tpool").unwrap();

        let vols: Vec<Lvol> = pool.lvols().unwrap().collect();
        assert_eq!(vols.len(), NUM_VOLS);

        for vol in vols {
            vol.destroy().await.unwrap();
        }
    })
    .await;

    info!(
        "{} blocks allocated for {} after destroying lvols",
        get_allocated_blocks(DISKNAME1).unwrap(),
        DISKNAME1
    );

    // Destroy the pool
    ms.spawn(async {
        let pool = Lvs::lookup("tpool").unwrap();
        assert_eq!(pool.lvols().unwrap().count(), 0);

        pool.destroy().await.unwrap();
    })
    .await;

    info!(
        "{} blocks allocated for {} after destroying pool",
        get_allocated_blocks(DISKNAME1).unwrap(),
        DISKNAME1
    );

    // Validate the state of mayastor.
    ms.spawn(async {
        // pools destroyed
        assert_eq!(Lvs::iter().count(), 0);

        // no bdevs
        assert_eq!(Bdev::bdev_first().into_iter().count(), 0);
    })
    .await;

    common::delete_file(&[DISKNAME1.into()]);
}
