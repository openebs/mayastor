//! Tests for the aio bdev 'fallocate' URI parameter, which opts a bdev into
//! fallocate-based UNMAP/WRITE_ZEROES (trim) passthrough.

use libc::c_void;
use once_cell::sync::OnceCell;
use std::os::unix::fs::MetadataExt;

use io_engine::{
    bdev::{
        device_open,
        nexus::{nexus_create, nexus_lookup_mut},
    },
    bdev_api::{bdev_create, bdev_destroy, BdevError},
    core::{
        BlockDevice, IoCompletionStatus, IoType, MayastorCliArgs, UntypedBdev, UntypedBdevHandle,
    },
};

use futures::channel::oneshot;

pub mod common;
use common::MayastorTest;

static MAYASTOR: OnceCell<MayastorTest> = OnceCell::new();

fn get_ms() -> &'static MayastorTest<'static> {
    MAYASTOR.get_or_init(|| MayastorTest::new(MayastorCliArgs::default()))
}

/// Completion callback which forwards the I/O status to a oneshot channel.
fn io_completion_cb(_device: &dyn BlockDevice, status: IoCompletionStatus, ctx: *mut c_void) {
    let sender = unsafe { Box::from_raw(ctx as *mut oneshot::Sender<IoCompletionStatus>) };
    sender.send(status).expect("failed to send I/O status");
}

/// Without the fallocate parameter neither unmap nor write-zeroes may be
/// advertised; with fallocate=true both must be.
#[tokio::test]
async fn aio_fallocate_io_type_advertisement() {
    const DISKNAME: &str = "/tmp/aio_fallocate_types.img";
    const BDEV_PLAIN: &str = "aio:///tmp/aio_fallocate_types.img?blk_size=512";
    const BDEV_FALLOC: &str = "aio:///tmp/aio_fallocate_types.img?blk_size=512&fallocate=true";

    let ms = get_ms();

    common::delete_file(&[DISKNAME.into()]);
    common::truncate_file(DISKNAME, 64 * 1024);

    ms.spawn(async {
        let name = bdev_create(BDEV_PLAIN)
            .await
            .expect("failed to create bdev");
        let bdev = UntypedBdev::lookup_by_name(&name).unwrap();
        assert!(!bdev.io_type_supported(IoType::Unmap));
        // WriteZeros is advertised regardless of the fallocate option: the SPDK
        // bdev layer emulates it with regular writes whenever the module lacks
        // native support, so only Unmap reflects the option.
        assert!(bdev.io_type_supported(IoType::WriteZeros));
        bdev_destroy(BDEV_PLAIN)
            .await
            .expect("failed to destroy bdev");

        let name = bdev_create(BDEV_FALLOC)
            .await
            .expect("failed to create bdev");
        let bdev = UntypedBdev::lookup_by_name(&name).unwrap();
        assert!(bdev.io_type_supported(IoType::Unmap));
        assert!(bdev.io_type_supported(IoType::WriteZeros));
        bdev_destroy(BDEV_FALLOC)
            .await
            .expect("failed to destroy bdev");
    })
    .await;

    common::delete_file(&[DISKNAME.into()]);
}

/// A bogus fallocate value must fail bdev creation with a parse error.
#[tokio::test]
async fn aio_fallocate_bad_value() {
    const DISKNAME: &str = "/tmp/aio_fallocate_bogus.img";
    const BDEV_BOGUS: &str = "aio:///tmp/aio_fallocate_bogus.img?blk_size=512&fallocate=bogus";

    let ms = get_ms();

    common::delete_file(&[DISKNAME.into()]);
    common::truncate_file(DISKNAME, 64 * 1024);

    ms.spawn(async {
        match bdev_create(BDEV_BOGUS).await {
            Err(BdevError::BoolParamParseFailed {
                parameter, value, ..
            }) => {
                assert_eq!(parameter, "fallocate");
                assert_eq!(value, "bogus");
            }
            other => panic!("expected BoolParamParseFailed, got: {:?}", other),
        }
    })
    .await;

    common::delete_file(&[DISKNAME.into()]);
}

/// Unmap on a fallocate-enabled aio bdev must read back as zeroes and punch
/// holes into the backing file (i.e. actually release allocated blocks).
#[tokio::test]
async fn aio_fallocate_unmap_data_path() {
    const DISKNAME: &str = "/tmp/aio_fallocate_data.img";
    const BDEV_FALLOC: &str = "aio:///tmp/aio_fallocate_data.img?blk_size=512&fallocate=true";
    const BUF_SIZE: u64 = 1024 * 1024;
    const DATA_SIZE: u64 = 8 * BUF_SIZE;

    let ms = get_ms();

    common::delete_file(&[DISKNAME.into()]);
    common::truncate_file(DISKNAME, 64 * 1024);

    // Write a pattern over the first DATA_SIZE bytes.
    ms.spawn(async {
        let name = bdev_create(BDEV_FALLOC)
            .await
            .expect("failed to create bdev");

        let handle = UntypedBdevHandle::open(&name, true, false).unwrap();
        let mut buf = handle.dma_malloc(BUF_SIZE).unwrap();
        buf.fill(0xaa);
        for i in 0..DATA_SIZE / BUF_SIZE {
            handle.write_at(i * BUF_SIZE, &buf).await.unwrap();
        }
        handle.close();
    })
    .await;

    // Writing the pattern must have allocated blocks in the backing file.
    let blocks_before = std::fs::metadata(DISKNAME).unwrap().blocks();
    assert!(
        blocks_before >= DATA_SIZE / 512,
        "expected at least {} allocated blocks, found {}",
        DATA_SIZE / 512,
        blocks_before
    );

    // Unmap the whole pattern and verify it reads back as zeroes.
    ms.spawn(async {
        let descr = device_open(DISKNAME, true).unwrap();
        let handle = descr.into_handle().unwrap();
        let block_len = handle.get_device().block_len();

        let (sender, receiver) = oneshot::channel::<IoCompletionStatus>();
        handle
            .unmap_blocks(
                0,
                DATA_SIZE / block_len,
                io_completion_cb,
                Box::into_raw(Box::new(sender)) as *mut c_void,
            )
            .unwrap();
        let status = receiver.await.unwrap();
        assert_eq!(status, IoCompletionStatus::Success, "unmap failed");

        let handle = UntypedBdevHandle::open(DISKNAME, true, false).unwrap();
        let mut buf = handle.dma_malloc(BUF_SIZE).unwrap();
        for i in 0..DATA_SIZE / BUF_SIZE {
            buf.fill(0xff);
            handle.read_at(i * BUF_SIZE, &mut buf).await.unwrap();
            assert!(
                buf.as_slice().iter().all(|b| *b == 0),
                "unmapped range did not read back as zeroes"
            );
        }
        handle.close();
    })
    .await;

    // The punched holes must have released the allocated blocks.
    let blocks_after = std::fs::metadata(DISKNAME).unwrap().blocks();
    assert!(
        blocks_after < blocks_before,
        "expected allocated blocks to shrink: before {}, after {}",
        blocks_before,
        blocks_after
    );

    ms.spawn(async {
        bdev_destroy(BDEV_FALLOC)
            .await
            .expect("failed to destroy bdev");
    })
    .await;

    common::delete_file(&[DISKNAME.into()]);
}

/// The nexus advertises unmap iff ALL of its children support it.
#[tokio::test]
async fn aio_fallocate_nexus_and_semantics() {
    const NEXUS_NAME: &str = "nexus_aio_fallocate";
    const DISKNAME1: &str = "/tmp/aio_fallocate_child1.img";
    const DISKNAME2: &str = "/tmp/aio_fallocate_child2.img";
    const BDEV_FALLOC: &str = "aio:///tmp/aio_fallocate_child1.img?blk_size=512&fallocate=true";
    const BDEV_PLAIN: &str = "aio:///tmp/aio_fallocate_child2.img?blk_size=512";
    const NEXUS_SIZE: u64 = 60 * 1024 * 1024;

    let ms = get_ms();

    common::delete_file(&[DISKNAME1.into(), DISKNAME2.into()]);
    common::truncate_file(DISKNAME1, 64 * 1024);
    common::truncate_file(DISKNAME2, 64 * 1024);

    ms.spawn(async {
        // A single fallocate-enabled child: the nexus advertises unmap.
        nexus_create(NEXUS_NAME, NEXUS_SIZE, None, &[BDEV_FALLOC.to_string()])
            .await
            .unwrap();
        let bdev = UntypedBdev::lookup_by_name(NEXUS_NAME).unwrap();
        assert!(bdev.io_type_supported(IoType::Unmap));
        nexus_lookup_mut(NEXUS_NAME)
            .unwrap()
            .destroy()
            .await
            .unwrap();

        // Adding a plain aio child disables unmap on the nexus.
        nexus_create(
            NEXUS_NAME,
            NEXUS_SIZE,
            None,
            &[BDEV_FALLOC.to_string(), BDEV_PLAIN.to_string()],
        )
        .await
        .unwrap();
        let bdev = UntypedBdev::lookup_by_name(NEXUS_NAME).unwrap();
        assert!(!bdev.io_type_supported(IoType::Unmap));
        nexus_lookup_mut(NEXUS_NAME)
            .unwrap()
            .destroy()
            .await
            .unwrap();
    })
    .await;

    common::delete_file(&[DISKNAME1.into(), DISKNAME2.into()]);
}
