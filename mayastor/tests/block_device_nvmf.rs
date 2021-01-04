use libc::c_void;
use once_cell::sync::OnceCell;

use mayastor::{
    bdev::{device_create, device_destroy, device_lookup, device_open},
    core::{DmaBuf, MayastorCliArgs},
};

use std::{slice, str, sync::Arc};

use core::sync::atomic::AtomicPtr;

use spdk_sys::{self, iovec};

pub mod common;
use common::compose::MayastorTest;
use uuid::Uuid;

static MAYASTOR: OnceCell<MayastorTest> = OnceCell::new();

const MAYASTOR_CTRLR_TITLE: &str = "Mayastor NVMe controler";
//const MAYASTOR_NQN_PREFIX: &str = "nqn.2019-05.io.openebs:";

fn get_ms() -> &'static MayastorTest<'static> {
    let instance =
        MAYASTOR.get_or_init(|| MayastorTest::new(MayastorCliArgs::default()));
    &instance
}

async fn launch_instance() -> String {
    return "nvmf://172.16.175.130:8420/nqn.2019-05.io.openebs:disk0"
        .to_string();
}

#[tokio::test]
async fn nvmf_device_create_destroy() {
    let ms = get_ms();
    let url = launch_instance().await;

    ms.spawn(async move {
        let name1 = device_create(&url).await.unwrap();

        // Check device properties for sanity.
        let bdev = device_lookup(&name1).unwrap();
        assert_eq!(bdev.product_name(), "NVMe disk");
        assert_eq!(bdev.driver_name(), "nvme");
        assert_eq!(bdev.device_name(), name1);

        println!("-> block_len: {}", bdev.block_len());
        println!("-> num_blocks: {}", bdev.num_blocks());
        assert_ne!(bdev.block_len(), 0);
        assert_ne!(bdev.num_blocks(), 0);
        assert_ne!(bdev.size_in_bytes(), 0);
        assert_eq!(bdev.block_len() * bdev.num_blocks(), bdev.size_in_bytes());

        Uuid::parse_str(&bdev.uuid()).unwrap();

        // Destroy the device the first time - should succeed.
        device_destroy(&url).await.unwrap();

        // Destroy the device which is supposed to be already destroyed -
        // should fail.
        assert!(device_destroy(&url).await.is_err());

        // Create the same device one more time - should succeed.
        let name2 = device_create(&url).await.unwrap();

        // Destroy the device the second time - should succeed.
        device_destroy(&url).await.unwrap();

        // Device paths should match.
        assert_eq!(name1, name2);
    })
    .await;
}

#[tokio::test]
async fn nvmf_device_identify_controller() {
    let ms = get_ms();
    let url = launch_instance().await;

    ms.spawn(async move {
        let name = device_create(&url).await.unwrap();
        let descr = device_open(&name, false).unwrap();
        let handle = descr.into_handle().unwrap();

        let _buf = handle.nvme_identify_ctrlr().await.unwrap();
        device_destroy(&url).await.unwrap();
    })
    .await;
}

const GUARD_PATTERN: u8 = 0xFF;
const IO_PATTERN: u8 = 0x77;

fn check_buf_pattern(buf: &DmaBuf, pattern: u8) {
    for i in buf.as_slice() {
        assert_eq!(*i, pattern, "Buffer doesn't match the pattern");
    }
}

fn create_io_buffer(alignment: u64, size: u64, pattern: u8) -> DmaBuf {
    let mut buf = DmaBuf::new(size, alignment).unwrap();

    for i in buf.as_mut_slice() {
        *i = pattern;
    }

    buf
}

#[tokio::test]
async fn nvmf_device_read_write_at() {
    let ms = get_ms();
    let url = launch_instance().await;

    // Perform a sequence of write-read operations to write test pattern to the
    // device via write_at() and verify data integrity via read_at().
    ms.spawn(async move {
        const BUF_SIZE: u64 = 32768;
        const OP_OFFSET: u64 = 1024 * 1024;

        let name = device_create(&url).await.unwrap();
        let descr = device_open(&name, false).unwrap();
        let handle = descr.into_handle().unwrap();
        let device = handle.get_device();

        let guard_buf =
            create_io_buffer(device.alignment(), BUF_SIZE, GUARD_PATTERN);

        // First, write 2 guard buffers before and after target I/O location.
        let mut r = handle.write_at(OP_OFFSET, &guard_buf).await.unwrap();
        assert_eq!(r, BUF_SIZE, "The amount of data written mismatches");
        r = handle
            .write_at(OP_OFFSET + 2 * BUF_SIZE, &guard_buf)
            .await
            .unwrap();
        assert_eq!(r, BUF_SIZE, "The amount of data written mismatches");

        // Write data buffer between guard buffers.
        let data_buf =
            create_io_buffer(device.alignment(), BUF_SIZE, IO_PATTERN);
        r = handle
            .write_at(OP_OFFSET + BUF_SIZE, &data_buf)
            .await
            .unwrap();
        assert_eq!(r, BUF_SIZE, "The amount of data written mismatches");

        // Check the first guard buffer.
        let g1 = DmaBuf::new(BUF_SIZE, device.alignment()).unwrap();
        r = handle.read_at(OP_OFFSET, &g1).await.unwrap();
        assert_eq!(r, BUF_SIZE, "The amount of data read mismatches");
        check_buf_pattern(&g1, GUARD_PATTERN);

        // Check the second guard buffer.
        let g2 = DmaBuf::new(BUF_SIZE, device.alignment()).unwrap();
        r = handle.read_at(OP_OFFSET + 2 * BUF_SIZE, &g2).await.unwrap();
        assert_eq!(r, BUF_SIZE, "The amount of data read mismatches");
        check_buf_pattern(&g2, GUARD_PATTERN);

        // Check the data region.
        let dbuf = DmaBuf::new(BUF_SIZE, device.alignment()).unwrap();
        r = handle.read_at(OP_OFFSET + BUF_SIZE, &dbuf).await.unwrap();
        assert_eq!(r, BUF_SIZE, "The amount of data read mismatches");
        check_buf_pattern(&dbuf, IO_PATTERN);

        device_destroy(&url).await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn nvmf_device_read_write_test() {
    const BUF_SIZE: u64 = 32768;

    let ms = get_ms();
    let u = Arc::new(launch_instance().await);
    let mut url = Arc::clone(&u);

    // Placeholder structure to let all the fields outlive API invocations.
    struct IoCtx {
        iov: iovec,
        iovcnt: i32,
        dma_buf: DmaBuf,
    }

    // Read completion callback.
    fn read_completion_callback(success: bool, ctx: *const c_void) {
        println!("readv_blocks() completed !");

        assert!(success, "readv_blocks() failed");
        // Make sure we were passed tha same pattern string as requested.
        let s = unsafe {
            let slice = slice::from_raw_parts(
                ctx as *const u8,
                MAYASTOR_CTRLR_TITLE.len(),
            );
            str::from_utf8(slice).unwrap()
        };

        assert_eq!(s, MAYASTOR_CTRLR_TITLE);
    }

    let buf_ptr = ms
        .spawn(async move {
            let name = device_create(&(*url)).await.unwrap();
            let descr = device_open(&name, false).unwrap();
            let handle = descr.into_handle().unwrap();
            let device = handle.get_device();

            // Create a buffer with the guard pattern.
            let mut io_ctx = IoCtx {
                iov: iovec::default(),
                iovcnt: 1,
                dma_buf: create_io_buffer(
                    device.alignment(),
                    BUF_SIZE,
                    GUARD_PATTERN,
                ),
            };

            io_ctx.iov.iov_base = *io_ctx.dma_buf;
            io_ctx.iov.iov_len = BUF_SIZE;

            // Initiate a read operation into the buffer.
            handle
                .readv_blocks(
                    &mut io_ctx.iov,
                    io_ctx.iovcnt,
                    (3 * 1024 * 1024) / device.block_len(),
                    BUF_SIZE / device.block_len(),
                    read_completion_callback,
                    // Use a predefined string to check that we receive the
                    // same context pointer as we pass upon
                    // invocation. For this call we don't need any
                    // specific, operation-related context.
                    MAYASTOR_CTRLR_TITLE.as_ptr() as *const c_void,
                )
                .unwrap();

            AtomicPtr::new(Box::into_raw(Box::new(io_ctx)))
        })
        .await;

    // Sleep for a few seconds to let I/O operation complete.
    println!("Sleeping for 3 secs to let I/O operation complete");
    tokio::time::delay_for(std::time::Duration::from_secs(3)).await;
    println!("Awakened.");

    // Check the contents of the buffer to make sure it has been overwritten
    // with data pattern. We should see all zeroes in the buffer instead of
    // the guard pattern.
    let b = buf_ptr.into_inner();
    check_buf_pattern(unsafe { &((*b).dma_buf) }, 0);

    url = Arc::clone(&u);
    ms.send(async move {
        device_destroy(&(*url)).await.unwrap();
    });
}
