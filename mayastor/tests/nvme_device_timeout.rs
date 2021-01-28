use common::compose::{Builder, MayastorTest};
use crossbeam::atomic::AtomicCell;
use libc::c_void;
use mayastor::{
    bdev::{device_create, device_destroy, device_open},
    core::{BlockDeviceHandle, DeviceTimeoutAction, DmaBuf, MayastorCliArgs},
    subsys::{Config, NvmeBdevOpts},
};
use once_cell::sync::Lazy;
use rpc::mayastor::{BdevShareRequest, BdevUri, Null};
use spdk_sys::iovec;
use std::{slice, str, sync::atomic::AtomicPtr};

pub mod common;

const TEST_CTX_STRING: &str = "test context";

static MAYASTOR: Lazy<MayastorTest> =
    Lazy::new(|| MayastorTest::new(MayastorCliArgs::default()));

static CALLBACK_FLAG: AtomicCell<bool> = AtomicCell::new(false);

const BUF_SIZE: u64 = 32768;

struct IoOpCtx {
    iov: iovec,
    device_url: String,
    dma_buf: DmaBuf,
    handle: Box<dyn BlockDeviceHandle>,
}

async fn test_io_timeout(action_on_timeout: DeviceTimeoutAction) {
    Config::get_or_init(|| Config {
        nvme_bdev_opts: NvmeBdevOpts {
            timeout_us: 2_000_000,
            keep_alive_timeout_ms: 5_000,
            retry_count: 2,
            ..Default::default()
        },
        ..Default::default()
    })
    .apply();

    let test = Builder::new()
        .name("cargo-test")
        .network("10.1.0.0/16")
        .add_container("ms1")
        .with_clean(true)
        .build()
        .await
        .unwrap();

    // get the handles if needed, to invoke methods to the containers
    let mut hdls = test.grpc_handles().await.unwrap();

    // create and share a bdev on each container
    for h in &mut hdls {
        h.bdev.list(Null {}).await.unwrap();
        h.bdev
            .create(BdevUri {
                uri: "malloc:///disk0?size_mb=128".into(),
            })
            .await
            .unwrap();

        h.bdev
            .share(BdevShareRequest {
                name: "disk0".into(),
                proto: "nvmf".into(),
            })
            .await
            .unwrap();
    }

    let bdev_url = format!(
        "nvmf://{}:8420/nqn.2019-05.io.openebs:disk0",
        hdls[0].endpoint.ip()
    );

    struct IoCtx {
        handle: Box<dyn BlockDeviceHandle>,
        device_url: String,
    }

    let cptr = MAYASTOR
        .spawn(async move {
            let device_name = device_create(&bdev_url).await.unwrap();
            let descr = device_open(&device_name, false).unwrap();
            let handle = descr.into_handle().unwrap();

            // Set requested I/O timeout action.
            let device = handle.get_device();
            let mut io_controller = device.get_io_controller().unwrap();
            io_controller.set_timeout_action(action_on_timeout).unwrap();
            assert_eq!(
                io_controller.get_timeout_action().unwrap(),
                action_on_timeout,
                "I/O timeout action mismatches"
            );

            AtomicPtr::new(Box::into_raw(Box::new(IoCtx {
                handle,
                device_url: bdev_url,
            })))
        })
        .await;

    test.pause("ms1").await.unwrap();
    for i in 1 .. 6 {
        tokio::time::delay_for(std::time::Duration::from_secs(1)).await;
        println!("waiting for the container to be fully suspended... {}/5", i);
    }

    // Read completion callback.
    fn read_completion_callback(success: bool, ctx: *mut c_void) {
        assert_eq!(success, false, "I/O operation completed successfully");
        assert_eq!(
            CALLBACK_FLAG.load(),
            false,
            "Callback called multiple times"
        );

        // Make sure we were passed the same pattern string as requested.
        let s = unsafe {
            let slice =
                slice::from_raw_parts(ctx as *const u8, TEST_CTX_STRING.len());
            str::from_utf8(slice).unwrap()
        };

        assert_eq!(s, TEST_CTX_STRING);
        CALLBACK_FLAG.store(true);
    }

    println!("Issuing I/O operation against disconnected device");
    let io_ctx = MAYASTOR
        .spawn(async move {
            let ctx = unsafe { Box::from_raw(cptr.into_inner()) };
            let device = ctx.handle.get_device();

            let mut io_ctx = IoOpCtx {
                iov: iovec::default(),
                device_url: ctx.device_url,
                dma_buf: DmaBuf::new(BUF_SIZE, device.alignment()).unwrap(),
                handle: ctx.handle,
            };

            io_ctx.iov.iov_base = *io_ctx.dma_buf;
            io_ctx.iov.iov_len = BUF_SIZE;

            CALLBACK_FLAG.store(false);

            io_ctx
                .handle
                .readv_blocks(
                    &mut io_ctx.iov,
                    1,
                    (3 * 1024 * 1024) / device.block_len(),
                    BUF_SIZE / device.block_len(),
                    read_completion_callback,
                    TEST_CTX_STRING.as_ptr() as *mut c_void,
                )
                .unwrap();

            AtomicPtr::new(Box::into_raw(Box::new(io_ctx)))
        })
        .await;

    let mut io_timedout = false;

    // Wait up to 120 seconds till I/O times out.
    for i in 1 .. 25 {
        println!("waiting for I/O to be timed out... {}/24", i);
        tokio::time::delay_for(std::time::Duration::from_secs(5)).await;
        // Break the loop if the callback has been called in response to I/O
        // cancelling.
        if CALLBACK_FLAG.load() {
            println!("I/O timed out");
            io_timedout = true;
            break;
        }
    }

    MAYASTOR
        .spawn(async move {
            let ctx = unsafe { Box::from_raw(io_ctx.into_inner()) };

            device_destroy(&ctx.device_url).await.unwrap();
        })
        .await;

    // Check test result after all the resources are freed.
    assert!(
        io_timedout,
        "I/O was not timed out in response to timeout action"
    );
}

#[tokio::test]
async fn io_timeout_reset() {
    test_io_timeout(DeviceTimeoutAction::Reset).await;
}

#[tokio::test]
async fn io_timeout_ignore() {
    Config::get_or_init(|| Config {
        nvme_bdev_opts: NvmeBdevOpts {
            timeout_us: 2_000_000,
            keep_alive_timeout_ms: 5_000,
            retry_count: 2,
            ..Default::default()
        },
        ..Default::default()
    })
    .apply();

    let test = Builder::new()
        .name("cargo-test")
        .network("10.1.0.0/16")
        .add_container("ms1")
        .with_clean(true)
        .build()
        .await
        .unwrap();

    // get the handles if needed, to invoke methods to the containers
    let mut hdls = test.grpc_handles().await.unwrap();

    // create and share a bdev on each container
    for h in &mut hdls {
        h.bdev.list(Null {}).await.unwrap();
        h.bdev
            .create(BdevUri {
                uri: "malloc:///disk0?size_mb=128".into(),
            })
            .await
            .unwrap();

        h.bdev
            .share(BdevShareRequest {
                name: "disk0".into(),
                proto: "nvmf".into(),
            })
            .await
            .unwrap();
    }

    let bdev_url = format!(
        "nvmf://{}:8420/nqn.2019-05.io.openebs:disk0",
        hdls[0].endpoint.ip()
    );

    struct IoCtx {
        handle: Box<dyn BlockDeviceHandle>,
        device_url: String,
    }

    let cptr = MAYASTOR
        .spawn(async move {
            let device_name = device_create(&bdev_url).await.unwrap();
            let descr = device_open(&device_name, false).unwrap();
            let handle = descr.into_handle().unwrap();
            let device = handle.get_device();

            let action_on_timeout = DeviceTimeoutAction::Ignore;
            let mut io_controller = device.get_io_controller().unwrap();
            io_controller.set_timeout_action(action_on_timeout).unwrap();
            assert_eq!(
                io_controller.get_timeout_action().unwrap(),
                action_on_timeout,
                "I/O timeout action mismatches"
            );

            AtomicPtr::new(Box::into_raw(Box::new(IoCtx {
                handle,
                device_url: bdev_url,
            })))
        })
        .await;

    test.pause("ms1").await.unwrap();
    for i in 1 .. 6 {
        tokio::time::delay_for(std::time::Duration::from_secs(1)).await;
        println!("waiting for the container to be fully suspended... {}/5", i);
    }

    // Read completion callback.
    fn read_completion_callback(success: bool, ctx: *mut c_void) {
        assert_eq!(success, false, "I/O operation completed successfully");
        assert_eq!(
            CALLBACK_FLAG.load(),
            false,
            "Callback called multiple times"
        );

        // Make sure we were passed the same pattern string as requested.
        let s = unsafe {
            let slice =
                slice::from_raw_parts(ctx as *const u8, TEST_CTX_STRING.len());
            str::from_utf8(slice).unwrap()
        };

        assert_eq!(s, TEST_CTX_STRING);
        CALLBACK_FLAG.store(true);
    }

    println!("Issuing I/O operation against disconnected device");
    // We can't use synchronous I/O operations because all I/O timeouts are
    // supposed to be ignored, so no possibility to interrupt active I/O
    // operations.
    let io_ctx = MAYASTOR
        .spawn(async move {
            let ctx = unsafe { Box::from_raw(cptr.into_inner()) };
            let device = ctx.handle.get_device();

            let mut io_ctx = IoOpCtx {
                iov: iovec::default(),
                device_url: ctx.device_url,
                dma_buf: DmaBuf::new(BUF_SIZE, device.alignment()).unwrap(),
                handle: ctx.handle,
            };

            io_ctx.iov.iov_base = *io_ctx.dma_buf;
            io_ctx.iov.iov_len = BUF_SIZE;

            CALLBACK_FLAG.store(false);

            io_ctx
                .handle
                .readv_blocks(
                    &mut io_ctx.iov,
                    1,
                    (3 * 1024 * 1024) / device.block_len(),
                    BUF_SIZE / device.block_len(),
                    read_completion_callback,
                    TEST_CTX_STRING.as_ptr() as *mut c_void,
                )
                .unwrap();

            AtomicPtr::new(Box::into_raw(Box::new(io_ctx)))
        })
        .await;

    // Wait 5 times longer than timeout interval. Make sure I/O operation not
    // interrupted.
    for i in 1 .. 6 {
        println!("waiting for I/O timeout to happen... {}/5", i);
        tokio::time::delay_for(std::time::Duration::from_secs(5)).await;
        assert_eq!(CALLBACK_FLAG.load(), false, "I/O operation interrupted");
    }

    // Destroy device.
    MAYASTOR
        .spawn(async move {
            let ctx = unsafe { Box::from_raw(io_ctx.into_inner()) };

            device_destroy(&ctx.device_url).await.unwrap();
        })
        .await
}
