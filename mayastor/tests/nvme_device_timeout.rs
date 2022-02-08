use std::{slice, str, sync::atomic::AtomicPtr};

use crossbeam::atomic::AtomicCell;
use libc::c_void;
use once_cell::sync::{Lazy, OnceCell};

use common::compose::{Builder, MayastorTest};
use mayastor::{
    bdev::{device_create, device_destroy, device_open},
    core::{
        BlockDevice,
        BlockDeviceHandle,
        DeviceTimeoutAction,
        IoCompletionStatus,
        MayastorCliArgs,
    },
    subsys::{Config, NvmeBdevOpts},
};
use rpc::mayastor::{BdevShareRequest, BdevUri, Null};
use spdk_rs::{DmaBuf, IoVec};

pub mod common;

const TEST_CTX_STRING: &str = "test context";

static MAYASTOR: Lazy<MayastorTest> =
    Lazy::new(|| MayastorTest::new(MayastorCliArgs::default()));

static CALLBACK_FLAG: AtomicCell<bool> = AtomicCell::new(false);

const BUF_SIZE: u64 = 32768;

struct IoOpCtx {
    iov: IoVec,
    device_url: String,
    dma_buf: DmaBuf,
    handle: Box<dyn BlockDeviceHandle>,
}

fn get_config() -> &'static Config {
    Config::get_or_init(|| Config {
        nvme_bdev_opts: NvmeBdevOpts {
            timeout_us: 7_000_000,
            keep_alive_timeout_ms: 5_000,
            transport_retry_count: 2,
            ..Default::default()
        },
        ..Default::default()
    })
}

async fn test_io_timeout(action_on_timeout: DeviceTimeoutAction) {
    get_config().apply();

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

    static DEVICE_NAME: OnceCell<String> = OnceCell::new();

    let cptr = MAYASTOR
        .spawn(async move {
            let device_name = device_create(&bdev_url).await.unwrap();
            let descr = device_open(&device_name, false).unwrap();
            let handle = descr.into_handle().unwrap();

            // Store device name for further checking from I/O callback.
            DEVICE_NAME.set(device_name.clone()).unwrap();

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
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        println!("waiting for the container to be fully suspended... {}/5", i);
    }

    // Read completion callback.
    fn read_completion_callback(
        device: &dyn BlockDevice,
        status: IoCompletionStatus,
        ctx: *mut c_void,
    ) {
        assert_ne!(
            status,
            IoCompletionStatus::Success,
            "I/O operation completed successfully"
        );
        assert!(!CALLBACK_FLAG.load(), "Callback called multiple times");

        // Make sure we have the correct device.
        assert_eq!(
            &device.device_name(),
            DEVICE_NAME.get().unwrap(),
            "Device name mismatch"
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
            let (block_len, alignment) = {
                let device = ctx.handle.get_device();

                (device.block_len(), device.alignment())
            };

            let mut io_ctx = IoOpCtx {
                iov: IoVec::default(),
                device_url: ctx.device_url,
                dma_buf: DmaBuf::new(BUF_SIZE, alignment).unwrap(),
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
                    (3 * 1024 * 1024) / block_len,
                    BUF_SIZE / block_len,
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
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
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
    get_config().apply();

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

    static DEVICE_NAME: OnceCell<String> = OnceCell::new();

    let cptr = MAYASTOR
        .spawn(async move {
            let device_name = device_create(&bdev_url).await.unwrap();
            let descr = device_open(&device_name, false).unwrap();
            let handle = descr.into_handle().unwrap();

            // handle.get_device() returns a reference, so it should not
            // interfere with the move of the handle itself, hence
            // device is accessed with a different lifetime.
            let device = handle.get_device();
            let action_on_timeout = DeviceTimeoutAction::Ignore;
            let mut io_controller = device.get_io_controller().unwrap();

            io_controller.set_timeout_action(action_on_timeout).unwrap();
            assert_eq!(
                io_controller.get_timeout_action().unwrap(),
                action_on_timeout,
                "I/O timeout action mismatches"
            );

            // Store device name for further checking from I/O callback.
            DEVICE_NAME.set(device_name.clone()).unwrap();

            AtomicPtr::new(Box::into_raw(Box::new(IoCtx {
                handle,
                device_url: bdev_url,
            })))
        })
        .await;

    test.pause("ms1").await.unwrap();
    for i in 1 .. 6 {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        println!("waiting for the container to be fully suspended... {}/5", i);
    }

    // Read completion callback.
    fn read_completion_callback(
        device: &dyn BlockDevice,
        status: IoCompletionStatus,
        ctx: *mut c_void,
    ) {
        assert_ne!(
            status,
            IoCompletionStatus::Success,
            "I/O operation completed successfully"
        );
        assert!(!CALLBACK_FLAG.load(), "Callback called multiple times");

        // Make sure we have the correct device.
        assert_eq!(
            &device.device_name(),
            DEVICE_NAME.get().unwrap(),
            "Device name mismatch"
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
            let (block_len, alignment) = {
                let device = ctx.handle.get_device();

                (device.block_len(), device.alignment())
            };

            let mut io_ctx = IoOpCtx {
                iov: IoVec::default(),
                device_url: ctx.device_url,
                dma_buf: DmaBuf::new(BUF_SIZE, alignment).unwrap(),
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
                    (3 * 1024 * 1024) / block_len,
                    BUF_SIZE / block_len,
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
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        assert!(!CALLBACK_FLAG.load(), "I/O operation interrupted");
    }

    // Destroy device.
    MAYASTOR
        .spawn(async move {
            let ctx = unsafe { Box::from_raw(io_ctx.into_inner()) };

            device_destroy(&ctx.device_url).await.unwrap();
        })
        .await
}
