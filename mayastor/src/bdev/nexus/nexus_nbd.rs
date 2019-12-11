//! Utility functions and wrappers for working with nbd devices in SPDK.

use crate::executor::{cb_arg, errno_result_from_i32, ErrnoResult};
use futures::channel::oneshot;
use futures_timer::Delay;
use nix::{convert_ioctl_res, errno::Errno, libc};
use snafu::{ResultExt, Snafu};
use spdk_sys::{
    spdk_nbd_disk,
    spdk_nbd_disk_find_by_nbd_path,
    spdk_nbd_get_path,
    spdk_nbd_start,
    spdk_nbd_stop,
};
use std::{
    convert::TryInto,
    ffi::{c_void, CStr, CString},
    fmt,
    fs::OpenOptions,
    io,
    os::unix::io::AsRawFd,
    path::Path,
    time::Duration,
};
use sysfs::parse_value;

// include/uapi/linux/fs.h
const IOCTL_BLKGETSIZE: u32 = ior!(0x12, 114, std::mem::size_of::<u64>());

#[derive(Debug, Snafu)]
pub enum NbdError {
    #[snafu(display("No free NBD devices available (is nbd kmod loaded?)"))]
    Unavailable {},
    #[snafu(display("Failed to start NBD on {}", dev))]
    StartNbd { source: Errno, dev: String },
}

/// Wait until it is possible to read size of nbd device or time out with error.
/// If we can read the size that means that the device is ready for IO.
async fn wait_until_ready(path: &str) -> Result<(), ()> {
    let device_size: u32 = 0;
    // each iteration sleeps 100ms => total time out is 10s
    for _i in 1i32 .. 100 {
        let _ = Delay::new(Duration::from_millis(100)).await;

        let f = OpenOptions::new().read(true).open(Path::new(&path));
        if f.is_err() {
            trace!("Failed to open nbd device {}, retrying ...", path);
            continue;
        }

        let res = unsafe {
            convert_ioctl_res!(libc::ioctl(
                f.unwrap().as_raw_fd(),
                u64::from(IOCTL_BLKGETSIZE).try_into().unwrap(),
                &device_size
            ))
        };
        if res.is_err() || device_size == 0 {
            trace!("Failed ioctl to nbd device {}, retrying ...", path);
            continue;
        }
        trace!("Device {} reported {} size", path, device_size);
        return Ok(());
    }

    // no size reported within given time window
    Err(())
}

/// Return first unused nbd device in /dev.
///
/// NOTE: We do a couple of syscalls in this function which by normal
/// circumstances do not block. So it is reasonably safe to call this function
/// from executor/reactor.
pub fn find_unused() -> Result<String, NbdError> {
    let nbd_max =
        parse_value(Path::new("/sys/class/modules/nbd/parameters"), "nbds_max")
            .unwrap_or(16);

    for i in 0 .. nbd_max {
        let name = format!("nbd{}", i);
        match parse_value::<u32>(
            Path::new(&format!("/sys/class/block/{}", name)),
            "pid",
        ) {
            // if we find a pid file the device is in use
            Ok(_) => continue,
            Err(e) => match e.kind() {
                io::ErrorKind::NotFound => {
                    // No PID file is found, which implies it is free to used.
                    // The kernel needs time to construct the device
                    // so we need to make sure we are not using it internally
                    // already.
                    let nbd_device =
                        CString::new(format!("/dev/{}", name)).unwrap();
                    let ptr = unsafe {
                        spdk_nbd_disk_find_by_nbd_path(nbd_device.as_ptr())
                    };

                    if ptr.is_null() {
                        return Ok(nbd_device.into_string().unwrap());
                    }
                    continue;
                }
                _ => continue,
            },
        }
    }

    Err(NbdError::Unavailable {})
}

/// Callback for spdk_nbd_start().
extern "C" fn start_cb(
    sender_ptr: *mut c_void,
    nbd_disk: *mut spdk_nbd_disk,
    errno: i32,
) {
    let sender = unsafe {
        Box::from_raw(
            sender_ptr as *mut oneshot::Sender<ErrnoResult<*mut spdk_nbd_disk>>,
        )
    };
    sender
        .send(errno_result_from_i32(nbd_disk, errno))
        .expect("NBD start receiver is gone");
}

/// Start nbd disk using provided device name.
pub async fn start(
    bdev_name: &str,
    device_path: &str,
) -> Result<*mut spdk_nbd_disk, NbdError> {
    let c_bdev_name = CString::new(bdev_name).unwrap();
    let c_device_path = CString::new(device_path).unwrap();
    let (sender, receiver) =
        oneshot::channel::<ErrnoResult<*mut spdk_nbd_disk>>();

    unsafe {
        spdk_nbd_start(
            c_bdev_name.as_ptr(),
            c_device_path.as_ptr(),
            Some(start_cb),
            cb_arg(sender),
        );
    }
    receiver
        .await
        .expect("Cancellation is not supported")
        .context(StartNbd {
            dev: device_path.to_owned(),
        })
}

/// NBD disk representation.
pub struct Disk {
    nbd_ptr: *mut spdk_nbd_disk,
}

impl Disk {
    /// Allocate nbd device for the bdev and start it.
    /// When the function returns the nbd disk is ready for IO.
    pub async fn create(bdev_name: &str) -> Result<Self, NbdError> {
        // find nbd device which is available
        let device_path = find_unused()?;
        let nbd_ptr = start(bdev_name, &device_path).await?;

        info!("Started nbd disk {} for {}", device_path, bdev_name);

        // we wait for the dev to come up online because
        // otherwise the mount done too early would fail.
        // If it times out, continue anyway and let the mount fail.
        if wait_until_ready(&device_path).await.is_err() {
            error!(
                "Timed out waiting for nbd device {} to come up (10s)",
                device_path,
            )
        }
        Ok(Self {
            nbd_ptr,
        })
    }

    /// Stop and release nbd device.
    pub fn destroy(self) {
        if !self.nbd_ptr.is_null() {
            unsafe { spdk_nbd_stop(self.nbd_ptr) };
        }
    }

    /// Get nbd device path (/dev/nbd...) for the nbd disk.
    pub fn get_path(&self) -> String {
        unsafe {
            CStr::from_ptr(spdk_nbd_get_path(self.nbd_ptr))
                .to_str()
                .unwrap()
                .to_string()
        }
    }
}

impl fmt::Debug for Disk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{:?}", self.get_path(), self.nbd_ptr)
    }
}

impl fmt::Display for Disk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.get_path())
    }
}
