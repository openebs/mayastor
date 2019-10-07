//! Utility functions and wrappers for working with nbd devices in SPDK.

use crate::{bdev::nexus::Error, executor::cb_arg};
use futures::channel::oneshot;
use futures_timer::Delay;
use nix::{convert_ioctl_res, errno::Errno, libc};
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
    os::unix::io::AsRawFd,
    path::Path,
    time::Duration,
};

// include/uapi/linux/fs.h
const IOCTL_BLKGETSIZE: u32 = ior!(0x12, 114, std::mem::size_of::<u64>());

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
pub fn find_unused() -> Option<String> {
    let mut idx = 0;
    loop {
        let device_path = format!("/dev/nbd{}", idx);
        let c_device_path = CString::new(device_path.clone()).unwrap();
        let rc = unsafe { libc::access(c_device_path.as_ptr(), libc::F_OK) };
        if rc != 0 {
            // no more devices
            return None;
        }
        // check the device is not used by us
        let nbd =
            unsafe { spdk_nbd_disk_find_by_nbd_path(c_device_path.as_ptr()) };
        if nbd.is_null() {
            let pid_file = format!("/sys/block/nbd{}/pid", idx);
            let pid_file_c = CString::new(pid_file.clone()).unwrap();
            let fd = unsafe { libc::open(pid_file_c.as_ptr(), libc::O_RDONLY) };
            if fd > 0 {
                // the nbd is used
                unsafe { libc::close(fd) };
            } else if Errno::last() == Errno::ENOENT {
                // looks promising
                return Some(device_path);
            } else {
                error!("Failed to open nbd pid file {}", pid_file);
            }
        }
        idx += 1;
    }
}

/// Callback for spdk_nbd_start().
extern "C" fn start_cb(
    sender_ptr: *mut c_void,
    nbd_disk: *mut spdk_nbd_disk,
    errno: i32,
) {
    let sender = unsafe {
        Box::from_raw(
            sender_ptr as *mut oneshot::Sender<(i32, *mut spdk_nbd_disk)>,
        )
    };
    sender.send((errno, nbd_disk)).expect("Receiver is gone");
}

/// Start nbd disk using provided device name.
pub async fn start(
    bdev_name: &str,
    device_path: &str,
) -> Result<*mut spdk_nbd_disk, String> {
    let c_bdev_name = CString::new(bdev_name).unwrap();
    let c_device_path = CString::new(device_path).unwrap();
    let (sender, receiver) = oneshot::channel::<(i32, *mut spdk_nbd_disk)>();

    unsafe {
        spdk_nbd_start(
            c_bdev_name.as_ptr(),
            c_device_path.as_ptr(),
            Some(start_cb),
            cb_arg(sender),
        );
    }
    let res = receiver.await.expect("Cancellation is not supported");
    if res.0 != 0 {
        Err(format!(
            "Failed to start nbd disk {} (errno {})",
            bdev_name, res.0
        ))
    } else {
        info!("Started nbd disk {} for nexus {}", device_path, bdev_name);
        Ok(res.1)
    }
}

/// NBD disk representation.
pub struct Disk {
    nbd_ptr: *mut spdk_nbd_disk,
}

impl Disk {
    /// Allocate nbd device for the bdev and start it.
    /// When the function returns the nbd disk is ready for IO.
    pub async fn create(bdev_name: &str) -> Result<Self, Error> {
        // find nbd device which is available
        let device_path = match find_unused() {
            Some(device_path) => device_path,
            None => {
                return Err(Error::Internal(
                    "NBD devices on the system were depleted".to_owned(),
                ))
            }
        };
        match start(bdev_name, &device_path).await {
            Ok(nbd_ptr) => {
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
            Err(msg) => Err(Error::Internal(msg)),
        }
    }

    /// Stop and release nbd device.
    pub fn destroy(self) {
        unsafe { spdk_nbd_stop(self.nbd_ptr) };
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
