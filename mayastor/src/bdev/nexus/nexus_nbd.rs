//! Utility functions and wrappers for working with nbd devices in SPDK.

use core::sync::atomic::Ordering::SeqCst;
use futures::channel::oneshot;
use nix::{convert_ioctl_res, errno::Errno, libc};
use snafu::{ResultExt, Snafu};
use std::{
    ffi::{c_void, CStr, CString},
    fmt,
    fs::OpenOptions,
    io,
    os::unix::io::AsRawFd,
    path::Path,
    sync::{atomic::AtomicBool, Arc},
    time::Duration,
};
use sysfs::parse_value;

use spdk_rs::libspdk::{
    nbd_disk_find_by_nbd_path,
    spdk_nbd_disk,
    spdk_nbd_get_path,
    spdk_nbd_start,
};

use crate::{
    core::{Mthread, Reactors},
    ffihelper::{cb_arg, errno_result_from_i32, ErrnoResult},
};

// include/uapi/linux/fs.h
const IOCTL_BLKGETSIZE: u32 = ior!(0x12, 114, std::mem::size_of::<u64>());
const SET_TIMEOUT: u32 = io!(0xab, 9);
const SET_SIZE: u32 = io!(0xab, 2);

#[derive(Debug, Snafu)]
pub enum NbdError {
    #[snafu(display("No free NBD devices available (is NBD kmod loaded?)"))]
    Unavailable {},
    #[snafu(display("Failed to start NBD on {}", dev))]
    StartNbd { source: Errno, dev: String },
}

extern "C" {
    fn nbd_disconnect(nbd: *mut spdk_nbd_disk);
}

/// We need to wait for the device to be ready. That is, it takes a certain
/// amount of time for the device to be fully operational from a kernel
/// perspective. This is somewhat annoying, but what makes matters worse is that
/// if we are running the device creation path, on the same core that is
/// handling the IO, we get into a state where we make no forward progress.
pub(crate) fn wait_until_ready(path: &str) {
    let started = Arc::new(AtomicBool::new(false));

    let tpath = String::from(path);
    let s = started.clone();

    debug!("Waiting for NBD device {} to become ready...", path);
    // start a thread that loops and tries to open us and asks for our size
    Mthread::spawn_unaffinitized(move || {
        // this should not be needed but for some unknown reason, we end up with
        // stale NBD devices. Setting this to non zero, prevents that from
        // happening (although we dont actually timeout).
        let timeout = 3;
        let f = OpenOptions::new().read(true).open(Path::new(&tpath));
        unsafe {
            convert_ioctl_res!(libc::ioctl(
                f.unwrap().as_raw_fd(),
                SET_TIMEOUT as u64,
                timeout
            ))
        }
        .unwrap();
        debug!("Timeout of NBD device {} was set to {}", tpath, timeout);
        let size: u64 = 0;
        let mut delay = 1;
        for _i in 0i32 .. 10 {
            if let Ok(f) = OpenOptions::new().read(true).open(Path::new(&tpath))
            {
                let res = unsafe {
                    convert_ioctl_res!(libc::ioctl(
                        f.as_raw_fd(),
                        u64::from(IOCTL_BLKGETSIZE),
                        &size
                    ))
                };
                if res.is_ok() && size != 0 {
                    break;
                }
            }
            debug!("Disk {} not ready yet, sleeping {} ms...", tpath, delay);
            std::thread::sleep(Duration::from_millis(delay));
            delay *= 2;
        }
        if size > 0 {
            debug!("Disk {} is ready (size={})", tpath, size);
        } else {
            warn!(
                "Disk {} not ready but continuing anyway - expect problems",
                tpath
            );
        }

        s.store(true, SeqCst);
    });

    // the above thread is running, make sure we keep polling/turning the
    // reactor. We keep doing this until the above thread has updated the
    // atomic. In the future we might be able call yield()
    while !started.load(SeqCst) {
        Reactors::current().poll_once();
    }
}

/// Return first unused nbd device in /dev.
///
/// NOTE: We do a couple of syscalls in this function which by normal
/// circumstances do not block.
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
                        nbd_disk_find_by_nbd_path(nbd_device.as_ptr())
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
        .map(|ok| {
            info!(
                "Nbd device {} for parent {} started",
                device_path, bdev_name
            );
            ok
        })
}

/// NBD disk representation.
pub struct NbdDisk {
    nbd_ptr: *mut spdk_nbd_disk,
}

impl NbdDisk {
    /// Allocate nbd device for the bdev and start it.
    /// When the function returns the nbd disk is ready for IO.
    pub async fn create(bdev_name: &str) -> Result<Self, NbdError> {
        // find a NBD device which is available
        let device_path = find_unused()?;
        let nbd_ptr = start(bdev_name, &device_path).await?;

        // we wait for the dev to come up online because
        // otherwise the mount done too early would fail.
        // If it times out, continue anyway and let the mount fail.
        wait_until_ready(&device_path);
        info!("Started nbd disk {} for {}", device_path, bdev_name);

        Ok(Self {
            nbd_ptr,
        })
    }

    /// Stop and release nbd device.
    pub fn destroy(self) {
        let started = Arc::new(AtomicBool::new(false));
        let s = started.clone();

        // this is a hack to wait for any IO in the NBD driver. Typically this
        // is not they way to do this but, NBD is mostly for testing so
        // its fine. as we can not make FFI struct send, we copy the
        // pointer to usize and pass that to the other threads.

        let ptr = self.nbd_ptr as usize;
        let name = self.get_path();
        let nbd_name = name.clone();
        debug!("Stopping NBD device {}...", name);
        Mthread::spawn_unaffinitized(move || {
            unsafe {
                // After disconnecting a disk changed event is triggered which
                // causes a refresh of the size back to the
                // original size and a partition scan.
                // Set the size to 0 before disconnecting in hopes of stopping
                // that.
                let f =
                    OpenOptions::new().read(true).open(Path::new(&nbd_name));
                convert_ioctl_res!(libc::ioctl(
                    f.unwrap().as_raw_fd(),
                    SET_SIZE as u64,
                    0
                ))
                .unwrap();
                nbd_disconnect(ptr as *mut _);
            };
            debug!("NBD device disconnected successfully");
            s.store(true, SeqCst);
        });

        while !started.load(SeqCst) {
            Reactors::current().poll_once();
        }

        info!("NBD {} device stopped", name);
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

    /// Get nbd device path uri (file:///dev/nbd...) for the nbd disk.
    pub fn as_uri(&self) -> String {
        format!("file://{}", self.get_path())
    }
}

impl fmt::Debug for NbdDisk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{:?}", self.get_path(), self.nbd_ptr)
    }
}

impl fmt::Display for NbdDisk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.get_path())
    }
}
