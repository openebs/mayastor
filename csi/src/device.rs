use nix::{convert_ioctl_res, libc::ioctl};
use std::convert::TryInto;
// include/uapi/linux/fs.h
const IOCTL_BLKGETSIZE: u32 = ior!(0x12, 114, std::mem::size_of::<u64>());

use std::{fs::OpenOptions, os::unix::io::AsRawFd, path::Path};

pub fn await_size(path: &str) -> Result<usize, String> {
    let device_size = 0;
    for i in 1 .. 100 {
        trace!("trying to get device size from {}", path);
        std::thread::sleep(std::time::Duration::from_millis(1000));
        let f = OpenOptions::new().read(true).open(Path::new(&path));

        if f.is_err() {
            trace!(
                "Failed to open device {}, its not there yet retrying ({})",
                path,
                i
            );
            continue;
        }

        let res = unsafe {
            convert_ioctl_res!(ioctl(
                f.unwrap().as_raw_fd(),
                u64::from(IOCTL_BLKGETSIZE).try_into().unwrap(),
                &device_size
            ))
        };

        if res.is_err() || device_size == 0 {
            trace!("Failed ioctl to device {}, retrying ({})", path, i);
            continue;
        }
        assert!(res.is_ok(), true);
        trace!("Device {} reported {} size", path, device_size);
        return Ok(device_size);
    }

    // no size reported within given time window
    Err("device not ready; invalid size".into())
}
