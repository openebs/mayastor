use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, ErrorKind},
    os::unix::fs::OpenOptionsExt,
};

pub fn fs_supports_direct_io(path: &str) -> bool {
    // SPDK uring bdev uses IORING_SETUP_IOPOLL which requires O_DIRECT
    // which works on at least XFS filesystems
    match OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_DIRECT)
        .open(path)
    {
        Ok(_f) => true,
        Err(e) => {
            assert_eq!(e.kind(), ErrorKind::InvalidInput);
            println!("Skipping uring bdev, open: {:?}", e);
            false
        }
    }
}

fn get_mount_filesystem(path: &str) -> Option<String> {
    let mut path = std::path::Path::new(path);
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

pub fn fs_type_supported(path: &str) -> bool {
    match get_mount_filesystem(path) {
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

pub fn kernel_supports_io_uring() -> bool {
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
