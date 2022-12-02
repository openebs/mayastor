//! Utility functions for mounting and unmounting filesystems.

use std::{collections::HashSet, io::Error};

use devinfo::mountinfo::{MountInfo, MountIter};
use sys_mount::{unmount, FilesystemType, Mount, MountFlags, UnmountFlags};

// Simple trait for checking if the readonly (ro) option
// is present in a "list" of options, while allowing for
// flexibility as to the type of "list".
pub(super) trait ReadOnly {
    fn readonly(&self) -> bool;
}

impl ReadOnly for Vec<String> {
    fn readonly(&self) -> bool {
        self.iter().any(|entry| entry == "ro")
    }
}

impl ReadOnly for &str {
    fn readonly(&self) -> bool {
        self.split(',').any(|entry| entry == "ro")
    }
}

/// Return mountinfo matching source and/or destination.
pub fn find_mount(
    source: Option<&str>,
    target: Option<&str>,
) -> Option<MountInfo> {
    let mut found: Option<MountInfo> = None;

    for mount in MountIter::new().unwrap().flatten() {
        if let Some(value) = source {
            if mount.source.to_string_lossy() == value {
                if let Some(value) = target {
                    if mount.dest.to_string_lossy() == value {
                        found = Some(mount);
                    }
                    continue;
                }
                found = Some(mount);
            }
            continue;
        }
        if let Some(value) = target {
            if mount.dest.to_string_lossy() == value {
                found = Some(mount);
            }
        }
    }

    found.map(MountInfo::from)
}

/// Return all mounts for a matching source.
/// Optionally ignore the given destination path.
pub(crate) fn find_src_mounts(
    source: &str,
    dest_ignore: Option<&str>,
) -> Vec<MountInfo> {
    MountIter::new()
        .unwrap()
        .flatten()
        .filter(|mount| {
            mount.source.to_string_lossy() == source
                && match dest_ignore {
                    None => true,
                    Some(ignore) => ignore != mount.dest.to_string_lossy(),
                }
        })
        .collect()
}

/// Check if options in "first" are also present in "second",
/// but exclude values "ro" and "rw" from the comparison.
pub(super) fn subset(first: &[String], second: &[String]) -> bool {
    let set: HashSet<&String> = second.iter().collect();
    for entry in first {
        if entry == "ro" {
            continue;
        }
        if entry == "rw" {
            continue;
        }
        if set.get(entry).is_none() {
            return false;
        }
    }
    true
}

/// Return supported filesystems.
pub fn probe_filesystems() -> Vec<String> {
    vec![String::from("xfs"), String::from("ext4")]
}

// Utility function to transform a vector of options
// to the format required by sys_mount::Mount::new()
fn parse(options: &[String]) -> (bool, String) {
    let mut list: Vec<&str> = Vec::new();
    let mut readonly: bool = false;

    for entry in options {
        if entry == "ro" {
            readonly = true;
            continue;
        }

        if entry == "rw" {
            continue;
        }

        list.push(entry);
    }

    (readonly, list.join(","))
}

// Utility function to wrap a string in an Option.
// Note that, in particular, the empty string is mapped to None.
fn option(value: &str) -> Option<&str> {
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}

// Utility function used for displaying a list of options.
fn show(options: &[String]) -> String {
    let list: Vec<String> = options
        .iter()
        .cloned()
        .filter(|value| value != "rw")
        .collect();

    if list.is_empty() {
        return String::from("none");
    }

    list.join(",")
}

/// Mount a device to a directory (mountpoint)
pub fn filesystem_mount(
    device: &str,
    target: &str,
    fstype: &str,
    options: &[String],
) -> Result<Mount, Error> {
    let mut flags = MountFlags::empty();

    let (readonly, value) = parse(options);

    if readonly {
        flags.insert(MountFlags::RDONLY);
    }

    let mount = Mount::new(
        device,
        target,
        FilesystemType::Manual(fstype),
        flags,
        option(&value),
    )?;

    debug!(
        "Filesystem ({}) on device {} mounted onto target {} (options: {})",
        fstype,
        device,
        target,
        show(options)
    );

    Ok(mount)
}

/// Unmount a device from a directory (mountpoint)
/// Should not be used for removing bind mounts.
pub fn filesystem_unmount(target: &str) -> Result<(), Error> {
    let flags = UnmountFlags::empty();
    // read more about the umount system call and it's flags at `man 2 umount`

    unmount(target, flags)?;

    debug!("Target {} unmounted", target);

    Ok(())
}

/// Bind mount a source path to a target path.
/// Supports both directories and files.
pub fn bind_mount(
    source: &str,
    target: &str,
    file: bool,
) -> Result<Mount, Error> {
    let mut flags = MountFlags::empty();

    flags.insert(MountFlags::BIND);

    if file {
        flags.insert(MountFlags::RDONLY);
    }

    let mount = Mount::new(
        source,
        target,
        FilesystemType::Manual("none"),
        flags,
        None,
    )?;

    debug!("Source {} bind mounted onto target {}", source, target);

    Ok(mount)
}

/// Bind remount a path to modify mount options.
/// Assumes that target has already been bind mounted.
pub fn bind_remount(target: &str, options: &[String]) -> Result<Mount, Error> {
    let mut flags = MountFlags::empty();

    let (readonly, value) = parse(options);

    flags.insert(MountFlags::BIND);

    if readonly {
        flags.insert(MountFlags::RDONLY);
    }

    flags.insert(MountFlags::REMOUNT);

    let mount = Mount::new(
        "none",
        target,
        FilesystemType::Manual("none"),
        flags,
        option(&value),
    )?;

    debug!(
        "Target {} bind remounted (options: {})",
        target,
        show(options)
    );

    Ok(mount)
}

/// Unmounts a path that has previously been bind mounted.
/// Should not be used for unmounting devices.
pub fn bind_unmount(target: &str) -> Result<(), Error> {
    let flags = UnmountFlags::empty();

    unmount(target, flags)?;

    debug!("Target {} bind unmounted", target);

    Ok(())
}

/// Remount existing mount as read only or read write.
pub(crate) fn remount(target: &str, ro: bool) -> Result<Mount, Error> {
    let mut flags = MountFlags::empty();
    flags.insert(MountFlags::REMOUNT);

    if ro {
        flags.insert(MountFlags::RDONLY);
    }

    let mount =
        Mount::new("", target, FilesystemType::Manual("none"), flags, None)?;

    debug!("Target {} remounted with {:?}", target, flags);

    Ok(mount)
}

/// Mount a block device
pub fn blockdevice_mount(
    source: &str,
    target: &str,
    readonly: bool,
) -> Result<Mount, Error> {
    debug!("Mounting {} ...", source);

    let mut flags = MountFlags::empty();
    flags.insert(MountFlags::BIND);

    let mount = Mount::new(
        source,
        target,
        FilesystemType::Manual("none"),
        flags,
        None,
    )?;
    info!("Block device {} mounted to {}", source, target,);

    if readonly {
        flags.insert(MountFlags::REMOUNT);
        flags.insert(MountFlags::RDONLY);

        let mount =
            Mount::new("", target, FilesystemType::Manual(""), flags, None)?;
        info!("Remounted block device {} (readonly) to {}", source, target,);
        return Ok(mount);
    }

    Ok(mount)
}

/// Unmount a block device.
pub fn blockdevice_unmount(target: &str) -> Result<(), Error> {
    let flags = UnmountFlags::empty();

    debug!("Unmounting block device {} (flags={:?}) ...", target, flags);

    unmount(&target, flags)?;
    info!("block device at {} has been unmounted", target);
    Ok(())
}

/// Waits until a device's filesystem is shutdown.
/// This is useful to know if it's safe to detach a device from a node or not as
/// it seems that even after a umount completes the filesystem and more
/// specifically the filesystem's journal might not be completely shutdown.
/// Specifically, this waits for the filesystem (eg: ext4) shutdown and the
/// filesystem's journal shutdown: jbd2.
pub(crate) async fn wait_fs_shutdown(
    device: &str,
    fstype: Option<String>,
) -> Result<(), Error> {
    let device_trim = device.replace("/dev/", "");

    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(2);

    if let Some(fstype) = fstype {
        let proc_fs_str = format!("/proc/fs/{}/{}", fstype, device_trim);
        let proc_fs = std::path::Path::new(&proc_fs_str);
        wait_file_removal(proc_fs, start, timeout).await?;
    }

    let jbd2_pattern = format!("/proc/fs/jbd2/{}-*", device_trim);
    let proc_jbd2 = glob::glob(&jbd2_pattern)
        .expect("valid pattern")
        .next()
        .and_then(|v| v.ok());
    if let Some(proc_jbd2) = proc_jbd2 {
        wait_file_removal(&proc_jbd2, start, timeout).await?;
    }

    Ok(())
}

/// Waits until a file is removed, up to a timeout.
async fn wait_file_removal(
    proc: &std::path::Path,
    start: std::time::Instant,
    timeout: std::time::Duration,
) -> Result<(), Error> {
    let check_interval = std::time::Duration::from_millis(200);
    let proc_str = proc.to_string_lossy().to_string();
    let mut exists = proc.exists();
    while start.elapsed() < timeout && exists {
        tracing::debug!(proc = %proc_str, "proc entry still exists");
        tokio::time::sleep(check_interval).await;
        exists = proc.exists();
    }
    match exists {
        false => Ok(()),
        true => Err(Error::new(
            std::io::ErrorKind::TimedOut,
            format!("Timed out waiting for '{}' to be removed", proc_str),
        )),
    }
}
