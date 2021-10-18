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
    let mut flags = UnmountFlags::empty();

    flags.insert(UnmountFlags::DETACH);

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
