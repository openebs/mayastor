//! Utility functions for working with mountpoints

use std::process::Command;

use proc_mounts::MountIter;
use run_script::ScriptOptions;
use sys_mount::{unmount, FilesystemType, Mount, MountFlags, UnmountFlags};

// Information about a mounted filesystem.
pub struct MountInfo {
    pub source: String,
    pub dest: String,
    pub opts: Vec<String>,
}

// Filesystem type info and default mount options
#[derive(Clone, Debug)]
pub struct Fs {
    pub name: String,
    pub defaults: Vec<String>,
}

// Return mountinfo matching source or destination or source and destination
// depending on 'and' flag.
pub fn match_mount(
    source: Option<&str>,
    destination: Option<&str>,
    and: bool,
) -> Option<MountInfo> {
    for mount in MountIter::new().unwrap() {
        if let Ok(mount) = mount {
            let source_match = if let Some(src) = source {
                if mount.source.to_string_lossy() == src {
                    Some(true)
                } else {
                    Some(false)
                }
            } else {
                None
            };
            let dest_match = if let Some(dst) = destination {
                if mount.dest.to_string_lossy() == dst {
                    Some(true)
                } else {
                    Some(false)
                }
            } else {
                None
            };

            let found = if and {
                match source_match {
                    Some(true) => match dest_match {
                        Some(true) => true,
                        Some(false) => false,
                        None => true,
                    },
                    Some(false) => false,
                    None => match dest_match {
                        Some(true) => true,
                        Some(false) => false,
                        None => true,
                    },
                }
            } else {
                match source_match {
                    Some(true) => true,
                    Some(false) => match dest_match {
                        Some(true) => true,
                        Some(false) => false,
                        None => false,
                    },
                    None => match dest_match {
                        Some(true) => true,
                        Some(false) => false,
                        None => false,
                    },
                }
            };

            if found {
                trace!("Matched mount: {:?}", mount);
                return Some(MountInfo {
                    source: mount.source.to_string_lossy().to_string(),
                    dest: mount.dest.to_string_lossy().to_string(),
                    opts: mount.options,
                });
            }
        }
    }
    None
}

// XXX we rely that ordering of options between the two mounts is the same
// which is a bit fragile.
pub fn mount_opts_compare(m1: &[String], m2: &[String], ro: bool) -> bool {
    if m1.len() != m2.len() {
        return false;
    }

    for i in 0..m1.len() {
        if m2[i] == "rw" && ro {
            debug!("we are mounted as RW but request is RO that is OK");
            continue;
        }
        if m1[i] != m2[i] {
            return false;
        }
    }
    true
}

// Return supported filesystems and their default mount options.
pub fn probe_filesystems() -> Result<Vec<Fs>, String> {
    let mut filesystems = Vec::new();
    // the first filesystem is the default one
    let supported_fs = ["xfs", "ext4"];
    // im using run_script as its a very nice macro to not "exec" something
    // rather it actually builds a proper script.
    let mut options = ScriptOptions::new();
    options.exit_on_error = true;

    // truncate in busybox does not support units, so be express the size in
    // bytes
    let (code, _stdout, stderr) = run_script!(
        r#"
        truncate -s 67108864 /tmp/fs.img
        mkdir -p /tmp/fs_default
        "#,
        &options
    )
    .unwrap();
    if code != 0 {
        return Err(format!(
            "Failed to set up default mount options files: {}",
            stderr
        ));
    }

    for fsname in supported_fs.iter() {
        match probe_defaults(fsname) {
            Ok(opts) => filesystems.push(Fs {
                name: (*fsname).to_string(),
                defaults: opts,
            }),
            Err(err) => {
                warn!("Filesystem {} will not be available: {}", fsname, err)
            }
        }
    }

    // stdout of clean up script is always empty
    let (code, _stdout, stderr) = run_script!(
        r#"
        rmdir /tmp/fs_default
        rm -f /tmp/fs.img
        "#,
        &options
    )
    .unwrap();
    if code != 0 {
        error!("Failed to cleanup default mount options files: {}", stderr);
    }

    Ok(filesystems)
}

// After some research, it turns out that default mount options
// are depending on various CONFIG_XXXX options during kernel
// config. Depending on FS (sigh) the defaults can be determined
// by an util like tune2fs. However, this requires you to have a
// filesystem of that type to begin with... (how useful)
//
// I have found no way to determine these options by digging through
// sysfs so here is a hack. I feel bad about this. I hate to do this
// but I've given up. Linux won, there you have it.
fn probe_defaults(fsname: &str) -> Result<Vec<String>, String> {
    let output = Command::new(format!("mkfs.{}", fsname))
        .arg("/tmp/fs.img")
        .output()
        .expect("Failed to execute mkfs command");
    if !output.status.success() {
        return Err(format!(
            "Failed to mkfs {} fs: {}",
            fsname,
            String::from_utf8(output.stderr).unwrap()
        ));
    }
    trace!(
        "Output of mkfs.{} command: {}",
        fsname,
        String::from_utf8(output.stdout).unwrap()
    );

    let output = Command::new("mount")
        .arg("/tmp/fs.img")
        .arg("/tmp/fs_default")
        .output()
        .expect("Failed to execute mount command");
    if !output.status.success() {
        return Err(format!(
            "Failed to mount {} fs: {}",
            fsname,
            String::from_utf8(output.stderr).unwrap()
        ));
    }

    let mut options = match_mount(None, Some("/tmp/fs_default"), true)
        .unwrap()
        .opts;

    let output = Command::new("umount")
        .arg("-vf")
        .arg("/tmp/fs_default")
        .output()
        .expect("Failed to execute umount command");

    if !output.status.success() {
        error!(
            "Failed to unmount {} probe fs: {}",
            fsname,
            String::from_utf8(output.stderr).unwrap()
        );
    }

    options
        .iter()
        .position(|n| n == "rw")
        .map(|e| options.remove(e));

    info!("Default {} mount options: {}", fsname, options.join(","));
    Ok(options)
}

/// Mount filesystem
pub fn mount_fs(
    from: &str,
    to: &str,
    bind_mount: bool,
    fstype: &str,
    mnt_opts: &[String],
) -> Result<(), String> {
    debug!("Mounting {} ...", from);

    let mut flags = MountFlags::empty();
    if bind_mount {
        flags.insert(MountFlags::BIND);
    }

    // convert ro mount option to mount flag
    let mut opts = Vec::new();
    for opt in mnt_opts {
        if opt == "ro" {
            flags.insert(MountFlags::RDONLY);
        } else {
            opts.push(opt.to_owned())
        }
    }
    let opts = opts.join(",");

    let res = Mount::new(
        from,
        to,
        FilesystemType::Manual(fstype),
        flags,
        Some(&opts),
    );

    match res {
        Ok(_) => {
            info!(
                "Mounted {} fs on {} with opts \"{}\" to {}",
                fstype, from, opts, to,
            );
            Ok(())
        }
        Err(err) => Err(format!(
            "Failed to mount {} fs on {} with opts \"{}\" to {}: {}",
            fstype, from, opts, to, err,
        )),
    }
}

/// Unmount a filesystem. We use different unmount flags for bind and non-bind
/// mounts (corresponds to stage and publish type of mounts).
pub fn unmount_fs(from: &str, bound: bool) -> Result<(), String> {
    let mut flags = UnmountFlags::empty();

    if bound {
        flags.insert(UnmountFlags::FORCE);
    } else {
        flags.insert(UnmountFlags::DETACH);
    }

    debug!("Unmounting {} (flags={:?}) ...", from, flags);

    match unmount(&from, flags) {
        Ok(_) => {
            info!("Filesystem at {} has been unmounted", from);
            Ok(())
        }
        Err(err) => Err(format!("Failed to unmount fs at {}: {}", from, err)),
    }
}
