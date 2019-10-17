//! Utility function for formatting a device with filesystem

use std::process::Command;

// Move these to csi_common.rs in the future
use blkid::probe::Probe;

/// We probe the device for a filesystem, if there we leave it as is. We do
/// not check at current -- if the FS is the desired FS. This is done with the
/// mindset of, never over write/delete data.

// TODO implicit probed_format_and_mount()
pub(crate) async fn probed_format(
    device: &str,
    fstype: &str,
) -> Result<(), String> {
    let probe = Probe::new_from_filename(device);

    if probe.is_err() {
        return Err("Failed to init device probing".into());
    }

    let probe = probe.unwrap();

    if probe.do_probe().is_err() {
        return Err("Failed to probe device".into());
    }

    // blkid used char **data as a buffer to fill in the value of the
    // TYPE we are looking for or returns NULL on failure. The
    // library then does a CStr::from_ptr().to_str() which will fail
    // if we are NULL. Therefor is_err() here means no value for the given
    // TYPE, and thus no filesystem.
    match probe.lookup_value("TYPE") {
        Err(_) => {
            debug!("Formatting device {} with a {} filesystem", device, fstype);
            let output = Command::new(format!("mkfs.{}", fstype))
                .arg(device)
                .output()
                .expect("Failed to execute mkfs command");
            trace!(
                "Output of mkfs.{} command: {}",
                fstype,
                String::from_utf8(output.stdout).unwrap()
            );
            if !output.status.success() {
                return Err(format!(
                    "Failed to format {} with {} fs: {}",
                    device,
                    fstype,
                    String::from_utf8(output.stderr).unwrap()
                ));
            }
            info!("Device {} formatted with {} filesystem", device, fstype);
        }
        Ok(fs) => {
            info!(
                "Skipping format: device {} contains a preexisting {} filesystem",
                device, fs
            );
        }
    }

    Ok(())
}
