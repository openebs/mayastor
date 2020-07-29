//! Utility function for formatting a device with filesystem

use std::process::Command;

use blkid::probe::Probe;

pub(crate) async fn prepare_device(
    device: &str,
    fstype: &str,
) -> Result<(), String> {
    debug!("Probing device {}", device);

    let probe = Probe::new_from_filename(device)
        .map_err(|error| format!("probe setup failed: {}", error))?;

    if let Err(error) = probe.do_probe() {
        return Err(format!("probe failed: {}", error));
    }

    if let Ok(fs) = probe.lookup_value("TYPE") {
        debug!("Found existing filesystem ({}) on device {}", fs, device);
        return Ok(());
    }

    debug!("Creating new filesystem ({}) on device {}", fstype, device);

    let binary = format!("mkfs.{}", fstype);
    let output = Command::new(&binary)
        .arg(device)
        .output()
        .map_err(|error| format!("failed to execute {}: {}", binary, error))?;

    trace!(
        "Output from {} command: {}",
        binary,
        String::from_utf8(output.stdout).unwrap()
    );

    if output.status.success() {
        return Ok(());
    }

    Err(format!(
        "{} command failed: {}",
        binary,
        String::from_utf8(output.stderr).unwrap()
    ))
}
