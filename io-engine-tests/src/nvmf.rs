use super::{
    file_io::{compute_file_checksum, test_write_to_file},
    nvme::{find_mayastor_nvme_device_path, NmveConnectGuard},
};
use std::net::SocketAddr;

#[derive(Debug, Clone)]
pub struct NvmfLocation {
    pub addr: SocketAddr,
    pub nqn: String,
    pub serial: String,
}

pub async fn test_write_to_nvmf(
    nvmf: &NvmfLocation,
    count: usize,
    buf_size_mb: usize,
) -> std::io::Result<()> {
    let _cg = NmveConnectGuard::connect_addr(&nvmf.addr, &nvmf.nqn);
    let path = find_mayastor_nvme_device_path(&nvmf.serial)?;
    test_write_to_file(path, count, buf_size_mb).await
}

/// Checks that all given NVMF devices contain identical copies of data.
pub async fn test_devices_identical(
    devices: &[NvmfLocation],
) -> std::io::Result<()> {
    assert!(!devices.is_empty());

    let mut first: Option<String> = None;

    for d in devices {
        let _cg = NmveConnectGuard::connect_addr(&d.addr, &d.nqn);
        let path = find_mayastor_nvme_device_path(&d.serial)?;
        let ch = compute_file_checksum(&path).await?;
        if let Some(f) = &first {
            assert_eq!(
                f, &ch,
                "Device {}/{}: checksum mismatch",
                d.addr, d.nqn
            );
        } else {
            first = Some(ch);
        }
    }

    Ok(())
}
