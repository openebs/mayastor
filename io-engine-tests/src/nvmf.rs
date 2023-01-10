use super::{
    file_io::{compare_files, test_write_to_file, BufferSize},
    nvme::{find_mayastor_nvme_device_path, NmveConnectGuard},
};
use std::{net::SocketAddr, path::PathBuf};

#[derive(Debug, Clone)]
pub struct NvmfLocation {
    pub addr: SocketAddr,
    pub nqn: String,
    pub serial: String,
}

impl NvmfLocation {
    fn open(&self) -> std::io::Result<(NmveConnectGuard, PathBuf)> {
        let cg = NmveConnectGuard::connect_addr(&self.addr, &self.nqn);
        let path = find_mayastor_nvme_device_path(&self.serial)?;
        Ok((cg, path))
    }
}

pub async fn test_write_to_nvmf(
    nvmf: &NvmfLocation,
    offset: u64,
    count: usize,
    buf_size: BufferSize,
) -> std::io::Result<()> {
    let _cg = NmveConnectGuard::connect_addr(&nvmf.addr, &nvmf.nqn);
    let path = find_mayastor_nvme_device_path(&nvmf.serial)?;
    test_write_to_file(path, offset, count, buf_size).await
}

/// Checks that all given NVMF devices contain identical copies of data.
pub async fn test_devices_identical(
    devices: &[NvmfLocation],
) -> std::io::Result<()> {
    assert!(devices.len() > 1);

    let (_cg0, path0) = devices[0].open()?;

    for dev in devices.iter().skip(1) {
        let (_cgi, pathi) = dev.open()?;
        compare_files(&path0, &pathi).await?;
    }

    Ok(())
}
