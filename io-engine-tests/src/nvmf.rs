use std::{net::SocketAddr, path::PathBuf};

use super::{
    file_io::{compare_files, test_write_to_file, DataSize},
    fio::{spawn_fio_task, Fio},
    nexus::{make_nexus_nqn, make_nexus_serial},
    nvme::{find_mayastor_nvme_device_path, NmveConnectGuard},
};

#[derive(Debug, Clone)]
pub struct NvmfLocation {
    pub addr: SocketAddr,
    pub nqn: String,
    pub serial: String,
}

impl NvmfLocation {
    pub fn from_nexus_info(
        addr: &str,
        nexus_name: &str,
        nexus_uuid: &str,
    ) -> Self {
        Self {
            addr: addr.parse().unwrap(),
            nqn: make_nexus_nqn(nexus_name),
            serial: make_nexus_serial(nexus_uuid),
        }
    }

    pub fn open(&self) -> std::io::Result<(NmveConnectGuard, PathBuf)> {
        let cg = NmveConnectGuard::connect_addr(&self.addr, &self.nqn);
        let path = find_mayastor_nvme_device_path(&self.serial)?;
        Ok((cg, path))
    }

    pub fn as_args(&self) -> Vec<String> {
        vec![
            format!("trtype=tcp"),
            format!("adrfam=IPv4"),
            format!("traddr={}", self.addr.ip()),
            format!("trsvcid=8420"),
            format!("subnqn={}", self.nqn.replace(':', "\\:")),
            format!("ns=1"),
        ]
    }
}

pub async fn test_write_to_nvmf(
    nvmf: &NvmfLocation,
    offset: DataSize,
    count: usize,
    buf_size: DataSize,
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

/// TODO
pub async fn test_fio_to_nvmf(
    nvmf: &NvmfLocation,
    mut fio: Fio,
) -> std::io::Result<()> {
    let tgt = format!("'{}'", nvmf.as_args().join(" "));

    fio.jobs.iter_mut().for_each(|j| {
        j.filename = tgt.clone();
        j.ioengine = "spdk".to_string();
        j.direct = true;
    });

    spawn_fio_task(&fio).await
}

/// TODO
pub async fn test_fio_to_nvmf_aio(
    nvmf: &NvmfLocation,
    mut fio: Fio,
) -> std::io::Result<()> {
    let _cg = NmveConnectGuard::connect_addr(&nvmf.addr, &nvmf.nqn);
    let path = find_mayastor_nvme_device_path(&nvmf.serial)?;
    let path_str = path.to_str().unwrap();

    fio.jobs.iter_mut().for_each(|j| {
        j.filename = path_str.to_string();
        j.ioengine = "libaio".to_string();
        j.direct = true;
    });

    spawn_fio_task(&fio).await
}
