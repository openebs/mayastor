use std::{io::SeekFrom, path::Path};
use tokio::{
    fs::OpenOptions,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

use super::{
    compose::rpc::v1::RpcHandle,
    nvme::{find_mayastor_nvme_device_path, NmveConnectGuard},
};

fn create_test_buf(size_mb: usize) -> Vec<u8> {
    assert!(size_mb > 0);

    let sz = size_mb * 1024 * 1024;
    std::iter::repeat(5).take(sz).collect()
}

pub async fn test_write_to_file(
    path: impl AsRef<Path>,
    count: usize,
    buf_size_mb: usize,
) -> std::io::Result<()> {
    let src_buf = create_test_buf(buf_size_mb);

    let mut dst_buf: Vec<u8> = vec![];
    dst_buf.resize(src_buf.len(), 0);

    let mut f = OpenOptions::new()
        .write(true)
        .read(true)
        .create(false)
        .truncate(false)
        .open(&path)
        .await?;

    for _i in 0 .. count {
        let pos = f.stream_position().await?;

        // Write buffer.
        f.write_all(&src_buf).await?;

        // Validate written data.
        f.seek(SeekFrom::Start(pos)).await?;
        f.read_exact(&mut dst_buf).await?;

        for k in 0 .. src_buf.len() {
            if src_buf[k] != dst_buf[k] {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!(
                        "Data validation failed at {}: written {:?}, read {:?}",
                        pos + k as u64,
                        src_buf[k],
                        dst_buf[k]
                    ),
                ));
            }
        }
    }

    Ok(())
}

pub async fn test_write_to_nvme(
    rpc: &mut RpcHandle,
    nqn: &str,
    nvme_serial: &str,
    count: usize,
    buf_size_mb: usize,
) -> std::io::Result<()> {
    let _cg = NmveConnectGuard::connect_addr(&rpc.endpoint, nqn);

    let dev = find_mayastor_nvme_device_path(nvme_serial).unwrap();

    test_write_to_file(dev, count, buf_size_mb).await
}
