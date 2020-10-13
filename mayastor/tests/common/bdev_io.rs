use mayastor::core::{BdevHandle, CoreError};

pub async fn write_some(
    nexus_name: &str,
    offset: u64,
    fill: u8,
) -> Result<(), CoreError> {
    let h = BdevHandle::open(nexus_name, true, false).unwrap();
    let mut buf = h.dma_malloc(512).expect("failed to allocate buffer");
    buf.fill(fill);

    let s = buf.as_slice();
    assert_eq!(s[0], fill);

    h.write_at(offset, &buf).await?;
    Ok(())
}

pub async fn read_some(
    nexus_name: &str,
    offset: u64,
    fill: u8,
) -> Result<(), CoreError> {
    let h = BdevHandle::open(nexus_name, true, false).unwrap();
    let mut buf = h.dma_malloc(1024).expect("failed to allocate buffer");
    let slice = buf.as_mut_slice();

    assert_eq!(slice[0], 0);
    slice[512] = fill;
    assert_eq!(slice[512], fill);

    let len = h.read_at(offset, &mut buf).await?;
    assert_eq!(len, 1024);

    let slice = buf.as_slice();

    for &it in slice.iter().take(512) {
        assert_eq!(it, fill);
    }
    assert_eq!(slice[512], 0);
    Ok(())
}
