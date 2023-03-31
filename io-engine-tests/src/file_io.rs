use once_cell::sync::OnceCell;
use rand::{distributions::Uniform, Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::{io::SeekFrom, path::Path};
use tokio::{
    fs::OpenOptions,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

static RNG_SEED: OnceCell<ChaCha8Rng> = OnceCell::new();

pub fn set_test_buf_rng_seed(seed: u64) {
    RNG_SEED.set(ChaCha8Rng::seed_from_u64(seed)).unwrap();
}

fn create_test_buf(buf_size: BufferSize) -> Vec<u8> {
    let rng = RNG_SEED.get_or_init(ChaCha8Rng::from_entropy).clone();

    let range = Uniform::new_inclusive(0, u8::MAX);

    rng.sample_iter(&range)
        .take(buf_size.size_bytes() as usize)
        .collect()
}

/// TODO
#[derive(Debug, Clone)]
pub enum BufferSize {
    Bytes(u64),
    Mb(u64),
    Kb(u64),
}

impl BufferSize {
    pub fn size_bytes(&self) -> u64 {
        match &self {
            BufferSize::Bytes(s) => *s,
            BufferSize::Mb(s) => s * 1024 * 1024,
            BufferSize::Kb(s) => s * 1024,
        }
    }

    pub fn size_str_with_suffix(&self) -> String {
        match &self {
            BufferSize::Bytes(s) => s.to_string(),
            BufferSize::Mb(s) => format!("{s}M"),
            BufferSize::Kb(s) => format!("{s}K"),
        }
    }
}

/// TODO
pub async fn test_write_to_file(
    path: impl AsRef<Path>,
    offset: u64,
    count: usize,
    buf_size: BufferSize,
) -> std::io::Result<()> {
    let src_buf = create_test_buf(buf_size);

    let mut dst_buf: Vec<u8> = vec![];
    dst_buf.resize(src_buf.len(), 0);

    let mut f = OpenOptions::new()
        .write(true)
        .read(true)
        .create(false)
        .truncate(false)
        .open(&path)
        .await?;

    f.seek(SeekFrom::Start(offset)).await?;

    // Write.
    for _i in 0 .. count {
        f.write_all(&src_buf).await?;
    }

    // Validate.
    f.seek(SeekFrom::Start(offset)).await?;
    let mut pos = offset;
    for _i in 0 .. count {
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

        pos += src_buf.len() as u64;
    }

    Ok(())
}

pub async fn compute_file_checksum(
    path: impl AsRef<Path>,
) -> std::io::Result<String> {
    let mut f = OpenOptions::new()
        .write(false)
        .read(true)
        .create(false)
        .truncate(false)
        .open(&path)
        .await?;

    let mut buf = [0; 16384];
    let mut hasher = md5::Context::new();

    loop {
        let n = f.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.consume(&buf[.. n]);
    }

    Ok(hex::encode(hasher.compute().0))
}

pub async fn compare_files(
    path_a: impl AsRef<Path>,
    path_b: impl AsRef<Path>,
) -> std::io::Result<()> {
    use std::io::{Error, ErrorKind};

    let name_a = path_a.as_ref();
    let name_b = path_b.as_ref();

    let mut fa = OpenOptions::new()
        .write(false)
        .read(true)
        .create(false)
        .truncate(false)
        .open(&path_a)
        .await?;

    let mut fb = OpenOptions::new()
        .write(false)
        .read(true)
        .create(false)
        .truncate(false)
        .open(&path_b)
        .await?;

    let mut buf_a = [0; 16384];
    let mut buf_b = [0; 16384];

    let mut pos = 0;

    loop {
        let na = fa.read(&mut buf_a).await?;
        let nb = fb.read(&mut buf_b).await?;

        if na != nb {
            return Err(Error::new(
                ErrorKind::Other,
                format!("Size of file {name_a:?} != size of {name_b:?}"),
            ));
        }

        if na == 0 {
            break;
        }

        for i in 0 .. na {
            if buf_a[i] != buf_b[i] {
                return Err(Error::new(
                    ErrorKind::Other,
                    format!(
                        "Miscompare at {pos}: {na:?} {va:#02x} != {nb:?} {vb:#02x}",
                        na = name_a,
                        va = buf_a[i],
                        nb = name_b,
                        vb = buf_b[i],
                    ),
                ));
            }
            pos += 1;
        }
    }

    Ok(())
}
