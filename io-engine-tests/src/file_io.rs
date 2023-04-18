use once_cell::sync::OnceCell;
use rand::{distributions::Uniform, Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::{
    fmt::{Display, Formatter},
    io::SeekFrom,
    path::Path,
};
use tokio::{
    fs::OpenOptions,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

static RNG_SEED: OnceCell<ChaCha8Rng> = OnceCell::new();

pub fn set_test_buf_rng_seed(seed: u64) {
    RNG_SEED.set(ChaCha8Rng::seed_from_u64(seed)).unwrap();
}

fn create_test_buf(buf_size: DataSize) -> Vec<u8> {
    let rng = RNG_SEED.get_or_init(ChaCha8Rng::from_entropy).clone();

    let range = Uniform::new_inclusive(0, u8::MAX);

    rng.sample_iter(&range).take(buf_size.into()).collect()
}

/// TODO
#[derive(Debug, Clone)]
pub struct DataSize(u64);

impl Display for DataSize {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<DataSize> for u64 {
    fn from(value: DataSize) -> Self {
        value.0
    }
}

impl From<DataSize> for usize {
    fn from(value: DataSize) -> Self {
        value.0 as usize
    }
}

impl Default for DataSize {
    fn default() -> Self {
        Self::from_bytes(0)
    }
}

impl DataSize {
    pub fn bytes(&self) -> u64 {
        self.0
    }

    pub fn from_bytes(s: u64) -> Self {
        Self(s)
    }

    pub fn from_kb(s: u64) -> Self {
        Self(s * 1024)
    }

    pub fn from_mb(s: u64) -> Self {
        Self(s * 1024 * 1024)
    }

    pub fn from_gb(s: u64) -> Self {
        Self(s * 1024 * 1024 * 1024)
    }

    pub fn from_blocks(s: u64, blk_size: u64) -> Self {
        Self(s * blk_size)
    }

    pub fn from_kb_blocks(s: u64, blk_size_kb: u64) -> Self {
        Self(s * blk_size_kb * 1024)
    }
}

/// TODO
pub async fn test_write_to_file(
    path: impl AsRef<Path>,
    offset: DataSize,
    count: usize,
    buf_size: DataSize,
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

    f.seek(SeekFrom::Start(offset.bytes())).await?;

    // Write.
    for _i in 0 .. count {
        f.write_all(&src_buf).await?;
    }

    // Validate.
    f.seek(SeekFrom::Start(offset.bytes())).await?;
    let mut pos: u64 = offset.bytes();
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
