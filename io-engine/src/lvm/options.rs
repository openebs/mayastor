//! Pool-level options for the LVM backend.
//!
//! A create request asks for a thin pool with `lvm.thin_pool_size`, and its
//! `cluster_size` is the thin pool's chunk size. Neither is stored: the thin
//! pool carries its own size and chunk size.

use super::Error;
use crate::pool_backend::PoolArgs;

/// Name of the thin pool logical volume owned by the backend within a volume
/// group. Replica names are uuids, so this cannot collide.
pub(super) const THIN_POOL_LV: &str = "mayastor-thinpool";

/// lvmthin(7): the chunk size is a multiple of 64KiB between 64KiB and 1GiB.
const CHUNK_MIN: u64 = 64 << 10;
const CHUNK_MAX: u64 = 1 << 30;

/// Thin pool options for the LVM backend.
#[derive(Debug, Default, Clone, PartialEq)]
pub(crate) struct LvmPoolOpts {
    /// Size of the thin pool to create within the volume group.
    thinpool: Option<u64>,
    /// Chunk size for the thin pool.
    chunk: Option<u64>,
}

impl LvmPoolOpts {
    /// Take the thin pool options from the pool arguments. The cluster size
    /// only counts with a thin pool, since the control plane always sends one.
    pub(crate) fn try_from_args(args: &PoolArgs) -> Result<Self, Error> {
        let thinpool = args.lvm.as_ref().and_then(|lvm| lvm.thin_pool_size);
        let opts = Self {
            thinpool,
            chunk: thinpool.and(args.cluster_size.map(u64::from)),
        };
        opts.validate()?;
        Ok(opts)
    }

    fn validate(&self) -> Result<(), Error> {
        if self.thinpool == Some(0) {
            return Err(Error::InvalidOption {
                error: "the thin pool size must be larger than 0".to_string(),
            });
        }
        if let Some(chunk) = self.chunk {
            if !(CHUNK_MIN..=CHUNK_MAX).contains(&chunk) || chunk % CHUNK_MIN != 0 {
                return Err(Error::InvalidOption {
                    error: format!(
                        "the thin pool chunk size must be a multiple of 64KiB between 64KiB \
                         and 1GiB, not {chunk}b"
                    ),
                });
            }
        }
        Ok(())
    }

    /// The thin pool size, if a thin pool was requested.
    pub(super) fn thinpool(&self) -> Option<u64> {
        self.thinpool
    }
    /// The thin pool chunk size.
    pub(super) fn chunk(&self) -> Option<u64> {
        self.chunk
    }
}

impl std::fmt::Display for LvmPoolOpts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Some(size) = self.thinpool else {
            return write!(f, "no thin pool");
        };
        write!(f, "a thin pool of {size}b")?;
        match self.chunk {
            Some(chunk) => write!(f, " with {chunk}b chunks"),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pool_backend::LvmPoolArgs;

    fn args(thin_pool_size: Option<u64>, cluster_size: Option<u32>) -> PoolArgs {
        PoolArgs {
            disks: vec!["/dev/sda".to_string()],
            cluster_size,
            lvm: thin_pool_size.map(|size| LvmPoolArgs {
                thin_pool_size: Some(size),
            }),
            ..Default::default()
        }
    }

    #[test]
    fn pool_opts_from_args() {
        let opts = LvmPoolOpts::try_from_args(&args(None, None)).unwrap();
        assert_eq!(opts, LvmPoolOpts::default());
        // a cluster size alone asks for no thin pool
        let opts = LvmPoolOpts::try_from_args(&args(None, Some(4 << 20))).unwrap();
        assert_eq!(opts, LvmPoolOpts::default());
        let opts = LvmPoolOpts::try_from_args(&args(Some(512 << 20), Some(64 << 10))).unwrap();
        assert_eq!(opts.thinpool(), Some(512 << 20));
        assert_eq!(opts.chunk(), Some(64 << 10));
        assert_eq!(
            opts.to_string(),
            "a thin pool of 536870912b with 65536b chunks"
        );
        let opts = LvmPoolOpts::try_from_args(&args(Some(1 << 30), None)).unwrap();
        assert_eq!(opts.chunk(), None);
        // lvm options without a size ask for no thin pool either
        let mut empty = args(None, None);
        empty.lvm = Some(LvmPoolArgs::default());
        assert_eq!(
            LvmPoolOpts::try_from_args(&empty).unwrap(),
            LvmPoolOpts::default()
        );
    }

    #[test]
    fn thinpool_limits() {
        let parse = |size, chunk| LvmPoolOpts::try_from_args(&args(Some(size), chunk));
        assert!(parse(0, None).is_err());
        assert!(parse(1 << 30, Some(64 << 10)).is_ok());
        assert!(parse(1 << 30, Some(1 << 30)).is_ok());
        assert!(parse(1 << 30, Some(32 << 10)).is_err());
        assert!(parse(1 << 30, Some(96 << 10)).is_err());
        assert!(parse(1 << 30, Some(u32::MAX)).is_err());
    }
}
