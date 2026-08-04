//! Pool-level options for the LVM backend.
//!
//! A create request asks for a thin pool with `lvm.thin_pool`, in bytes or as
//! a percentage of the volume group, and its `cluster_size` is the thin pool's
//! chunk size. Neither is stored: the thin pool carries its own size and chunk
//! size.

use super::Error;
use crate::pool_backend::{PoolArgs, ThinPoolSize};

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
    thinpool: Option<ThinPoolSize>,
    /// Chunk size for the thin pool.
    chunk: Option<u64>,
}

impl LvmPoolOpts {
    /// Take the thin pool options from the pool arguments. The cluster size
    /// only counts with a thin pool, since the control plane always sends one.
    pub(crate) fn try_from_args(args: &PoolArgs) -> Result<Self, Error> {
        let thinpool = args.lvm.as_ref().and_then(|lvm| lvm.thin_pool);
        let opts = Self {
            thinpool,
            chunk: thinpool.and(args.cluster_size.map(u64::from)),
        };
        opts.validate()?;
        Ok(opts)
    }

    fn validate(&self) -> Result<(), Error> {
        match self.thinpool {
            Some(ThinPoolSize::Bytes(0)) => {
                return Err(Error::InvalidOption {
                    error: "the thin pool size must be larger than 0".to_string(),
                });
            }
            // lvcreate takes whole percentages, and fails past 100.
            Some(ThinPoolSize::FreePercent(percent) | ThinPoolSize::VgPercent(percent))
                if !(1..=100).contains(&percent) =>
            {
                return Err(Error::InvalidOption {
                    error: format!("the thin pool percentage must be 1 to 100, not {percent}"),
                });
            }
            _ => {}
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
    pub(super) fn thinpool(&self) -> Option<ThinPoolSize> {
        self.thinpool
    }
    /// The thin pool chunk size.
    pub(super) fn chunk(&self) -> Option<u64> {
        self.chunk
    }
    /// The lvcreate argument for the thin pool size, if a thin pool was
    /// requested.
    pub(super) fn size_arg(&self) -> Option<String> {
        self.thinpool.map(|size| match size {
            ThinPoolSize::Bytes(bytes) => format!("-L{bytes}b"),
            ThinPoolSize::FreePercent(percent) => format!("-l{percent}%FREE"),
            ThinPoolSize::VgPercent(percent) => format!("-l{percent}%VG"),
        })
    }
}

impl std::fmt::Display for LvmPoolOpts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.thinpool {
            None => return write!(f, "no thin pool"),
            Some(ThinPoolSize::Bytes(bytes)) => write!(f, "a thin pool of {bytes}b")?,
            Some(ThinPoolSize::FreePercent(percent)) => {
                write!(f, "a thin pool of {percent}% of the free space")?
            }
            Some(ThinPoolSize::VgPercent(percent)) => {
                write!(f, "a thin pool of {percent}% of the volume group")?
            }
        }
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

    fn args(thin_pool: Option<ThinPoolSize>, cluster_size: Option<u32>) -> PoolArgs {
        PoolArgs {
            disks: vec!["/dev/sda".to_string()],
            cluster_size,
            lvm: thin_pool.map(|size| LvmPoolArgs {
                thin_pool: Some(size),
            }),
            ..Default::default()
        }
    }

    #[test]
    fn pool_opts_from_args() {
        let opts = LvmPoolOpts::try_from_args(&args(None, None)).unwrap();
        assert_eq!(opts, LvmPoolOpts::default());
        assert_eq!(opts.size_arg(), None);
        // a cluster size alone asks for no thin pool
        let opts = LvmPoolOpts::try_from_args(&args(None, Some(4 << 20))).unwrap();
        assert_eq!(opts, LvmPoolOpts::default());
        let opts =
            LvmPoolOpts::try_from_args(&args(Some(ThinPoolSize::Bytes(512 << 20)), Some(64 << 10)))
                .unwrap();
        assert_eq!(opts.thinpool(), Some(ThinPoolSize::Bytes(512 << 20)));
        assert_eq!(opts.chunk(), Some(64 << 10));
        assert_eq!(opts.size_arg().as_deref(), Some("-L536870912b"));
        assert_eq!(
            opts.to_string(),
            "a thin pool of 536870912b with 65536b chunks"
        );
        let opts =
            LvmPoolOpts::try_from_args(&args(Some(ThinPoolSize::Bytes(1 << 30)), None)).unwrap();
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
    fn pool_opts_percent() {
        let opts = LvmPoolOpts::try_from_args(&args(
            Some(ThinPoolSize::FreePercent(100)),
            Some(128 << 10),
        ))
        .unwrap();
        assert_eq!(opts.size_arg().as_deref(), Some("-l100%FREE"));
        assert_eq!(opts.chunk(), Some(128 << 10));
        assert_eq!(
            opts.to_string(),
            "a thin pool of 100% of the free space with 131072b chunks"
        );
        let opts =
            LvmPoolOpts::try_from_args(&args(Some(ThinPoolSize::VgPercent(50)), None)).unwrap();
        assert_eq!(opts.size_arg().as_deref(), Some("-l50%VG"));
        assert_eq!(opts.to_string(), "a thin pool of 50% of the volume group");
    }

    #[test]
    fn thinpool_limits() {
        let parse = |size, chunk| LvmPoolOpts::try_from_args(&args(Some(size), chunk));
        assert!(parse(ThinPoolSize::Bytes(0), None).is_err());
        assert!(parse(ThinPoolSize::Bytes(1 << 30), Some(64 << 10)).is_ok());
        assert!(parse(ThinPoolSize::Bytes(1 << 30), Some(1 << 30)).is_ok());
        assert!(parse(ThinPoolSize::Bytes(1 << 30), Some(32 << 10)).is_err());
        assert!(parse(ThinPoolSize::Bytes(1 << 30), Some(96 << 10)).is_err());
        assert!(parse(ThinPoolSize::Bytes(1 << 30), Some(u32::MAX)).is_err());
        for percent in [1, 100] {
            assert!(parse(ThinPoolSize::FreePercent(percent), None).is_ok());
            assert!(parse(ThinPoolSize::VgPercent(percent), None).is_ok());
        }
        for percent in [0, 101, u32::MAX] {
            assert!(parse(ThinPoolSize::FreePercent(percent), None).is_err());
            assert!(parse(ThinPoolSize::VgPercent(percent), None).is_err());
        }
        assert!(parse(ThinPoolSize::FreePercent(100), Some(32 << 10)).is_err());
    }
}
