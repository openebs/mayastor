use libc::{rlimit, setrlimit, RLIMIT_CORE};

// Same limit as the one spdk app uses.
pub const DEFAULT_CORE_LIMIT: libc::rlim_t = 0x140000000; /* 5 GiB */

/// Enable coredumps by setting limit to high value.
pub fn enable(limit: libc::rlim_t) -> Result<(), nix::Error> {
    let lim = rlimit {
        rlim_cur: limit,
        rlim_max: limit,
    };

    let rc = unsafe { setrlimit(RLIMIT_CORE, &lim) };
    nix::Error::result(rc).map(drop)
}
