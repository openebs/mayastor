use libc::c_int;
use std::io::{Error, Result};

const PR_SET_IO_FLUSHER: c_int = 57;

pub struct Prctl;

impl Prctl {
    /// From man page of prctl: If a user process is involved in the block layer
    /// or filesystem I/O path, and can allocate memory while processing I/O requests
    /// it must set arg2 to 1. This will put the process in the IO_FLUSHER  state,
    /// which allows it special treatment to make progress when allocating memory.
    /// If arg2 is 0, the process will clear the IO_FLUSHER state, and the default
    /// behavior will be used.
    pub fn set_io_flusher() -> Result<()> {
        let ret = unsafe { libc::prctl(PR_SET_IO_FLUSHER, 1, 0, 0, 0) };
        if ret != 0 {
            return Err(Error::last_os_error());
        }
        Ok(())
    }
}
