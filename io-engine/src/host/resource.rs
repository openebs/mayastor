//!
//! This module implements the get_resource_usage() gRPC method,
//! which retrieves information via the getrusage(2) system call.

use std::{io::Error, mem::MaybeUninit, os::raw::c_int};

fn getrusage(who: c_int) -> Result<libc::rusage, Error> {
    let mut data: MaybeUninit<libc::rusage> = MaybeUninit::uninit();

    if unsafe { libc::getrusage(who, data.as_mut_ptr()) } < 0 {
        return Err(Error::last_os_error());
    }

    Ok(unsafe { data.assume_init() })
}

pub struct Usage(pub libc::rusage);

/// Obtain resource usage statistics for the current process.
pub async fn get_resource_usage() -> Result<Usage, Error> {
    let rusage = getrusage(libc::RUSAGE_SELF)?;
    Ok(Usage(rusage))
}
