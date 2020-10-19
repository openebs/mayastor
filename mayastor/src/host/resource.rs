//!
//! This module implements the get_resource_usage() gRPC method,
//! which retrieves information via the getrusage(2) system call.

use ::rpc::mayastor::ResourceUsage;
use std::{io::Error, mem::MaybeUninit, os::raw::c_int};

fn getrusage(who: c_int) -> Result<libc::rusage, Error> {
    let mut data: MaybeUninit<libc::rusage> = MaybeUninit::uninit();

    if unsafe { libc::getrusage(who, data.as_mut_ptr()) } < 0 {
        return Err(Error::last_os_error());
    }

    Ok(unsafe { data.assume_init() })
}

struct Usage<'a>(&'a libc::rusage);

impl From<Usage<'_>> for ResourceUsage {
    fn from(usage: Usage) -> ResourceUsage {
        let rusage = usage.0;
        ResourceUsage {
            soft_faults: rusage.ru_minflt,
            hard_faults: rusage.ru_majflt,
            swaps: rusage.ru_nswap,
            in_block_ops: rusage.ru_inblock,
            out_block_ops: rusage.ru_oublock,
            ipc_msg_send: rusage.ru_msgsnd,
            ipc_msg_rcv: rusage.ru_msgrcv,
            signals: rusage.ru_nsignals,
            vol_csw: rusage.ru_nvcsw,
            invol_csw: rusage.ru_nivcsw,
        }
    }
}

/// Obtain resource usage statistics for the current process.
pub async fn get_resource_usage() -> Result<ResourceUsage, Error> {
    let rusage = getrusage(libc::RUSAGE_SELF)?;
    Ok(Usage(&rusage).into())
}
