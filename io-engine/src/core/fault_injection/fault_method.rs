use rand::RngCore;
use regex::Regex;
use std::fmt::{Debug, Display, Formatter};

use spdk_rs::NvmeStatus;

use crate::core::{IoCompletionStatus, IoSubmissionFailure, LvolFailure};

use super::{InjectIoCtx, InjectionState};

/// Injection method.
#[derive(Clone, Copy, PartialEq)]
pub enum FaultMethod {
    /// Faults I/O by returning the given status for an affected operation.
    Status(IoCompletionStatus),
    /// Introduces data buffer corruption.
    Data,
}

impl Debug for FaultMethod {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Status(s) => {
                write!(f, "Status[{s:?}]")
            }
            Self::Data => f.write_str("Data"),
        }
    }
}

impl Display for FaultMethod {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use IoCompletionStatus::*;

        match self {
            Self::Status(NvmeError(NvmeStatus::DATA_TRANSFER_ERROR)) => {
                f.write_str("status")
            }
            Self::Status(NvmeError(s)) => {
                let (sct, sc) = s.as_sct_sc_codes();
                write!(f, "status-nvme-{sct:x}-{sc:x}",)
            }
            Self::Status(LvolError(s)) => {
                write!(
                    f,
                    "status-lvol-{s}",
                    s = format!("{s:?}").to_ascii_lowercase()
                )
            }
            Self::Status(IoSubmissionError(s)) => {
                write!(
                    f,
                    "status-submit-{s}",
                    s = format!("{s:?}").to_ascii_lowercase()
                )
            }
            Self::Status(AdminCommandError) => {
                write!(f, "status-admin")
            }
            Self::Data => f.write_str("data"),
            _ => f.write_str("invalid"),
        }
    }
}

impl FaultMethod {
    /// A shorthand for a generic data transfer error.
    pub const DATA_TRANSFER_ERROR: Self = Self::Status(
        IoCompletionStatus::NvmeError(NvmeStatus::DATA_TRANSFER_ERROR),
    );

    /// TODO
    pub(super) fn inject(
        &self,
        state: &mut InjectionState,
        ctx: &InjectIoCtx,
    ) -> Option<IoCompletionStatus> {
        match self {
            FaultMethod::Status(status) => Some(*status),
            FaultMethod::Data => {
                self.inject_data_errors(state, ctx);
                Some(IoCompletionStatus::Success)
            }
        }
    }

    /// TODO
    fn inject_data_errors(&self, s: &mut InjectionState, ctx: &InjectIoCtx) {
        let Some(iovs) = ctx.iovs_mut() else {
            return;
        };

        for iov in iovs {
            for i in 0 .. iov.len() {
                iov[i] = s.rng.next_u32() as u8;
            }
        }
    }

    /// TODO
    pub fn parse(s: &str) -> Option<Self> {
        lazy_static::lazy_static! {
            static ref NVME_RE: Regex =
                Regex::new(r"^status-nvme-([0-9a-f.]+)-([0-9a-f.]+)$").unwrap();
        }

        if let Some(cap) = NVME_RE.captures(s) {
            let sct = i32::from_str_radix(cap.get(1).unwrap().as_str(), 16);
            let sc = i32::from_str_radix(cap.get(2).unwrap().as_str(), 16);

            if let Ok(sct) = sct {
                if let Ok(sc) = sc {
                    return Some(Self::Status(IoCompletionStatus::NvmeError(
                        NvmeStatus::from((sct, sc)),
                    )));
                }
            }
        }

        let r = match s {
            "status-lvol-nospace" => {
                IoCompletionStatus::LvolError(LvolFailure::NoSpace)
            }
            "status-submit-read" => {
                IoCompletionStatus::IoSubmissionError(IoSubmissionFailure::Read)
            }
            "status-submit-write" => IoCompletionStatus::IoSubmissionError(
                IoSubmissionFailure::Write,
            ),
            "status-admin" => IoCompletionStatus::AdminCommandError,
            _ => return None,
        };
        Some(Self::Status(r))
    }
}
