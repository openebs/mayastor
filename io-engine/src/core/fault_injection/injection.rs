#![cfg(feature = "fault-injection")]

use rand::{rngs::StdRng, RngCore, SeedableRng};
use std::{
    fmt::{Debug, Display, Formatter},
    ops::Range,
    time::{Duration, Instant},
};

use url::Url;

use crate::core::IoCompletionStatus;

use super::{
    FaultDomain,
    FaultInjectionError,
    FaultIoStage,
    FaultIoType,
    FaultType,
    InjectIoCtx,
};

/// Fault injection.
#[derive(Debug, Clone)]
pub struct FaultInjection {
    pub uri: String,
    pub domain: FaultDomain,
    pub device_name: String,
    pub fault_io_type: FaultIoType,
    pub fault_io_stage: FaultIoStage,
    pub fault_type: FaultType,
    pub started: Option<Instant>,
    pub begin: Duration,
    pub end: Duration,
    pub range: Range<u64>,
    rng: StdRng,
}

impl Display for FaultInjection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        fn fmt_duration(u: &Duration) -> String {
            if *u == Duration::MAX {
                "INF".to_string()
            } else {
                format!("{u:?}")
            }
        }

        fn fmt_u64(u: u64) -> String {
            if u == u64::MAX {
                "INF".to_string()
            } else {
                format!("{u:?}")
            }
        }

        write!(
            f,
            "{io}::{stage}::{ft} injection <{d}::{n}> [{b:?} -> \
            {e} ({t:?})] @ {rs}..{re}",
            io = self.fault_io_type,
            stage = self.fault_io_stage,
            ft = self.fault_type,
            d = self.domain,
            n = self.device_name,
            b = self.begin,
            e = fmt_duration(&self.end),
            t = self.now(),
            rs = self.range.start,
            re = fmt_u64(self.range.end),
        )
    }
}

fn new_rng() -> StdRng {
    let seed = [
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,
    ];
    StdRng::from_seed(seed)
}

impl FaultInjection {
    /// Creates a new injection.
    #[allow(dead_code)]
    pub fn new(
        domain: FaultDomain,
        name: &str,
        fault_io_type: FaultIoType,
        fault_io_stage: FaultIoStage,
        fault_type: FaultType,
        begin: Duration,
        end: Duration,
        range: Range<u64>,
    ) -> Self {
        let opts = vec![
            format!("domain={domain}"),
            format!("op={fault_io_type}"),
            format!("stage={fault_io_stage}"),
            format!("type={fault_type}"),
            format!("begin={begin:?}"),
            format!("end={end:?}"),
            format!("offset={}", range.start),
            format!("num_blk={}", range.end),
        ]
        .join("&");

        let uri = format!("inject://{name}?{opts}");

        Self {
            uri,
            domain,
            device_name: name.to_owned(),
            fault_io_type,
            fault_io_stage,
            fault_type,
            started: None,
            begin,
            end,
            range,
            rng: new_rng(),
        }
    }

    /// Parses an injection URI and creates injection object.
    pub fn from_uri(uri: &str) -> Result<Self, FaultInjectionError> {
        if !uri.starts_with("inject://") {
            return Err(FaultInjectionError::NotInjectionUri {
                uri: uri.to_owned(),
            });
        }

        let p =
            Url::parse(uri).map_err(|e| FaultInjectionError::InvalidUri {
                source: e,
                uri: uri.to_owned(),
            })?;

        let mut r = Self {
            uri: uri.to_owned(),
            domain: FaultDomain::None,
            device_name: format!(
                "{host}{port}{path}",
                host = p.host_str().unwrap_or_default(),
                port = if let Some(port) = p.port() {
                    format!(":{port}")
                } else {
                    "".to_string()
                },
                path = p.path()
            ),
            fault_io_type: FaultIoType::Read,
            fault_io_stage: FaultIoStage::Completion,
            fault_type: FaultType::status_data_transfer_error(),
            started: None,
            begin: Duration::ZERO,
            end: Duration::MAX,
            range: 0 .. u64::MAX,
            rng: new_rng(),
        };

        for (k, v) in p.query_pairs() {
            match k.as_ref() {
                "domain" => r.domain = parse_domain(&k, &v)?,
                "op" => r.fault_io_type = parse_fault_io_type(&k, &v)?,
                "stage" => r.fault_io_stage = parse_fault_io_stage(&k, &v)?,
                "type" => r.fault_type = parse_fault_type(&k, &v)?,
                "begin" => r.begin = parse_timer(&k, &v)?,
                "end" => r.end = parse_timer(&k, &v)?,
                "offset" => r.range.start = parse_num(&k, &v)?,
                "num_blk" => r.range.end = parse_num(&k, &v)?,
                _ => {
                    return Err(FaultInjectionError::UnknownParameter {
                        name: k.to_string(),
                        value: v.to_string(),
                    })
                }
            };
        }

        r.range.end = r.range.start.saturating_add(r.range.end);

        if r.begin > r.end {
            return Err(FaultInjectionError::BadDurations {
                name: r.device_name,
                begin: r.begin,
                end: r.end,
            });
        }

        Ok(r)
    }

    /// Returns current time relative to injection start.
    fn now(&self) -> Duration {
        self.started.map_or(Duration::MAX, |s| {
            Instant::now().saturating_duration_since(s)
        })
    }

    /// True if the injection is currently active.
    pub fn is_active(&self) -> bool {
        let d = self.now();
        d >= self.begin && d < self.end
    }

    /// Injects an error for the given I/O context.
    /// If this injected fault does not apply to this context, returns `None`.
    /// Otherwise, returns an operation status to be returned by the calling I/O
    /// routine.
    #[inline]
    pub fn inject(
        &mut self,
        domain: FaultDomain,
        fault_io_type: FaultIoType,
        fault_io_stage: FaultIoStage,
        ctx: &InjectIoCtx,
    ) -> Option<IoCompletionStatus> {
        if domain != self.domain
            || fault_io_type != self.fault_io_type
            || fault_io_stage != self.fault_io_stage
            || ctx.device_name() != self.device_name
        {
            return None;
        }

        if self.started.is_none() {
            debug!("{self:?}: starting");
            self.started = Some(Instant::now());
        }

        if !self.is_active() || !is_overlapping(&self.range, &ctx.range) {
            return None;
        }

        match self.fault_type {
            FaultType::Status(status) => Some(status),
            FaultType::Data => {
                self.inject_data_errors(ctx);
                Some(IoCompletionStatus::Success)
            }
        }
    }

    fn inject_data_errors(&mut self, ctx: &InjectIoCtx) {
        let Some(iovs) = ctx.iovs_mut() else {
            return;
        };

        for iov in iovs {
            for i in 0 .. iov.len() {
                iov[i] = self.rng.next_u32() as u8;
            }
        }
    }
}

/// TODO
fn parse_domain(k: &str, v: &str) -> Result<FaultDomain, FaultInjectionError> {
    let r = match v {
        "none" => FaultDomain::None,
        "nexus" => FaultDomain::Nexus,
        "block" | "block_device" => FaultDomain::BlockDevice,
        _ => {
            return Err(FaultInjectionError::UnknownParameter {
                name: k.to_string(),
                value: v.to_string(),
            })
        }
    };
    Ok(r)
}

/// TODO
fn parse_fault_io_type(
    k: &str,
    v: &str,
) -> Result<FaultIoType, FaultInjectionError> {
    let res = match v {
        "read" | "r" => FaultIoType::Read,
        "write" | "w" => FaultIoType::Write,
        _ => {
            return Err(FaultInjectionError::UnknownParameter {
                name: k.to_string(),
                value: v.to_string(),
            })
        }
    };
    Ok(res)
}

/// TODO
fn parse_fault_io_stage(
    k: &str,
    v: &str,
) -> Result<FaultIoStage, FaultInjectionError> {
    let res = match v {
        "submit" | "s" | "submission" => FaultIoStage::Submission,
        "compl" | "c" | "completion" => FaultIoStage::Submission,
        _ => {
            return Err(FaultInjectionError::UnknownParameter {
                name: k.to_string(),
                value: v.to_string(),
            })
        }
    };
    Ok(res)
}

/// TODO
fn parse_fault_type(
    k: &str,
    v: &str,
) -> Result<FaultType, FaultInjectionError> {
    let res = match v {
        // TODO: add more statuses.
        "status" => FaultType::status_data_transfer_error(),
        // TODO: add data corruption methods.
        "data" => FaultType::Data,
        _ => {
            return Err(FaultInjectionError::UnknownParameter {
                name: k.to_string(),
                value: v.to_string(),
            })
        }
    };
    Ok(res)
}

/// TODO
fn parse_timer(k: &str, v: &str) -> Result<Duration, FaultInjectionError> {
    let b = v.parse::<u64>().map_err(|_| {
        FaultInjectionError::BadParameterValue {
            name: k.to_string(),
            value: v.to_string(),
        }
    })?;

    Ok(Duration::from_millis(b))
}

/// TODO
fn parse_num(k: &str, v: &str) -> Result<u64, FaultInjectionError> {
    v.parse::<u64>()
        .map_err(|_| FaultInjectionError::BadParameterValue {
            name: k.to_string(),
            value: v.to_string(),
        })
}

/// Tests if teo ranges overlap.
fn is_overlapping(a: &Range<u64>, b: &Range<u64>) -> bool {
    a.end > b.start && b.end > a.start
}
