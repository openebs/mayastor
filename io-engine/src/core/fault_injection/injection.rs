#![cfg(feature = "fault-injection")]

use spdk_rs::NvmeStatus;
use std::{
    cell::RefCell,
    fmt::{Debug, Formatter},
    ops::Range,
    time::Duration,
};
use url::Url;

use crate::core::IoCompletionStatus;

use super::{
    FaultDomain,
    FaultInjectionError,
    FaultIoOperation,
    FaultIoStage,
    FaultMethod,
    InjectIoCtx,
    InjectionState,
};

/// Fault injection.
#[derive(Clone, Builder)]
#[builder(setter(prefix = "with"))]
#[builder(default)]
#[builder(build_fn(validate = "Self::validate"))]
pub struct Injection {
    /// URI this injection was created from.
    #[builder(setter(skip))]
    uri: Option<String>,
    /// Fault domain.
    pub domain: FaultDomain,
    /// Target device name.
    pub device_name: String,
    /// I/O operation to which the fault applies.
    pub io_operation: FaultIoOperation,
    /// I/O stage.
    pub io_stage: FaultIoStage,
    /// Injection method.
    pub method: FaultMethod,
    /// Time time.
    pub time_range: Range<Duration>,
    /// Block range.
    pub block_range: Range<u64>,
    /// Number of retries.
    pub retries: u64,
    /// Injection state.
    #[builder(setter(skip))]
    state: RefCell<InjectionState>,
}

impl InjectionBuilder {
    /// TODO
    pub fn with_offset(&mut self, offset: u64, num_blocks: u64) -> &mut Self {
        self.block_range = Some(offset .. offset + num_blocks);
        self
    }

    /// TODO
    pub fn with_method_nvme_error(&mut self, err: NvmeStatus) -> &mut Self {
        self.method =
            Some(FaultMethod::Status(IoCompletionStatus::NvmeError(err)));
        self
    }

    /// TODO
    pub fn build_uri(&mut self) -> Result<String, InjectionBuilderError> {
        self.build().map(|inj| inj.as_uri())
    }

    /// TODO
    fn validate(&self) -> Result<(), String> {
        match &self.device_name {
            Some(s) if !s.is_empty() => Ok(()),
            _ => Err("Device not configured".to_string()),
        }
    }
}

impl Debug for Injection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        fn fmt_duration(u: &Duration) -> String {
            if *u == Duration::MAX {
                "-".to_string()
            } else {
                format!("{u:?}")
            }
        }

        fn fmt_u64(u: u64) -> String {
            if u == u64::MAX {
                "MAX".to_string()
            } else {
                format!("{u:?}")
            }
        }

        if f.alternate() {
            f.debug_struct("Injection")
                .field("uri", &self.uri())
                .field("domain", &self.domain)
                .field("device_name", &self.device_name)
                .field("io_operation", &self.io_operation)
                .field("stage", &self.io_stage)
                .field("method", &self.method)
                .field("begin_at", &fmt_duration(&self.time_range.start))
                .field("end_at", &fmt_duration(&self.time_range.end))
                .field("block_range_start", &fmt_u64(self.block_range.start))
                .field("block_range_end", &fmt_u64(self.block_range.end))
                .field(
                    "num_blocks",
                    &fmt_u64(self.block_range.end - self.block_range.start),
                )
                .field("retries", &fmt_u64(self.retries))
                .field("hits", &self.state.borrow().hits)
                .field("started", &fmt_duration(&self.state.borrow().now()))
                .finish()
        } else {
            let info = format!(
                "{d}/{io}/{stage}/{ft}",
                d = self.domain,
                io = self.io_operation,
                stage = self.io_stage,
                ft = self.method,
            );

            let timed = if !self.time_range.start.is_zero()
                || self.time_range.end != Duration::MAX
            {
                format!(
                    " for period {b} -> {e} ({t})",
                    b = fmt_duration(&self.time_range.start),
                    e = fmt_duration(&self.time_range.end),
                    t = fmt_duration(&self.state.borrow().now()),
                )
            } else {
                String::default()
            };

            let range = if self.block_range.start != 0
                || self.block_range.end != u64::MAX
            {
                format!(
                    " at blocks {rs}..{re}",
                    rs = self.block_range.start,
                    re = fmt_u64(self.block_range.end),
                )
            } else {
                String::default()
            };

            let retries = if self.retries != u64::MAX {
                format!(
                    " | {h}/{n} retries",
                    h = self.state.borrow().hits,
                    n = self.retries
                )
            } else {
                "".to_string()
            };

            write!(
                f,
                "{info} on '{n}'{timed}{range}{retries}",
                n = self.device_name,
            )
        }
    }
}

impl Default for Injection {
    fn default() -> Self {
        Self {
            uri: None,
            domain: FaultDomain::BlockDevice,
            device_name: Default::default(),
            io_operation: FaultIoOperation::ReadWrite,
            io_stage: FaultIoStage::Submission,
            method: FaultMethod::DATA_TRANSFER_ERROR,
            time_range: Duration::ZERO .. Duration::MAX,
            block_range: 0 .. u64::MAX,
            retries: u64::MAX,
            state: Default::default(),
        }
    }
}

impl Injection {
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
            uri: Some(uri.to_owned()),
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
            ..Default::default()
        };

        for (k, v) in p.query_pairs() {
            match k.as_ref() {
                "domain" => r.domain = parse_domain(&k, &v)?,
                "op" => r.io_operation = parse_fault_io_type(&k, &v)?,
                "stage" => r.io_stage = parse_fault_io_stage(&k, &v)?,
                "method" => r.method = parse_method(&k, &v)?,
                "begin_at" => r.time_range.start = parse_timer(&k, &v)?,
                "end_at" => r.time_range.end = parse_timer(&k, &v)?,
                "offset" => r.block_range.start = parse_num(&k, &v)?,
                "num_blk" | "num_blocks" => {
                    r.block_range.end = parse_num(&k, &v)?
                }
                "retries" => r.retries = parse_num(&k, &v)?,
                _ => {
                    return Err(FaultInjectionError::UnknownParameter {
                        name: k.to_string(),
                        value: v.to_string(),
                    })
                }
            };
        }

        r.block_range.end =
            r.block_range.start.saturating_add(r.block_range.end);

        if r.time_range.start > r.time_range.end {
            return Err(FaultInjectionError::BadDurations {
                name: r.device_name,
                begin: r.time_range.start,
                end: r.time_range.end,
            });
        }

        Ok(r)
    }

    /// Returns injection's URI.
    pub fn uri(&self) -> String {
        match &self.uri {
            Some(s) => s.to_owned(),
            None => self.as_uri(),
        }
    }

    /// Builds URI for the injection.
    pub fn as_uri(&self) -> String {
        let d = Self::default();

        let mut opts = vec![
            format!("domain={}", self.domain),
            format!("op={}", self.io_operation),
            format!("stage={}", self.io_stage),
        ];

        if self.method != d.method {
            opts.push(format!("method={}", self.method));
        }

        if self.time_range.start != d.time_range.start {
            opts.push(format!(
                "begin_at={:?}",
                self.time_range.start.as_millis()
            ));
        }

        if self.time_range.end != d.time_range.end {
            opts.push(format!("end_at={}", self.time_range.end.as_millis()));
        }

        if self.block_range.start != d.block_range.start {
            opts.push(format!("offset={}", self.block_range.start));
        }

        if self.block_range.end != d.block_range.end {
            opts.push(format!(
                "num_blk={}",
                self.block_range.end - self.block_range.start
            ));
        }

        if self.retries != d.retries {
            opts.push(format!("retries={}", self.retries));
        }

        format!(
            "inject://{name}?{opts}",
            name = self.device_name,
            opts = opts.join("&")
        )
    }

    /// Returns device name.
    pub fn device_name(&self) -> &str {
        &self.device_name
    }

    /// True if the injection is currently active.
    #[inline(always)]
    pub fn is_active(&self) -> bool {
        let s = self.state.borrow();

        if s.hits >= self.retries {
            return false;
        }

        let d = s.now();
        d >= self.time_range.start && d < self.time_range.end
    }

    /// Injects an error for the given I/O context.
    /// If this injected fault does not apply to this context, returns `None`.
    /// Otherwise, returns an operation status to be returned by the calling I/O
    /// routine.
    #[inline]
    pub fn inject(
        &self,
        stage: FaultIoStage,
        ctx: &InjectIoCtx,
    ) -> Option<IoCompletionStatus> {
        if !ctx.is_valid()
            || !ctx.domain_ok(self.domain)
            || stage != self.io_stage
            || !ctx.io_type_ok(self.io_operation)
            || !ctx.device_name_ok(&self.device_name)
            || !ctx.block_range_ok(&self.block_range)
        {
            return None;
        }
        if self.state.borrow_mut().tick() {
            debug!("{self:?}: starting");
        }

        if !self.is_active() {
            return None;
        }

        self.method.inject(&mut self.state.borrow_mut(), ctx)
    }
}

/// TODO
fn parse_domain(k: &str, v: &str) -> Result<FaultDomain, FaultInjectionError> {
    let r = match v {
        "child" | "nexus_child" | "NexusChild" => FaultDomain::NexusChild,
        "block" | "block_device" | "BlockDevice" => FaultDomain::BlockDevice,
        "bdev_io" | "BdevIo" => FaultDomain::BdevIo,
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
) -> Result<FaultIoOperation, FaultInjectionError> {
    let res = match v {
        "read" | "r" | "Read" => FaultIoOperation::Read,
        "write" | "w" | "Write" => FaultIoOperation::Write,
        "read_write" | "rw" | "ReadWrite" => FaultIoOperation::ReadWrite,
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
        "submit" | "s" | "submission" | "Submission" => {
            FaultIoStage::Submission
        }
        "compl" | "c" | "completion" | "Completion" => FaultIoStage::Completion,
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
fn parse_method(k: &str, v: &str) -> Result<FaultMethod, FaultInjectionError> {
    match v {
        "status" | "Status" => Ok(FaultMethod::DATA_TRANSFER_ERROR),
        // TODO: add data corruption methods.
        "data" | "Data" => Ok(FaultMethod::Data),
        _ => FaultMethod::parse(v).ok_or_else(|| {
            FaultInjectionError::UnknownParameter {
                name: k.to_string(),
                value: v.to_string(),
            }
        }),
    }
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
