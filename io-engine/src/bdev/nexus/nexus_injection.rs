use snafu::Snafu;
use std::time::Duration;
use url::ParseError;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)), context(suffix(false)))]
pub enum InjectionError {
    #[snafu(display("Injections are disabled"))]
    InjectionsDisabled {},
    #[snafu(display("URI is not an injection: '{}'", uri))]
    NotInjectionUri { uri: String },
    #[snafu(display("Invalid injection URI: '{}'", uri))]
    InvalidUri { source: ParseError, uri: String },
    #[snafu(display("Unknown injection parameter: '{}={}'", name, value))]
    UnknownParameter { name: String, value: String },
    #[snafu(display("Bad injection parameter value: '{}={}'", name, value))]
    BadParameterValue { name: String, value: String },
    #[snafu(display(
        "Bad injection '{}' timer durations: {:?}, {:?}",
        name,
        begin,
        end
    ))]
    BadDurations {
        name: String,
        begin: Duration,
        end: Duration,
    },
}

/// Information about a injected fault.
pub struct InjectionInfo {
    pub device_name: String,
    pub is_active: bool,
}

pub use inj_impl::{injections_enabled, Injections};

/// Operation type.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum InjectionOp {
    Read,
    ReadSubmission,
    Write,
    WriteSubmission,
}

#[cfg(feature = "nexus-fault-injection")]
mod inj_impl {
    use std::{
        fmt::{Debug, Formatter},
        ops::Range,
        pin::Pin,
        sync::atomic::{AtomicBool, Ordering},
        time::{Duration, Instant},
    };

    use snafu::ResultExt;
    use url::Url;

    use crate::core::{BlockDevice, VerboseError};

    use super::{
        super::{nexus_err, Error, Nexus},
        InjectionError,
        InjectionInfo,
        InjectionOp,
    };

    static INJECTIONS_ENABLED: AtomicBool = AtomicBool::new(false);

    /// Checks if injections are globally enabled.
    /// This method is fast and can used in I/O code path to quick check
    /// before checking if an injection has to be applied to a particular
    /// device.
    #[inline]
    pub fn injections_enabled() -> bool {
        INJECTIONS_ENABLED.load(Ordering::SeqCst)
    }

    /// TODO
    fn parse_op(k: &str, v: &str) -> Result<InjectionOp, InjectionError> {
        let op = match v {
            "read" => InjectionOp::Read,
            "sread" => InjectionOp::ReadSubmission,
            "write" => InjectionOp::Write,
            "swrite" => InjectionOp::WriteSubmission,
            _ => {
                return Err(InjectionError::UnknownParameter {
                    name: k.to_string(),
                    value: v.to_string(),
                })
            }
        };
        Ok(op)
    }

    /// TODO
    fn parse_timer(k: &str, v: &str) -> Result<Duration, InjectionError> {
        let b = v.parse::<u64>().map_err(|_| {
            InjectionError::BadParameterValue {
                name: k.to_string(),
                value: v.to_string(),
            }
        })?;

        Ok(Duration::from_millis(b))
    }

    /// TODO
    fn parse_num(k: &str, v: &str) -> Result<u64, InjectionError> {
        v.parse::<u64>()
            .map_err(|_| InjectionError::BadParameterValue {
                name: k.to_string(),
                value: v.to_string(),
            })
    }

    /// Tests if teo ranges overlap.
    fn is_overlapping(a: &Range<u64>, b: &Range<u64>) -> bool {
        a.end > b.start && b.end > a.start
    }

    /// Injected failures.
    struct Injection {
        name: String,
        op: InjectionOp,
        started: Option<Instant>,
        begin: Duration,
        end: Duration,
        range: Range<u64>,
    }

    impl Debug for Injection {
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
                "{op:?} Injection '{name}' [{b:?} -> {e} ({t:?})] @ {rs}..{re}",
                op = self.op,
                name = self.name,
                b = self.begin,
                e = fmt_duration(&self.end),
                t = self.now(),
                rs = self.range.start,
                re = fmt_u64(self.range.end),
            )
        }
    }

    impl Injection {
        /// Parses an injection URI and creates injection object.
        fn from_uri(uri: &str) -> Result<Self, InjectionError> {
            if !uri.starts_with("inject://") {
                return Err(InjectionError::NotInjectionUri {
                    uri: uri.to_owned(),
                });
            }

            let p =
                Url::parse(uri).map_err(|e| InjectionError::InvalidUri {
                    source: e,
                    uri: uri.to_owned(),
                })?;

            let mut r = Self {
                name: format!(
                    "{host}{port}{path}",
                    host = p.host_str().unwrap_or_default(),
                    port = if let Some(port) = p.port() {
                        format!(":{port}")
                    } else {
                        "".to_string()
                    },
                    path = p.path()
                ),
                op: InjectionOp::Read,
                started: None,
                begin: Duration::ZERO,
                end: Duration::MAX,
                range: 0 .. u64::MAX,
            };

            for (k, v) in p.query_pairs() {
                match k.as_ref() {
                    "op" => r.op = parse_op(&k, &v)?,
                    "begin" => r.begin = parse_timer(&k, &v)?,
                    "end" => r.end = parse_timer(&k, &v)?,
                    "offset" => r.range.start = parse_num(&k, &v)?,
                    "num_blk" => r.range.end = parse_num(&k, &v)?,
                    _ => {
                        return Err(InjectionError::UnknownParameter {
                            name: k.to_string(),
                            value: v.to_string(),
                        })
                    }
                };
            }

            r.range.end = r.range.start.saturating_add(r.range.end);

            if r.begin > r.end {
                return Err(InjectionError::BadDurations {
                    name: r.name,
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
        fn is_active(&self) -> bool {
            let d = self.now();
            d >= self.begin && d < self.end
        }

        /// Checks if the injection is applied to the given device.
        fn is_applied(
            &mut self,
            dev: &dyn BlockDevice,
            op: InjectionOp,
            range: Range<u64>,
        ) -> bool {
            if op != self.op || dev.device_name() != self.name {
                return false;
            }

            if self.started.is_none() {
                debug!("{self:?}: starting");
                self.started = Some(Instant::now());
            }

            self.is_active() && is_overlapping(&self.range, &range)
        }
    }

    /// A list of fault injections.
    pub struct Injections {
        items: parking_lot::Mutex<Vec<Injection>>,
    }

    impl Injections {
        pub fn new() -> Self {
            Self {
                items: parking_lot::Mutex::new(Vec::new()),
            }
        }
    }

    impl From<&Injection> for InjectionInfo {
        fn from(src: &Injection) -> Self {
            InjectionInfo {
                device_name: src.name.clone(),
                is_active: src.is_active(),
            }
        }
    }

    impl<'n> Nexus<'n> {
        /// Creates a injected fault from URI.
        pub async fn inject_add_fault(
            self: Pin<&mut Self>,
            uri: &str,
        ) -> Result<(), Error> {
            let name = self.name.clone();

            self.inject_from_uri(uri)
                .map_err(|e| {
                    error!(
                        "Failed to add injected fault '{uri}': {err}",
                        err = e.verbose()
                    );
                    e
                })
                .context(nexus_err::BadFaultInjection {
                    name,
                })
        }

        /// Creates a injection from URI.
        fn inject_from_uri(
            self: Pin<&mut Self>,
            uri: &str,
        ) -> Result<(), InjectionError> {
            if INJECTIONS_ENABLED
                .compare_exchange(
                    false,
                    true,
                    Ordering::Acquire,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                warn!("Enabling nexus fault injections globally");
            }

            let inj = Injection::from_uri(uri)?;

            info!("{self:?}: add a injected fault: {inj:?} from URI '{uri}'");
            self.injections.items.lock().push(inj);

            Ok(())
        }

        /// Removes an injected fault by its name from URI.
        pub async fn inject_remove_fault(
            self: Pin<&mut Self>,
            uri: &str,
        ) -> Result<(), Error> {
            let t = Injection::from_uri(uri)
                .map_err(|e| {
                    error!(
                        "Failed to remove injected fault '{uri}': {err}",
                        err = e.verbose()
                    );
                    e
                })
                .context(nexus_err::BadFaultInjection {
                    name: self.name.clone(),
                })?;

            info!(
                "{self:?}: removing injected fault(s) for device '{name}'",
                name = t.name
            );

            self.injections
                .items
                .lock()
                .retain(|inj| inj.name != t.name);

            Ok(())
        }

        /// Returns list of injected faults.
        pub async fn list_injections(
            &self,
        ) -> Result<Vec<InjectionInfo>, Error> {
            Ok(self
                .injections
                .items
                .lock()
                .iter()
                .map(InjectionInfo::from)
                .collect())
        }

        /// Tests if there exists an active injected fault for the device.
        pub fn inject_is_faulted(
            &self,
            dev: &dyn BlockDevice,
            op: InjectionOp,
            offset: u64,
            num_blocks: u64,
        ) -> bool {
            if !injections_enabled() {
                return false;
            }

            self.injections.items.lock().iter_mut().any(|inj| {
                inj.is_applied(dev, op, offset .. offset + num_blocks)
            })
        }
    }
}

#[cfg(not(feature = "nexus-fault-injection"))]
#[allow(dead_code)]
mod inj_impl {
    use std::pin::Pin;

    use super::{
        super::{Error, Nexus},
        InjectionError,
        InjectionInfo,
    };

    #[inline]
    pub fn injections_enabled() -> bool {
        false
    }

    pub struct Injections();

    impl Injections {
        pub fn new() -> Self {
            Self()
        }
    }

    impl<'n> Nexus<'n> {
        /// TODO
        fn injections_disabled(&self) -> Result<(), Error> {
            warn!("{self:?}: injections are disabled");

            Err(Error::BadFaultInjection {
                source: InjectionError::InjectionsDisabled {},
                name: self.name.clone(),
            })
        }

        /// Creates a injected fault from URI.
        pub async fn inject_add_fault(
            self: Pin<&mut Self>,
            _uri: &str,
        ) -> Result<(), Error> {
            self.injections_disabled()
        }

        /// Removes an injected fault by its name from URI.
        pub async fn inject_remove_fault(
            self: Pin<&mut Self>,
            _uri: &str,
        ) -> Result<(), Error> {
            self.injections_disabled()
        }

        /// Returns list of injected faults.
        pub async fn list_injections(
            &self,
        ) -> Result<Vec<InjectionInfo>, Error> {
            self.injections_disabled().map(|_| Vec::new())
        }
    }
}
