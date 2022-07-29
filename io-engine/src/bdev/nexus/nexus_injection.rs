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

pub(super) use inj_impl::Injections;

#[cfg(feature = "fault_injection")]
mod inj_impl {
    use std::{
        fmt::{Debug, Formatter},
        pin::Pin,
        time::{Duration, Instant},
    };

    use snafu::ResultExt;
    use url::Url;

    use crate::core::{BlockDevice, VerboseError};

    use super::{
        super::{nexus_err, Error, Nexus},
        InjectionError,
        InjectionInfo,
    };

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

    /// Injected failures.
    struct Injection {
        name: String,
        started: Option<Instant>,
        begin: Duration,
        end: Duration,
    }

    impl Debug for Injection {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(
                f,
                "Injection '{}' [{:?} -> {}]",
                self.name,
                self.begin,
                if self.end == Duration::MAX {
                    "INF".to_string()
                } else {
                    format!("{:?}", self.end)
                }
            )
        }
    }

    impl Injection {
        /// TODO
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
                name: p.path().to_owned(),
                started: None,
                begin: Duration::ZERO,
                end: Duration::MAX,
            };

            for (k, v) in p.query_pairs() {
                match k.as_ref() {
                    "begin" => r.begin = parse_timer(&k, &v)?,
                    "end" => r.end = parse_timer(&k, &v)?,
                    _ => {
                        return Err(InjectionError::UnknownParameter {
                            name: k.to_string(),
                            value: v.to_string(),
                        })
                    }
                };
            }

            if r.begin > r.end {
                return Err(InjectionError::BadDurations {
                    name: r.name,
                    begin: r.begin,
                    end: r.end,
                });
            }

            Ok(r)
        }

        /// TODO
        fn now(&self) -> Duration {
            self.started.map_or(Duration::MAX, |s| {
                Instant::now().saturating_duration_since(s)
            })
        }

        /// TODO
        fn tick(&mut self) {
            if self.started.is_none() {
                debug!("{:?}: starting", self);
                self.started = Some(Instant::now());
            }
        }

        /// TODO
        fn is_active(&self) -> bool {
            let d = self.now();
            d >= self.begin && d < self.end
        }

        /// TODO
        fn is_applied(&self, dev: &dyn BlockDevice) -> bool {
            self.is_active() && dev.device_name() == self.name
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
                        "Failed to add injected fault '{}': {}",
                        uri,
                        e.verbose()
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
            let inj = Injection::from_uri(uri)?;

            info!(
                "{:?}: add a injected fault: {:?} from URI '{}'",
                self, inj, uri
            );
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
                        "Failed to remove injected fault '{}': {}",
                        uri,
                        e.verbose()
                    );
                    e
                })
                .context(nexus_err::BadFaultInjection {
                    name: self.name.clone(),
                })?;

            info!(
                "{:?}: removing injected fault(s) for device '{}'",
                self, t.name
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
        pub fn inject_is_faulted(&self, dev: &dyn BlockDevice) -> bool {
            self.injections.items.lock().iter_mut().any(|inj| {
                inj.tick();
                inj.is_applied(dev)
            })
        }
    }
}

#[cfg(not(feature = "fault_injection"))]
mod inj_impl {
    use std::pin::Pin;

    use super::{
        super::{Error, Nexus},
        InjectionError,
        InjectionInfo,
    };

    pub struct Injections();

    impl Injections {
        pub fn new() -> Self {
            Self()
        }
    }

    impl<'n> Nexus<'n> {
        /// TODO
        fn injections_disabled(&self) -> Result<(), Error> {
            warn!("{:?}: injections are disabled", self);

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
