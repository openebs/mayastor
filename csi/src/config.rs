use clap::ArgMatches;
use std::{
    collections::HashMap,
    convert::TryFrom,
    sync::{Arc, Mutex, MutexGuard},
};

/// Number of Nvme IO Queues.
pub(crate) const NVME_NR_IO_QUEUES_NAME: &str = "nvme-nr-io-queues";

/// Global configuration parameters.
#[derive(Debug, Default)]
pub(crate) struct Config {
    nvme: NvmeConfig,
}
impl Config {
    /// Get the `NvmeConfig` as mut.
    pub(crate) fn nvme_as_mut(&mut self) -> &mut NvmeConfig {
        &mut self.nvme
    }
    /// Get the `NvmeConfig`.
    pub(crate) fn nvme(&self) -> &NvmeConfig {
        &self.nvme
    }
}

/// Nvme Configuration parameters.
#[derive(Debug, Default)]
pub(crate) struct NvmeConfig {
    nr_io_queues: Option<u32>,
}
impl NvmeConfig {
    fn new(nr_io_queues: Option<u32>) -> Self {
        Self {
            nr_io_queues,
        }
    }
    /// Number of IO Queues.
    pub(crate) fn nr_io_queues(&self) -> Option<u32> {
        self.nr_io_queues
    }
}

/// Get a mutex guard over the `Config`.
pub(crate) fn config<'a>() -> MutexGuard<'a, Config> {
    lazy_static! {
        static ref CONFIG: Arc<Mutex<Config>> =
            Arc::new(Mutex::new(Config::default()));
    }
    CONFIG.lock().expect("not poisoned")
}

/// Nvme Arg Values taken from the command line
#[derive(Default)]
pub(crate) struct NvmeArgValues(HashMap<String, String>);
impl TryFrom<NvmeArgValues> for NvmeConfig {
    type Error = String;
    fn try_from(src: NvmeArgValues) -> Result<Self, Self::Error> {
        let nvme_nr_ioq = match src.0.get(NVME_NR_IO_QUEUES_NAME) {
            None => None,
            Some(value) => {
                let value = value.parse::<u32>().map_err(|error| {
                    format!(
                        "Invalid value for {}, error = {}",
                        NVME_NR_IO_QUEUES_NAME, error
                    )
                })?;
                Some(value)
            }
        };
        Ok(Self::new(nvme_nr_ioq))
    }
}
/// Nvme Arguments taken from the CSI volume calls (storage class parameters)
pub type NvmeParseParams<'a> = &'a HashMap<String, String>;
impl TryFrom<NvmeParseParams<'_>> for NvmeArgValues {
    type Error = String;
    fn try_from(_value: NvmeParseParams) -> Result<Self, Self::Error> {
        // nothing parsed for now
        Ok(Default::default())
    }
}
impl TryFrom<NvmeParseParams<'_>> for NvmeConfig {
    type Error = String;
    fn try_from(value: NvmeParseParams) -> Result<Self, Self::Error> {
        Self::try_from(NvmeArgValues::try_from(value)?)
    }
}
impl TryFrom<&ArgMatches<'_>> for NvmeArgValues {
    type Error = String;
    fn try_from(matches: &ArgMatches<'_>) -> Result<Self, Self::Error> {
        let mut map = NvmeArgValues::default();
        if let Some(value) = matches.value_of(NVME_NR_IO_QUEUES_NAME) {
            map.0
                .insert(NVME_NR_IO_QUEUES_NAME.into(), value.to_string());
        }
        Ok(map)
    }
}
impl TryFrom<&ArgMatches<'_>> for NvmeConfig {
    type Error = String;
    fn try_from(matches: &ArgMatches) -> Result<Self, Self::Error> {
        Self::try_from(NvmeArgValues::try_from(matches)?)
    }
}
