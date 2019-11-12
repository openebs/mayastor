//! Logger implementation for standard rust log module piping all log messages
//! to spdk logger. Some code here has been borrowed from stderr rust logger.

use log::{Level, LevelFilter, Log, Metadata, Record};
use std::ffi::CString;

fn is_submodule(parent: &str, possible_child: &str) -> bool {
    // Treat as bytes, because we'll be doing slicing, and we only care about
    // ':' chars
    let parent = parent.as_bytes();
    let possible_child = possible_child.as_bytes();

    // a longer module path cannot be a parent of a shorter module path
    if parent.len() > possible_child.len() {
        return false;
    }

    // If the path up to the parent isn't the same as the child,
    if parent != &possible_child[.. parent.len()] {
        return false;
    }

    // Either the path is exactly the same, or the sub module should have a "::"
    // after the length of the parent path. This prevents things like
    // 'a::bad' being considered a submodule of 'a::b'
    parent.len() == possible_child.len()
        || possible_child.get(parent.len() .. parent.len() + 2) == Some(b"::")
}

// Rust wrapper to SPDK logger.
#[derive(Clone, Default)]
pub struct SpdkLog {
    modules: Vec<String>,
}

impl SpdkLog {
    /// Create new SPDK logger.
    /// The mapping between rust logger and spdk log level is as follows:
    ///   error -> error
    ///   warn -> warn
    ///   notice -> info
    ///   info -> debug
    ///   debug -> trace
    ///
    /// Note: Must be called after spdk options were parsed and log level set.
    pub fn new() -> Self {
        Self {
            modules: Vec::new(),
        }
    }

    fn current_level_filter() -> LevelFilter {
        unsafe {
            match spdk_sys::spdk_log_get_print_level() {
                spdk_sys::SPDK_LOG_ERROR => LevelFilter::Error,
                spdk_sys::SPDK_LOG_WARN => LevelFilter::Warn,
                spdk_sys::SPDK_LOG_NOTICE => LevelFilter::Info,
                spdk_sys::SPDK_LOG_INFO => LevelFilter::Debug,
                spdk_sys::SPDK_LOG_DEBUG => LevelFilter::Trace,
                _ => unreachable!("Invalid SPDK log level"),
            }
        }
    }

    fn includes_module(&self, module_path: &str) -> bool {
        // If modules is empty, include all module paths
        if self.modules.is_empty() {
            return true;
        }

        // if a prefix of module_path is in `self.modules`, it must be
        // located at the first location before where module_path would be.
        match self
            .modules
            .binary_search_by(|module| module.as_str().cmp(&module_path))
        {
            Ok(_) => {
                // Found exact module: return true
                true
            }
            Err(0) => {
                // if there's no item which would be located before module_path,
                // no prefix is there
                false
            }
            Err(i) => is_submodule(&self.modules[i - 1], module_path),
        }
    }

    /// This registers spdklog as a global logger module which will be used
    /// with error!, info!, ... macros.
    pub fn init(&self) -> Result<(), log::SetLoggerError> {
        // TODO: set this to higher level when it is known in advance that
        // trace level will not be used to save some cpu cycles.
        log::set_max_level(LevelFilter::Trace);
        log::set_boxed_logger(Box::new(self.clone()))
    }
}

impl Log for SpdkLog {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= Self::current_level_filter()
            && self.includes_module(metadata.target())
    }

    fn log(&self, record: &Record) {
        let spdk_log_level = match record.metadata().level() {
            Level::Error => spdk_sys::SPDK_LOG_ERROR,
            Level::Warn => spdk_sys::SPDK_LOG_WARN,
            Level::Info => spdk_sys::SPDK_LOG_NOTICE,
            Level::Debug => spdk_sys::SPDK_LOG_INFO,
            Level::Trace => spdk_sys::SPDK_LOG_DEBUG,
        };
        let msg = CString::new(format!("{}\n", record.args())).unwrap();
        let file = record.file().unwrap_or_default();
        // get basename i.e.: spdk/src/lib.rs -> lib.rs
        let basename = &file[file.rfind('/').map_or(0, |v| v + 1) ..];
        let basename = CString::new(basename).unwrap();
        let line = record.line().unwrap_or_default() as i32;
        let empty = CString::new("").unwrap();

        unsafe {
            spdk_sys::spdk_log(
                spdk_log_level,
                basename.as_ptr(),
                line,
                empty.as_ptr(), // func name
                msg.as_ptr(),
            );
        }
    }

    fn flush(&self) {}
}
