use std::{ffi::CStr, os::raw::c_char, str::FromStr};

use tracing_log::format_trace;
use tracing_subscriber::{
    fmt::{format::FmtSpan, time::FormatTime, Subscriber},
    EnvFilter,
};

use spdk_sys::{spdk_log_get_print_level, spdk_log_level};

fn from_spdk_level(level: spdk_log_level) -> log::Level {
    match level {
        spdk_sys::SPDK_LOG_ERROR => log::Level::Error,
        spdk_sys::SPDK_LOG_WARN => log::Level::Warn,
        spdk_sys::SPDK_LOG_INFO => log::Level::Info,
        spdk_sys::SPDK_LOG_NOTICE => log::Level::Debug,
        spdk_sys::SPDK_LOG_DEBUG => log::Level::Trace,
        // any other level unknown to us is logged as an error
        _ => log::Level::Error,
    }
}

/// Log messages originating from SPDK, are processed by this function.
/// Note that the log levels between spdk and rust do not exactly match.
///
/// The function should have been unsafe because we dereference raw pointer
/// arguments, but the pointer in spdk_sys where this fn is assigned expects
/// a safe function.
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn log_impl(
    spdk_level: spdk_log_level,
    file: *const c_char,
    line: u32,
    _func: *const c_char,
    buf: *const c_char,
    _n: i32, // the number of bytes written into buf
) {
    if spdk_level == spdk_sys::SPDK_LOG_DISABLED {
        return;
    }

    if unsafe { spdk_log_get_print_level() } < spdk_level {
        return;
    }

    // remove new line characters from the log messages if any
    let fmt =
        unsafe { CStr::from_ptr(buf).to_string_lossy().trim_end().to_string() };
    let filename = unsafe { CStr::from_ptr(file).to_str().unwrap() };

    format_trace(
        &log::Record::builder()
            .args(format_args!("{}", fmt))
            .target(module_path!())
            .file(Some(filename))
            .line(Some(line))
            .level(from_spdk_level(spdk_level))
            .build(),
    )
    .unwrap();
}

struct CustomTime<'a>(&'a str);
impl FormatTime for CustomTime<'_> {
    fn format_time(&self, w: &mut dyn std::fmt::Write) -> std::fmt::Result {
        write!(w, "{}", chrono::Local::now().format(self.0))
    }
}

/// This function configures the logging format. The loglevel is also processed
/// here i.e `RUST_LOG=mayastor=TRACE` will print all trace!() and higher
/// messages to the console.
///
/// We might want to suppress certain messages, as some of them are redundant,
/// in particular, the NOTICE messages as such, they are mapped to debug.
pub fn init(level: &str) {
    let builder = Subscriber::builder()
        .with_timer(CustomTime("%FT%T%.9f%Z"))
        .with_span_events(FmtSpan::FULL);

    if let Ok(filter) = EnvFilter::try_from_default_env() {
        let subscriber = builder.with_env_filter(filter).finish();
        tracing::subscriber::set_global_default(subscriber)
    } else {
        let max_level =
            tracing::Level::from_str(level).unwrap_or(tracing::Level::INFO);
        let subscriber = builder.with_max_level(max_level).finish();
        tracing::subscriber::set_global_default(subscriber)
    }
    .expect("failed to set default subscriber");
}
