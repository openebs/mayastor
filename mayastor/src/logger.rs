use env_logger::{Builder, Env};
use log::{logger, Level, Record};
use spdk_sys::spdk_log_get_print_level;
use std::{ffi::CStr, io::Write, os::raw::c_char, path::Path};

/// Log messages originating from SPDK, are processed by this function.
/// Note that the log levels between spdk and rust do not exactly match.
///
/// The function should have been unsafe because we dereference raw pointer
/// arguments, but the pointer in spdk_sys where this fn is assigned expects
/// a safe function.
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn log_impl(
    level: i32,
    file: *const c_char,
    line: u32,
    _func: *const c_char,
    buf: *const c_char,
    _n: i32, // the number of bytes written into buf
) {
    // remove new line characters from the log messages if any
    let fmt =
        unsafe { CStr::from_ptr(buf).to_string_lossy().trim_end().to_string() };
    let filename = unsafe { CStr::from_ptr(file).to_str().unwrap() };

    if unsafe { spdk_log_get_print_level() } < level {
        return;
    }

    let lvl = match level {
        spdk_sys::SPDK_LOG_DISABLED => return,
        spdk_sys::SPDK_LOG_ERROR => Level::Error,
        spdk_sys::SPDK_LOG_WARN => Level::Warn,
        // the default level for now
        spdk_sys::SPDK_LOG_INFO => Level::Info,
        spdk_sys::SPDK_LOG_NOTICE => Level::Debug,
        spdk_sys::SPDK_LOG_DEBUG => Level::Trace,
        // if the error level is unknown to us we log it as an error by
        // default
        _ => Level::Error,
    };

    logger().log(
        &Record::builder()
            .args(format_args!("{}", fmt))
            .target(module_path!())
            .file(Some(filename))
            .line(Some(line))
            .level(lvl)
            .build(),
    );
}

/// This function configures the logging format. The loglevel is also processed
/// here i.e `RUST_LOG=mayastor=TRACE` will print all trace!() and higher
/// messages to the console.
///
/// We might want to suppress certain messages, as some of them are redundant,
/// in particular, the NOTICE messages as such, they are mapped to debug.
pub fn init(level: &str) {
    let mut builder =
        Builder::from_env(Env::default().default_filter_or(level.to_string()));

    builder.format(|buf, record| {
        let mut level_style = buf.default_level_style(record.level());
        level_style.set_intense(true);
        writeln!(
            buf,
            "[{} {} {}:{}] {}",
            buf.timestamp_nanos(),
            level_style.value(record.level()),
            Path::new(record.file().unwrap())
                .file_name()
                .unwrap()
                .to_str()
                .unwrap(),
            record.line().unwrap(),
            record.args()
        )
    });
    builder.init();
}
