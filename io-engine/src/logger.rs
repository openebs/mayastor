use std::{ffi::CStr, os::raw::c_char, path::Path};

use ansi_term::{Colour, Style};

use tracing_core::{event::Event, Metadata};
use tracing_log::{LogTracer, NormalizeEvent};
use tracing_subscriber::{
    fmt::{
        format::{FmtSpan, FormatEvent, FormatFields},
        FmtContext,
        FormattedFields,
    },
    registry::LookupSpan,
    EnvFilter,
};

use spdk_rs::libspdk::{spdk_log_get_print_level, spdk_log_level};

fn from_spdk_level(level: spdk_log_level) -> log::Level {
    match level {
        spdk_rs::libspdk::SPDK_LOG_ERROR => log::Level::Error,
        spdk_rs::libspdk::SPDK_LOG_WARN => log::Level::Warn,
        spdk_rs::libspdk::SPDK_LOG_INFO => log::Level::Info,
        spdk_rs::libspdk::SPDK_LOG_NOTICE => log::Level::Debug,
        spdk_rs::libspdk::SPDK_LOG_DEBUG => log::Level::Trace,
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
    if spdk_level == spdk_rs::libspdk::SPDK_LOG_DISABLED {
        return;
    }

    if unsafe { spdk_log_get_print_level() } < spdk_level {
        return;
    }

    let arg =
        unsafe { CStr::from_ptr(buf).to_string_lossy().trim_end().to_string() };
    let filename = unsafe { CStr::from_ptr(file).to_str().unwrap() };

    log::logger().log(
        &log::Record::builder()
            .args(format_args!("{}", arg))
            .level(from_spdk_level(spdk_level))
            .target("mayastor::spdk")
            .file(Some(filename))
            .line(Some(line))
            .build(),
    );
}

// Custom struct used to format the log/trace LEVEL
struct FormatLevel<'a> {
    level: &'a tracing::Level,
    ansi: bool,
}

impl<'a> FormatLevel<'a> {
    fn new(level: &'a tracing::Level, ansi: bool) -> Self {
        Self {
            level,
            ansi,
        }
    }
}

// Display trace LEVEL.
impl std::fmt::Display for FormatLevel<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        const TRACE: &str = "TRACE";
        const DEBUG: &str = "DEBUG";
        const INFO: &str = " INFO";
        const WARN: &str = " WARN";
        const ERROR: &str = "ERROR";

        if self.ansi {
            match *self.level {
                tracing::Level::TRACE => {
                    write!(f, "{}", Colour::Purple.paint(TRACE))
                }
                tracing::Level::DEBUG => {
                    write!(f, "{}", Colour::Blue.paint(DEBUG))
                }
                tracing::Level::INFO => {
                    write!(f, "{}", Colour::Green.paint(INFO))
                }
                tracing::Level::WARN => {
                    write!(f, "{}", Colour::Yellow.paint(WARN))
                }
                tracing::Level::ERROR => {
                    write!(f, "{}", Colour::Red.paint(ERROR))
                }
            }
        } else {
            match *self.level {
                tracing::Level::TRACE => f.pad(TRACE),
                tracing::Level::DEBUG => f.pad(DEBUG),
                tracing::Level::INFO => f.pad(INFO),
                tracing::Level::WARN => f.pad(WARN),
                tracing::Level::ERROR => f.pad(ERROR),
            }
        }
    }
}

// Custom struct used to format trace context (span) information
struct CustomContext<'a, S, N>
where
    S: tracing_core::subscriber::Subscriber + for<'s> LookupSpan<'s>,
    N: for<'w> FormatFields<'w> + 'static,
{
    context: &'a FmtContext<'a, S, N>,
    span: Option<&'a tracing_core::span::Id>,
    ansi: bool,
}

impl<'a, S, N> CustomContext<'a, S, N>
where
    S: tracing_core::subscriber::Subscriber + for<'s> LookupSpan<'s>,
    N: for<'w> FormatFields<'w> + 'static,
{
    fn new(
        context: &'a FmtContext<'a, S, N>,
        span: Option<&'a tracing_core::span::Id>,
        ansi: bool,
    ) -> Self {
        Self {
            context,
            span,
            ansi,
        }
    }
}

// Display trace context (span) information
impl<'a, S, N> std::fmt::Display for CustomContext<'a, S, N>
where
    S: tracing_core::subscriber::Subscriber + for<'s> LookupSpan<'s>,
    N: for<'w> FormatFields<'w> + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let bold = if self.ansi {
            Style::new().bold()
        } else {
            Style::new()
        };
        let scope = self
            .span
            .and_then(|id| self.context.span(id))
            .or_else(|| self.context.lookup_current())
            .into_iter()
            .flat_map(|span| span.scope().from_root());

        for span in scope {
            write!(f, ":{}", bold.paint(span.metadata().name()))?;

            let extensions = span.extensions();

            let fields = &extensions
                .get::<FormattedFields<N>>()
                .expect("unable to find FormattedFields in extensions");

            if !fields.is_empty() {
                write!(f, "{}{}{}", bold.paint("{"), fields, bold.paint("}"))?;
            }
        }

        Ok(())
    }
}

fn basename(path: &str) -> &str {
    Path::new(path).file_name().unwrap().to_str().unwrap()
}

// Custom struct used to format a callsite location (filename and line number)
struct Location<'a> {
    meta: &'a Metadata<'a>,
}

impl<'a> Location<'a> {
    fn new(meta: &'a Metadata<'a>) -> Self {
        Self {
            meta,
        }
    }
}

// Display callsite location (filename and line number) from metadata
impl std::fmt::Display for Location<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(file) = self.meta.file() {
            if let Some(line) = self.meta.line() {
                write!(f, ":{}:{}", basename(file), line)?;
            }
        }
        Ok(())
    }
}

// Custom struct used to format trace events.
struct CustomFormat {
    ansi: bool,
}

// Format a trace event.
impl<S, N> FormatEvent<S, N> for CustomFormat
where
    S: tracing_core::subscriber::Subscriber + for<'s> LookupSpan<'s>,
    N: for<'w> FormatFields<'w> + 'static,
{
    fn format_event(
        &self,
        context: &FmtContext<'_, S, N>,
        writer: &mut dyn std::fmt::Write,
        event: &Event<'_>,
    ) -> std::fmt::Result {
        let normalized = event.normalized_metadata();
        let meta = normalized.as_ref().unwrap_or_else(|| event.metadata());

        write!(
            writer,
            "[{} {} {}{}{}] ",
            chrono::Local::now().format("%FT%T%.9f%Z"),
            FormatLevel::new(meta.level(), self.ansi),
            meta.target(),
            CustomContext::new(context, event.parent(), self.ansi),
            Location::new(meta)
        )?;

        context.format_fields(writer, event)?;

        writeln!(writer)
    }
}

/// This function configures the logging format. The loglevel is also processed
/// here i.e `RUST_LOG=mayastor=TRACE` will print all trace!() and higher
/// messages to the console.
///
/// We might want to suppress certain messages, as some of them are redundant,
/// in particular, the NOTICE messages as such, they are mapped to debug.
pub fn init(level: &str) {
    // Set up a "logger" that simply translates any "log" messages it receives
    // to trace events. This is for our custom spdk log messages, but also
    // for any other third party crates still using the logging facade.
    LogTracer::init().expect("failed to initialise LogTracer");

    // Our own custom format for displaying trace events.
    let format = CustomFormat {
        ansi: atty::is(atty::Stream::Stdout),
    };

    // Create a default subscriber.
    let builder = tracing_subscriber::fmt::Subscriber::builder()
        .with_span_events(FmtSpan::FULL)
        .event_format(format);

    let subscriber = match EnvFilter::try_from_default_env() {
        Ok(filter) => builder.with_env_filter(filter).finish(),
        Err(_) => builder.with_env_filter(level).finish(),
    };

    tracing::subscriber::set_global_default(subscriber)
        .expect("failed to set default subscriber");
}
