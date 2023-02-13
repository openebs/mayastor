use std::{ffi::CStr, fmt::Write, os::raw::c_char, path::Path, str::FromStr};

use ansi_term::{Colour, Style};

use tracing_core::{event::Event, Level, Metadata};
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
            .args(format_args!("{arg}"))
            .level(from_spdk_level(spdk_level))
            .target("mayastor::spdk")
            .file(Some(filename))
            .line(Some(line))
            .build(),
    );
}

// Custom struct used to format the log/trace LEVEL
struct FormatLevel<'a> {
    level: &'a Level,
    ansi: bool,
}

impl<'a> FormatLevel<'a> {
    fn new(level: &'a Level, ansi: bool) -> Self {
        Self {
            level,
            ansi,
        }
    }

    fn short(&self) -> &str {
        match *self.level {
            Level::TRACE => "T",
            Level::DEBUG => "D",
            Level::INFO => "I",
            Level::WARN => "W",
            Level::ERROR => "E",
        }
    }

    fn fmt_line(&self, f: &mut dyn Write, line: &str) -> std::fmt::Result {
        if self.ansi {
            write!(
                f,
                "{}",
                match *self.level {
                    Level::TRACE => Colour::Cyan.dimmed().paint(line),
                    Level::DEBUG => Colour::White.dimmed().paint(line),
                    Level::INFO => Colour::White.paint(line),
                    Level::WARN => Colour::Yellow.paint(line),
                    Level::ERROR => Colour::Red.paint(line),
                }
            )
        } else {
            write!(f, "{line}")
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
                Level::TRACE => write!(f, "{}", Colour::Purple.paint(TRACE)),
                Level::DEBUG => write!(f, "{}", Colour::Blue.paint(DEBUG)),
                Level::INFO => write!(f, "{}", Colour::Green.paint(INFO)),
                Level::WARN => write!(f, "{}", Colour::Yellow.paint(WARN)),
                Level::ERROR => write!(f, "{}", Colour::Red.paint(ERROR)),
            }
        } else {
            match *self.level {
                Level::TRACE => f.pad(TRACE),
                Level::DEBUG => f.pad(DEBUG),
                Level::INFO => f.pad(INFO),
                Level::WARN => f.pad(WARN),
                Level::ERROR => f.pad(ERROR),
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
                write!(f, "{}:{}", basename(file), line)?;
            }
        }
        Ok(())
    }
}

/// Log output styles.
#[derive(Debug, Copy, Clone)]
pub enum LogStyle {
    Default,
    Compact,
}

// Custom struct used to format trace events.
#[derive(Debug, Copy, Clone)]
pub struct LogFormat {
    ansi: bool,
    style: LogStyle,
    no_date: bool,
}

impl Default for LogFormat {
    fn default() -> Self {
        Self {
            ansi: atty::is(atty::Stream::Stdout),
            style: LogStyle::Default,
            no_date: false,
        }
    }
}

impl FromStr for LogFormat {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let mut r = Self::default();

        for p in s.split(',').filter(|i| !i.is_empty()) {
            match p {
                "default" => r.style = LogStyle::Default,
                "compact" => r.style = LogStyle::Compact,
                "color" => r.ansi = true,
                "nocolor" => r.ansi = false,
                "nodate" => r.no_date = true,
                _ => return Err(format!("Bad log format option: {p}")),
            }
        }

        Ok(r)
    }
}

// Format a trace event.
impl<S, N> FormatEvent<S, N> for LogFormat
where
    S: tracing_core::subscriber::Subscriber + for<'s> LookupSpan<'s>,
    N: for<'w> FormatFields<'w> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        w: &mut dyn Write,
        evt: &Event<'_>,
    ) -> std::fmt::Result {
        match self.style {
            LogStyle::Default => self.default_style(ctx, w, evt),
            LogStyle::Compact => self.compact_style(ctx, w, evt),
        }
    }
}

fn ellipsis(s: &str, w: usize) -> String {
    if w < 8 || s.len() <= w {
        s.to_owned()
    } else {
        format!("{}...", &s[.. w - 3])
    }
}

impl LogFormat {
    /// Formats an event in default mode.
    fn default_style<S, N>(
        &self,
        context: &FmtContext<'_, S, N>,
        writer: &mut dyn Write,
        event: &Event<'_>,
    ) -> std::fmt::Result
    where
        S: tracing_core::subscriber::Subscriber + for<'s> LookupSpan<'s>,
        N: for<'w> FormatFields<'w> + 'static,
    {
        let normalized = event.normalized_metadata();
        let meta = normalized.as_ref().unwrap_or_else(|| event.metadata());
        let chrono_fmt = if self.no_date {
            "%T%.6f"
        } else {
            "%FT%T%.9f%Z"
        };

        write!(
            writer,
            "[{} {} {}{}:{}] ",
            chrono::Local::now().format(chrono_fmt),
            FormatLevel::new(meta.level(), self.ansi),
            meta.target(),
            CustomContext::new(context, event.parent(), self.ansi),
            Location::new(meta)
        )?;

        context.format_fields(writer, event)?;

        writeln!(writer)
    }

    /// Formats an event in compact mode.
    fn compact_style<S, N>(
        &self,
        context: &FmtContext<'_, S, N>,
        writer: &mut dyn Write,
        event: &Event<'_>,
    ) -> std::fmt::Result
    where
        S: tracing_core::subscriber::Subscriber + for<'s> LookupSpan<'s>,
        N: for<'w> FormatFields<'w> + 'static,
    {
        let normalized = event.normalized_metadata();
        let meta = normalized.as_ref().unwrap_or_else(|| event.metadata());
        let loc = ellipsis(&Location::new(meta).to_string(), 18);
        let fmt = FormatLevel::new(meta.level(), self.ansi);
        let now = chrono::Local::now();

        let mut buf = String::new();

        write!(
            buf,
            "{} | {:<18} [{}] ",
            now.format(if self.no_date { "%T%.6f" } else { "%x %T%.6f" }),
            loc,
            fmt.short(),
        )?;

        let ctx =
            CustomContext::new(context, event.parent(), false).to_string();
        if ctx.len() > 1 {
            write!(buf, "{}: ", &ctx[1 ..])?;
        }

        context.format_fields(&mut buf, event)?;

        fmt.fmt_line(writer, &buf)?;

        writeln!(writer)
    }
}

/// This function configures the logging format. The loglevel is also processed
/// here i.e `RUST_LOG=io_engine=TRACE` will print all trace!() and higher
/// messages to the console.
///
/// We might want to suppress certain messages, as some of them are redundant,
/// in particular, the NOTICE messages as such, they are mapped to debug.
pub fn init_ex(level: &str, format: LogFormat) {
    // Set up a "logger" that simply translates any "log" messages it receives
    // to trace events. This is for our custom spdk log messages, but also
    // for any other third party crates still using the logging facade.
    LogTracer::init().expect("failed to initialise LogTracer");

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

pub fn init(level: &str) {
    init_ex(level, Default::default())
}
