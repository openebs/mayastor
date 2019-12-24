#[macro_use]
extern crate ioctl_gen;
#[macro_use]
extern crate lazy_static;
extern crate nix;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde;
extern crate serde_json;
extern crate snafu;
extern crate spdk_sys;

use env_logger::{Builder, Env};
use log::{logger, Level, Record};
use spdk_sys::{
    maya_log,
    spdk_app_opts,
    spdk_app_opts_init,
    spdk_app_parse_args,
    spdk_app_start,
    spdk_app_stop,
    spdk_log_get_print_level,
};
use std::{
    boxed::Box,
    env,
    ffi::{CStr, CString},
    io::Write,
    iter::Iterator,
    net::Ipv4Addr,
    os::raw::{c_char, c_int, c_void},
    path::Path,
    ptr::null_mut,
    time::Duration,
    vec::Vec,
};

pub mod aio_dev;
pub mod bdev;
pub mod descriptor;
pub mod dma;
pub mod environment;
pub mod event;
pub mod executor;
pub mod iscsi_dev;
pub mod iscsi_target;
pub mod jsonrpc;
pub mod nexus_uri;
pub mod nvmf_dev;
pub mod nvmf_target;
pub mod poller;
pub mod pool;
pub mod rebuild;
pub mod replica;
pub mod spdklog;
#[macro_export]
macro_rules! CPS_INIT {
    () => {
        #[link_section = ".init_array"]
        #[used]
        pub static INITIALIZE: extern "C" fn() = ::mayastor::cps_init;
    };
}

pub extern "C" fn cps_init() {
    bdev::nexus::register_module();
}

extern "C" fn log_impl(
    level: i32,
    file: *const c_char,
    line: u32,
    _func: *const c_char,
    buf: *const c_char,
    _n: i32, // the number of bytes written into buf
) {
    unsafe {
        // remove new line characters from the log messages if any
        let fmt = CStr::from_ptr(buf).to_str().unwrap().trim_end();
        let filename = CStr::from_ptr(file).to_str().unwrap();

        if spdk_log_get_print_level() < level {
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
}

/// This function configures the logging format. The loglevel is also processed
/// here i.e `RUST_LOG=mayastor=TRACE` will print all trace!() and higher
/// messages to the console.
///
/// Log messages originating from SPDK, are processed in `log_impl` the log
/// levels don not exactly match. See `log_impl` for the exact mapping.
///
/// We might want to suppress certain messages, as some of them are redundant,
/// in particular, the NOTICE messages as such, they are mapped to debug.
pub fn mayastor_logger_init(level: &str) {
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

/// A callback to print help for extra options that we use.
/// Mayastor app does not use it because it has it's own code to initialize
/// spdk env. This is used only by legacy apps, which don't have any extra
/// options.
extern "C" fn usage() {
    // i.e. println!(" -f <path>                 save pid to this file");
}

/// Rust friendly wrapper around SPDK app start function.
/// The application code is a closure passed as argument and called
/// when spdk initialization is done.
///
/// This function relies on spdk argument parser. Extra parameters can be
/// passed in environment variables.
///
/// NOTE: Should be used only for test and utility programs which don't
/// require custom argument parser. Otherwise use mayastor environment
/// module.
pub fn mayastor_start<T, F>(name: &str, mut args: Vec<T>, start_cb: F) -> i32
where
    T: Into<Vec<u8>>,
    F: FnOnce(),
{
    // hand over command line args to spdk arg parser
    let args = args
        .drain(..)
        .map(|arg| CString::new(arg).unwrap())
        .collect::<Vec<CString>>();
    let mut c_args = args
        .iter()
        .map(|arg| arg.as_ptr())
        .collect::<Vec<*const c_char>>();
    c_args.push(std::ptr::null());

    let mut opts: spdk_app_opts = Default::default();

    unsafe {
        spdk_app_opts_init(&mut opts as *mut spdk_app_opts);
        opts.rpc_addr =
            CString::new("/var/tmp/mayastor.sock").unwrap().into_raw();

        if let Ok(log_level) = env::var("MAYASTOR_LOGLEVEL") {
            opts.print_level = match log_level.parse() {
                Ok(-1) => spdk_sys::SPDK_LOG_DISABLED,
                Ok(0) => spdk_sys::SPDK_LOG_ERROR,
                Ok(1) => spdk_sys::SPDK_LOG_WARN,
                Ok(2) => spdk_sys::SPDK_LOG_NOTICE,
                Ok(3) => spdk_sys::SPDK_LOG_INFO,
                Ok(4) => spdk_sys::SPDK_LOG_DEBUG,
                // default
                _ => spdk_sys::SPDK_LOG_DEBUG,
            }
        } else {
            opts.print_level = spdk_sys::SPDK_LOG_NOTICE;
        }

        if spdk_app_parse_args(
            (c_args.len() as c_int) - 1,
            c_args.as_ptr() as *mut *mut i8,
            &mut opts,
            null_mut(), // extra short options i.e. "f:S:"
            null_mut(), // extra long options
            None,       // extra options parse callback
            Some(usage),
        ) != spdk_sys::SPDK_APP_PARSE_ARGS_SUCCESS
        {
            return -1;
        }
    }

    opts.name = CString::new(name).unwrap().into_raw();
    opts.shutdown_cb = Some(mayastor_shutdown_cb);

    // set the function pointer to use our maya_log function which is statically
    // linked into the sys create
    opts.log = Some(maya_log);

    unsafe {
        // set the pointer which the maya_log function uses after unpacking the
        // va_list
        spdk_sys::logfn = Some(log_impl);
    }

    unsafe {
        let rc = spdk_app_start(
            &mut opts,
            Some(app_start_cb::<F>),
            // Double box to convert from fat to thin pointer
            Box::into_raw(Box::new(Box::new(start_cb))) as *mut c_void,
        );

        // this will remove shm file in /dev/shm and do other cleanups
        spdk_sys::spdk_app_fini();

        rc
    }
}

extern "C" fn developer_delay(_ctx: *mut c_void) -> i32 {
    std::thread::sleep(Duration::from_millis(1));
    0
}

/// spdk_all_start callback which starts the future executor and finally calls
/// user provided start callback.
extern "C" fn app_start_cb<F>(arg1: *mut c_void)
where
    F: FnOnce(),
{
    // use in cases when you want to burn less cpu and speed does not matter
    if let Some(_key) = env::var_os("DELAY") {
        warn!("*** Delaying reactor every 1000us ***");
        unsafe {
            spdk_sys::spdk_poller_register(
                Some(developer_delay),
                std::ptr::null_mut(),
                1000,
            )
        };
    }
    let address = match env::var("MY_POD_IP") {
        Ok(val) => {
            let _ipv4: Ipv4Addr = match val.parse() {
                Ok(val) => val,
                Err(_) => {
                    error!("Invalid IP address: MY_POD_IP={}", val);
                    mayastor_stop(-1);
                    return;
                }
            };
            val
        }
        Err(_) => "127.0.0.1".to_owned(),
    };
    executor::start();
    pool::register_pool_methods();
    replica::register_replica_methods();
    if let Err(msg) = iscsi_target::init_iscsi(&address) {
        error!("Failed to initialize Mayastor iscsi: {}", msg);
        mayastor_stop(-1);
        return;
    }

    // asynchronous initialization routines
    let fut = async move {
        if let Err(msg) = nvmf_target::init_nvmf(&address).await {
            error!("Failed to initialize Mayastor nvmf target: {}", msg);
            mayastor_stop(-1);
            return;
        }
        let cb: Box<Box<F>> = unsafe { Box::from_raw(arg1 as *mut Box<F>) };
        cb();
    };
    executor::spawn(fut);
}

/// Cleanly exit from the program.
/// NOTE: Use only on programs started by mayastor_start.
pub fn mayastor_stop(rc: i32) -> i32 {
    iscsi_target::fini_iscsi();
    let fut = async move {
        if let Err(msg) = nvmf_target::fini_nvmf().await {
            error!("Failed to finalize nvmf target: {}", msg);
        }
    };
    executor::stop(fut, Box::new(move || unsafe { spdk_app_stop(rc) }));
    rc
}

/// A callback called by spdk when it is shutting down.
unsafe extern "C" fn mayastor_shutdown_cb() {
    mayastor_stop(0);
}
