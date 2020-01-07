//! Rust friendly wrappers around SPDK app start stop functions.
//!
//! NOTE: Should be used only for test and utility programs which don't
//! require custom argument parser. Otherwise use mayastor environment
//! module.

use crate::{delay, executor, logger, pool, replica};
use spdk_sys::{
    maya_log,
    spdk_app_opts,
    spdk_app_opts_init,
    spdk_app_parse_args,
    spdk_app_start,
    spdk_app_stop,
};
use std::{
    boxed::Box,
    env,
    ffi::CString,
    iter::Iterator,
    os::raw::{c_char, c_int, c_void},
    ptr::null_mut,
    vec::Vec,
};

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
pub fn start<T, F>(name: &str, mut args: Vec<T>, start_cb: F) -> i32
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
    opts.shutdown_cb = Some(shutdown_cb);

    // set the function pointer to use our maya_log function which is statically
    // linked into the sys create
    opts.log = Some(maya_log);

    unsafe {
        // set the pointer which the maya_log function uses after unpacking the
        // va_list
        spdk_sys::logfn = Some(logger::log_impl);
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

/// spdk_all_start callback which starts the future executor and finally calls
/// user provided start callback.
extern "C" fn app_start_cb<F>(arg1: *mut c_void)
where
    F: FnOnce(),
{
    // use in cases when you want to burn less cpu and speed does not matter
    if let Some(_key) = env::var_os("MAYASTOR_DELAY") {
        delay::register();
    }
    executor::start();
    pool::register_pool_methods();
    replica::register_replica_methods();

    // asynchronous initialization routines
    let fut = async move {
        let cb: Box<Box<F>> = unsafe { Box::from_raw(arg1 as *mut Box<F>) };
        cb();
    };
    executor::spawn(fut);
}

/// Cleanly exit from the program.
/// NOTE: Use only on programs started by mayastor_start.
pub fn stop(rc: i32) -> i32 {
    delay::unregister();
    let fut = async {};
    executor::stop(fut, Box::new(move || unsafe { spdk_app_stop(rc) }));
    rc
}

/// A callback called by spdk when it is shutting down.
unsafe extern "C" fn shutdown_cb() {
    stop(0);
}
