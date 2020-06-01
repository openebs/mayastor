//
// the improper_ctypes is needed as because
// spdk_nvme_ctrlr_data is 128 bit
#![allow(
    clippy::all,
    elided_lifetimes_in_paths,
    improper_ctypes,
    non_camel_case_types,
    non_snake_case,
    non_upper_case_globals,
    unknown_lints,
    unused
)]

use std::os::raw::c_char;
include!(concat!(env!("OUT_DIR"), "/libspdk.rs"));

pub type LogProto = Option<
    extern "C" fn(
        level: i32,
        file: *const c_char,
        line: u32,
        func: *const c_char,
        buf: *const c_char,
        n: i32,
    ),
>;

#[link(name = "logwrapper", kind = "static")]
extern "C" {
    pub fn maya_log(
        level: i32,
        file: *const c_char,
        line: i32,
        func: *const c_char,
        format: *const c_char,
        args: *mut __va_list_tag,
    );

    pub static mut logfn: LogProto;
}
