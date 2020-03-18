#![allow(
    non_snake_case,
    non_upper_case_globals,
    non_camel_case_types,
    unused,
    elided_lifetimes_in_paths,
    clippy::all,
    unknown_lints
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
