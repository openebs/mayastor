extern crate clap;

use clap::{Arg, App};
use mayastor::bdev::uring_util;

fn main() {
    let matches = App::new("io_uring support")
        .version("0.1.0")
        .author("Jonathan Teh <jonathan.teh@mayadata.io>")
        .about("Determines io_uring support")
        .arg(Arg::with_name("uring-path")
                 .required(true)
                 .help("Path to file")
                 .index(1))
        .get_matches();

    let path = matches.value_of("uring-path").unwrap();

    let supported = uring_util::fs_supports_direct_io(path)
        && uring_util::fs_type_supported(path)
        && uring_util::kernel_supports_io_uring();

    if supported {
        std::process::exit(0);
    } else {
        std::process::exit(1);
    }
}
