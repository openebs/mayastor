extern crate clap;

use clap::{App, Arg};
use mayastor::bdev::uring_util;

fn main() {
    let matches = App::new("io_uring support")
        .version("0.1.0")
        .author("Jonathan Teh <jonathan.teh@mayadata.io>")
        .about("Determines io_uring support")
        .arg(Arg::with_name("uring-path").help("Path to file").index(1))
        .get_matches();

    let supported = match matches.value_of("uring-path") {
        None => true,
        Some(path) => {
            uring_util::fs_supports_direct_io(path)
                && uring_util::fs_type_supported(path)
        }
    } && uring_util::kernel_support();

    if supported {
        std::process::exit(0);
    } else {
        std::process::exit(1);
    }
}
