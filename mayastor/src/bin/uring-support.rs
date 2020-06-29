extern crate clap;

use clap::{App, Arg};

use mayastor::bdev::util::uring;

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
            uring::fs_supports_direct_io(path) && uring::fs_type_supported(path)
        }
    } && uring::kernel_support();

    std::process::exit(!supported as i32)
}
