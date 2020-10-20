extern crate clap;

use clap::App;

use mayastor::bdev::util::uring;

fn main() {
    let _matches = App::new("io_uring support")
        .version("0.1.0")
        .author("Jonathan Teh <jonathan.teh@mayadata.io>")
        .about("Determines io_uring support")
        .get_matches();

    let supported = uring::kernel_support();

    std::process::exit(!supported as i32)
}
