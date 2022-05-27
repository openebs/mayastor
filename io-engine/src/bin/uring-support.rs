extern crate clap;

use clap::App;

use io_engine::bdev::util::uring;
use version_info::version_info_str;

fn main() {
    let _matches = App::new("Detect io_uring support")
        .version(version_info_str!())
        .author("Jonathan Teh <jonathan.teh@mayadata.io>")
        .about("Determines io_uring support")
        .get_matches();

    let supported = uring::kernel_support();

    std::process::exit(!supported as i32)
}
