use clap::Parser;
use io_engine::bdev::util::uring;
use version_info::version_info_str;

/// Determines io_uring support.
#[derive(Debug, Parser)]
#[command(
    name = "Detect io_uring support",
    version = version_info_str!(),
    author = "Jonathan Teh <jonathan.teh@mayadata.io>",
    about = "Determines io_uring support"
)]
struct Args {}

fn main() {
    let _args = Args::parse();

    let supported = uring::kernel_support();

    std::process::exit(!supported as i32)
}
