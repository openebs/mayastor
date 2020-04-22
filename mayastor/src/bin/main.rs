#[macro_use]
extern crate log;

use std::path::Path;

use structopt::StructOpt;

use git_version::git_version;
use mayastor::{
    bdev::uring_util,
    core::{MayastorCliArgs, MayastorEnvironment},
    logger,
};

mayastor::CPS_INIT!();

fn main() -> Result<(), std::io::Error> {
    let args = MayastorCliArgs::from_args();

    // setup our logger first if -L is passed, raise the log level
    // automatically. trace maps to debug at FFI level. If RUST_LOG is
    // passed, we will use it regardless.
    if !args.log_components.is_empty() {
        logger::init("TRACE");
    } else {
        logger::init("INFO");
    }

    let hugepage_path = Path::new("/sys/kernel/mm/hugepages/hugepages-2048kB");
    let nr_pages: u32 = sysfs::parse_value(&hugepage_path, "nr_hugepages")?;

    if nr_pages == 0 {
        warn!("No hugepages available, trying to allocate 512 2MB hugepages");
        sysfs::write_value(&hugepage_path, "nr_hugepages", 512)?;
    }

    let free_pages: u32 = sysfs::parse_value(&hugepage_path, "free_hugepages")?;
    let nr_pages: u32 = sysfs::parse_value(&hugepage_path, "nr_hugepages")?;
    let uring_supported = uring_util::kernel_support();

    info!("Starting Mayastor version {}", git_version!());
    info!(
        "kernel io_uring support: {}",
        if uring_supported { "yes" } else { "no" }
    );
    info!("free_pages: {} nr_pages: {}", free_pages, nr_pages);
    let mut env = MayastorEnvironment::new(args);
    env.enable_grpc = true;
    env.start(|| {
        info!("Mayastor started {} ({})...", '\u{1F680}', git_version!());
    })
    .unwrap();
    Ok(())
}
