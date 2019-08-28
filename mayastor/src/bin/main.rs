extern crate libc;
#[macro_use]
extern crate log;
use sysfs;
extern crate git_version;

use git_version::git_version;
use mayastor::{mayastor_start, spdklog::SpdkLog};
use std::path::Path;

mayastor::CPS_INIT!();

fn main() -> Result<(), std::io::Error> {
    let log = SpdkLog::new();
    log.init().expect("Failed to set logger");

    let hugepage_path = Path::new("/sys/kernel/mm/hugepages/hugepages-2048kB");
    let nr_pages: u32 = sysfs::parse_value(&hugepage_path, "nr_hugepages")?;

    if nr_pages == 0 {
        info!("no hugepages available, allocating 512 default pages of 2048k");
        sysfs::write_value(&hugepage_path, "nr_hugepages", 512)?;
    }

    let free_pages: u32 = sysfs::parse_value(&hugepage_path, "free_hugepages")?;
    let nr_pages: u32 = sysfs::parse_value(&hugepage_path, "nr_hugepages")?;

    info!("free_pages: {} nr_pages: {}", free_pages, nr_pages);

    mayastor_start("MayaStor", std::env::args().collect(), || {
        info!("MayaStor started ({})...", git_version!());
    });
    Ok(())
}
