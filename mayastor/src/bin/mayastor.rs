#[macro_use]
extern crate tracing;

use futures::FutureExt;
use mayastor::{
    bdev::util::uring,
    core::{MayastorCliArgs, MayastorEnvironment, Reactors},
    grpc,
    logger,
    subsys,
};
use std::path::Path;
use structopt::StructOpt;
mayastor::CPS_INIT!();
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = MayastorCliArgs::from_args();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

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
    let uring_supported = uring::kernel_support();
    let nvme_core_path = Path::new("/sys/module/nvme_core/parameters");
    let nvme_mp: String =
        match sysfs::parse_value::<String>(&nvme_core_path, "multipath") {
            Ok(s) => match s.as_str() {
                "Y" => "yes".to_string(),
                "N" => "disabled".to_string(),
                u => format!("unknown value {}", u),
            },
            Err(e) => {
                if e.kind() == std::io::ErrorKind::NotFound {
                    if nvme_core_path.exists() {
                        "not built".to_string()
                    } else {
                        "nvme not loaded".to_string()
                    }
                } else {
                    format!("unknown error: {}", e)
                }
            }
        };

    info!("Starting Mayastor ..");
    info!(
        "kernel io_uring support: {}",
        if uring_supported { "yes" } else { "no" }
    );
    info!("kernel nvme initiator multipath support: {}", nvme_mp);
    info!("free_pages: {} nr_pages: {}", free_pages, nr_pages);

    let grpc_endpoint = grpc::endpoint(args.grpc_endpoint.clone());
    let rpc_address = args.rpc_address.clone();

    let guard = rt.enter();
    let ms = MayastorEnvironment::new(args).init();
    drop(guard);

    let master = Reactors::master();
    master.send_future(async { info!("Mayastor started {} ...", '\u{1F680}') });
    let futures = vec![
        master.boxed_local(),
        subsys::Registration::run().boxed_local(),
        grpc::MayastorGrpcServer::run(grpc_endpoint, rpc_address).boxed_local(),
    ];

    rt.block_on(futures::future::try_join_all(futures))
        .expect_err("reactor exit in abnormal state");

    ms.fini();
    Ok(())
}
