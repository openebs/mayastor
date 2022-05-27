//!
//! This utility assists in testing nexus behaviour by simply allow one to
//! start it against well-known targets. Example usage is:
//!
//! ```bash
//! nvmet -u nvmf://10.1.0.101/replica1 \
//!         nvmf://10.1.0.102/replica1 \
//!         nvmf://10.1.0.103/replica1
//! ```
//! This will start a nexus which is shared over MY_POD_IP. Another env variable
//! is set to ignore labeling errors. This does not work for rebuild tests
//! however.
use clap::{App, AppSettings, Arg, ArgMatches};
use futures::FutureExt;
use io_engine::{
    bdev::nexus::{nexus_create, nexus_lookup_mut},
    core::{MayastorCliArgs, MayastorEnvironment, Mthread, Reactors, Share},
    grpc,
    logger,
};
use version_info::version_info_str;

io_engine::CPS_INIT!();
extern crate tracing;

const NEXUS: &str = "nexus-e1e27668-fbe1-4c8a-9108-513f6e44d342";

fn start_tokio_runtime(args: &MayastorCliArgs) {
    let grpc_endpoint = grpc::endpoint(args.grpc_endpoint.clone());
    let rpc_address = args.rpc_address.clone();

    Mthread::spawn_unaffinitized(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .max_blocking_threads(4)
            .on_thread_start(Mthread::unaffinitize)
            .enable_all()
            .build()
            .unwrap();

        let futures =
            vec![grpc::MayastorGrpcServer::run(grpc_endpoint, rpc_address)
                .boxed_local()];

        rt.block_on(futures::future::try_join_all(futures))
            .expect_err("reactor exit in abnormal state");
    });
}

async fn create_nexus(args: &ArgMatches<'_>) {
    let ep = args.values_of("uri").expect("invalid endpoints");

    let size = args.value_of("size").unwrap().parse::<u64>().unwrap();

    let children = ep.into_iter().map(|v| v.into()).collect::<Vec<String>>();

    nexus_create(NEXUS, size * 1024 * 1024, Some(NEXUS), &children)
        .await
        .unwrap();

    let nexus = nexus_lookup_mut(NEXUS).unwrap();
    nexus.share_nvmf(None).await.unwrap();
}

fn main() {
    let matches = App::new("NVMeT CLI")
        .version(version_info_str!())
        .settings(&[
            AppSettings::ColoredHelp,
            AppSettings::ColorAlways])
        .about("NVMe test utility to quickly create a nexus over existing nvme targets")
        .arg(Arg::with_name("size")
            .required(true)
            .default_value("64")
            .short("s")
            .long("size")
            .help("Size of the nexus to create in MB")
        )
        .arg(
            Arg::with_name("uri")
                .short("u")
                .min_values(1)
                .long("uris")
                .help("NVMe-OF TCP targets to connect to"))
        .get_matches();

    let margs = MayastorCliArgs {
        rpc_address: "0.0.0.0:10124".to_string(),
        reactor_mask: "0xF".to_string(),
        ..Default::default()
    };

    logger::init("mayastor=trace");

    let ms = MayastorEnvironment::new(margs.clone()).init();
    start_tokio_runtime(&margs);
    Reactors::current()
        .send_future(async move { create_nexus(&matches).await });

    Reactors::current().running();
    Reactors::current().poll_reactor();

    ms.fini();
}
