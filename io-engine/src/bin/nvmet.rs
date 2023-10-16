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
use clap::{Arg, ArgMatches, Command};
use futures::FutureExt;
use io_engine::{
    bdev::nexus::{nexus_create, nexus_lookup_mut},
    core::{MayastorCliArgs, MayastorEnvironment, Mthread, Reactors, Share},
    grpc,
    logger,
};
use version_info::version_info_str;

io_engine::CPS_INIT!();

const NEXUS: &str = "nexus-e1e27668-fbe1-4c8a-9108-513f6e44d342";

fn start_tokio_runtime(args: &MayastorCliArgs) {
    let node_name = grpc::node_name(&args.node_name);
    let node_nqn = args.make_hostnqn();
    let grpc_endpoint = grpc::endpoint(args.grpc_endpoint.clone());
    let rpc_address = args.rpc_address.clone();
    let api_versions = args.api_versions.clone();

    Mthread::spawn_unaffinitized(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .max_blocking_threads(4)
            .on_thread_start(Mthread::unaffinitize)
            .enable_all()
            .build()
            .unwrap();

        let futures = vec![grpc::MayastorGrpcServer::run(
            &node_name,
            &node_nqn,
            grpc_endpoint,
            rpc_address,
            api_versions,
        )
        .boxed_local()];

        rt.block_on(futures::future::try_join_all(futures))
            .expect_err("reactor exit in abnormal state");
    });
}

async fn create_nexus(args: &ArgMatches) {
    let ep = args.get_many::<String>("uri").expect("invalid endpoints");

    let size = args
        .get_one::<String>("size")
        .unwrap()
        .parse::<u64>()
        .unwrap();

    let children = ep.into_iter().map(|v| v.into()).collect::<Vec<String>>();

    nexus_create(NEXUS, size * 1024 * 1024, Some(NEXUS), &children)
        .await
        .unwrap();

    let nexus = nexus_lookup_mut(NEXUS).unwrap();
    nexus.share_nvmf(None).await.unwrap();
}

fn main() {
    let matches = Command::new("NVMeT CLI")
        .version(version_info_str!())
        .about("NVMe test utility to quickly create a nexus over existing nvme targets")
        .arg(Arg::new("size")
            .default_value("64")
            .short('s')
            .long("size")
            .help("Size of the nexus to create in MB")
        )
        .arg(
            Arg::new("uri")
                .short('u')
                .required(true)
                .long("uris")
                .help("NVMe-OF TCP targets to connect to"))
        .get_matches();

    let margs = MayastorCliArgs {
        rpc_address: "0.0.0.0:10124".to_string(),
        reactor_mask: "0xF".to_string(),
        ..Default::default()
    };

    logger::init("io_engine=trace");

    let ms = MayastorEnvironment::new(margs.clone()).init();
    start_tokio_runtime(&margs);
    Reactors::current()
        .send_future(async move { create_nexus(&matches).await });

    Reactors::current().running();
    Reactors::current().poll_reactor();

    ms.fini();
}
