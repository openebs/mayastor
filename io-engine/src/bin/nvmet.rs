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
use clap::Parser;
use futures::FutureExt;
use io_engine::{
    bdev::nexus::{nexus_create, nexus_lookup_mut},
    core::{MayastorCliArgs, MayastorEnvironment, Mthread, Reactors, Share},
    grpc, logger,
};
use version_info::version_info_string;

io_engine::CPS_INIT!();

const NEXUS: &str = "nexus-e1e27668-fbe1-4c8a-9108-513f6e44d342";

fn start_tokio_runtime(args: &MayastorCliArgs) {
    let node_name = grpc::node_name(&args.node_name);
    let node_nqn = args.make_hostnqn();
    let grpc_endpoint = args.grpc_endpoint();
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

/// NVMe test utility to quickly create a nexus over existing nvme targets.
#[derive(Debug, Parser)]
#[command(
    name = "NVMeT CLI",
    version = version_info_string!(),
    about = "NVMe test utility to quickly create a nexus over existing nvme targets"
)]
struct Args {
    /// Size of the nexus to create in MB.
    #[arg(short = 's', long, default_value_t = 64)]
    size: u64,

    /// NVMe-OF TCP targets to connect to.
    #[arg(short = 'u', long = "uris", required = true, num_args = 1..)]
    uri: Vec<url::Url>,
}

async fn create_nexus(args: &Args) {
    let size = args.size;
    let children = args.uri.iter().map(|u| u.to_string()).collect::<Vec<_>>();

    nexus_create(NEXUS, size * 1024 * 1024, Some(NEXUS), &children)
        .await
        .unwrap();

    let nexus = nexus_lookup_mut(NEXUS).unwrap();
    nexus.share_nvmf(None).await.unwrap();
}

fn main() {
    let args = Args::parse();

    let margs = MayastorCliArgs {
        rpc_address: "0.0.0.0:10124".to_string(),
        reactor_mask: "0xF".to_string(),
        ..Default::default()
    };

    logger::init("io_engine=trace");

    let ms = MayastorEnvironment::new(margs.clone()).init();
    start_tokio_runtime(&margs);
    Reactors::current().send_future(async move { create_nexus(&args).await });

    Reactors::current().running();
    Reactors::current().poll_reactor();

    ms.fini();
}
