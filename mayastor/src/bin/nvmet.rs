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
use mayastor::{
    bdev::{nexus_create, nexus_lookup},
    core::{MayastorCliArgs, MayastorEnvironment, Reactors, Share},
    grpc,
    logger,
};

mayastor::CPS_INIT!();
#[macro_use]
extern crate tracing;

const NEXUS: &str = "nexus-e1e27668-fbe1-4c8a-9108-513f6e44d342";

async fn create_nexus(args: &ArgMatches<'_>) {
    let ep = args.values_of("uri").expect("invalid endpoints");

    let size = args.value_of("size").unwrap().parse::<u64>().unwrap();

    let children = ep.into_iter().map(|v| v.into()).collect::<Vec<String>>();

    nexus_create(NEXUS, size * 1024 * 1024, Some(NEXUS), &children)
        .await
        .unwrap();

    let nexus = nexus_lookup(NEXUS).unwrap();
    nexus.share_nvmf(None).await.unwrap();
}

fn main() {
    let matches = App::new("NVMeT CLI")
        .version("0.1")
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

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let grpc_endpoint = grpc::endpoint(margs.grpc_endpoint.clone());
    let rpc_address = margs.rpc_address.clone();

    let guard = rt.enter();
    let ms = MayastorEnvironment::new(margs).init();
    drop(guard);

    let master = Reactors::master();

    master.send_future(async { info!("NVMeT started {} ...", '\u{1F680}') });
    master.send_future(async move {
        create_nexus(&matches).await;
    });

    let futures = vec![
        master.boxed_local(),
        grpc::MayastorGrpcServer::run(grpc_endpoint, rpc_address).boxed_local(),
    ];

    rt.block_on(futures::future::try_join_all(futures))
        .expect_err("reactor exit in abnormal state");

    ms.fini();
}
