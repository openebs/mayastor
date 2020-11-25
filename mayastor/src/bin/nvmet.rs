use clap::{App, AppSettings, Arg, ArgMatches};
use futures::FutureExt;
use mayastor::{
    bdev::{nexus_create, nexus_lookup, util::uring},
    core::{MayastorCliArgs, MayastorEnvironment, Reactors, Share},
    grpc,
    grpc::endpoint,
    logger,
    subsys,
};
use std::path::Path;
mayastor::CPS_INIT!();
#[macro_use]
extern crate tracing;

const NEXUS: &str = "nexus-e1e27668-fbe1-4c8a-9108-513f6e44d342";

async fn create_nexus(args: &ArgMatches<'_>) {
    let ep = args.values_of("endpoint").expect("invalid endpoints");

    let children = ep
        .into_iter()
        .map(|v| {
            dbg!(v);
            format!("nvmf://{}/replica1", v)
        })
        .collect::<Vec<String>>();

    nexus_create(NEXUS, 250 * 1024 * 1024 * 1024, Some(NEXUS), &children)
        .await
        .unwrap();

    let nexus = nexus_lookup(NEXUS).unwrap();
    nexus.share_nvmf().await.unwrap();
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    std::env::set_var("NEXUS_LABEL_IGNORE_ERRORS", "1");
    std::env::set_var("MY_POD_IP", "192.168.1.4");

    let matches = App::new("NVMeT CLI")
        .version("0.1")
        .settings(&[
            AppSettings::ColoredHelp,
            AppSettings::ColorAlways])
        .about("NVMe test utility to quickly create a nexus over existing nvme targets")
        .arg(
            Arg::with_name("replica-index")
                .short("n")
                .long("replica-index")
                .default_value("1")
                .help("index of the NQN to connect to e.g 1 for replica1"))
        .arg(
            Arg::with_name("endpoint")
                .short("e")
                .min_values(1)
                .long("endpoint")
                .help("endpoints to connect to"))
        .get_matches();

    let mut margs = MayastorCliArgs::default();

    margs.rpc_address = "0.0.0.0:10124".to_string();

    logger::init("INFO");

    let mut rt = tokio::runtime::Builder::new()
        .basic_scheduler()
        .enable_all()
        .build()
        .unwrap();

    let grpc_endpoint = grpc::endpoint(margs.grpc_endpoint.clone());
    let rpc_address = margs.rpc_address.clone();

    let ms = rt.enter(|| MayastorEnvironment::new(margs).init());
    let master = Reactors::master();

    master.send_future(async { info!("NVMeT started {} ...", '\u{1F680}') });
    master.send_future(async move {
        create_nexus(&matches).await;
    });

    let mut futures = Vec::new();

    futures.push(master.boxed_local());
    futures.push(subsys::Registration::run().boxed_local());
    futures.push(
        grpc::MayastorGrpcServer::run(grpc_endpoint, rpc_address).boxed_local(),
    );

    rt.block_on(futures::future::try_join_all(futures))
        .expect_err("reactor exit in abnormal state");

    ms.fini();

    Ok(())
}
