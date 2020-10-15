#[macro_use]
extern crate clap;

use byte_unit::Byte;
use clap::{App, AppSettings, Arg};
use tonic::{transport::Channel, Status};

use ::rpc::mayastor::{
    bdev_rpc_client::BdevRpcClient,
    json_rpc_client::JsonRpcClient,
    mayastor_client::MayastorClient,
};

use crate::context::Context;

mod bdev_cli;
mod context;
mod device_cli;
mod jsonrpc_cli;
mod nexus_child_cli;
mod nexus_cli;
mod pool_cli;
mod rebuild_cli;
mod replica_cli;
mod snapshot_cli;

type MayaClient = MayastorClient<Channel>;
type BdevClient = BdevRpcClient<Channel>;
type JsonClient = JsonRpcClient<Channel>;

pub(crate) fn parse_size(src: &str) -> Result<Byte, String> {
    Byte::from_str(src).map_err(|_| src.to_string())
}

#[tokio::main(max_threads = 2)]
async fn main() -> Result<(), Status> {
    env_logger::init();

    let matches = App::new("Mayastor CLI")
        .version("0.1")
        .settings(&[
            AppSettings::SubcommandRequiredElseHelp,
            AppSettings::ColoredHelp,
            AppSettings::ColorAlways])
        .about("CLI utility for Mayastor")
        .arg(
            Arg::with_name("address")
                .short("a")
                .long("address")
                .default_value("127.0.0.1")
                .value_name("HOST")
                .help("IP address of mayastor instance"))
        .arg(
            Arg::with_name("port")
                .short("p")
                .long("port")
                .default_value("10124").value_name("NUMBER")
                .help("Port number of mayastor server"))
        .arg(
            Arg::with_name("quiet")
                .short("q")
                .long("quiet")
                .help("Do not print any output except for list records"))
        .arg(
            Arg::with_name("verbose")
                .short("v")
                .long("verbose")
                .multiple(true)
                .help("Verbose output")
                .conflicts_with("quiet"))
        .arg(
            Arg::with_name("units")
                .short("u")
                .long("units")
                .value_name("BASE")
                .possible_values(&["i", "d"])
                .hide_possible_values(true)
                .next_line_help(true)
                .help("Output with large units: i for kiB, etc. or d for kB, etc."))
        .subcommand(pool_cli::subcommands())
        .subcommand(nexus_cli::subcommands())
        .subcommand(replica_cli::subcommands())
        .subcommand(bdev_cli::subcommands())
        .subcommand(device_cli::subcommands())
        .subcommand(rebuild_cli::subcommands())
        .subcommand(snapshot_cli::subcommands())
        .subcommand(jsonrpc_cli::subcommands())
        .get_matches();

    let ctx = Context::new(&matches).await;

    match matches.subcommand() {
        ("bdev", Some(args)) => bdev_cli::handler(ctx, args).await?,
        ("device", Some(args)) => device_cli::handler(ctx, args).await?,
        ("nexus", Some(args)) => nexus_cli::handler(ctx, args).await?,
        ("pool", Some(args)) => pool_cli::handler(ctx, args).await?,
        ("replica", Some(args)) => replica_cli::handler(ctx, args).await?,
        ("rebuild", Some(args)) => rebuild_cli::handler(ctx, args).await?,
        ("snapshot", Some(args)) => snapshot_cli::handler(ctx, args).await?,
        ("jsonrpc", Some(args)) => {
            jsonrpc_cli::json_rpc_call(ctx, args).await?
        }

        _ => eprintln!("Internal Error: Not implemented"),
    };
    Ok(())
}
