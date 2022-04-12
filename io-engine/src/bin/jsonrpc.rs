//! The tool is currently a bit over-engineered because it used to have many
//! subcommands with user friendly arguments. That's no longer true as
//! currently we use io-engine-client tool that uses gRPC API for that.
//! Nevertheless the complex code for parsing subcommands has been preserved
//! just in case that we would like to make the tool smarter again in the
//! future. The only motivation for keeping the command now is to be able to
//! send raw json to SPDK json-rpc methods.

extern crate serde_json;

use jsonrpc::call;
use structopt::StructOpt;
use version_info::{package_description, version_info_str};

/// TODO
#[derive(Debug, StructOpt)]
#[structopt(
    name = package_description!(),
    version = version_info_str!(),
    about = "Mayastor json-rpc client",
    setting = structopt::clap::AppSettings::ColoredHelp
)]
struct Opt {
    #[structopt(short = "s", default_value = "/var/tmp/mayastor.sock")]
    socket: String,
    #[structopt(subcommand)]
    cmd: Sub,
}

/// TODO
#[derive(StructOpt, Debug)]
enum Sub {
    #[structopt(name = "raw")]
    /// call a method with a raw JSON payload
    Raw {
        #[structopt(name = "method")]
        method: String,
        arg: Option<String>,
    },
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let opt = Opt::from_args();

    let fut = match opt.cmd {
        Sub::Raw {
            method,
            arg,
        } => {
            if let Some(arg) = arg {
                let args: serde_json::Value = serde_json::from_str(&arg)?;

                let out: serde_json::Value =
                    call(&opt.socket, &method, Some(args)).await?;
                // we don't always get valid json back which is a bug in the RPC
                // method really.
                if let Ok(json) = serde_json::to_string_pretty(&out) {
                    json
                } else {
                    dbg!(out);
                    "".into()
                }
            } else {
                serde_json::to_string_pretty(
                    &call::<(), serde_json::Value>(&opt.socket, &method, None)
                        .await?,
                )?
            }
        }
    };
    println!("{}", fut);

    Ok(())
}
