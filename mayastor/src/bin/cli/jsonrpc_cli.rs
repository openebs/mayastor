use super::context::Context;
use ::rpc::mayastor as rpc;
use clap::{App, Arg, ArgMatches, SubCommand};
use colored_json::ToColoredJson;
use tonic::Status;

pub fn subcommands<'a, 'b>() -> App<'a, 'b> {
    SubCommand::with_name("jsonrpc")
        .about("Call a json-rpc method with a raw JSON payload")
        .arg(
            Arg::with_name("method")
                .required(true)
                .index(1)
                .help("Name of method to call"),
        )
        .arg(
            Arg::with_name("params")
                .default_value("")
                .index(2)
                .help("Parameters (JSON string) to pass to method call"),
        )
}

pub async fn json_rpc_call(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> Result<(), Status> {
    let method = matches.value_of("method").unwrap().to_owned();
    let params = matches.value_of("params").unwrap().to_owned();

    let reply = ctx
        .json
        .json_rpc_call(rpc::JsonRpcRequest {
            method,
            params,
        })
        .await?;

    println!("{}", reply.get_ref().result.to_colored_json_auto().unwrap());

    Ok(())
}
