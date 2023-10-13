use crate::{
    context::{Context, OutputFormat},
    ClientError,
    GrpcStatus,
};
use clap::{Arg, ArgMatches, Command};
use colored_json::ToColoredJson;
use mayastor_api::v0 as rpc;
use snafu::ResultExt;
use tracing::debug;

pub fn subcommands() -> Command {
    Command::new("jsonrpc")
        .arg_required_else_help(true)
        .about("Call a json-rpc method with a raw JSON payload")
        .arg(
            Arg::new("method")
                .required(true)
                .index(1)
                .help("Name of method to call"),
        )
        .arg(
            Arg::new("params")
                .default_value("")
                .index(2)
                .help("Parameters (JSON string) to pass to method call"),
        )
}

pub async fn json_rpc_call(
    mut ctx: Context,
    matches: &ArgMatches,
) -> crate::Result<()> {
    let method = matches
        .get_one::<String>("method")
        .ok_or_else(|| ClientError::MissingValue {
            field: "method".to_string(),
        })?
        .to_owned();
    let params = matches
        .get_one::<String>("params")
        .ok_or_else(|| ClientError::MissingValue {
            field: "params".to_string(),
        })?
        .to_owned();

    let response = ctx
        .json
        .json_rpc_call(rpc::JsonRpcRequest {
            method,
            params,
        })
        .await
        .context(GrpcStatus)?;

    if ctx.output == OutputFormat::Default {
        debug!("Default output for jsonrpc calls is JSON.");
    };

    println!(
        "{}",
        response.get_ref().result.to_colored_json_auto().unwrap()
    );

    Ok(())
}
