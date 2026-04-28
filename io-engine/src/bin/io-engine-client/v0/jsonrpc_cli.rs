use crate::{
    context::{Context, OutputFormat},
    GrpcStatus,
};
use clap::Args;
use colored_json::ToColoredJson;
use io_engine_api::v0 as rpc;
use snafu::ResultExt;
use tracing::debug;

#[derive(Debug, Args)]
#[command(
    about = "Call a json-rpc method with a raw JSON payload",
    arg_required_else_help = true
)]
pub struct JsonrpcArgs {
    /// Name of method to call
    method: String,
    /// Parameters (JSON string) to pass to method call
    #[arg(default_value = "")]
    params: String,
}

pub async fn json_rpc_call(mut ctx: Context, args: JsonrpcArgs) -> crate::Result<()> {
    let method = args.method;
    let params = args.params;

    let response = ctx
        .json
        .json_rpc_call(rpc::JsonRpcRequest { method, params })
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
