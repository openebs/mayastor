use crate::{
    context::{Context, OutputFormat},
    GrpcStatus,
};
use clap::{Args, Subcommand};
use colored_json::ToColoredJson;
use io_engine_api::v1 as v1rpc;
use snafu::ResultExt;
use uuid::Uuid;

#[derive(Debug, Args)]
#[command(subcommand_required = true, arg_required_else_help = true)]
pub struct ChildArgs {
    #[command(subcommand)]
    command: ChildCommands,
}

#[derive(Debug, Subcommand)]
enum ChildCommands {
    /// fault a child
    Fault(FaultArgs),
    /// offline a child
    Offline(ChildOpArgs),
    /// online a child
    Online(ChildOpArgs),
    /// retire a child
    Retire(ChildOpArgs),
}

#[derive(Debug, Args)]
struct FaultArgs {
    uuid: Uuid,
    uri: String,
}

#[derive(Debug, Args)]
struct ChildOpArgs {
    uuid: Uuid,
    uri: String,
}

pub async fn handler(ctx: Context, args: ChildArgs) -> crate::Result<()> {
    match args.command {
        ChildCommands::Fault(args) => fault(ctx, args).await,
        ChildCommands::Offline(args) => child_operation(ctx, args, 0).await,
        ChildCommands::Online(args) => child_operation(ctx, args, 1).await,
        ChildCommands::Retire(args) => child_operation(ctx, args, 2).await,
    }
}

async fn fault(mut ctx: Context, args: FaultArgs) -> crate::Result<()> {
    let uuid = args.uuid.to_string();
    let uri = args.uri;
    let response = ctx
        .v1
        .nexus
        .fault_nexus_child(v1rpc::nexus::FaultNexusChildRequest {
            uuid,
            uri: uri.clone(),
        })
        .await
        .context(GrpcStatus)?;
    match ctx.output {
        OutputFormat::Json => {
            let json = serde_json::to_string_pretty(&response.get_ref())
                .unwrap()
                .to_colored_json_auto()
                .unwrap();
            println!("{json}");
        }
        OutputFormat::Default => {
            println!("{uri}");
        }
    };
    Ok(())
}

async fn child_operation(mut ctx: Context, args: ChildOpArgs, action: i32) -> crate::Result<()> {
    let uuid = args.uuid.to_string();
    let uri = args.uri;
    let response = ctx
        .v1
        .nexus
        .child_operation(v1rpc::nexus::ChildOperationRequest {
            nexus_uuid: uuid,
            uri: uri.clone(),
            action,
        })
        .await
        .context(GrpcStatus)?;
    match ctx.output {
        OutputFormat::Json => {
            let json = serde_json::to_string_pretty(&response.get_ref())
                .unwrap()
                .to_colored_json_auto()
                .unwrap();
            println!("{json}");
        }
        OutputFormat::Default => {
            println!("{uri}");
        }
    };
    Ok(())
}
