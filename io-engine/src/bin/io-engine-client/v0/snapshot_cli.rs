use crate::{
    context::{Context, OutputFormat},
    GrpcStatus,
};
use clap::{Args, Subcommand};
use colored_json::ToColoredJson;
use io_engine_api::v0 as rpc;
use snafu::ResultExt;
use uuid::Uuid;

#[derive(Debug, Args)]
#[command(subcommand_required = true, arg_required_else_help = true)]
pub struct SnapshotArgs {
    #[command(subcommand)]
    command: SnapshotCommands,
}

#[derive(Debug, Subcommand)]
enum SnapshotCommands {
    /// create a snapshot
    Create(CreateArgs),
}

#[derive(Debug, Args)]
struct CreateArgs {
    /// uuid of the nexus
    uuid: Uuid,
}

pub async fn handler(ctx: Context, args: SnapshotArgs) -> crate::Result<()> {
    match args.command {
        SnapshotCommands::Create(args) => create(ctx, args).await,
    }
}

async fn create(mut ctx: Context, args: CreateArgs) -> crate::Result<()> {
    let uuid = args.uuid.to_string();
    let response = ctx
        .client
        .create_snapshot(rpc::CreateSnapshotRequest { uuid: uuid.clone() })
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
            println!("{uuid}");
        }
    };
    Ok(())
}
