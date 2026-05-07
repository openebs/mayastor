use crate::{
    context::{Context, OutputFormat},
    GrpcStatus,
};
use byte_unit::Byte;
use clap::{Args, Subcommand};
use colored_json::ToColoredJson;
use futures::StreamExt;
use io_engine_api::v1 as v1_rpc;
use snafu::ResultExt;
use std::convert::TryInto;
use strum_macros::{AsRefStr, EnumString};
use uuid::Uuid;

fn parse_byte(s: &str) -> Result<Byte, String> {
    Byte::parse_str(s, true).map_err(|e| e.to_string())
}

#[derive(Debug, Args)]
#[command(subcommand_required = true, arg_required_else_help = true)]
pub struct TestArgs {
    #[command(subcommand)]
    command: TestCommands,
}

#[derive(Debug, Subcommand)]
enum TestCommands {
    /// Get the test features
    Features,
    /// Manage fault injections
    Inject(InjectArgs),
    /// Wipe a resource
    Wipe(WipeArgs),
}

#[derive(Debug, Args)]
struct InjectArgs {
    /// Injection URI(s) to add
    #[arg(short = 'a', long = "add", action = clap::ArgAction::Append)]
    add: Vec<String>,
    /// Injection URI(s) to remove
    #[arg(short = 'r', long = "remove", action = clap::ArgAction::Append)]
    remove: Vec<String>,
}

/// Resource type for wipe operations
#[derive(Debug, Clone, clap::ValueEnum, EnumString, AsRefStr)]
#[strum(serialize_all = "camelCase")]
pub enum Resource {
    Replica,
}

/// Wipe method
#[derive(Debug, Clone, Copy, clap::ValueEnum, EnumString)]
#[clap(rename_all = "PascalCase")]
#[strum(serialize_all = "PascalCase")]
pub enum WipeMethod {
    None,
    WriteZeroes,
    Unmap,
    WritePattern,
    CheckSum,
}

impl From<WipeMethod> for v1_rpc::test::wipe_options::WipeMethod {
    fn from(value: WipeMethod) -> Self {
        match value {
            WipeMethod::None => Self::None,
            WipeMethod::WriteZeroes => Self::WriteZeroes,
            WipeMethod::Unmap => Self::Unmap,
            WipeMethod::WritePattern => Self::WritePattern,
            WipeMethod::CheckSum => Self::Checksum,
        }
    }
}

impl From<WipeMethod> for v1_rpc::test::wipe_options::CheckSumAlgorithm {
    fn from(_: WipeMethod) -> Self {
        v1_rpc::test::wipe_options::CheckSumAlgorithm::Crc32c
    }
}

#[derive(Debug, Args)]
struct WipeArgs {
    /// Resource to wipe
    resource: Resource,
    /// Resource uuid
    uuid: Uuid,
    /// Uuid of the pool where the replica resides (conflicts with --pool-name)
    #[arg(long = "pool-uuid", conflicts_with = "pool_name")]
    pool_uuid: Option<Uuid>,
    /// Name of the pool where the replica resides (conflicts with --pool-uuid)
    #[arg(long = "pool-name", conflicts_with = "pool_uuid")]
    pool_name: Option<String>,
    /// Method used to wipe the replica
    #[arg(short = 'm', long, default_value = "WriteZeroes")]
    method: WipeMethod,
    /// Reporting back stats after each chunk is wiped
    #[arg(short = 'c', long = "chunk-size", value_parser = parse_byte)]
    chunk_size: Option<Byte>,
}

pub async fn handler(ctx: Context, args: TestArgs) -> crate::Result<()> {
    match args.command {
        TestCommands::Features => features(ctx).await,
        TestCommands::Inject(args) => injections(ctx, args).await,
        TestCommands::Wipe(args) => wipe(ctx, args).await,
    }
}

async fn features(mut ctx: Context) -> crate::Result<()> {
    let response = ctx.v1.test.get_features(()).await.context(GrpcStatus)?;
    let features = response.into_inner();
    match ctx.output {
        OutputFormat::Json => {
            let json = serde_json::to_string_pretty(&features).unwrap();
            println!("{json}");
        }
        OutputFormat::Default => {
            println!("{features:#?}");
        }
    }
    Ok(())
}

async fn wipe(ctx: Context, args: WipeArgs) -> crate::Result<()> {
    match args.resource {
        Resource::Replica => replica_wipe(ctx, args).await,
    }
}

async fn replica_wipe(mut ctx: Context, args: WipeArgs) -> crate::Result<()> {
    let uuid = args.uuid.to_string();

    let pool = match args.pool_uuid {
        Some(uuid) => Some(v1_rpc::test::wipe_replica_request::Pool::PoolUuid(
            uuid.to_string(),
        )),
        None => args
            .pool_name
            .map(v1_rpc::test::wipe_replica_request::Pool::PoolName),
    };

    let method = args.method;
    let chunk_size = args.chunk_size.unwrap_or(Byte::from_u64(0));

    let response = ctx
        .v1
        .test
        .wipe_replica(v1_rpc::test::WipeReplicaRequest {
            uuid,
            pool,
            wipe_options: Some(v1_rpc::test::StreamWipeOptions {
                options: Some(v1_rpc::test::WipeOptions {
                    wipe_method: v1_rpc::test::wipe_options::WipeMethod::from(method) as i32,
                    write_pattern: None,
                    cksum_alg: v1_rpc::test::wipe_options::CheckSumAlgorithm::from(method) as i32,
                }),
                chunk_size: chunk_size.as_u64(),
            }),
        })
        .await
        .context(GrpcStatus)?;

    let mut resp = response.into_inner();

    fn bandwidth(response: &v1_rpc::test::WipeReplicaResponse) -> String {
        let unknown = String::new();
        let Some(Ok(elapsed)) = response.since.map(TryInto::<std::time::Duration>::try_into) else {
            return unknown;
        };
        let elapsed_f = elapsed.as_secs_f64();
        if !elapsed_f.is_normal() {
            return unknown;
        }
        let bandwidth = (response.wiped_bytes as f64 / elapsed_f) as u64;
        format!(
            "{:.2}/s",
            Byte::from_u64(bandwidth).get_appropriate_unit(byte_unit::UnitType::Binary)
        )
    }

    fn checksum(response: &v1_rpc::test::WipeReplicaResponse) -> String {
        response
            .checksum
            .map(|c| match c {
                v1_rpc::test::wipe_replica_response::Checksum::Crc32(crc) => {
                    format!("{crc:#x}")
                }
            })
            .unwrap_or_default()
    }

    match ctx.output {
        OutputFormat::Json => {
            while let Some(response) = resp.next().await {
                let response = response.context(GrpcStatus)?;
                println!(
                    "{}",
                    serde_json::to_string_pretty(&response)
                        .unwrap()
                        .to_colored_json_auto()
                        .unwrap()
                );
            }
        }
        OutputFormat::Default => {
            let header = vec![
                "UUID",
                "TOTAL_BYTES",
                "CHUNK_SIZE",
                "LAST_CHUNK_SIZE",
                "TOTAL_CHUNKS",
                "WIPED_BYTES",
                "WIPED_CHUNKS",
                "REMAINING_BYTES",
                "BANDWIDTH",
                "CHECKSUM",
            ];

            let (s, r) = tokio::sync::mpsc::channel(10);
            tokio::spawn(async move {
                while let Some(response) = resp.next().await {
                    let response = response.map(|response| {
                        let bandwidth = format!("{: <12}", bandwidth(&response));
                        let checksum = checksum(&response);
                        vec![
                            response.uuid,
                            adjust_bytes(response.total_bytes),
                            adjust_bytes(response.chunk_size),
                            adjust_bytes(response.last_chunk_size),
                            response.total_chunks.to_string(),
                            adjust_bytes(response.wiped_bytes),
                            response.wiped_chunks.to_string(),
                            adjust_bytes(response.remaining_bytes),
                            bandwidth,
                            checksum,
                        ]
                    });
                    s.send(response).await.unwrap();
                }
            });
            ctx.print_streamed_list(header, r)
                .await
                .context(GrpcStatus)?;
        }
    }

    Ok(())
}

fn adjust_bytes(bytes: u64) -> String {
    let byte = Byte::from_u64(bytes);
    let adjusted_byte = byte.get_appropriate_unit(byte_unit::UnitType::Binary);
    format!("{adjusted_byte:.2}")
}

async fn injections(mut ctx: Context, args: InjectArgs) -> crate::Result<()> {
    if args.add.is_empty() && args.remove.is_empty() {
        return list_injections(ctx).await;
    }

    for uri in &args.add {
        println!("Injection: '{uri}'");
        ctx.v1
            .test
            .add_fault_injection(v1_rpc::test::AddFaultInjectionRequest {
                uri: uri.to_owned(),
            })
            .await
            .context(GrpcStatus)?;
    }

    for uri in &args.remove {
        println!("Removing injected fault: {uri}");
        ctx.v1
            .test
            .remove_fault_injection(v1_rpc::test::RemoveFaultInjectionRequest {
                uri: uri.to_owned(),
            })
            .await
            .context(GrpcStatus)?;
    }

    Ok(())
}

async fn list_injections(mut ctx: Context) -> crate::Result<()> {
    let response = ctx
        .v1
        .test
        .list_fault_injections(v1_rpc::test::ListFaultInjectionsRequest {})
        .await
        .context(GrpcStatus)?;

    println!(
        "{}",
        serde_json::to_string_pretty(response.get_ref())
            .unwrap()
            .to_colored_json_auto()
            .unwrap()
    );

    Ok(())
}
