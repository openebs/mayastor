use crate::{
    context::{Context, OutputFormat},
    GrpcStatus,
};
use byte_unit::Byte;
use clap::{Args, Subcommand};
use colored_json::ToColoredJson;
use io_engine_api::{v1 as v1rpc, v1::pool::Pool};
use snafu::ResultExt;
use std::convert::TryFrom;
use tonic::Status;
use uuid::Uuid;

fn parse_byte(s: &str) -> Result<Byte, String> {
    Byte::parse_str(s, true).map_err(|e| e.to_string())
}

#[derive(Debug, Args)]
#[command(subcommand_required = true, arg_required_else_help = true)]
pub struct PoolArgs {
    #[command(subcommand)]
    command: PoolCommands,
}

#[derive(Debug, Subcommand)]
enum PoolCommands {
    /// Create new or import existing storage pool
    Create(CreateArgs),
    /// Import existing storage pool, fail if pool does not exist
    Import(ImportArgs),
    /// Destroy storage pool
    Destroy(DestroyArgs),
    /// Export storage pool without destroying it
    Export(ExportArgs),
    /// Expand a storage pool to span the entire underlying device
    Expand(ExpandArgs),
    /// List storage pools
    List(ListArgs),
    /// Clears errors from the storage pool
    #[command(name = "clear-errors")]
    ClearErrors(ClearErrorsArgs),
    /// Probes storage pool
    Probe(ProbeArgs),
}

#[derive(Debug, Args)]
struct CreateArgs {
    /// Storage pool name
    pool: String,
    /// Disk device files
    #[arg(action = clap::ArgAction::Append)]
    disk: Vec<String>,
    #[arg(long)]
    uuid: Option<Uuid>,
    #[arg(long = "cluster-size", value_parser = parse_byte)]
    cluster_size: Option<Byte>,
    #[arg(long = "max-expansion")]
    max_expansion: Option<String>,
    #[arg(short = 't', long = "type", default_value = "lvs")]
    pool_type: PoolType,
    #[arg(short = 'c', long, requires = "encryption_key")]
    cipher: Option<Cipher>,
    #[arg(short = 'k', long = "encryption-key")]
    encryption_key: Option<String>,
    #[arg(short = 'e', long = "xts-key")]
    xts_key: Option<String>,
}

#[derive(Debug, Args)]
struct ImportArgs {
    /// Storage pool name
    pool: String,
    /// Disk device files
    #[arg(action = clap::ArgAction::Append)]
    disk: Vec<String>,
    #[arg(short = 'u', long)]
    uuid: Option<Uuid>,
    #[arg(short = 't', long = "type", default_value = "lvs")]
    pool_type: PoolType,
    #[arg(short = 'c', long, requires = "encryption_key")]
    cipher: Option<Cipher>,
    #[arg(short = 'k', long = "encryption-key")]
    encryption_key: Option<String>,
    #[arg(short = 'e', long = "xts-key")]
    xts_key: Option<String>,
}

#[derive(Debug, Args)]
struct DestroyArgs {
    /// Storage pool name
    pool: String,
    #[arg(short = 'u', long)]
    uuid: Option<Uuid>,
    #[arg(short = 't', long = "type", default_value = "lvs")]
    pool_type: PoolType,
}

#[derive(Debug, Args)]
struct ExportArgs {
    /// Storage pool name
    name: String,
    #[arg(short = 'u', long)]
    uuid: Option<Uuid>,
    #[arg(short = 't', long = "type", default_value = "lvs")]
    pool_type: PoolType,
}

#[derive(Debug, Args)]
struct ExpandArgs {
    /// Storage pool name
    name: String,
    #[arg(short = 'u', long)]
    uuid: Option<Uuid>,
    #[arg(short = 't', long = "type", default_value = "lvs")]
    pool_type: PoolType,
}

#[derive(Debug, Args)]
struct ListArgs {
    /// Storage pool name
    name: Option<String>,
    /// Storage pool uuid
    uuid: Option<Uuid>,
    #[arg(short = 't', long = "type")]
    pool_type: Option<PoolType>,
}

#[derive(Debug, Args)]
struct ClearErrorsArgs {
    /// Storage pool name
    name: String,
    /// Disk devices to clear errors or all if not specified
    #[arg(action = clap::ArgAction::Append)]
    disk: Vec<String>,
    #[arg(short = 'u', long)]
    uuid: Option<Uuid>,
}

#[derive(Debug, Args)]
struct ProbeArgs {
    /// Storage pool name
    pool: String,
    /// Disk device files
    #[arg(action = clap::ArgAction::Append)]
    disk: Vec<String>,
    #[arg(short = 'u', long)]
    uuid: Option<Uuid>,
    #[arg(short = 't', long = "type", default_value = "lvs")]
    pool_type: PoolType,
    #[arg(short = 'c', long, requires = "encryption_key")]
    cipher: Option<Cipher>,
    #[arg(short = 'k', long = "encryption-key")]
    encryption_key: Option<String>,
    #[arg(short = 'e', long = "xts-key")]
    xts_key: Option<String>,
    #[arg(long)]
    import: bool,
}

pub async fn handler(ctx: Context, args: PoolArgs) -> crate::Result<()> {
    match args.command {
        PoolCommands::Create(args) => create(ctx, args).await,
        PoolCommands::Import(args) => import(ctx, args).await,
        PoolCommands::Destroy(args) => destroy(ctx, args).await,
        PoolCommands::Export(args) => export(ctx, args).await,
        PoolCommands::Expand(args) => expand(ctx, args).await,
        PoolCommands::List(args) => list(ctx, args).await,
        PoolCommands::ClearErrors(args) => clear_errors(ctx, args).await,
        PoolCommands::Probe(args) => probe(ctx, args).await,
    }
}

fn build_encryption(
    cipher: Option<Cipher>,
    enc_key: Option<&String>,
    xts_key: Option<&String>,
) -> Result<Option<v1rpc::common::create_pool_request::Encryption>, tonic::Status> {
    let enc_key_msg = enc_key.map(|k| v1rpc::common::EncryptionKey {
        key_name: "key_".to_owned() + k.as_str(),
        key: k.clone().into(),
        key_length: (k.len() * 4) as u32,
        key2: xts_key.map(|k2| k2.clone().into()),
        key2_length: xts_key.map(|x| (x.len() * 4) as u32),
    });
    Ok(enc_key_msg.map(|e| {
        v1rpc::common::create_pool_request::Encryption::Data(v1rpc::common::EncryptionData {
            cipher: cipher
                .map(|c| v1rpc::common::Cipher::from(c) as i32)
                .unwrap_or_default(),
            key: Some(e),
        })
    }))
}

fn build_import_encryption(
    cipher: Option<Cipher>,
    enc_key: Option<&String>,
    xts_key: Option<&String>,
) -> Result<Option<v1rpc::common::import_pool_request::Encryption>, tonic::Status> {
    let enc_key_msg = enc_key.map(|k| v1rpc::common::EncryptionKey {
        key_name: "key_".to_owned() + k,
        key: k.clone().into(),
        key_length: k.len() as u32,
        key2: xts_key.map(|k2| k2.clone().into()),
        key2_length: xts_key.map(|x| x.len() as u32),
    });
    Ok(enc_key_msg.map(|e| {
        v1rpc::common::import_pool_request::Encryption::Data(v1rpc::common::EncryptionData {
            cipher: cipher
                .map(|c| v1rpc::common::Cipher::from(c) as i32)
                .unwrap_or_default(),
            key: Some(e),
        })
    }))
}

async fn create(mut ctx: Context, args: CreateArgs) -> crate::Result<()> {
    let name = args.pool;
    let cluster_size = args.cluster_size.map(|b| b.as_u64() as u32);
    let max_expansion = args.max_expansion;
    let enc_msg = build_encryption(
        args.cipher,
        args.encryption_key.as_ref(),
        args.xts_key.as_ref(),
    )
    .context(GrpcStatus)?;

    let response = ctx
        .v1
        .pool
        .create_pool(v1rpc::pool::CreatePoolRequest {
            name: name.clone(),
            uuid: args.uuid.map(|u| u.to_string()),
            disks: args.disk,
            pooltype: v1rpc::pool::PoolType::from(args.pool_type) as i32,
            cluster_size,
            md_args: Some(v1rpc::pool::PoolMetadataArgs {
                md_resv_ratio: None,
                max_expansion,
            }),
            encryption: enc_msg,
        })
        .await
        .context(GrpcStatus)?;

    match ctx.output {
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&response.get_ref())
                    .unwrap()
                    .to_colored_json_auto()
                    .unwrap()
            );
        }
        OutputFormat::Default => {
            println!("{name}");
        }
    };

    Ok(())
}

#[derive(Debug, Clone, clap::ValueEnum)]
pub(super) enum Cipher {
    #[value(name = "AES_CBC")]
    AesCbc,
    #[value(name = "AES_XTS")]
    AesXts,
}
impl From<Cipher> for v1rpc::common::Cipher {
    fn from(value: Cipher) -> Self {
        match value {
            Cipher::AesCbc => Self::AesCbc,
            Cipher::AesXts => Self::AesXts,
        }
    }
}

#[derive(Debug, Clone, clap::ValueEnum)]
pub(super) enum PoolType {
    Lvs,
    Lvm,
}
impl From<PoolType> for v1rpc::pool::PoolType {
    fn from(value: PoolType) -> Self {
        match value {
            PoolType::Lvs => Self::Lvs,
            PoolType::Lvm => Self::Lvm,
        }
    }
}

async fn import(mut ctx: Context, args: ImportArgs) -> crate::Result<()> {
    let name = args.pool;
    let enc_msg = build_import_encryption(
        args.cipher,
        args.encryption_key.as_ref(),
        args.xts_key.as_ref(),
    )
    .context(GrpcStatus)?;

    let response = ctx
        .v1
        .pool
        .import_pool(v1rpc::pool::ImportPoolRequest {
            name: name.clone(),
            uuid: args.uuid.map(|u| u.to_string()),
            disks: args.disk,
            pooltype: v1rpc::pool::PoolType::from(args.pool_type) as i32,
            encryption: enc_msg,
        })
        .await
        .context(GrpcStatus)?;

    match ctx.output {
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&response.get_ref())
                    .unwrap()
                    .to_colored_json_auto()
                    .unwrap()
            );
        }
        OutputFormat::Default => {
            println!("{name}");
        }
    };

    Ok(())
}

async fn destroy(mut ctx: Context, args: DestroyArgs) -> crate::Result<()> {
    let name = args.pool;
    let uuid = args.uuid.map(|u| u.to_string());

    let _ = ctx
        .v1
        .pool
        .destroy_pool(v1rpc::pool::DestroyPoolRequest {
            name: name.clone(),
            uuid,
        })
        .await
        .context(GrpcStatus)?;

    match ctx.output {
        OutputFormat::Json => {}
        OutputFormat::Default => {
            println!("pool: {} is deleted", &name);
        }
    };

    Ok(())
}

async fn export(mut ctx: Context, args: ExportArgs) -> crate::Result<()> {
    let name = args.name;
    let uuid = args.uuid.map(|u| u.to_string());

    let _ = ctx
        .v1
        .pool
        .export_pool(v1rpc::pool::ExportPoolRequest {
            name: name.clone(),
            uuid,
        })
        .await
        .context(GrpcStatus)?;

    match ctx.output {
        OutputFormat::Json => {}
        OutputFormat::Default => {
            println!("pool: {} is exported", &name);
        }
    };

    Ok(())
}

async fn expand(mut ctx: Context, args: ExpandArgs) -> crate::Result<()> {
    let name = args.name;
    let uuid = args.uuid.map(|u| u.to_string());
    let pooltype = Some(args.pool_type);

    let list_response = ctx
        .v1
        .pool
        .list_pools(v1rpc::pool::ListPoolOptions {
            name: Some(name.clone()),
            pooltype: pooltype.map(|pooltype| v1rpc::pool::PoolTypeValue {
                value: v1rpc::pool::PoolType::from(pooltype) as i32,
            }),
            uuid: None,
        })
        .await
        .context(GrpcStatus)?;

    if list_response.get_ref().pools.is_empty() {
        return Err(crate::ClientError::GrpcStatus {
            source: Status::not_found(format!("Pool {name} not found")),
            backtrace: None,
        });
    }

    let pool = &list_response.get_ref().pools[0];
    let previous_capacity = pool.capacity;

    let grow_response = ctx
        .v1
        .pool
        .grow_pool_v2(v1rpc::pool::GrowPoolRequest {
            name: name.clone(),
            uuid,
        })
        .await
        .context(GrpcStatus)?;

    let pool = &grow_response.get_ref();
    let current_capacity = pool.capacity;

    match ctx.output {
        OutputFormat::Json => {}
        OutputFormat::Default => {
            println!("pool expanded from {previous_capacity} to {current_capacity}");
        }
    };

    Ok(())
}

fn list_pools(ctx: Context, pools: either::Either<Pool, Vec<Pool>>) -> crate::Result<()> {
    match ctx.output {
        OutputFormat::Json => {
            let json = match pools {
                either::Either::Left(pool) => serde_json::to_string_pretty(&pool).unwrap(),
                either::Either::Right(pools) => serde_json::to_string_pretty(&pools).unwrap(),
            };
            let json = json.to_colored_json_auto().unwrap();
            println!("{json}");
        }
        OutputFormat::Default => {
            if let either::Either::Right(ref pools) = pools {
                if pools.is_empty() {
                    ctx.v1("No pools found");
                    return Ok(());
                }
            }

            fn percentage_str(a: u64, b: u64) -> String {
                if b > 0 {
                    let v = 100.0 * a as f64 / b as f64;
                    format!("{v:.2}%")
                } else {
                    "-".to_string()
                }
            }
            fn map_pool(ctx: &Context, p: Pool) -> Vec<String> {
                let cap = Byte::from_u64(p.capacity);
                let used = Byte::from_u64(p.used);
                let state = pool_state_to_str(p.state);
                let errors = p.errors.unwrap_or_default();
                let alerts = errors.alerts.unwrap_or_default();
                let status = pool_status_to_str(alerts.status);
                let cluster = Byte::from_u64(p.cluster_size.into());
                let page_size = p
                    .page_size
                    .map(|s| ctx.units_with(Byte::from_u64(s.into()), byte_unit::UnitType::Binary))
                    .unwrap_or("-".to_string());
                let disk_cap = Byte::from_u64(p.disk_capacity);

                let (md_page_size, md_pages, md_used_pages, md_usage) =
                    if let Some(t) = p.md_info.as_ref() {
                        (
                            ctx.units_with(t.md_page_size.into(), byte_unit::UnitType::Binary),
                            t.md_pages.to_string(),
                            t.md_used_pages.to_string(),
                            percentage_str(t.md_used_pages, t.md_pages),
                        )
                    } else {
                        (
                            "-".to_string(),
                            "-".to_string(),
                            "-".to_string(),
                            "-".to_string(),
                        )
                    };

                vec![
                    p.name.clone(),
                    p.uuid.clone(),
                    state.to_string(),
                    status.to_string(),
                    ctx.units(cap),
                    ctx.units(used),
                    percentage_str(p.used, p.capacity),
                    ctx.units_with(cluster, byte_unit::UnitType::Binary),
                    page_size,
                    md_page_size,
                    md_pages,
                    md_used_pages,
                    md_usage,
                    p.disks.join(" "),
                    ctx.units(disk_cap),
                    p.encrypted.unwrap_or_default().to_string(),
                    errors.io_error_count.to_string(),
                ]
            }
            let headers = vec![
                "NAME",
                "UUID",
                "STATE",
                "STATUS",
                "CAPACITY",
                "USED",
                "USED%",
                "CLUSTER_SIZE",
                "PAGE_SIZE",
                "MD_PAGE_SIZE",
                "MD_PAGES",
                "MD_USED_PAGES",
                "MD_USED%",
                "DISKS",
                "DISK_CAPACITY",
                "ENCRYPTED",
                "ERRORS",
            ];
            let pools = match pools {
                either::Either::Left(pool) => {
                    vec![pool]
                }
                either::Either::Right(pools) => pools,
            };
            let table = pools.into_iter().map(|p| map_pool(&ctx, p)).collect();
            ctx.print_list(headers, table);
        }
    }

    Ok(())
}

async fn list(mut ctx: Context, args: ListArgs) -> crate::Result<()> {
    ctx.v2("Requesting a list of pools");

    let response = ctx
        .v1
        .pool
        .list_pools(v1rpc::pool::ListPoolOptions {
            name: args.name,
            uuid: args.uuid.map(|u| u.to_string()),
            pooltype: args.pool_type.map(|pooltype| v1rpc::pool::PoolTypeValue {
                value: v1rpc::pool::PoolType::from(pooltype) as i32,
            }),
        })
        .await
        .context(GrpcStatus)?;

    list_pools(ctx, either::Right(response.into_inner().pools))
}

async fn clear_errors(mut ctx: Context, args: ClearErrorsArgs) -> crate::Result<()> {
    let name = args.name;
    let uuid = args.uuid.map(|u| u.to_string());
    let disks = args.disk;

    let response = ctx
        .v1
        .pool
        .clear_errors(v1rpc::pool::ClearErrorRequest {
            name: name.clone(),
            uuid,
            disks,
            clear: 0,
        })
        .await
        .context(GrpcStatus)?;

    list_pools(ctx, either::Left(response.into_inner()))
}

async fn probe(mut ctx: Context, args: ProbeArgs) -> crate::Result<()> {
    let name = args.pool;
    let enc_msg = build_import_encryption(
        args.cipher,
        args.encryption_key.as_ref(),
        args.xts_key.as_ref(),
    )
    .context(GrpcStatus)?;

    let response = ctx
        .v1
        .pool
        .probe_pool(v1rpc::pool::ProbePoolRequest {
            request: Some(v1rpc::pool::ImportPoolRequest {
                name: name.clone(),
                uuid: args.uuid.map(|u| u.to_string()),
                disks: args.disk,
                pooltype: v1rpc::pool::PoolType::from(args.pool_type) as i32,
                encryption: enc_msg,
            }),
            import: args.import,
            probes: None,
        })
        .await
        .context(GrpcStatus)?
        .into_inner();

    match ctx.output {
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&response)
                    .unwrap()
                    .to_colored_json_auto()
                    .unwrap()
            );
        }
        OutputFormat::Default => {
            println!("{response:#?}");
        }
    };

    Ok(())
}

fn pool_state_to_str(idx: i32) -> &'static str {
    match v1rpc::pool::PoolState::try_from(idx).unwrap_or_default() {
        v1rpc::pool::PoolState::PoolUnknown => "unknown",
        v1rpc::pool::PoolState::PoolOnline => "online",
        v1rpc::pool::PoolState::PoolSuspected => "suspected",
        v1rpc::pool::PoolState::PoolDegraded => "degraded",
        v1rpc::pool::PoolState::PoolFaulted => "faulted",
    }
}
fn pool_status_to_str(idx: i32) -> &'static str {
    match v1rpc::pool::PoolAlertStatus::try_from(idx).unwrap_or_default() {
        v1rpc::pool::PoolAlertStatus::Healthy => "healthy",
        v1rpc::pool::PoolAlertStatus::Attention => "attention",
        v1rpc::pool::PoolAlertStatus::Warning => "warning",
        v1rpc::pool::PoolAlertStatus::Critical => "critical",
    }
}
