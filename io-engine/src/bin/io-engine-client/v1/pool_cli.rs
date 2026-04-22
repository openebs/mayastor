use crate::{
    context::{Context, OutputFormat},
    parse_size, ClientError, GrpcStatus,
};
use byte_unit::Byte;
use clap::{Arg, ArgMatches, Command};
use colored_json::ToColoredJson;
use io_engine_api::{v1 as v1rpc, v1::pool::Pool};
use snafu::ResultExt;
use std::{convert::TryFrom, str::FromStr};
use strum::VariantNames;
use strum_macros::{AsRefStr, EnumString, VariantNames};
use tonic::Status;

pub fn subcommands() -> Command {
    let create = Command::new("create")
        .about("Create new or import existing storage pool")
        .arg(
            Arg::new("pool")
                .required(true)
                .index(1)
                .help("Storage pool name"),
        )
        .arg(
            Arg::new("uuid")
                .long("uuid")
                .required(false)
                .help("Storage pool uuid"),
        )
        .arg(
            Arg::new("cluster-size")
                .long("cluster-size")
                .required(false)
                .help("SPDK cluster size"),
        )
        .arg(
            Arg::new("max-expansion")
                .long("max-expansion")
                .required(false)
                .help("Max expected expansion in factor or absolute size"),
        )
        .arg(
            Arg::new("disk")
                .required(true)
                .action(clap::ArgAction::Append)
                .index(2)
                .help("Disk device files"),
        )
        .arg(
            Arg::new("type")
                .short('t')
                .long("type")
                .help("The type of the pool")
                .required(false)
                .value_parser(PoolType::types().to_vec())
                .default_value(PoolType::Lvs.as_ref()),
        )
        .arg(
            Arg::new("cipher")
                .short('c')
                .long("cipher")
                .help("The cipher to use for encryption")
                .required(false)
                .requires("encryption-key"),
        )
        .arg(
            Arg::new("encryption-key")
                .short('k')
                .long("encryption-key")
                .help("The encryption key of the pool in hexlified format")
                .required(false),
        )
        .arg(
            Arg::new("xts-key")
                .short('e')
                .long("xts-key")
                .help("encryption key2 required for AES_XTS")
                .required(false)
                .required_if_eq("cipher", "AES_XTS"),
        );

    let import = Command::new("import")
        .about("Import existing storage pool, fail if pool does not exist")
        .arg(
            Arg::new("pool")
                .required(true)
                .index(1)
                .help("Storage pool name"),
        )
        .arg(
            Arg::new("uuid")
                .short('u')
                .long("uuid")
                .required(false)
                .help("Storage pool uuid"),
        )
        .arg(
            Arg::new("disk")
                .required(true)
                .action(clap::ArgAction::Append)
                .index(2)
                .help("Disk device files"),
        )
        .arg(
            Arg::new("type")
                .short('t')
                .long("type")
                .help("The type of the pool")
                .required(false)
                .value_parser(PoolType::types().to_vec())
                .default_value(PoolType::Lvs.as_ref()),
        )
        .arg(
            Arg::new("cipher")
                .short('c')
                .long("cipher")
                .help("The cipher to use for encryption")
                .required(false)
                .requires("encryption-key"),
        )
        .arg(
            Arg::new("encryption-key")
                .short('k')
                .long("encryption-key")
                .help("The encryption key of the pool in hexlified format")
                .required(false),
        )
        .arg(
            Arg::new("xts-key")
                .short('e')
                .long("xts-key")
                .help("encryption key2 required for AES_XTS")
                .required_if_eq("cipher", "AES_XTS")
                .required(false),
        );

    let destroy = Command::new("destroy")
        .about("Destroy storage pool")
        .arg(
            Arg::new("pool")
                .required(true)
                .index(1)
                .help("Storage pool name"),
        )
        .arg(
            Arg::new("uuid")
                .short('u')
                .long("uuid")
                .required(false)
                .help("Storage pool uuid"),
        )
        .arg(
            Arg::new("type")
                .short('t')
                .long("type")
                .help("The type of the pool")
                .required(false)
                .value_parser(PoolType::types().to_vec())
                .default_value(PoolType::Lvs.as_ref()),
        );

    let export = Command::new("export")
        .about("Export storage pool without destroying it")
        .arg(
            Arg::new("name")
                .required(true)
                .index(1)
                .help("Storage pool name"),
        )
        .arg(
            Arg::new("uuid")
                .short('u')
                .long("uuid")
                .required(false)
                .help("Storage pool uuid"),
        )
        .arg(
            Arg::new("type")
                .short('t')
                .long("type")
                .help("The type of the pool")
                .required(false)
                .value_parser(PoolType::types().to_vec())
                .default_value(PoolType::Lvs.as_ref()),
        );

    let expand = Command::new("expand")
        .about("Expand a storage pool to span the entire underlying device")
        .arg(
            Arg::new("name")
                .required(true)
                .index(1)
                .help("Storage pool name"),
        )
        .arg(
            Arg::new("uuid")
                .short('u')
                .long("uuid")
                .required(false)
                .help("Storage pool uuid"),
        )
        .arg(
            Arg::new("type")
                .short('t')
                .long("type")
                .help("The type of the pool")
                .required(false)
                .value_parser(PoolType::types().to_vec())
                .default_value(PoolType::Lvs.as_ref()),
        );

    let list = Command::new("list")
        .about("List storage pools")
        .arg(Arg::new("name").required(false).help("Storage pool name"))
        .arg(Arg::new("uuid").required(false).help("Storage pool uuid"))
        .arg(
            Arg::new("type")
                .short('t')
                .long("type")
                .help("The type of the pool")
                .required(false)
                .value_parser(PoolType::types().to_vec()),
        );

    let clear = Command::new("clear-errors")
        .about("Clears errors from the storage pool")
        .arg(
            Arg::new("name")
                .required(true)
                .index(1)
                .help("Storage pool name"),
        )
        .arg(
            Arg::new("uuid")
                .short('u')
                .long("uuid")
                .required(false)
                .help("Storage pool uuid"),
        )
        .arg(
            Arg::new("disk")
                .required(false)
                .action(clap::ArgAction::Append)
                .index(2)
                .help("Disk devices to clear errors or all if not specified"),
        );

    let probe = Command::new("probe")
        .about("Probes storage pool")
        .arg(
            Arg::new("pool")
                .required(true)
                .index(1)
                .help("Storage pool name"),
        )
        .arg(
            Arg::new("uuid")
                .short('u')
                .long("uuid")
                .required(false)
                .help("Storage pool uuid"),
        )
        .arg(
            Arg::new("disk")
                .required(true)
                .action(clap::ArgAction::Append)
                .index(2)
                .help("Disk device files"),
        )
        .arg(
            Arg::new("type")
                .short('t')
                .long("type")
                .help("The type of the pool")
                .required(false)
                .value_parser(PoolType::types().to_vec())
                .default_value(PoolType::Lvs.as_ref()),
        )
        .arg(
            Arg::new("cipher")
                .short('c')
                .long("cipher")
                .help("The cipher to use for encryption")
                .required(false)
                .requires("encryption-key"),
        )
        .arg(
            Arg::new("encryption-key")
                .short('k')
                .long("encryption-key")
                .help("The encryption key of the pool in hexlified format")
                .required(false),
        )
        .arg(
            Arg::new("xts-key")
                .short('e')
                .long("xts-key")
                .help("encryption key2 required for AES_XTS")
                .required_if_eq("cipher", "AES_XTS")
                .required(false),
        )
        .arg(
            Arg::new("import")
                .long("import")
                .help("Probe for imports")
                .action(clap::ArgAction::SetTrue),
        );

    Command::new("pool")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .about("Storage pool management")
        .subcommand(create)
        .subcommand(import)
        .subcommand(destroy)
        .subcommand(export)
        .subcommand(expand)
        .subcommand(list)
        .subcommand(clear)
        .subcommand(probe)
}

pub async fn handler(ctx: Context, matches: &ArgMatches) -> crate::Result<()> {
    match matches.subcommand().unwrap() {
        ("create", args) => create(ctx, args).await,
        ("import", args) => import(ctx, args).await,
        ("destroy", args) => destroy(ctx, args).await,
        ("export", args) => export(ctx, args).await,
        ("expand", args) => expand(ctx, args).await,
        ("list", args) => list(ctx, args).await,
        ("clear-errors", args) => clear_errors(ctx, args).await,
        ("probe", args) => probe(ctx, args).await,
        (cmd, _) => {
            Err(Status::not_found(format!("command {cmd} does not exist"))).context(GrpcStatus)
        }
    }
}

async fn create(mut ctx: Context, matches: &ArgMatches) -> crate::Result<()> {
    let name = matches
        .get_one::<String>("pool")
        .ok_or_else(|| ClientError::MissingValue {
            field: "pool".to_string(),
        })?
        .to_owned();

    let uuid = matches.get_one::<String>("uuid");
    let cipher = matches.get_one::<String>("cipher");

    if let Some(c) = cipher {
        if !c.eq_ignore_ascii_case("AES_XTS") && !c.eq_ignore_ascii_case("AES_CBC") {
            return Err(Status::invalid_argument(
                "Need valid cipher(AES_XTS or AES_CBC)",
            ))
            .context(GrpcStatus);
        }
    }

    let enc_key = matches.get_one::<String>("encryption-key");
    let xts_key = matches.get_one::<String>("xts-key");

    let disks_list = matches
        .get_many::<String>("disk")
        .ok_or_else(|| ClientError::MissingValue {
            field: "disk".to_string(),
        })?
        .map(|dev| dev.to_owned())
        .collect();

    let pooltype = matches
        .get_one::<String>("type")
        .map(|s| PoolType::from_str(s.as_str()))
        .unwrap()
        .map_err(|e| Status::invalid_argument(e.to_string()))
        .context(GrpcStatus)?;

    let cluster_size = match matches.get_one::<String>("cluster-size") {
        Some(s) => match parse_size(s) {
            Ok(s) => Some(s.as_u64() as u32),
            Err(err) => {
                return Err(Status::invalid_argument(format!("Bad size '{err}'")))
                    .context(GrpcStatus);
            }
        },
        None => None,
    };

    let max_expansion = match matches.get_one::<String>("max-expansion") {
        Some(s) => match s.parse::<String>() {
            Ok(v) => Some(v),
            Err(err) => {
                return Err(Status::invalid_argument(format!(
                    "Bad metadata reservation hint '{err}'"
                )))
                .context(GrpcStatus);
            }
        },
        None => None,
    };

    let enc_key_msg = enc_key.map(|k| v1rpc::common::EncryptionKey {
        key_name: "key_".to_owned() + k.as_str(),
        key: k.clone().into(),
        key_length: (k.len() * 4) as u32,
        key2: xts_key.map(|k2| k2.clone().into()),
        key2_length: xts_key.map(|x| { x.len() * 4 } as u32),
    });

    let enc_msg = enc_key_msg.map(|e| {
        v1rpc::common::create_pool_request::Encryption::Data(v1rpc::common::EncryptionData {
            cipher: v1rpc::common::Cipher::from_str_name(cipher.unwrap())
                .unwrap()
                .into(),
            key: Some(e),
        })
    });

    let response = ctx
        .v1
        .pool
        .create_pool(v1rpc::pool::CreatePoolRequest {
            name: name.clone(),
            uuid: uuid.map(ToString::to_string),
            disks: disks_list,
            pooltype: v1rpc::pool::PoolType::from(pooltype) as i32,
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
            println!("{}", &name);
        }
    };

    Ok(())
}

#[derive(EnumString, VariantNames, AsRefStr)]
#[strum(serialize_all = "UPPERCASE")]
pub(super) enum Cipher {
    AesCbc,
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

#[derive(EnumString, VariantNames, AsRefStr)]
#[strum(serialize_all = "camelCase")]
pub(super) enum PoolType {
    Lvs,
    Lvm,
}
impl PoolType {
    pub(crate) fn types() -> &'static [&'static str] {
        Self::VARIANTS
    }
}
impl From<PoolType> for v1rpc::pool::PoolType {
    fn from(value: PoolType) -> Self {
        match value {
            PoolType::Lvs => Self::Lvs,
            PoolType::Lvm => Self::Lvm,
        }
    }
}

async fn import(mut ctx: Context, matches: &ArgMatches) -> crate::Result<()> {
    let name = matches
        .get_one::<String>("pool")
        .ok_or_else(|| ClientError::MissingValue {
            field: "pool".to_string(),
        })?
        .to_owned();
    let uuid = matches.get_one::<String>("uuid");
    let cipher = matches.get_one::<String>("cipher");

    if let Some(c) = cipher {
        if !c.eq_ignore_ascii_case("AES_XTS") && !c.eq_ignore_ascii_case("AES_CBC") {
            return Err(Status::invalid_argument(
                "Need valid cipher(AES_XTS or AES_CBC)",
            ))
            .context(GrpcStatus);
        }
    }

    let enc_key = matches.get_one::<String>("encryption-key");
    let xts_key = matches.get_one::<String>("xts-key");

    let disks_list = matches
        .get_many::<String>("disk")
        .ok_or_else(|| ClientError::MissingValue {
            field: "disk".to_string(),
        })?
        .map(|dev| dev.to_owned())
        .collect();
    let pooltype = matches
        .get_one::<String>("type")
        .map(|s| PoolType::from_str(s.as_str()))
        .unwrap()
        .map_err(|e| Status::invalid_argument(e.to_string()))
        .context(GrpcStatus)?;

    let enc_key_msg = enc_key.map(|k| v1rpc::common::EncryptionKey {
        key_name: "key_".to_owned() + k,
        key: k.clone().into(),
        key_length: k.len() as u32,
        key2: xts_key.map(|k2| k2.clone().into()),
        key2_length: xts_key.map(|x| x.len() as u32),
    });

    let enc_msg = enc_key_msg.map(|e| {
        v1rpc::common::import_pool_request::Encryption::Data(v1rpc::common::EncryptionData {
            cipher: v1rpc::common::Cipher::from_str_name(cipher.unwrap())
                .unwrap()
                .into(),
            key: Some(e),
        })
    });

    let response = ctx
        .v1
        .pool
        .import_pool(v1rpc::pool::ImportPoolRequest {
            name: name.clone(),
            uuid: uuid.map(ToString::to_string),
            disks: disks_list,
            pooltype: v1rpc::pool::PoolType::from(pooltype) as i32,
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
            println!("{}", &name);
        }
    };

    Ok(())
}

async fn destroy(mut ctx: Context, matches: &ArgMatches) -> crate::Result<()> {
    let name = matches
        .get_one::<String>("pool")
        .ok_or_else(|| ClientError::MissingValue {
            field: "pool".to_string(),
        })?
        .to_owned();
    let uuid = matches.get_one::<String>("uuid").cloned();

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

async fn export(mut ctx: Context, matches: &ArgMatches) -> crate::Result<()> {
    let name = matches
        .get_one::<String>("name")
        .ok_or_else(|| ClientError::MissingValue {
            field: "name".to_string(),
        })?
        .to_owned();
    let uuid = matches.get_one::<String>("uuid").cloned();

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

async fn expand(mut ctx: Context, matches: &ArgMatches) -> crate::Result<()> {
    let name = matches
        .get_one::<String>("name")
        .ok_or_else(|| ClientError::MissingValue {
            field: "name".to_string(),
        })?
        .to_owned();
    let uuid = matches.get_one::<String>("uuid").cloned();

    let pooltype = matches
        .get_one::<String>("type")
        .map(|s| PoolType::from_str(s.as_str()))
        .transpose()
        .map_err(|e| Status::invalid_argument(e.to_string()))
        .context(GrpcStatus)?;

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
        return Err(ClientError::GrpcStatus {
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
            println!("{}", json.to_colored_json_auto().unwrap());
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

async fn list(mut ctx: Context, matches: &ArgMatches) -> crate::Result<()> {
    ctx.v2("Requesting a list of pools");

    let name = matches.get_one::<String>("name").cloned();
    let uuid = matches.get_one::<String>("uuid").cloned();
    let pooltype = matches
        .get_one::<String>("type")
        .map(|s| PoolType::from_str(s.as_str()))
        .transpose()
        .map_err(|e| Status::invalid_argument(e.to_string()))
        .context(GrpcStatus)?;

    let response = ctx
        .v1
        .pool
        .list_pools(v1rpc::pool::ListPoolOptions {
            name,
            uuid,
            pooltype: pooltype.map(|pooltype| v1rpc::pool::PoolTypeValue {
                value: v1rpc::pool::PoolType::from(pooltype) as i32,
            }),
        })
        .await
        .context(GrpcStatus)?;

    list_pools(ctx, either::Right(response.into_inner().pools))
}

async fn clear_errors(mut ctx: Context, matches: &ArgMatches) -> crate::Result<()> {
    let name = matches
        .get_one::<String>("name")
        .ok_or_else(|| ClientError::MissingValue {
            field: "name".to_string(),
        })?
        .to_owned();
    let uuid = matches.get_one::<String>("uuid").cloned();

    let disks = matches.get_many::<String>("disk").unwrap_or_default();

    let response = ctx
        .v1
        .pool
        .clear_errors(v1rpc::pool::ClearErrorRequest {
            name: name.clone(),
            uuid,
            disks: disks.map(|dev| dev.to_owned()).collect(),
            clear: 0,
        })
        .await
        .context(GrpcStatus)?;

    list_pools(ctx, either::Left(response.into_inner()))
}

async fn probe(mut ctx: Context, matches: &ArgMatches) -> crate::Result<()> {
    let name = matches
        .get_one::<String>("pool")
        .ok_or_else(|| ClientError::MissingValue {
            field: "pool".to_string(),
        })?
        .to_owned();
    let uuid = matches.get_one::<String>("uuid");
    let cipher = matches.get_one::<String>("cipher");
    let import = matches.get_flag("import");

    if let Some(c) = cipher {
        if !c.eq_ignore_ascii_case("AES_XTS") && !c.eq_ignore_ascii_case("AES_CBC") {
            return Err(Status::invalid_argument(
                "Need valid cipher(AES_XTS or AES_CBC)",
            ))
            .context(GrpcStatus);
        }
    }

    let enc_key = matches.get_one::<String>("encryption-key");
    let xts_key = matches.get_one::<String>("xts-key");

    let disks_list = matches
        .get_many::<String>("disk")
        .ok_or_else(|| ClientError::MissingValue {
            field: "disk".to_string(),
        })?
        .map(|dev| dev.to_owned())
        .collect();
    let pooltype = matches
        .get_one::<String>("type")
        .map(|s| PoolType::from_str(s.as_str()))
        .unwrap()
        .map_err(|e| Status::invalid_argument(e.to_string()))
        .context(GrpcStatus)?;

    let enc_key_msg = enc_key.map(|k| v1rpc::common::EncryptionKey {
        key_name: "key_".to_owned() + k,
        key: k.clone().into(),
        key_length: k.len() as u32,
        key2: xts_key.map(|k2| k2.clone().into()),
        key2_length: xts_key.map(|x| x.len() as u32),
    });

    let enc_msg = enc_key_msg.map(|e| {
        v1rpc::common::import_pool_request::Encryption::Data(v1rpc::common::EncryptionData {
            cipher: v1rpc::common::Cipher::from_str_name(cipher.unwrap())
                .unwrap()
                .into(),
            key: Some(e),
        })
    });

    let response = ctx
        .v1
        .pool
        .probe_pool(v1rpc::pool::ProbePoolRequest {
            request: Some(v1rpc::pool::ImportPoolRequest {
                name: name.clone(),
                uuid: uuid.map(ToString::to_string),
                disks: disks_list,
                pooltype: v1rpc::pool::PoolType::from(pooltype) as i32,
                encryption: enc_msg,
            }),
            import,
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
