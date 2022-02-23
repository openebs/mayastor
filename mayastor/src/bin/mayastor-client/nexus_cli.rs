use crate::{
    context::{Context, OutputFormat},
    nexus_child_cli,
    parse_size,
    Error,
    GrpcStatus,
};
use ::rpc::mayastor as rpc;
use byte_unit::Byte;
use clap::{value_t, App, AppSettings, Arg, ArgMatches, SubCommand};
use colored_json::ToColoredJson;
use snafu::ResultExt;
use tonic::{Code, Status};

pub fn subcommands<'a, 'b>() -> App<'a, 'b> {
    let create = SubCommand::with_name("create")
        .about("Create a new nexus device")
        .arg(
            Arg::with_name("uuid")
                .required(true)
                .index(1)
                .help("uuid for the nexus"),
        )
        .arg(
            Arg::with_name("size")
                .required(true)
                .index(2)
                .help("size with optional unit suffix"),
        )
        .arg(
            Arg::with_name("children")
                .required(true)
                .multiple(true)
                .index(3)
                .help("list of children to add"),
        );

    let create_v2 = SubCommand::with_name("create2")
        .about("Create a new nexus device with NVMe options")
        .arg(
            Arg::with_name("name")
                .required(true)
                .index(1)
                .help("name of the nexus"),
        )
        .arg(
            Arg::with_name("uuid")
                .required(true)
                .help("uuid for the nexus"),
        )
        .arg(
            Arg::with_name("size")
                .required(true)
                .help("size with optional unit suffix"),
        )
        .arg(
            Arg::with_name("min-cntlid")
                .required(true)
                .help("minimum NVMe controller ID for sharing over NVMf"),
        )
        .arg(
            Arg::with_name("max-cntlid")
                .required(true)
                .help("maximum NVMe controller ID"),
        )
        .arg(
            Arg::with_name("resv-key")
                .required(true)
                .help("NVMe reservation key for children"),
        )
        .arg(
            Arg::with_name("preempt-key")
                .required(true)
                .help("NVMe preempt key for children, 0 for no preemption"),
        )
        .arg(
            Arg::with_name("nexus-info-key")
                .required(true)
                .help("Key used to persist the NexusInfo structure to the persistent store"),
        )
        .arg(
            Arg::with_name("children")
                .required(true)
                .multiple(true)
                .help("list of children to add"),
        );

    let destroy = SubCommand::with_name("destroy")
        .about("destroy the nexus with given name")
        .arg(
            Arg::with_name("uuid")
                .required(true)
                .index(1)
                .help("uuid for the nexus"),
        );

    let publish = SubCommand::with_name("publish")
        .about("publish the nexus")
        .arg(Arg::with_name("protocol").short("p").long("protocol").value_name("PROTOCOL")
            .help("Name of a protocol (nvmf) used for publishing the nexus remotely"))
        .arg(Arg::with_name("uuid").required(true).index(1)
            .help("uuid for the nexus"))
        .arg(Arg::with_name("key").required(false).index(2)
            .help("crypto key to use"));

    let unpublish = SubCommand::with_name("unpublish")
        .about("unpublish the nexus")
        .arg(
            Arg::with_name("uuid")
                .required(true)
                .index(1)
                .help("uuid for the nexus"),
        );

    let ana_state = SubCommand::with_name("ana_state")
        .about("get or set the NVMe ANA state of the nexus")
        .arg(
            Arg::with_name("uuid")
                .required(true)
                .index(1)
                .help("uuid for the nexus"),
        )
        .arg(
            Arg::with_name("state")
                .required(false)
                .index(2)
                .possible_value("optimized")
                .possible_value("non_optimized")
                .possible_value("inaccessible")
                .help("NVMe ANA state of the nexus"),
        );

    let add = SubCommand::with_name("add")
        .about("add a child")
        .arg(
            Arg::with_name("uuid")
                .required(true)
                .index(1)
                .help("uuid for the nexus"),
        )
        .arg(
            Arg::with_name("uri")
                .required(true)
                .index(2)
                .help("uri of child to add"),
        )
        .arg(
            Arg::with_name("norebuild")
                .default_value("false")
                .index(3)
                .help("specify if a rebuild job runs automatically"),
        );

    let remove = SubCommand::with_name("remove")
        .about("remove a child")
        .arg(
            Arg::with_name("uuid")
                .required(true)
                .index(1)
                .help("uuid for the nexus"),
        )
        .arg(
            Arg::with_name("uri")
                .required(true)
                .index(2)
                .help("uri of child to remove"),
        );

    let list = SubCommand::with_name("list")
        .about("list all nexus devices")
        .arg(
            Arg::with_name("children")
                .short("c")
                .long("show-children")
                .required(false)
                .takes_value(false),
        );

    let list2 = SubCommand::with_name("list2")
        .about("list all nexus devices")
        .arg(
            Arg::with_name("children")
                .short("c")
                .long("show-children")
                .required(false)
                .takes_value(false),
        );

    let children = SubCommand::with_name("children")
        .about("list nexus children")
        .arg(
            Arg::with_name("uuid")
                .required(true)
                .index(1)
                .help("uuid of nexus"),
        );

    SubCommand::with_name("nexus")
        .settings(&[
            AppSettings::SubcommandRequiredElseHelp,
            AppSettings::ColoredHelp,
            AppSettings::ColorAlways,
        ])
        .about("Nexus device management")
        .subcommand(create)
        .subcommand(create_v2)
        .subcommand(destroy)
        .subcommand(publish)
        .subcommand(add)
        .subcommand(remove)
        .subcommand(unpublish)
        .subcommand(ana_state)
        .subcommand(list)
        .subcommand(list2)
        .subcommand(children)
        .subcommand(nexus_child_cli::subcommands())
}

pub async fn handler(
    ctx: Context,
    matches: &ArgMatches<'_>,
) -> crate::Result<()> {
    match matches.subcommand() {
        ("create", Some(args)) => nexus_create(ctx, args).await,
        ("create2", Some(args)) => nexus_create_v2(ctx, args).await,
        ("destroy", Some(args)) => nexus_destroy(ctx, args).await,
        ("list", Some(args)) => nexus_list(ctx, args).await,
        ("list2", Some(args)) => nexus_list_v2(ctx, args).await,
        ("children", Some(args)) => nexus_children(ctx, args).await,
        ("publish", Some(args)) => nexus_publish(ctx, args).await,
        ("unpublish", Some(args)) => nexus_unpublish(ctx, args).await,
        ("ana_state", Some(args)) => nexus_nvme_ana_state(ctx, args).await,
        ("add", Some(args)) => nexus_add(ctx, args).await,
        ("remove", Some(args)) => nexus_remove(ctx, args).await,
        ("child", Some(args)) => nexus_child_cli::handler(ctx, args).await,
        (cmd, _) => {
            Err(Status::not_found(format!("command {} does not exist", cmd)))
                .context(GrpcStatus)
        }
    }
}

fn nexus_create_parse(
    matches: &ArgMatches<'_>,
) -> crate::Result<(
    ::prost::alloc::string::String,
    u64,
    ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
)> {
    let uuid = matches.value_of("uuid").unwrap().to_string();
    let size = parse_size(matches.value_of("size").ok_or_else(|| {
        Error::MissingValue {
            field: "size".to_string(),
        }
    })?)
    .map_err(|s| Status::invalid_argument(format!("Bad size '{}'", s)))
    .context(GrpcStatus)?;
    let children = matches
        .values_of("children")
        .ok_or_else(|| Error::MissingValue {
            field: "children".to_string(),
        })?
        .map(|c| c.to_string())
        .collect::<Vec<String>>();
    let size = size.get_bytes() as u64;
    Ok((uuid, size, children))
}

async fn nexus_create(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> crate::Result<()> {
    let (uuid, size, children) = nexus_create_parse(matches)?;

    let response = ctx
        .client
        .create_nexus(rpc::CreateNexusRequest {
            uuid: uuid.clone(),
            size,
            children,
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
            println!("{}", &response.get_ref().uuid);
        }
    };

    Ok(())
}

async fn nexus_create_v2(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> crate::Result<()> {
    let (uuid, size, children) = nexus_create_parse(matches)?;
    let name = matches.value_of("name").unwrap().to_string();
    let min_cntl_id = value_t!(matches.value_of("min-cntlid"), u32)
        .unwrap_or_else(|e| e.exit());
    let max_cntl_id = value_t!(matches.value_of("max-cntlid"), u32)
        .unwrap_or_else(|e| e.exit());
    let resv_key = value_t!(matches.value_of("resv-key"), u64)
        .unwrap_or_else(|e| e.exit());
    let preempt_key = value_t!(matches.value_of("preempt-key"), u64)
        .unwrap_or_else(|e| e.exit());
    let nexus_info_key = matches
        .value_of("nexus-info-key")
        .unwrap_or_default()
        .to_string();

    let response = ctx
        .client
        .create_nexus_v2(rpc::CreateNexusV2Request {
            name: name.clone(),
            uuid: uuid.clone(),
            size,
            min_cntl_id,
            max_cntl_id,
            resv_key,
            preempt_key,
            children,
            nexus_info_key,
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
            println!("{}", &response.get_ref().uuid);
        }
    };

    Ok(())
}

async fn nexus_destroy(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> crate::Result<()> {
    let uuid = matches.value_of("uuid").unwrap().to_string();

    let response = ctx
        .client
        .destroy_nexus(rpc::DestroyNexusRequest {
            uuid: uuid.clone(),
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
            println!("{}", &uuid,);
        }
    };

    Ok(())
}

async fn nexus_list(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> crate::Result<()> {
    let response = ctx
        .client
        .list_nexus(rpc::Null {})
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
            let nexus = &response.get_ref().nexus_list;
            if nexus.is_empty() {
                ctx.v1("No nexus found");
                return Ok(());
            }

            ctx.v2("Found following nexus:");
            let show_child = matches.is_present("children");

            let table = nexus
                .iter()
                .map(|n| {
                    let size = ctx.units(Byte::from_bytes(n.size.into()));
                    let state = nexus_state_to_str(n.state);
                    let mut row = vec![
                        n.uuid.clone(),
                        size,
                        state.to_string(),
                        n.rebuilds.to_string(),
                        n.device_uri.clone(),
                    ];
                    if show_child {
                        row.push(
                            n.children
                                .iter()
                                .map(|c| c.uri.clone())
                                .collect::<Vec<String>>()
                                .join(","),
                        )
                    }
                    row
                })
                .collect();
            let mut hdr = vec!["NAME", ">SIZE", "STATE", ">REBUILDS", "PATH"];
            if show_child {
                hdr.push("CHILDREN");
            }
            ctx.print_list(hdr, table);
        }
    };

    Ok(())
}

async fn nexus_list_v2(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> crate::Result<()> {
    let response = ctx
        .client
        .list_nexus_v2(rpc::Null {})
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
            let nexus = &response.get_ref().nexus_list;
            if nexus.is_empty() {
                ctx.v1("No nexus found");
                return Ok(());
            }

            ctx.v2("Found following nexus:");
            let show_child = matches.is_present("children");

            let table = nexus
                .iter()
                .map(|n| {
                    let size = ctx.units(Byte::from_bytes(n.size.into()));
                    let state = nexus_state_to_str(n.state);
                    let mut row = vec![
                        n.name.clone(),
                        n.uuid.clone(),
                        size,
                        state.to_string(),
                        n.rebuilds.to_string(),
                        n.device_uri.clone(),
                    ];
                    if show_child {
                        row.push(
                            n.children
                                .iter()
                                .map(|c| c.uri.clone())
                                .collect::<Vec<String>>()
                                .join(","),
                        )
                    }
                    row
                })
                .collect();
            let mut hdr =
                vec!["NAME", "UUID", ">SIZE", "STATE", ">REBUILDS", "PATH"];
            if show_child {
                hdr.push("CHILDREN");
            }
            ctx.print_list(hdr, table);
        }
    };

    Ok(())
}

async fn nexus_children(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> crate::Result<()> {
    let uuid = matches
        .value_of("uuid")
        .ok_or_else(|| Error::MissingValue {
            field: "uuid".to_string(),
        })?
        .to_string();

    let response = ctx
        .client
        .list_nexus(rpc::Null {})
        .await
        .context(GrpcStatus)?;

    let nexus = response
        .get_ref()
        .nexus_list
        .iter()
        .find(|n| n.uuid == uuid)
        .ok_or_else(|| {
            Status::new(
                Code::InvalidArgument,
                "Specified nexus not found".to_owned(),
            )
        })
        .context(GrpcStatus)?;

    match ctx.output {
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&nexus.children)
                    .unwrap()
                    .to_colored_json_auto()
                    .unwrap()
            );
        }
        OutputFormat::Default => {
            let table = nexus
                .children
                .iter()
                .map(|c| {
                    let state = child_state_to_str(c.state);
                    vec![c.uri.clone(), state.to_string()]
                })
                .collect();
            ctx.print_list(vec!["NAME", "STATE"], table);
        }
    };

    Ok(())
}

async fn nexus_publish(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> crate::Result<()> {
    let uuid = matches
        .value_of("uuid")
        .ok_or_else(|| Error::MissingValue {
            field: "uuid".to_string(),
        })?
        .to_string();
    let key = matches.value_of("key").unwrap_or("").to_string();
    let protocol = match matches.value_of("protocol") {
        None => rpc::ShareProtocolNexus::NexusNbd,
        Some("nvmf") => rpc::ShareProtocolNexus::NexusNvmf,
        Some(_) => {
            return Err(Status::new(
                Code::Internal,
                "Invalid value of share protocol".to_owned(),
            ))
            .context(GrpcStatus);
        }
    };

    let response = ctx
        .client
        .publish_nexus(rpc::PublishNexusRequest {
            uuid,
            key,
            share: protocol.into(),
        })
        .await
        .context(GrpcStatus)?;

    match ctx.output {
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(response.get_ref())
                    .unwrap()
                    .to_colored_json_auto()
                    .unwrap()
            );
        }
        OutputFormat::Default => {
            println!("{}", response.get_ref().device_uri,)
        }
    };

    Ok(())
}

async fn nexus_unpublish(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> crate::Result<()> {
    let uuid = matches
        .value_of("uuid")
        .ok_or_else(|| Error::MissingValue {
            field: "uuid".to_string(),
        })?
        .to_string();

    let response = ctx
        .client
        .unpublish_nexus(rpc::UnpublishNexusRequest {
            uuid: uuid.clone(),
        })
        .await
        .context(GrpcStatus)?;

    match ctx.output {
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(response.get_ref())
                    .unwrap()
                    .to_colored_json_auto()
                    .unwrap()
            );
        }
        OutputFormat::Default => {
            println!("{}", &uuid,)
        }
    };

    Ok(())
}

async fn nexus_nvme_ana_state(
    ctx: Context,
    matches: &ArgMatches<'_>,
) -> crate::Result<()> {
    let uuid = matches.value_of("uuid").unwrap().to_string();
    let ana_state = matches.value_of("state").unwrap_or("").to_string();
    if ana_state.is_empty() {
        nexus_get_nvme_ana_state(ctx, uuid).await
    } else {
        nexus_set_nvme_ana_state(ctx, uuid, ana_state).await
    }
}

async fn nexus_get_nvme_ana_state(
    mut ctx: Context,
    uuid: String,
) -> crate::Result<()> {
    let resp = ctx
        .client
        .get_nvme_ana_state(rpc::GetNvmeAnaStateRequest {
            uuid: uuid.clone(),
        })
        .await
        .context(GrpcStatus)?;
    ctx.v1(ana_state_idx_to_str(resp.get_ref().ana_state));
    Ok(())
}

async fn nexus_set_nvme_ana_state(
    mut ctx: Context,
    uuid: String,
    ana_state_str: String,
) -> crate::Result<()> {
    let ana_state: rpc::NvmeAnaState = match ana_state_str.parse() {
        Ok(a) => a,
        _ => {
            return Err(Status::new(
                Code::Internal,
                "Invalid value of NVMe ANA state".to_owned(),
            ))
            .context(GrpcStatus);
        }
    };

    ctx.client
        .set_nvme_ana_state(rpc::SetNvmeAnaStateRequest {
            uuid: uuid.clone(),
            ana_state: ana_state.into(),
        })
        .await
        .context(GrpcStatus)?;
    ctx.v1(&uuid);
    Ok(())
}

async fn nexus_add(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> crate::Result<()> {
    let uuid = matches
        .value_of("uuid")
        .ok_or_else(|| Error::MissingValue {
            field: "uuid".to_string(),
        })?
        .to_string();
    let uri = matches
        .value_of("uri")
        .ok_or_else(|| Error::MissingValue {
            field: "uri".to_string(),
        })?
        .to_string();
    let norebuild = matches
        .value_of("norebuild")
        .unwrap_or("false")
        .parse::<bool>()
        .unwrap_or(false);

    let response = ctx
        .client
        .add_child_nexus(rpc::AddChildNexusRequest {
            uuid: uuid.clone(),
            uri: uri.clone(),
            norebuild,
        })
        .await
        .context(GrpcStatus)?;

    match ctx.output {
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(response.get_ref())
                    .unwrap()
                    .to_colored_json_auto()
                    .unwrap()
            );
        }
        OutputFormat::Default => {
            println!("{}", &uuid,)
        }
    };

    Ok(())
}

async fn nexus_remove(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> crate::Result<()> {
    let uuid = matches
        .value_of("uuid")
        .ok_or_else(|| Error::MissingValue {
            field: "uuid".to_string(),
        })?
        .to_string();
    let uri = matches
        .value_of("uri")
        .ok_or_else(|| Error::MissingValue {
            field: "uri".to_string(),
        })?
        .to_string();

    let response = ctx
        .client
        .remove_child_nexus(rpc::RemoveChildNexusRequest {
            uuid: uuid.clone(),
            uri: uri.clone(),
        })
        .await
        .context(GrpcStatus)?;

    match ctx.output {
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(response.get_ref())
                    .unwrap()
                    .to_colored_json_auto()
                    .unwrap()
            );
        }
        OutputFormat::Default => {
            println!("{}", &uri,)
        }
    };

    Ok(())
}

fn ana_state_idx_to_str(idx: i32) -> &'static str {
    match rpc::NvmeAnaState::from_i32(idx).unwrap() {
        rpc::NvmeAnaState::NvmeAnaInvalidState => "invalid",
        rpc::NvmeAnaState::NvmeAnaOptimizedState => "optimized",
        rpc::NvmeAnaState::NvmeAnaNonOptimizedState => "non_optimized",
        rpc::NvmeAnaState::NvmeAnaInaccessibleState => "inaccessible",
        rpc::NvmeAnaState::NvmeAnaPersistentLossState => "persistent_loss",
        rpc::NvmeAnaState::NvmeAnaChangeState => "change",
    }
}

fn nexus_state_to_str(idx: i32) -> &'static str {
    match rpc::NexusState::from_i32(idx).unwrap() {
        rpc::NexusState::NexusUnknown => "unknown",
        rpc::NexusState::NexusOnline => "online",
        rpc::NexusState::NexusDegraded => "degraded",
        rpc::NexusState::NexusFaulted => "faulted",
    }
}

fn child_state_to_str(idx: i32) -> &'static str {
    match rpc::ChildState::from_i32(idx).unwrap() {
        rpc::ChildState::ChildUnknown => "unknown",
        rpc::ChildState::ChildOnline => "online",
        rpc::ChildState::ChildDegraded => "degraded",
        rpc::ChildState::ChildFaulted => "faulted",
    }
}
