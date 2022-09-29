use crate::{
    context::{Context, OutputFormat},
    nexus_child_cli,
    parse_size,
    ClientError,
    GrpcStatus,
};
use byte_unit::Byte;
use clap::{value_t, App, AppSettings, Arg, ArgMatches, SubCommand};
use colored_json::ToColoredJson;
use mayastor_api::{v0, v1};
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

    let shutdown = SubCommand::with_name("shutdown")
        .about("shutdown the nexus with given name")
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

    let children_2 = SubCommand::with_name("children2")
        .about("list nexus children")
        .arg(
            Arg::with_name("uuid")
                .required(true)
                .index(1)
                .help("uuid of nexus"),
        );

    let inject = SubCommand::with_name("inject")
        .about("manage injected faults")
        .arg(
            Arg::with_name("uuid")
                .required(true)
                .index(1)
                .help("uuid of nexus"),
        )
        .arg(
            Arg::with_name("add")
                .short("a")
                .long("add")
                .required(false)
                .takes_value(true)
                .multiple(true)
                .number_of_values(1)
                .help("new injection uri"),
        )
        .arg(
            Arg::with_name("remove")
                .short("r")
                .long("remove")
                .required(false)
                .takes_value(true)
                .multiple(true)
                .number_of_values(1)
                .help("injection uri"),
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
        .subcommand(shutdown)
        .subcommand(publish)
        .subcommand(add)
        .subcommand(remove)
        .subcommand(unpublish)
        .subcommand(ana_state)
        .subcommand(list)
        .subcommand(list2)
        .subcommand(children)
        .subcommand(children_2)
        .subcommand(inject)
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
        ("shutdown", Some(args)) => nexus_shutdown(ctx, args).await,
        ("list", Some(args)) => nexus_list(ctx, args).await,
        ("list2", Some(args)) => nexus_list_v2(ctx, args).await,
        ("children", Some(args)) => nexus_children(ctx, args).await,
        ("children2", Some(args)) => nexus_children_2(ctx, args).await,
        ("publish", Some(args)) => nexus_publish(ctx, args).await,
        ("unpublish", Some(args)) => nexus_unpublish(ctx, args).await,
        ("ana_state", Some(args)) => nexus_nvme_ana_state(ctx, args).await,
        ("add", Some(args)) => nexus_add(ctx, args).await,
        ("remove", Some(args)) => nexus_remove(ctx, args).await,
        ("child", Some(args)) => nexus_child_cli::handler(ctx, args).await,
        ("inject", Some(args)) => injections(ctx, args).await,
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
        ClientError::MissingValue {
            field: "size".to_string(),
        }
    })?)
    .map_err(|s| Status::invalid_argument(format!("Bad size '{}'", s)))
    .context(GrpcStatus)?;
    let children = matches
        .values_of("children")
        .ok_or_else(|| ClientError::MissingValue {
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
        .create_nexus(v0::CreateNexusRequest {
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
        .create_nexus_v2(v0::CreateNexusV2Request {
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

async fn nexus_shutdown(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> crate::Result<()> {
    let uuid = matches.value_of("uuid").unwrap().to_string();

    let response = ctx
        .client
        .shutdown_nexus(v0::ShutdownNexusRequest {
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

async fn nexus_destroy(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> crate::Result<()> {
    let uuid = matches.value_of("uuid").unwrap().to_string();

    let response = ctx
        .client
        .destroy_nexus(v0::DestroyNexusRequest {
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
        .list_nexus(v0::Null {})
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
        .list_nexus_v2(v0::Null {})
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
        .ok_or_else(|| ClientError::MissingValue {
            field: "uuid".to_string(),
        })?
        .to_string();

    let response = ctx
        .client
        .list_nexus(v0::Null {})
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
                    let state = child_state_to_str_v0(
                        v0::ChildState::from_i32(c.state).unwrap(),
                    );
                    vec![c.uri.clone(), state.to_string()]
                })
                .collect();
            ctx.print_list(vec!["NAME", "STATE"], table);
        }
    };

    Ok(())
}

async fn nexus_children_2(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> crate::Result<()> {
    let uuid = matches
        .value_of("uuid")
        .ok_or_else(|| ClientError::MissingValue {
            field: "uuid".to_string(),
        })?
        .to_string();

    let response = ctx
        .v1
        .nexus
        .list_nexus(v1::nexus::ListNexusOptions {
            name: None,
        })
        .await
        .context(GrpcStatus)?;

    let nexus = response
        .get_ref()
        .nexus_list
        .iter()
        .find(|n| n.uuid == uuid || n.name == uuid)
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
                    let state = child_state_to_str_v1(
                        v1::nexus::ChildState::from_i32(c.state).unwrap(),
                    );
                    let reason = child_reason_to_str_v1(
                        v1::nexus::ChildStateReason::from_i32(c.state_reason)
                            .unwrap(),
                    );
                    vec![c.uri.clone(), state.to_string(), reason.to_string()]
                })
                .collect();
            ctx.print_list(vec!["NAME", "STATE", "REASON"], table);
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
        .ok_or_else(|| ClientError::MissingValue {
            field: "uuid".to_string(),
        })?
        .to_string();
    let key = matches.value_of("key").unwrap_or("").to_string();
    let protocol = match matches.value_of("protocol") {
        None => v0::ShareProtocolNexus::NexusNbd,
        Some("nvmf") => v0::ShareProtocolNexus::NexusNvmf,
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
        .publish_nexus(v0::PublishNexusRequest {
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
        .ok_or_else(|| ClientError::MissingValue {
            field: "uuid".to_string(),
        })?
        .to_string();

    let response = ctx
        .client
        .unpublish_nexus(v0::UnpublishNexusRequest {
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
        .get_nvme_ana_state(v0::GetNvmeAnaStateRequest {
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
    let ana_state: v0::NvmeAnaState = match ana_state_str.parse() {
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
        .set_nvme_ana_state(v0::SetNvmeAnaStateRequest {
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
        .ok_or_else(|| ClientError::MissingValue {
            field: "uuid".to_string(),
        })?
        .to_string();
    let uri = matches
        .value_of("uri")
        .ok_or_else(|| ClientError::MissingValue {
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
        .add_child_nexus(v0::AddChildNexusRequest {
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
        .ok_or_else(|| ClientError::MissingValue {
            field: "uuid".to_string(),
        })?
        .to_string();
    let uri = matches
        .value_of("uri")
        .ok_or_else(|| ClientError::MissingValue {
            field: "uri".to_string(),
        })?
        .to_string();

    let response = ctx
        .client
        .remove_child_nexus(v0::RemoveChildNexusRequest {
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

async fn injections(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> crate::Result<()> {
    let uuid = matches
        .value_of("uuid")
        .ok_or_else(|| ClientError::MissingValue {
            field: "uuid".to_string(),
        })?
        .to_string();

    let inj_add = matches.values_of("add");
    let inj_remove = matches.values_of("remove");

    if inj_add.is_none() && inj_remove.is_none() {
        return list_nexus_injections(ctx, &uuid).await;
    }

    if let Some(uris) = inj_add {
        for uri in uris {
            println!("Injecting fault: {}", uri);
            ctx.client
                .inject_nexus_fault(v0::InjectNexusFaultRequest {
                    uuid: uuid.clone(),
                    uri: uri.to_owned(),
                })
                .await
                .context(GrpcStatus)?;
        }
    }

    if let Some(uris) = inj_remove {
        for uri in uris {
            println!("Removing injected fault: {}", uri);
            ctx.client
                .remove_injected_nexus_fault(
                    v0::RemoveInjectedNexusFaultRequest {
                        uuid: uuid.clone(),
                        uri: uri.to_owned(),
                    },
                )
                .await
                .context(GrpcStatus)?;
        }
    }

    Ok(())
}

async fn list_nexus_injections(
    mut ctx: Context,
    uuid: &str,
) -> crate::Result<()> {
    let response = ctx
        .client
        .list_injected_nexus_faults(v0::ListInjectedNexusFaultsRequest {
            uuid: uuid.to_owned(),
        })
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

fn ana_state_idx_to_str(idx: i32) -> &'static str {
    match v0::NvmeAnaState::from_i32(idx).unwrap() {
        v0::NvmeAnaState::NvmeAnaInvalidState => "invalid",
        v0::NvmeAnaState::NvmeAnaOptimizedState => "optimized",
        v0::NvmeAnaState::NvmeAnaNonOptimizedState => "non_optimized",
        v0::NvmeAnaState::NvmeAnaInaccessibleState => "inaccessible",
        v0::NvmeAnaState::NvmeAnaPersistentLossState => "persistent_loss",
        v0::NvmeAnaState::NvmeAnaChangeState => "change",
    }
}

fn nexus_state_to_str(idx: i32) -> &'static str {
    match v0::NexusState::from_i32(idx).unwrap() {
        v0::NexusState::NexusUnknown => "unknown",
        v0::NexusState::NexusOnline => "online",
        v0::NexusState::NexusDegraded => "degraded",
        v0::NexusState::NexusFaulted => "faulted",
        v0::NexusState::NexusShuttingDown => "shutting_down",
        v0::NexusState::NexusShutdown => "shutdown",
    }
}

fn child_state_to_str_v0(s: v0::ChildState) -> &'static str {
    match s {
        v0::ChildState::ChildUnknown => "unknown",
        v0::ChildState::ChildOnline => "online",
        v0::ChildState::ChildDegraded => "degraded",
        v0::ChildState::ChildFaulted => "faulted",
    }
}

fn child_state_to_str_v1(s: v1::nexus::ChildState) -> &'static str {
    match s {
        v1::nexus::ChildState::Unknown => "unknown",
        v1::nexus::ChildState::Online => "online",
        v1::nexus::ChildState::Degraded => "degraded",
        v1::nexus::ChildState::Faulted => "faulted",
    }
}

fn child_reason_to_str_v1(r: v1::nexus::ChildStateReason) -> &'static str {
    match r {
        v1::nexus::ChildStateReason::None => "-",
        v1::nexus::ChildStateReason::Init => "init",
        v1::nexus::ChildStateReason::Closed => "closed",
        v1::nexus::ChildStateReason::CannotOpen => "cannot open",
        v1::nexus::ChildStateReason::ConfigInvalid => "config invalid",
        v1::nexus::ChildStateReason::RebuildFailed => "rebuild failed",
        v1::nexus::ChildStateReason::IoFailure => "I/O failure",
        v1::nexus::ChildStateReason::ByClient => "by client",
        v1::nexus::ChildStateReason::OutOfSync => "out of sync",
        v1::nexus::ChildStateReason::NoSpace => "no space",
        v1::nexus::ChildStateReason::TimedOut => "timed out",
        v1::nexus::ChildStateReason::AdminFailed => "admin failed",
    }
}
