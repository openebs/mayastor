use super::nexus_child_cli;
use crate::{
    context::{Context, OutputFormat},
    parse_size,
    ClientError,
    GrpcStatus,
};
use byte_unit::Byte;
use clap::{Arg, ArgMatches, Command};
use colored_json::ToColoredJson;
use io_engine_api::{v0, v1};
use snafu::ResultExt;
use std::convert::TryFrom;
use tonic::{Code, Status};
use uuid::Uuid;

pub fn subcommands() -> Command {
    let create = Command::new("create")
        .about("Create a new nexus device")
        .arg(
            Arg::new("uuid")
                .required(true)
                .index(1)
                .help("uuid for the nexus, if uuid is not known please provide \"\" to autogenerate"),
        )
        .arg(
            Arg::new("size")
                .required(true)
                .index(2)
                .help("size with optional unit suffix"),
        )
        .arg(
            Arg::new("children")
                .required(true)
                .action(clap::ArgAction::Append)
                .index(3)
                .help("list of children to add"),
        );

    let create_v2 = Command::new("create2")
        .about("Create a new nexus device with NVMe options")
        .arg(
            Arg::new("name")
                .required(true)
                .index(1)
                .help("name of the nexus"),
        )
        .arg(
            Arg::new("uuid")
                .required(true)
                .help("uuid for the nexus, if uuid is not known please provide \"\" to autogenerate"),
        )
        .arg(
            Arg::new("size")
                .required(true)
                .help("size with optional unit suffix"),
        )
        .arg(
            Arg::new("min-cntlid")
                .required(true)
                .value_parser(clap::value_parser!(u32))
                .help("minimum NVMe controller ID for sharing over NVMf"),
        )
        .arg(
            Arg::new("max-cntlid")
                .required(true)
                .value_parser(clap::value_parser!(u32))
                .help("maximum NVMe controller ID"),
        )
        .arg(
            Arg::new("resv-key")
                .required(true)
                .value_parser(clap::value_parser!(u64))
                .help("NVMe reservation key for children"),
        )
        .arg(
            Arg::new("preempt-key")
                .required(true)
                .value_parser(clap::value_parser!(u64))
                .help("NVMe preempt key for children, 0 for no preemption"),
        )
        .arg(
            Arg::new("nexus-info-key")
                .required(true)
                .help("Key used to persist the NexusInfo structure to the persistent store"),
        )
        .arg(
            Arg::new("children")
                .required(true)
                .action(clap::ArgAction::Append)
                .help("list of children to add"),
        );

    let destroy = Command::new("destroy")
        .about("destroy the nexus with given name")
        .arg(
            Arg::new("uuid")
                .required(true)
                .index(1)
                .help("uuid for the nexus"),
        );

    let shutdown = Command::new("shutdown")
        .about("shutdown the nexus with given name")
        .arg(
            Arg::new("uuid")
                .required(true)
                .index(1)
                .help("uuid for the nexus"),
        );

    let publish = Command::new("publish")
        .about("publish the nexus")
        .arg(Arg::new("protocol").short('p').long("protocol").value_name("PROTOCOL")
            .help("Name of a protocol (nvmf) used for publishing the nexus remotely"))
        .arg(Arg::new("uuid").required(true).index(1)
            .help("uuid for the nexus"))
        .arg(Arg::new("key").required(false).index(2)
            .help("crypto key to use"))
        .arg(
            Arg::new("allowed-host")
                .long("allowed-host")

                .action(clap::ArgAction::Append)
                .required(false)
                .help("NQN of hosts which are allowed to connect to the target"));

    let unpublish = Command::new("unpublish").about("unpublish the nexus").arg(
        Arg::new("uuid")
            .required(true)
            .index(1)
            .help("uuid for the nexus"),
    );

    let ana_state = Command::new("ana_state")
        .about("get or set the NVMe ANA state of the nexus")
        .arg(
            Arg::new("uuid")
                .required(true)
                .index(1)
                .help("uuid for the nexus"),
        )
        .arg(
            Arg::new("state")
                .required(false)
                .index(2)
                .value_parser(["optimized", "non_optimized", "inaccessible"])
                .help("NVMe ANA state of the nexus"),
        );

    let add = Command::new("add")
        .about("add a child")
        .arg(
            Arg::new("uuid")
                .required(true)
                .index(1)
                .help("uuid for the nexus"),
        )
        .arg(
            Arg::new("uri")
                .required(true)
                .index(2)
                .help("uri of child to add"),
        )
        .arg(
            Arg::new("norebuild")
                .default_value("false")
                .index(3)
                .help("specify if a rebuild job runs automatically"),
        );

    let remove = Command::new("remove")
        .about("remove a child")
        .arg(
            Arg::new("uuid")
                .required(true)
                .index(1)
                .help("uuid for the nexus"),
        )
        .arg(
            Arg::new("uri")
                .required(true)
                .index(2)
                .help("uri of child to remove"),
        );

    let list = Command::new("list").about("list all nexus devices").arg(
        Arg::new("children")
            .short('c')
            .long("show-children")
            .required(false)
            .action(clap::ArgAction::SetTrue),
    );

    let list2 = Command::new("list2").about("list all nexus devices").arg(
        Arg::new("children")
            .short('c')
            .long("show-children")
            .required(false)
            .action(clap::ArgAction::SetTrue),
    );

    let children = Command::new("children").about("list nexus children").arg(
        Arg::new("uuid")
            .required(true)
            .index(1)
            .help("uuid of nexus"),
    );

    let children_2 =
        Command::new("children2").about("list nexus children").arg(
            Arg::new("uuid")
                .required(true)
                .index(1)
                .help("uuid of nexus"),
        );

    Command::new("nexus")
        .subcommand_required(true)
        .arg_required_else_help(true)
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
        .subcommand(nexus_child_cli::subcommands())
}

pub async fn handler(ctx: Context, matches: &ArgMatches) -> crate::Result<()> {
    match matches.subcommand().unwrap() {
        ("create", args) => nexus_create(ctx, args).await,
        ("create2", args) => nexus_create_v2(ctx, args).await,
        ("destroy", args) => nexus_destroy(ctx, args).await,
        ("shutdown", args) => nexus_shutdown(ctx, args).await,
        ("list", args) => nexus_list(ctx, args).await,
        ("list2", args) => nexus_list_v2(ctx, args).await,
        ("children", args) => nexus_children(ctx, args).await,
        ("children2", args) => nexus_children_2(ctx, args).await,
        ("publish", args) => nexus_publish(ctx, args).await,
        ("unpublish", args) => nexus_unpublish(ctx, args).await,
        ("ana_state", args) => nexus_nvme_ana_state(ctx, args).await,
        ("add", args) => nexus_add(ctx, args).await,
        ("remove", args) => nexus_remove(ctx, args).await,
        ("child", args) => nexus_child_cli::handler(ctx, args).await,
        (cmd, _) => {
            Err(Status::not_found(format!("command {cmd} does not exist")))
                .context(GrpcStatus)
        }
    }
}

fn nexus_create_parse(
    matches: &ArgMatches,
) -> crate::Result<(
    ::prost::alloc::string::String,
    u64,
    ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
)> {
    let mut uuid = matches.get_one::<String>("uuid").unwrap().to_string();
    //If uuid is not specified then generate new uuid.
    if uuid.is_empty() {
        uuid = Uuid::new_v4().to_string()
    }
    let size =
        parse_size(matches.get_one::<String>("size").ok_or_else(|| {
            ClientError::MissingValue {
                field: "size".to_string(),
            }
        })?)
        .map_err(|s| Status::invalid_argument(format!("Bad size '{s}'")))
        .context(GrpcStatus)?;
    let children = matches
        .get_many::<String>("children")
        .ok_or_else(|| ClientError::MissingValue {
            field: "children".to_string(),
        })?
        .cloned()
        .collect();
    let size = size.get_bytes() as u64;
    Ok((uuid, size, children))
}

async fn nexus_create(
    mut ctx: Context,
    matches: &ArgMatches,
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
    matches: &ArgMatches,
) -> crate::Result<()> {
    let (uuid, size, children) = nexus_create_parse(matches)?;
    let name = matches.get_one::<String>("name").unwrap().to_string();
    let min_cntl_id = *matches.get_one::<u32>("min-cntlid").unwrap();
    let max_cntl_id = *matches.get_one::<u32>("max-cntlid").unwrap();
    let resv_key = *matches.get_one::<u64>("resv-key").unwrap();
    let preempt_key = *matches.get_one::<u64>("preempt-key").unwrap();
    let nexus_info_key = matches
        .get_one::<String>("nexus-info-key")
        .unwrap()
        .to_owned();

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
            resv_type: None,
            preempt_policy: 0,
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
    matches: &ArgMatches,
) -> crate::Result<()> {
    let uuid = matches.get_one::<String>("uuid").unwrap().to_string();

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
    matches: &ArgMatches,
) -> crate::Result<()> {
    let uuid = matches.get_one::<String>("uuid").unwrap().to_string();

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
    matches: &ArgMatches,
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
            let show_child = matches.get_flag("children");

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
    matches: &ArgMatches,
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
            let show_child = matches.get_flag("children");

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
    matches: &ArgMatches,
) -> crate::Result<()> {
    let uuid = matches
        .get_one::<String>("uuid")
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
                        v0::ChildState::try_from(c.state).unwrap(),
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
    matches: &ArgMatches,
) -> crate::Result<()> {
    let uuid = matches
        .get_one::<String>("uuid")
        .ok_or_else(|| ClientError::MissingValue {
            field: "uuid".to_string(),
        })?
        .to_string();

    let response = ctx
        .v1
        .nexus
        .list_nexus(v1::nexus::ListNexusOptions {
            name: None,
            uuid: None,
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
                        v1::nexus::ChildState::try_from(c.state).unwrap(),
                    );
                    let reason = child_reason_to_str_v1(
                        v1::nexus::ChildStateReason::try_from(c.state_reason)
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
    matches: &ArgMatches,
) -> crate::Result<()> {
    let uuid = matches
        .get_one::<String>("uuid")
        .ok_or_else(|| ClientError::MissingValue {
            field: "uuid".to_string(),
        })?
        .to_string();
    let key = matches
        .get_one::<String>("key")
        .cloned()
        .unwrap_or_default();
    let protocol =
        match matches.get_one::<String>("protocol").map(|s| s.as_str()) {
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
    let allowed_hosts = matches
        .get_many::<String>("allowed-host")
        .unwrap_or_default()
        .cloned()
        .collect();

    let response = ctx
        .client
        .publish_nexus(v0::PublishNexusRequest {
            uuid,
            key,
            share: protocol.into(),
            allowed_hosts,
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
    matches: &ArgMatches,
) -> crate::Result<()> {
    let uuid = matches
        .get_one::<String>("uuid")
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
    matches: &ArgMatches,
) -> crate::Result<()> {
    let uuid = matches.get_one::<String>("uuid").unwrap().to_string();
    let ana_state = matches
        .get_one::<String>("state")
        .cloned()
        .unwrap_or_default();
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
    matches: &ArgMatches,
) -> crate::Result<()> {
    let uuid = matches
        .get_one::<String>("uuid")
        .ok_or_else(|| ClientError::MissingValue {
            field: "uuid".to_string(),
        })?
        .to_string();
    let uri = matches
        .get_one::<String>("uri")
        .ok_or_else(|| ClientError::MissingValue {
            field: "uri".to_string(),
        })?
        .to_string();
    let norebuild = matches
        .get_one::<String>("norebuild")
        .unwrap()
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
    matches: &ArgMatches,
) -> crate::Result<()> {
    let uuid = matches
        .get_one::<String>("uuid")
        .ok_or_else(|| ClientError::MissingValue {
            field: "uuid".to_string(),
        })?
        .to_string();
    let uri = matches
        .get_one::<String>("uri")
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

fn ana_state_idx_to_str(idx: i32) -> &'static str {
    match v0::NvmeAnaState::try_from(idx).unwrap() {
        v0::NvmeAnaState::NvmeAnaInvalidState => "invalid",
        v0::NvmeAnaState::NvmeAnaOptimizedState => "optimized",
        v0::NvmeAnaState::NvmeAnaNonOptimizedState => "non_optimized",
        v0::NvmeAnaState::NvmeAnaInaccessibleState => "inaccessible",
        v0::NvmeAnaState::NvmeAnaPersistentLossState => "persistent_loss",
        v0::NvmeAnaState::NvmeAnaChangeState => "change",
    }
}

fn nexus_state_to_str(idx: i32) -> &'static str {
    match v0::NexusState::try_from(idx).unwrap() {
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
