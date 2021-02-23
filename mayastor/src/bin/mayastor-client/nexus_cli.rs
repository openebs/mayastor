use crate::{context::Context, nexus_child_cli, parse_size};
use ::rpc::mayastor as rpc;
use byte_unit::Byte;
use clap::{App, AppSettings, Arg, ArgMatches, SubCommand};
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
            .help("Name of a protocol (nvmf, iscsi) used for publishing the nexus remotely"))
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
        .subcommand(destroy)
        .subcommand(publish)
        .subcommand(add)
        .subcommand(remove)
        .subcommand(unpublish)
        .subcommand(list)
        .subcommand(children)
        .subcommand(nexus_child_cli::subcommands())
}

pub async fn handler(
    ctx: Context,
    matches: &ArgMatches<'_>,
) -> Result<(), Status> {
    match matches.subcommand() {
        ("create", Some(args)) => nexus_create(ctx, &args).await,
        ("destroy", Some(args)) => nexus_destroy(ctx, &args).await,
        ("list", Some(args)) => nexus_list(ctx, &args).await,
        ("children", Some(args)) => nexus_children(ctx, &args).await,
        ("publish", Some(args)) => nexus_publish(ctx, &args).await,
        ("unpublish", Some(args)) => nexus_unpublish(ctx, &args).await,
        ("add", Some(args)) => nexus_add(ctx, &args).await,
        ("remove", Some(args)) => nexus_remove(ctx, &args).await,
        ("child", Some(args)) => nexus_child_cli::handler(ctx, args).await,
        (cmd, _) => {
            Err(Status::not_found(format!("command {} does not exist", cmd)))
        }
    }
}

async fn nexus_create(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> Result<(), Status> {
    let uuid = matches.value_of("uuid").unwrap().to_string();
    let size = parse_size(matches.value_of("size").unwrap())
        .map_err(|s| Status::invalid_argument(format!("Bad size '{}'", s)))?;
    let children = matches
        .values_of("children")
        .unwrap() // It's required, it'll be here.
        .map(|c| c.to_string())
        .collect::<Vec<String>>();

    ctx.v2(&format!(
        "Creating nexus {} of size {} ",
        uuid,
        ctx.units(size)
    ));
    ctx.v2(&format!(" with children {:?}", children));
    let size = size.get_bytes() as u64;
    ctx.client
        .create_nexus(rpc::CreateNexusRequest {
            uuid: uuid.clone(),
            size,
            children,
        })
        .await?;
    ctx.v1(&format!("Nexus {} created", uuid));
    Ok(())
}

async fn nexus_destroy(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> Result<(), Status> {
    let uuid = matches.value_of("uuid").unwrap().to_string();

    ctx.v2(&format!("Destroying nexus {}", uuid));
    ctx.client
        .destroy_nexus(rpc::DestroyNexusRequest {
            uuid: uuid.clone(),
        })
        .await?;
    ctx.v1(&format!("Nexus {} destroyed", uuid));
    Ok(())
}

async fn nexus_list(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> Result<(), Status> {
    let resp = ctx.client.list_nexus(rpc::Null {}).await?;
    let nexus = &resp.get_ref().nexus_list;
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
                n.device_uri.clone(),
                size,
                state.to_string(),
                n.rebuilds.to_string(),
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
    let mut hdr = vec!["NAME", "PATH", ">SIZE", "STATE", ">REBUILDS"];
    if show_child {
        hdr.push("CHILDREN");
    }
    ctx.print_list(hdr, table);

    Ok(())
}

async fn nexus_children(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> Result<(), Status> {
    let uuid = matches.value_of("uuid").unwrap().to_string();

    let resp = ctx.client.list_nexus(rpc::Null {}).await?;
    let nexus = resp
        .get_ref()
        .nexus_list
        .iter()
        .find(|n| n.uuid == uuid)
        .ok_or_else(|| {
            Status::new(
                Code::InvalidArgument,
                "Specified nexus not found".to_owned(),
            )
        })?;

    ctx.v2(&format!("Children of nexus {}:", uuid));

    let table = nexus
        .children
        .iter()
        .map(|c| {
            let state = child_state_to_str(c.state);
            vec![c.uri.clone(), state.to_string()]
        })
        .collect();
    ctx.print_list(vec!["NAME", "STATE"], table);
    Ok(())
}

async fn nexus_publish(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> Result<(), Status> {
    let uuid = matches.value_of("uuid").unwrap().to_string();
    let key = matches.value_of("key").unwrap_or("").to_string();
    let prot = match matches.value_of("protocol") {
        None => rpc::ShareProtocolNexus::NexusNbd,
        Some("nvmf") => rpc::ShareProtocolNexus::NexusNvmf,
        Some("iscsi") => rpc::ShareProtocolNexus::NexusIscsi,
        Some(_) => {
            return Err(Status::new(
                Code::Internal,
                "Invalid value of share protocol".to_owned(),
            ));
        }
    };

    ctx.v2(&format!("Publishing nexus {} over {:?}", uuid, prot));
    let resp = ctx
        .client
        .publish_nexus(rpc::PublishNexusRequest {
            uuid,
            key,
            share: prot.into(),
        })
        .await?;
    ctx.v1(&format!("Nexus published at {}", resp.get_ref().device_uri));
    Ok(())
}

async fn nexus_unpublish(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> Result<(), Status> {
    let uuid = matches.value_of("uuid").unwrap().to_string();

    ctx.v2(&format!("Unpublishing nexus {}", uuid));
    ctx.client
        .unpublish_nexus(rpc::UnpublishNexusRequest {
            uuid: uuid.clone(),
        })
        .await?;
    ctx.v1(&format!("Nexus {} unpublished", uuid));
    Ok(())
}

async fn nexus_add(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> Result<(), Status> {
    let uuid = matches.value_of("uuid").unwrap().to_string();
    let uri = matches.value_of("uri").unwrap().to_string();
    let norebuild = matches
        .value_of("norebuild")
        .unwrap_or("false")
        .parse::<bool>()
        .unwrap_or(false);

    ctx.v2(&format!("Adding {} to children of {}", uri, uuid));
    ctx.client
        .add_child_nexus(rpc::AddChildNexusRequest {
            uuid: uuid.clone(),
            uri: uri.clone(),
            norebuild,
        })
        .await?;
    ctx.v1(&format!("Added {} to children of {}", uri, uuid));
    Ok(())
}

async fn nexus_remove(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> Result<(), Status> {
    let uuid = matches.value_of("uuid").unwrap().to_string();
    let uri = matches.value_of("uri").unwrap().to_string();

    ctx.v2(&format!("Removing {} from children of {}", uri, uuid));
    ctx.client
        .remove_child_nexus(rpc::RemoveChildNexusRequest {
            uuid: uuid.clone(),
            uri: uri.clone(),
        })
        .await?;
    ctx.v1(&format!("Removed {} from children of {}", uri, uuid));
    Ok(())
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
