use super::nexus_child_cli;
use crate::{
    context::{Context, OutputFormat},
    GrpcStatus,
};
use byte_unit::Byte;
use clap::{Args, Subcommand};
use colored_json::ToColoredJson;
use io_engine_api::{v0, v1};
use snafu::ResultExt;
use std::convert::TryFrom;
use tonic::{Code, Status};
use uuid::Uuid;

fn parse_byte(s: &str) -> Result<Byte, String> {
    Byte::parse_str(s, true).map_err(|e| e.to_string())
}

/// Share protocol for a nexus
#[derive(Debug, Clone, clap::ValueEnum)]
pub(super) enum NexusShareProtocol {
    Nvmf,
}

/// NVMe ANA state
#[derive(Debug, Clone, clap::ValueEnum)]
enum NvmeAnaState {
    Optimized,
    #[value(name = "non_optimized")]
    NonOptimized,
    Inaccessible,
}

#[derive(Debug, Args)]
#[command(subcommand_required = true, arg_required_else_help = true)]
pub struct NexusArgs {
    #[command(subcommand)]
    command: NexusCommands,
}

#[derive(Debug, Subcommand)]
enum NexusCommands {
    /// Create a new nexus device
    Create(CreateArgs),
    /// Create a new nexus device with NVMe options
    #[command(name = "create2")]
    Create2(Create2Args),
    /// Destroy the nexus with the given uuid
    Destroy(UuidArgs),
    /// Shutdown the nexus with the given uuid
    Shutdown(UuidArgs),
    /// Publish the nexus
    Publish(PublishArgs),
    /// Unpublish the nexus
    Unpublish(UuidArgs),
    /// Get or set the NVMe ANA state of the nexus
    #[command(name = "ana_state")]
    AnaState(AnaStateArgs),
    /// Add a child to the nexus
    Add(AddArgs),
    /// Remove a child from the nexus
    Remove(RemoveArgs),
    /// List all nexus devices
    List(ListArgs),
    /// List all nexus devices (v2)
    #[command(name = "list2")]
    List2(ListArgs),
    /// List children of a nexus
    Children(UuidArgs),
    /// List children of a nexus (v2)
    #[command(name = "children2")]
    Children2(UuidArgs),
    /// Nexus child operations
    Child(nexus_child_cli::ChildArgs),
}

#[derive(Debug, Args)]
struct CreateArgs {
    /// uuid for the nexus (omit to autogenerate)
    #[arg(long)]
    uuid: Option<Uuid>,
    /// size of the nexus
    #[arg(value_parser = parse_byte)]
    size: Byte,
    /// list of children to add
    #[arg(action = clap::ArgAction::Append)]
    children: Vec<url::Url>,
}

#[derive(Debug, Args)]
struct Create2Args {
    /// name of the nexus
    name: String,
    /// uuid for the nexus (omit to autogenerate)
    #[arg(long)]
    uuid: Option<Uuid>,
    /// size of the nexus
    #[arg(value_parser = parse_byte)]
    size: Byte,
    /// minimum NVMe controller ID for sharing over NVMf
    min_cntlid: u32,
    /// maximum NVMe controller ID
    max_cntlid: u32,
    /// NVMe reservation key for children
    resv_key: u64,
    /// NVMe preempt key for children (0 for no preemption)
    preempt_key: u64,
    /// key used to persist the NexusInfo structure
    nexus_info_key: String,
    /// list of children to add
    #[arg(action = clap::ArgAction::Append)]
    children: Vec<url::Url>,
}

#[derive(Debug, Args)]
struct UuidArgs {
    /// uuid for the nexus
    uuid: Uuid,
}

#[derive(Debug, Args)]
struct PublishArgs {
    /// uuid for the nexus
    uuid: Uuid,
    /// crypto key to use
    key: Option<String>,
    /// protocol used for publishing the nexus remotely
    #[arg(short = 'p', long)]
    protocol: Option<NexusShareProtocol>,
    /// NQN of hosts which are allowed to connect to the target
    #[arg(long = "allowed-host", action = clap::ArgAction::Append)]
    allowed_host: Vec<String>,
}

#[derive(Debug, Args)]
struct AnaStateArgs {
    /// uuid for the nexus
    uuid: Uuid,
    /// NVMe ANA state to set (omit to get current state)
    state: Option<NvmeAnaState>,
}

#[derive(Debug, Args)]
struct AddArgs {
    /// uuid for the nexus
    uuid: Uuid,
    /// uri of child to add
    uri: url::Url,
    /// disable automatic rebuild
    #[arg(long, default_value_t = false)]
    norebuild: bool,
}

#[derive(Debug, Args)]
struct RemoveArgs {
    /// uuid for the nexus
    uuid: Uuid,
    /// uri of child to remove
    uri: url::Url,
}

#[derive(Debug, Args)]
struct ListArgs {
    /// also show children
    #[arg(short = 'c', long = "show-children")]
    show_children: bool,
}

pub async fn handler(ctx: Context, args: NexusArgs) -> crate::Result<()> {
    match args.command {
        NexusCommands::Create(args) => nexus_create(ctx, args).await,
        NexusCommands::Create2(args) => nexus_create_v2(ctx, args).await,
        NexusCommands::Destroy(args) => nexus_destroy(ctx, args).await,
        NexusCommands::Shutdown(args) => nexus_shutdown(ctx, args).await,
        NexusCommands::Publish(args) => nexus_publish(ctx, args).await,
        NexusCommands::Unpublish(args) => nexus_unpublish(ctx, args).await,
        NexusCommands::AnaState(args) => nexus_nvme_ana_state(ctx, args).await,
        NexusCommands::Add(args) => nexus_add(ctx, args).await,
        NexusCommands::Remove(args) => nexus_remove(ctx, args).await,
        NexusCommands::List(args) => nexus_list(ctx, args).await,
        NexusCommands::List2(args) => nexus_list_v2(ctx, args).await,
        NexusCommands::Children(args) => nexus_children(ctx, args).await,
        NexusCommands::Children2(args) => nexus_children_2(ctx, args).await,
        NexusCommands::Child(args) => nexus_child_cli::handler(ctx, args).await,
    }
}

fn parse_uuid(uuid: Option<Uuid>) -> String {
    uuid.map(|u| u.to_string())
        .unwrap_or_else(|| Uuid::new_v4().to_string())
}

async fn nexus_create(mut ctx: Context, args: CreateArgs) -> crate::Result<()> {
    let uuid = parse_uuid(args.uuid);
    let size = args.size.as_u64();
    let children = args.children;
    if children.is_empty() {
        return Err(crate::ClientError::MissingValue {
            field: "children".to_string(),
        });
    }

    let response = ctx
        .client
        .create_nexus(v0::CreateNexusRequest {
            uuid: uuid.clone(),
            size,
            children: children.iter().map(|u| u.to_string()).collect(),
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
            let val = &response.get_ref().uuid;
            println!("{val}");
        }
    };

    Ok(())
}

async fn nexus_create_v2(mut ctx: Context, args: Create2Args) -> crate::Result<()> {
    let uuid = parse_uuid(args.uuid);
    let size = args.size.as_u64();
    let children = args.children;
    if children.is_empty() {
        return Err(crate::ClientError::MissingValue {
            field: "children".to_string(),
        });
    }

    let response = ctx
        .client
        .create_nexus_v2(v0::CreateNexusV2Request {
            name: args.name.clone(),
            uuid: uuid.clone(),
            size,
            min_cntl_id: args.min_cntlid,
            max_cntl_id: args.max_cntlid,
            resv_key: args.resv_key,
            preempt_key: args.preempt_key,
            children: children.iter().map(|u| u.to_string()).collect(),
            nexus_info_key: args.nexus_info_key,
            resv_type: None,
            preempt_policy: 0,
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
            let val = &response.get_ref().uuid;
            println!("{val}");
        }
    };

    Ok(())
}

async fn nexus_shutdown(mut ctx: Context, args: UuidArgs) -> crate::Result<()> {
    let uuid = args.uuid.to_string();
    let response = ctx
        .client
        .shutdown_nexus(v0::ShutdownNexusRequest { uuid: uuid.clone() })
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

async fn nexus_destroy(mut ctx: Context, args: UuidArgs) -> crate::Result<()> {
    let uuid = args.uuid.to_string();
    let response = ctx
        .client
        .destroy_nexus(v0::DestroyNexusRequest { uuid: uuid.clone() })
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

async fn nexus_list(mut ctx: Context, args: ListArgs) -> crate::Result<()> {
    let response = ctx
        .client
        .list_nexus(v0::Null {})
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
            let nexus = &response.get_ref().nexus_list;
            if nexus.is_empty() {
                ctx.v1("No nexus found");
                return Ok(());
            }
            ctx.v2("Found following nexus:");
            let table = nexus
                .iter()
                .map(|n| {
                    let size = ctx.units(Byte::from_u64(n.size));
                    let state = nexus_state_to_str(n.state);
                    let mut row = vec![
                        n.uuid.clone(),
                        size,
                        state.to_string(),
                        n.rebuilds.to_string(),
                        n.device_uri.clone(),
                    ];
                    if args.show_children {
                        row.push(
                            n.children
                                .iter()
                                .map(|c| c.uri.clone())
                                .collect::<Vec<_>>()
                                .join(","),
                        );
                    }
                    row
                })
                .collect();
            let mut hdr = vec!["NAME", ">SIZE", "STATE", ">REBUILDS", "PATH"];
            if args.show_children {
                hdr.push("CHILDREN");
            }
            ctx.print_list(hdr, table);
        }
    };

    Ok(())
}

async fn nexus_list_v2(mut ctx: Context, args: ListArgs) -> crate::Result<()> {
    let response = ctx
        .client
        .list_nexus_v2(v0::Null {})
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
            let nexus = &response.get_ref().nexus_list;
            if nexus.is_empty() {
                ctx.v1("No nexus found");
                return Ok(());
            }
            ctx.v2("Found following nexus:");
            let table = nexus
                .iter()
                .map(|n| {
                    let size = ctx.units(Byte::from_u64(n.size));
                    let state = nexus_state_to_str(n.state);
                    let mut row = vec![
                        n.name.clone(),
                        n.uuid.clone(),
                        size,
                        state.to_string(),
                        n.rebuilds.to_string(),
                        n.device_uri.clone(),
                    ];
                    if args.show_children {
                        row.push(
                            n.children
                                .iter()
                                .map(|c| c.uri.clone())
                                .collect::<Vec<_>>()
                                .join(","),
                        );
                    }
                    row
                })
                .collect();
            let mut hdr = vec!["NAME", "UUID", ">SIZE", "STATE", ">REBUILDS", "PATH"];
            if args.show_children {
                hdr.push("CHILDREN");
            }
            ctx.print_list(hdr, table);
        }
    };

    Ok(())
}

async fn nexus_children(mut ctx: Context, args: UuidArgs) -> crate::Result<()> {
    let uuid = args.uuid.to_string();
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
        .ok_or_else(|| Status::new(Code::InvalidArgument, "Specified nexus not found"))
        .context(GrpcStatus)?;

    match ctx.output {
        OutputFormat::Json => {
            let json = serde_json::to_string_pretty(&nexus.children)
                .unwrap()
                .to_colored_json_auto()
                .unwrap();
            println!("{json}");
        }
        OutputFormat::Default => {
            let table = nexus
                .children
                .iter()
                .map(|c| {
                    let state = child_state_to_str_v0(v0::ChildState::try_from(c.state).unwrap());
                    vec![c.uri.clone(), state.to_string()]
                })
                .collect();
            ctx.print_list(vec!["NAME", "STATE"], table);
        }
    };

    Ok(())
}

async fn nexus_children_2(mut ctx: Context, args: UuidArgs) -> crate::Result<()> {
    let uuid = args.uuid.to_string();
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
        .ok_or_else(|| Status::new(Code::InvalidArgument, "Specified nexus not found"))
        .context(GrpcStatus)?;

    match ctx.output {
        OutputFormat::Json => {
            let json = serde_json::to_string_pretty(&nexus.children)
                .unwrap()
                .to_colored_json_auto()
                .unwrap();
            println!("{json}");
        }
        OutputFormat::Default => {
            let table = nexus
                .children
                .iter()
                .map(|c| {
                    let state =
                        child_state_to_str_v1(v1::nexus::ChildState::try_from(c.state).unwrap());
                    let reason = child_reason_to_str_v1(
                        v1::nexus::ChildStateReason::try_from(c.state_reason).unwrap(),
                    );
                    vec![c.uri.clone(), state.to_string(), reason.to_string()]
                })
                .collect();
            ctx.print_list(vec!["NAME", "STATE", "REASON"], table);
        }
    };

    Ok(())
}

async fn nexus_publish(mut ctx: Context, args: PublishArgs) -> crate::Result<()> {
    let uuid = args.uuid.to_string();
    let key = args.key.unwrap_or_default();
    let protocol = match args.protocol {
        None => v0::ShareProtocolNexus::NexusNbd,
        Some(NexusShareProtocol::Nvmf) => v0::ShareProtocolNexus::NexusNvmf,
    };
    let allowed_hosts = args.allowed_host;

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
            let json = serde_json::to_string_pretty(response.get_ref())
                .unwrap()
                .to_colored_json_auto()
                .unwrap();
            println!("{json}");
        }
        OutputFormat::Default => {
            let device_uri = &response.get_ref().device_uri;
            println!("{device_uri}");
        }
    };

    Ok(())
}

async fn nexus_unpublish(mut ctx: Context, args: UuidArgs) -> crate::Result<()> {
    let uuid = args.uuid.to_string();
    let response = ctx
        .client
        .unpublish_nexus(v0::UnpublishNexusRequest { uuid: uuid.clone() })
        .await
        .context(GrpcStatus)?;

    match ctx.output {
        OutputFormat::Json => {
            let json = serde_json::to_string_pretty(response.get_ref())
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

async fn nexus_nvme_ana_state(ctx: Context, args: AnaStateArgs) -> crate::Result<()> {
    let uuid = args.uuid.to_string();
    match args.state {
        None => nexus_get_nvme_ana_state(ctx, uuid).await,
        Some(state) => nexus_set_nvme_ana_state(ctx, uuid, state).await,
    }
}

async fn nexus_get_nvme_ana_state(mut ctx: Context, uuid: String) -> crate::Result<()> {
    let resp = ctx
        .client
        .get_nvme_ana_state(v0::GetNvmeAnaStateRequest { uuid: uuid.clone() })
        .await
        .context(GrpcStatus)?;
    ctx.v1(ana_state_idx_to_str(resp.get_ref().ana_state));
    Ok(())
}

async fn nexus_set_nvme_ana_state(
    mut ctx: Context,
    uuid: String,
    state: NvmeAnaState,
) -> crate::Result<()> {
    let ana_state: v0::NvmeAnaState = match state {
        NvmeAnaState::Optimized => v0::NvmeAnaState::NvmeAnaOptimizedState,
        NvmeAnaState::NonOptimized => v0::NvmeAnaState::NvmeAnaNonOptimizedState,
        NvmeAnaState::Inaccessible => v0::NvmeAnaState::NvmeAnaInaccessibleState,
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

async fn nexus_add(mut ctx: Context, args: AddArgs) -> crate::Result<()> {
    let uuid = args.uuid.to_string();
    let response = ctx
        .client
        .add_child_nexus(v0::AddChildNexusRequest {
            uuid: uuid.clone(),
            uri: args.uri.to_string(),
            norebuild: args.norebuild,
        })
        .await
        .context(GrpcStatus)?;

    match ctx.output {
        OutputFormat::Json => {
            let json = serde_json::to_string_pretty(response.get_ref())
                .unwrap()
                .to_colored_json_auto()
                .unwrap();
            println!("{json}");
        }
        OutputFormat::Default => {
            let uuid = &args.uuid;
            println!("{uuid}");
        }
    };

    Ok(())
}

async fn nexus_remove(mut ctx: Context, args: RemoveArgs) -> crate::Result<()> {
    let response = ctx
        .client
        .remove_child_nexus(v0::RemoveChildNexusRequest {
            uuid: args.uuid.to_string(),
            uri: args.uri.to_string(),
        })
        .await
        .context(GrpcStatus)?;

    match ctx.output {
        OutputFormat::Json => {
            let json = serde_json::to_string_pretty(response.get_ref())
                .unwrap()
                .to_colored_json_auto()
                .unwrap();
            println!("{json}");
        }
        OutputFormat::Default => {
            let uri = &args.uri;
            println!("{uri}");
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
        v1::nexus::ChildStateReason::HotRemoved => "hot removed",
    }
}
