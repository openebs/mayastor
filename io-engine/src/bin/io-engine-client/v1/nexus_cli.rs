use super::nexus_child_cli;
use crate::{
    context::{Context, OutputFormat},
    GrpcStatus,
};
use byte_unit::Byte;
use clap::{Args, Subcommand};
use colored_json::ToColoredJson;
use io_engine_api::{v1, v1::nexus::NvmeReservation};
use snafu::ResultExt;
use std::convert::TryFrom;
use tonic::{Code, Status};
use uuid::Uuid;

fn parse_byte(s: &str) -> Result<Byte, String> {
    Byte::parse_str(s, true).map_err(|e| e.to_string())
}

/// Share protocol for nexus publish
#[derive(Debug, Clone, clap::ValueEnum)]
pub(super) enum NexusShareProtocol {
    Nvmf,
}

/// NVMe reservation type
#[derive(Debug, Clone, clap::ValueEnum)]
enum ResvType {
    Reserved,
    WriteExclusive,
    WriteExclusiveRegsOnly,
    ExclusiveAccessRegsOnly,
    ExclusiveAccessAllRegs,
    WriteExclusiveAllRegs,
}

impl From<ResvType> for NvmeReservation {
    fn from(v: ResvType) -> Self {
        match v {
            ResvType::Reserved => NvmeReservation::Reserved,
            ResvType::WriteExclusive => NvmeReservation::WriteExclusive,
            ResvType::WriteExclusiveRegsOnly => NvmeReservation::WriteExclusiveRegsOnly,
            ResvType::ExclusiveAccessRegsOnly => NvmeReservation::ExclusiveAccessRegsOnly,
            ResvType::ExclusiveAccessAllRegs => NvmeReservation::ExclusiveAccessAllRegs,
            ResvType::WriteExclusiveAllRegs => NvmeReservation::WriteExclusiveAllRegs,
        }
    }
}

/// NVMe ANA state
#[derive(Debug, Clone, clap::ValueEnum)]
enum NvmeAnaState {
    Optimized,
    #[value(name = "non_optimized")]
    NonOptimized,
    Inaccessible,
}

/// Nexus on-disk label version
#[derive(Debug, Default, Clone, clap::ValueEnum)]
enum NexusLabelVersion {
    #[default]
    V1,
    V2,
}

impl From<NexusLabelVersion> for v1::nexus::NexusLabelVersion {
    fn from(v: NexusLabelVersion) -> Self {
        match v {
            NexusLabelVersion::V1 => v1::nexus::NexusLabelVersion::LabelV1,
            NexusLabelVersion::V2 => v1::nexus::NexusLabelVersion::LabelV2,
        }
    }
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
    /// List children of a nexus
    Children(UuidArgs),
    /// Resize the nexus
    Resize(ResizeArgs),
    /// Nexus child operations
    Child(nexus_child_cli::ChildArgs),
}

#[derive(Debug, Args)]
struct CreateArgs {
    /// uuid for the nexus (use empty string "" to autogenerate)
    uuid: String,
    /// size of the nexus
    #[arg(value_parser = parse_byte)]
    size: Byte,
    /// list of children to add
    #[arg(action = clap::ArgAction::Append)]
    children: Vec<url::Url>,
    /// name of the nexus (defaults to uuid)
    #[arg(long)]
    name: Option<String>,
    /// minimum NVMe controller ID for sharing over NVMf
    #[arg(long = "min-cntlid", default_value_t = 1u32)]
    min_cntlid: u32,
    /// maximum NVMe controller ID
    #[arg(long = "max-cntlid", default_value_t = 65519u32)]
    max_cntlid: u32,
    /// NVMe reservation key for children
    #[arg(long = "resv-key", default_value_t = 0u64)]
    resv_key: u64,
    /// NVMe preempt key for children (0 for no preemption)
    #[arg(long = "preempt-key", default_value_t = 0u64)]
    preempt_key: u64,
    /// NVMe reservation type
    #[arg(long = "resv-type")]
    resv_type: Option<ResvType>,
    /// key used to persist the NexusInfo structure
    #[arg(long = "nexus-info-key", default_value = "")]
    nexus_info_key: String,
    /// The nexus on-disk label version.
    #[arg(long)]
    label_version: Option<NexusLabelVersion>,
    /// Disable requested-size enforcement.
    #[arg(long)]
    required_size: Option<bool>,
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

#[derive(Debug, Args)]
struct ResizeArgs {
    /// nexus uuid
    uuid: Uuid,
    /// requested new size of the nexus
    #[arg(value_parser = parse_byte)]
    size: Byte,
}

pub async fn handler(ctx: Context, args: NexusArgs) -> crate::Result<()> {
    match args.command {
        NexusCommands::Create(args) => nexus_create(ctx, args).await,
        NexusCommands::Destroy(args) => nexus_destroy(ctx, args).await,
        NexusCommands::Shutdown(args) => nexus_shutdown(ctx, args).await,
        NexusCommands::Publish(args) => nexus_publish(ctx, args).await,
        NexusCommands::Unpublish(args) => nexus_unpublish(ctx, args).await,
        NexusCommands::AnaState(args) => nexus_nvme_ana_state(ctx, args).await,
        NexusCommands::Add(args) => nexus_add(ctx, args).await,
        NexusCommands::Remove(args) => nexus_remove(ctx, args).await,
        NexusCommands::List(args) => nexus_list(ctx, args).await,
        NexusCommands::Children(args) => nexus_children_2(ctx, args).await,
        NexusCommands::Resize(args) => nexus_resize(ctx, args).await,
        NexusCommands::Child(args) => nexus_child_cli::handler(ctx, args).await,
    }
}

fn parse_uuid(uuid: &str) -> String {
    if uuid.is_empty() {
        Uuid::new_v4().to_string()
    } else {
        uuid.to_string()
    }
}

async fn nexus_create(mut ctx: Context, args: CreateArgs) -> crate::Result<()> {
    let uuid = parse_uuid(&args.uuid);
    let name = args.name.unwrap_or_else(|| uuid.clone());

    if args.children.is_empty() {
        return Err(crate::ClientError::MissingValue {
            field: "children".to_string(),
        });
    }

    let resv_type = args.resv_type.map(|t| NvmeReservation::from(t) as i32);

    let create_request = v1::nexus::CreateNexusRequest {
        name,
        uuid: uuid.clone(),
        size: args.size.as_u64(),
        min_cntl_id: args.min_cntlid,
        max_cntl_id: args.max_cntlid,
        resv_key: args.resv_key,
        preempt_key: args.preempt_key,
        children: args.children.iter().map(|u| u.to_string()).collect(),
        nexus_info_key: args.nexus_info_key,
        resv_type,
        preempt_policy: 0,
    };

    let label_version = args.label_version.unwrap_or(NexusLabelVersion::V2);
    let required_size = args.required_size.unwrap_or(true);

    let api = &mut ctx.v1.nexus;
    let response = match api
        .create_nexus_v2(v1::nexus::CreateNexusV2Request {
            v1: Some(create_request.clone()),
            label_version: v1::nexus::NexusLabelVersion::from(label_version) as i32,
            required_size,
        })
        .await
    {
        Ok(response) => response,
        Err(status) if status.code() == Code::Unimplemented => {
            api.create_nexus(create_request).await.context(GrpcStatus)?
        }
        Err(status) => {
            return Err(crate::ClientError::GrpcStatus {
                source: status,
                backtrace: None,
            });
        }
    };

    match ctx.output {
        OutputFormat::Json => {
            let json = serde_json::to_string_pretty(&response.get_ref())
                .unwrap()
                .to_colored_json_auto()
                .unwrap();
            println!("{json}");
        }
        OutputFormat::Default => {
            let val = &response.get_ref().nexus.as_ref().unwrap().uuid;
            println!("{val}");
        }
    }

    Ok(())
}

async fn nexus_shutdown(mut ctx: Context, args: UuidArgs) -> crate::Result<()> {
    let uuid = args.uuid.to_string();
    let response = ctx
        .v1
        .nexus
        .shutdown_nexus(v1::nexus::ShutdownNexusRequest { uuid: uuid.clone() })
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
    let _response = ctx
        .v1
        .nexus
        .destroy_nexus(v1::nexus::DestroyNexusRequest { uuid: uuid.clone() })
        .await
        .context(GrpcStatus)?;
    let response = ctx
        .v1
        .nexus
        .list_nexus(v1::nexus::ListNexusOptions {
            name: None,
            uuid: None,
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
            println!("{uuid}");
        }
    };

    Ok(())
}

async fn nexus_list(mut ctx: Context, args: ListArgs) -> crate::Result<()> {
    let response = ctx
        .v1
        .nexus
        .list_nexus(v1::nexus::ListNexusOptions {
            name: None,
            uuid: None,
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
                    let fault_timestamp = match &c.fault_timestamp {
                        Some(d) => d.to_string(),
                        None => "-".to_string(),
                    };
                    vec![
                        c.uri.clone(),
                        state.to_string(),
                        reason.to_string(),
                        fault_timestamp,
                    ]
                })
                .collect();
            ctx.print_list(vec!["NAME", "STATE", "REASON", "LAST_FAULTED_AT"], table);
        }
    };

    Ok(())
}

async fn nexus_resize(mut ctx: Context, args: ResizeArgs) -> crate::Result<()> {
    let uuid = args.uuid.to_string();

    let response = ctx
        .v1
        .nexus
        .resize_nexus(v1::nexus::ResizeNexusRequest {
            uuid: uuid.clone(),
            requested_size: args.size.as_u64(),
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
            println!("Resized nexus {uuid} to {}", args.size);
        }
    };

    Ok(())
}

async fn nexus_publish(mut ctx: Context, args: PublishArgs) -> crate::Result<()> {
    let uuid = args.uuid.to_string();
    let key = args.key.unwrap_or_default();
    let protocol = match args.protocol {
        None | Some(NexusShareProtocol::Nvmf) => v1::common::ShareProtocol::Nvmf as i32,
    };
    let allowed_hosts = args.allowed_host;

    let response = ctx
        .v1
        .nexus
        .publish_nexus(v1::nexus::PublishNexusRequest {
            uuid,
            key,
            share: protocol,
            allowed_hosts,
            read_only: None,
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
            println!(
                "Nexus published over: {}",
                response.get_ref().nexus.clone().unwrap().device_uri
            );
        }
    };

    Ok(())
}

async fn nexus_unpublish(mut ctx: Context, args: UuidArgs) -> crate::Result<()> {
    let uuid = args.uuid.to_string();
    let response = ctx
        .v1
        .nexus
        .unpublish_nexus(v1::nexus::UnpublishNexusRequest { uuid: uuid.clone() })
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
        .v1
        .nexus
        .get_nvme_ana_state(v1::nexus::GetNvmeAnaStateRequest { uuid: uuid.clone() })
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
    let ana_state: v1::nexus::NvmeAnaState = match state {
        NvmeAnaState::Optimized => v1::nexus::NvmeAnaState::NvmeAnaOptimizedState,
        NvmeAnaState::NonOptimized => v1::nexus::NvmeAnaState::NvmeAnaNonOptimizedState,
        NvmeAnaState::Inaccessible => v1::nexus::NvmeAnaState::NvmeAnaInaccessibleState,
    };
    ctx.v1
        .nexus
        .set_nvme_ana_state(v1::nexus::SetNvmeAnaStateRequest {
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
        .v1
        .nexus
        .add_child_nexus(v1::nexus::AddChildNexusRequest {
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
        .v1
        .nexus
        .remove_child_nexus(v1::nexus::RemoveChildNexusRequest {
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
            println!("Removed {} from specified nexus", args.uri);
        }
    };

    Ok(())
}

fn ana_state_idx_to_str(idx: i32) -> &'static str {
    match v1::nexus::NvmeAnaState::try_from(idx).unwrap() {
        v1::nexus::NvmeAnaState::NvmeAnaInvalidState => "invalid",
        v1::nexus::NvmeAnaState::NvmeAnaOptimizedState => "optimized",
        v1::nexus::NvmeAnaState::NvmeAnaNonOptimizedState => "non_optimized",
        v1::nexus::NvmeAnaState::NvmeAnaInaccessibleState => "inaccessible",
        v1::nexus::NvmeAnaState::NvmeAnaPersistentLossState => "persistent_loss",
        v1::nexus::NvmeAnaState::NvmeAnaChangeState => "change",
    }
}

fn nexus_state_to_str(idx: i32) -> &'static str {
    match v1::nexus::NexusState::try_from(idx).unwrap() {
        v1::nexus::NexusState::NexusUnknown => "unknown",
        v1::nexus::NexusState::NexusOnline => "online",
        v1::nexus::NexusState::NexusDegraded => "degraded",
        v1::nexus::NexusState::NexusFaulted => "faulted",
        v1::nexus::NexusState::NexusShuttingDown => "shutting_down",
        v1::nexus::NexusState::NexusShutdown => "shutdown",
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
