//!
//! methods to obtain information about block devices on the current host

use super::context::Context;
use crate::{context::OutputFormat, GrpcStatus};
use clap::{Args, Subcommand};
use colored_json::ToColoredJson;
use io_engine_api::v0 as rpc;
use snafu::ResultExt;

#[derive(Debug, Args)]
#[command(subcommand_required = true, arg_required_else_help = true)]
pub struct DeviceArgs {
    #[command(subcommand)]
    command: DeviceCommands,
}

#[derive(Debug, Subcommand)]
enum DeviceCommands {
    /// List available block devices
    List(ListArgs),
}

#[derive(Debug, Args)]
struct ListArgs {
    #[arg(
        short = 'a',
        long,
        help = "List all block devices (ie. also include devices currently in use)"
    )]
    all: bool,
}

pub async fn handler(ctx: Context, args: DeviceArgs) -> crate::Result<()> {
    match args.command {
        DeviceCommands::List(args) => list_block_devices(ctx, args).await,
    }
}

fn get_partition_type(device: &rpc::BlockDevice) -> String {
    if let Some(partition) = &device.partition {
        format!("{}:{}", partition.scheme, partition.typeid)
    } else {
        String::from("")
    }
}

async fn list_block_devices(mut ctx: Context, args: ListArgs) -> crate::Result<()> {
    let all = args.all;

    let response = ctx
        .client
        .list_block_devices(rpc::ListBlockDevicesRequest { all })
        .await
        .context(GrpcStatus)?;

    match ctx.output {
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&response.into_inner())
                    .unwrap()
                    .to_colored_json_auto()
                    .unwrap()
            )
        }
        OutputFormat::Default => {
            let devices: &Vec<rpc::BlockDevice> = &response.get_ref().devices;

            if devices.is_empty() {
                ctx.v1("No devices found");
                return Ok(());
            }

            let table = devices
                .iter()
                .map(|device| {
                    let fstype: String;
                    let uuid: String;
                    let mountpoint: String;

                    if let Some(filesystem) = &device.filesystem {
                        fstype = filesystem.fstype.clone();
                        uuid = filesystem.uuid.clone();
                        mountpoint = filesystem.mountpoint.clone();
                    } else {
                        fstype = String::from("");
                        uuid = String::from("");
                        mountpoint = String::from("");
                    }

                    vec![
                        device.devname.clone(),
                        device.devtype.clone(),
                        device.devmajor.to_string(),
                        device.devminor.to_string(),
                        device.size.to_string(),
                        String::from(if device.available { "yes" } else { "no" }),
                        device.model.clone(),
                        get_partition_type(device),
                        fstype,
                        uuid,
                        mountpoint,
                        device.devpath.clone(),
                        device
                            .devlinks
                            .iter()
                            .map(|s| format!("\"{s}\""))
                            .collect::<Vec<String>>()
                            .join(" "),
                    ]
                })
                .collect();

            ctx.print_list(
                vec![
                    "DEVNAME",
                    "DEVTYPE",
                    ">MAJOR",
                    "MINOR",
                    ">SIZE",
                    "AVAILABLE",
                    "MODEL",
                    "PARTTYPE",
                    "FSTYPE",
                    "FSUUID",
                    "MOUNTPOINT",
                    "DEVPATH",
                    "DEVLINKS",
                ],
                table,
            );
        }
    }

    Ok(())
}
