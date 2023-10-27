//!
//! methods to obtain information about block devices on the current host

use super::context::Context;
use crate::{context::OutputFormat, GrpcStatus};
use clap::{Arg, ArgMatches, Command};
use colored_json::ToColoredJson;
use io_engine_api::v0 as rpc;
use snafu::ResultExt;
use tonic::Status;

pub fn subcommands() -> Command {
    let list =
        Command::new("list").about("List available block devices")
            .arg(
                Arg::new("all")
                    .short('a')
                    .long("all")
                    .action(clap::ArgAction::SetTrue)
                    .help("List all block devices (ie. also include devices currently in use)"),
            );

    Command::new("device")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .about("Host devices")
        .subcommand(list)
}

pub async fn handler(ctx: Context, matches: &ArgMatches) -> crate::Result<()> {
    match matches.subcommand().unwrap() {
        ("list", args) => list_block_devices(ctx, args).await,
        (cmd, _) => {
            Err(Status::not_found(format!("command {cmd} does not exist")))
                .context(GrpcStatus)
        }
    }
}

fn get_partition_type(device: &rpc::BlockDevice) -> String {
    if let Some(partition) = &device.partition {
        format!("{}:{}", partition.scheme, partition.typeid)
    } else {
        String::from("")
    }
}

async fn list_block_devices(
    mut ctx: Context,
    matches: &ArgMatches,
) -> crate::Result<()> {
    let all = matches.get_flag("all");

    let response = ctx
        .client
        .list_block_devices(rpc::ListBlockDevicesRequest {
            all,
        })
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
                        String::from(if device.available {
                            "yes"
                        } else {
                            "no"
                        }),
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
