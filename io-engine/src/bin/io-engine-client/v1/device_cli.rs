//!
//! methods to obtain information about block devices on the current host

use super::context::Context;
use crate::{context::OutputFormat, GrpcStatus};
use clap::{App, AppSettings, Arg, ArgMatches, SubCommand};
use colored_json::ToColoredJson;
use mayastor_api::v1 as v1rpc;
use snafu::ResultExt;
use tonic::Status;

pub fn subcommands<'a, 'b>() -> App<'a, 'b> {
    let list =
        SubCommand::with_name("list").about("List available block devices")
            .arg(
                Arg::with_name("all")
                    .short("a")
                    .long("all")
                    .takes_value(false)
                    .help("List all block devices (ie. also include devices currently in use)"),
            );

    SubCommand::with_name("device")
        .settings(&[
            AppSettings::SubcommandRequiredElseHelp,
            AppSettings::ColoredHelp,
            AppSettings::ColorAlways,
        ])
        .about("Host devices")
        .subcommand(list)
}

pub async fn handler(
    ctx: Context,
    matches: &ArgMatches<'_>,
) -> crate::Result<()> {
    match matches.subcommand() {
        ("list", Some(args)) => list_block_devices(ctx, args).await,
        (cmd, _) => {
            Err(Status::not_found(format!("command {cmd} does not exist")))
                .context(GrpcStatus)
        }
    }
}

fn get_partition_type(device: &v1rpc::host::BlockDevice) -> String {
    if let Some(partition) = &device.partition {
        format!("{}:{}", partition.scheme, partition.typeid)
    } else {
        String::from("")
    }
}

async fn list_block_devices(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> crate::Result<()> {
    let all = matches.is_present("all");
    let response = ctx
        .v1
        .host
        .list_block_devices(v1rpc::host::ListBlockDevicesRequest {
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
            let devices = response.into_inner().devices;

            if devices.is_empty() {
                ctx.v1("No devices found");
                return Ok(());
            }

            let table = devices
                .into_iter()
                .map(|device| {
                    let fstype: String;
                    let uuid: String;
                    let mountpoints: Vec<String>;

                    let part_type = get_partition_type(&device);
                    if let Some(filesystem) = device.filesystem {
                        fstype = filesystem.fstype;
                        uuid = filesystem.uuid;
                        mountpoints = filesystem.mountpoints;
                    } else {
                        fstype = String::from("");
                        uuid = String::from("");
                        mountpoints = vec![];
                    }

                    vec![
                        device.devname,
                        device.devtype,
                        device.devmajor.to_string(),
                        device.devminor.to_string(),
                        device.size.to_string(),
                        String::from(if device.available {
                            "yes"
                        } else {
                            "no"
                        }),
                        device.model,
                        part_type,
                        fstype,
                        uuid,
                        mountpoints
                            .iter()
                            .map(|s| format!("\"{s}\""))
                            .collect::<Vec<String>>()
                            .join(", "),
                        device.devpath,
                        device
                            .devlinks
                            .iter()
                            .map(|s| format!("\"{s}\""))
                            .collect::<Vec<String>>()
                            .join(", "),
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
                    "MOUNTPOINTS",
                    "DEVPATH",
                    "DEVLINKS",
                ],
                table,
            );
        }
    }

    Ok(())
}
