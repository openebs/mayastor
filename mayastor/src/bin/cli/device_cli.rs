//!
//! methods to obtain information about block devices on the current host

use super::context::Context;
use ::rpc::mayastor as rpc;
use clap::{App, AppSettings, Arg, ArgMatches, SubCommand};
use colored_json::ToColoredJson;
use tonic::Status;

pub fn subcommands<'a, 'b>() -> App<'a, 'b> {
    let list = SubCommand::with_name("list")
        .about("List available (ie. unused) block devices")
        .arg(
            Arg::with_name("all")
                .short("a")
                .long("all")
                .takes_value(false)
                .help("List all block devices (ie. also include devices currently in use)"),
        )
        .arg(
            Arg::with_name("raw")
                .long("raw")
                .takes_value(false)
                .help("Display output as raw JSON"),
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
) -> Result<(), Status> {
    match matches.subcommand() {
        ("list", Some(args)) => list_block_devices(ctx, args).await,
        (cmd, _) => {
            Err(Status::not_found(format!("command {} does not exist", cmd)))
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
    matches: &ArgMatches<'_>,
) -> Result<(), Status> {
    let all = matches.is_present("all");

    ctx.v2(&format!(
        "Requesting list of {} block devices",
        if all { "all" } else { "available" }
    ));

    let reply = ctx
        .client
        .list_block_devices(rpc::ListBlockDevicesRequest {
            all,
        })
        .await?;

    if matches.is_present("raw") {
        println!(
            "{}",
            serde_json::to_string_pretty(&reply.into_inner())
                .unwrap()
                .to_colored_json_auto()
                .unwrap()
        );
        return Ok(());
    }

    let devices: &Vec<rpc::BlockDevice> = &reply.get_ref().devices;

    if devices.is_empty() {
        ctx.v1("No devices found");
        return Ok(());
    }

    if all {
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
                    get_partition_type(&device),
                    fstype,
                    uuid,
                    mountpoint,
                    device.devpath.clone(),
                    device
                        .devlinks
                        .iter()
                        .map(|s| format!("\"{}\"", s))
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
    } else {
        let table = devices
            .iter()
            .map(|device| {
                vec![
                    device.devname.clone(),
                    device.devtype.clone(),
                    device.devmajor.to_string(),
                    device.devminor.to_string(),
                    device.size.to_string(),
                    device.model.clone(),
                    get_partition_type(&device),
                    device.devpath.clone(),
                    device
                        .devlinks
                        .iter()
                        .map(|s| format!("\"{}\"", s))
                        .collect::<Vec<String>>()
                        .join(" "),
                ]
            })
            .collect();

        ctx.print_list(
            vec![
                "DEVNAME", "DEVTYPE", ">MAJOR", "MINOR", ">SIZE", "MODEL",
                "PARTTYPE", "DEVPATH", "DEVLINKS",
            ],
            table,
        );
    }

    Ok(())
}
