#[macro_use]
extern crate serde_json;
use structopt::StructOpt;

use jsonrpc::call;
use rpc::mayastor::*;
mod convert;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "mctl",
    about = "Mayastor nexus CLI management utility",
    raw(setting = "structopt::clap::AppSettings::ColoredHelp")
)]
struct Opt {
    #[structopt(short = "s", default_value = "/var/tmp/mayastor.sock")]
    socket: String,
    #[structopt(subcommand)]
    cmd: Sub,
}
#[derive(StructOpt, Debug)]
enum Sub {
    #[structopt(name = "raw")]
    /// call a method with a raw JSON payload
    Raw {
        #[structopt(name = "method")]
        method: String,
        arg: Option<String>,
    },
    #[structopt(name = "destroy")]
    /// Destroy a nexus and its children (does not delete the data)
    Destroy {
        #[structopt(name = "uuid")]
        uuid: String,
    },
    #[structopt(name = "create")]
    /// Create a nexus using the given URIs
    ///
    /// Example:
    ///
    /// mctl create `uuidgen -r` -c nvmf://host1/nqn nvmf://host2/nqn -s 1GiB
    ///
    /// The child URIs should be in the form of:
    ///
    /// nvmf://host/nqn
    /// iscsi://host/iqn
    /// aio:///path/to/file
    /// uring:///path/to/file
    Create {
        #[structopt(name = "uuid")]
        uuid: String,
        #[structopt(short, long, parse(try_from_str = "convert::parse_size"))]
        /// The size of the nexus to be created e.g. 100MiB
        size: u64,
        #[structopt(short, long, required = true, min_values = 1)]
        /// The URIs to be used for this nexus
        children: Vec<String>,
    },
    #[structopt(name = "list")]
    /// List the nexus instances on the system
    List,

    #[structopt(name = "offline")]
    /// Offline a child bdev from the nexus
    Offline {
        #[structopt(name = "uuid")]
        /// UUID of the nexus
        uuid: String,
        #[structopt(name = "child_name")]
        /// URI of the child
        uri: String,
    },

    #[structopt(name = "online")]
    /// Online a child bdev from the nexus
    Online {
        #[structopt(name = "uuid")]
        /// UUID of the nexus
        uuid: String,
        #[structopt(name = "child_name")]
        /// URI of the child
        uri: String,
    },
    #[structopt(name = "publish")]
    /// Share the nexus
    ///
    /// Currently only NBD is supported
    Publish {
        #[structopt(name = "uuid")]
        /// UUID of the nexus to be published
        uuid: String,
        /// Protocol to use when sharing the nexus.
        /// Can be NVMf, ISCSI, NBD
        #[structopt(
            name = "protocol",
            parse(try_from_str = "convert::parse_proto")
        )]
        protocol: ShareProtocolNexus,
        /// 128 bit encryption key to be used for encrypting the data section
        /// of the nexus.
        #[structopt(name = "key", default_value = "")]
        key: String,
    },
    #[structopt(name = "unpublish")]
    /// Unpublish the nexus
    ///
    /// Currently only NBD is supported
    Unpublish {
        #[structopt(name = "uuid")]
        /// The UUID of the nexus device to unpublish
        uuid: String,
    },
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let opt = Opt::from_args();

    let fut = match opt.cmd {
        Sub::Create {
            uuid,
            size,
            children,
        } => serde_json::to_string_pretty(
            &call(
                &opt.socket,
                "create_nexus",
                Some(json!({
                    "uuid": uuid,
                    "size": size,
                    "children": children,
                })),
            )
            .await?,
        )?,
        Sub::Destroy {
            uuid,
        } => serde_json::to_string_pretty(
            &call(&opt.socket, "destroy_nexus", Some(json!({ "uuid": uuid })))
                .await?,
        )?,
        Sub::List => serde_json::to_string_pretty(
            &call::<_, ListNexusReply>(
                &opt.socket,
                "list_nexus",
                Some(json!(null)),
            )
            .await?,
        )?,
        Sub::Offline {
            uuid,
            uri,
        } => serde_json::to_string_pretty(
            &call(
                &opt.socket,
                "offline_child",
                Some(json!({
                    "uuid": uuid,
                    "uri": uri,
                    "action" : ChildAction::Offline as i32,
                })),
            )
            .await?,
        )?,
        Sub::Online {
            uuid,
            uri,
        } => serde_json::to_string_pretty(
            &call(
                &opt.socket,
                "online_child",
                Some(json!({
                    "uuid": uuid,
                    "uri": uri,
                    "action" : ChildAction::Online as i32,
                })),
            )
            .await?,
        )?,
        Sub::Publish {
            uuid,
            key,
            protocol,
        } => serde_json::to_string_pretty(
            &call::<_, PublishNexusReply>(
                &opt.socket,
                "publish_nexus",
                Some(json!({ "uuid": uuid,
                    "share" : protocol as i32,
                    "key" : key,
                })),
            )
            .await?,
        )?,

        Sub::Unpublish {
            uuid,
        } => serde_json::to_string_pretty(
            &call(
                &opt.socket,
                "unpublish_nexus",
                Some(json!({ "uuid": uuid })),
            )
            .await?,
        )?,
        Sub::Raw {
            method,
            arg,
        } => {
            if let Some(arg) = arg {
                let args: serde_json::Value = serde_json::from_str(&arg)?;

                let out: serde_json::Value =
                    call(&opt.socket, &method, Some(args)).await?;
                // we dont always get valid json back which is a bug in the RPC
                // method really.
                if let Ok(json) = serde_json::to_string_pretty(&out) {
                    json
                } else {
                    dbg!(out);
                    "".into()
                }
            } else {
                serde_json::to_string_pretty(
                    &call::<(), serde_json::Value>(&opt.socket, &method, None)
                        .await?,
                )?
            }
        }
    };
    println!("{}", fut);

    Ok(())
}
