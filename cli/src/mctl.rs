extern crate futures;
extern crate jsonrpc_client_transports;
extern crate jsonrpc_core;
extern crate serde;
#[macro_use]
extern crate serde_json;
extern crate tokio;

use futures::Future;
use jsonrpc_client_transports::{transports::ipc, RawClient};
use jsonrpc_core::Params;
use rpc::mayastor::ChildAction;

use structopt::StructOpt;
use tokio::{reactor::Handle, runtime::Runtime};

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
    #[structopt(name = "destroy")]
    /// Destroy a nexus and its children (does not delete the data)
    Destroy {
        #[structopt(name = "uuid")]
        uuid: String,
    },
    #[structopt(name = "create")]
    ///
    /// Create a nexus using the given URI's. The UUID should be a valid v4
    /// UUID
    ///
    /// Example:
    ///
    /// mctl create `uuidgen -r` -c nvmf://host1/nqn nvmf://host2/nqn -s 1GiB
    ///
    /// The child URI's should be in the form of:
    ///
    /// nvmf://host/nqn
    /// iscsi://host/iqn
    /// aio:///path/to/file
    Create {
        #[structopt(name = "uuid")]
        uuid: String,
        #[structopt(short, long, parse(try_from_str = "convert::parse_size"))]
        /// The size of the nexus to be created i.e 100MiB
        size: u64,
        #[structopt(short, long, required = true, min_values = 1)]
        /// The URI's to be should for this nexus
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
    /// Online a child from the nexus
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

fn fut(
    path: String,
    name: &'static str,
    args: serde_json::Value,
) -> Box<dyn Future<Item = (), Error = ()> + Send> {
    let p = match args.as_object() {
        Some(p) => Params::Map(p.clone()),
        None => Params::None,
    };

    Box::new(
        ipc::connect(path, &Handle::default())
            .expect("failed to connect to socket")
            .and_then(move |client: RawClient| client.call_method(name, p))
            .map(|res| {
                println!("{}", serde_json::to_string_pretty(&res).unwrap())
            })
            .map_err(|err| eprintln!("{}", err.to_string())),
    )
}

fn main() -> Result<(), ()> {
    let opt = Opt::from_args();

    let mut rt = Runtime::new().unwrap();

    let fut = match opt.cmd {
        Sub::Create {
            uuid,
            size,
            children,
        } => fut(
            opt.socket,
            "create_nexus",
            json!({
                "uuid": uuid,
                "size": size,
                "children": children,
            }),
        ),
        Sub::Destroy {
            uuid,
        } => fut(opt.socket, "destroy_nexus", json!({ "uuid": uuid })),
        Sub::List => fut(opt.socket, "list_nexus", json!(null)),
        Sub::Offline {
            uuid,
            uri,
        } => fut(
            opt.socket,
            "offline_child",
            json!({
                "uuid": uuid,
                "uri": uri,
                "action" : ChildAction::Offline as i32,
            }),
        ),
        Sub::Online {
            uuid,
            uri,
        } => fut(
            opt.socket,
            "online_child",
            json!({
                "uuid": uuid,
                "uri": uri,
                "action" : ChildAction::Online as i32,
            }),
        ),
        Sub::Publish {
            uuid,
            key,
        } => fut(
            opt.socket,
            "publish_nexus",
            json!({ "uuid": uuid , "key" : key}),
        ),
        Sub::Unpublish {
            uuid,
        } => fut(opt.socket, "unpublish_nexus", json!({ "uuid": uuid })),
    };

    let _res = rt.block_on(fut);
    rt.shutdown_now().wait()
}
