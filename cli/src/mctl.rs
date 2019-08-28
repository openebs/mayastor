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
    /// destroy a nexus and its children (does not delete the data)
    Destroy {
        #[structopt(name = "name")]
        name: String,
    },
    #[structopt(name = "create")]
    /// Create a nexus using the given uri's
    Create {
        #[structopt(name = "name")]
        name: String,
        #[structopt(
            short,
            long,
            parse(try_from_str = "convert::parse_block_len")
        )]
        /// the block_size given in bytes
        blk_len: u32,
        #[structopt(short, long, parse(try_from_str = "convert::parse_size"))]
        /// the size of the nexus to be created i.e 100MiB
        size: u64,
        #[structopt(short, long, required = true, min_values = 1)]
        /// the uris which should constitute the nexus
        replicas: Vec<String>,
    },
    #[structopt(name = "list")]
    /// List the nexus instances on the system
    List,

    #[structopt(name = "offline")]
    /// Offline a child bdev from the nexus
    Offline {
        #[structopt(name = "name")]
        /// name of the nexus
        name: String,
        #[structopt(name = "child_name")]
        /// name of the child
        child_name: String,
    },

    #[structopt(name = "online")]
    /// Online a child from the nexus
    Online {
        #[structopt(name = "name")]
        /// name of the nexus
        name: String,
        #[structopt(name = "child_name")]
        /// name of the child
        child_name: String,
    },
    #[structopt(name = "share")]
    /// Share the nexus
    ///
    /// Currently only NBD is supported
    Share {
        #[structopt(name = "name")]
        /// name of the nexus
        name: String,
    },
    #[structopt(name = "unshare")]
    /// Unshare the nexus
    ///
    /// Currently only NBD is supported
    UnShare {
        #[structopt(name = "name")]
        /// name of the nbd device to unshare
        name: String,
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
            name,
            blk_len,
            size,
            replicas,
        } => fut(
            opt.socket,
            "create_nexus",
            json!({ "name": name,
                "block_len": blk_len,
                "size": size,
                "replicas": replicas,
                "uuid": "",
            }),
        ),
        Sub::Destroy {
            name,
        } => fut(opt.socket, "destroy_nexus", json!({ "name": name })),
        Sub::List => fut(opt.socket, "list_nexus", json!(null)),
        Sub::Offline {
            name,
            child_name,
        } => fut(
            opt.socket,
            "offline_child",
            json!({
                "name": name,
                "child_name": child_name,
                "action" : ChildAction::Offline as i32,
            }),
        ),
        Sub::Online {
            name,
            child_name,
        } => fut(
            opt.socket,
            "online_child",
            json!({
                "name": name,
                "child_name": child_name,
                "action" : ChildAction::Online as i32,
            }),
        ),
        // just for demo purposes the share/unshare methods should be
        // implemented by the nexus itself and default to nvmf.
        Sub::Share {
            name,
        } => fut(opt.socket, "start_nbd_disk", json!({ "bdev_name": name })),
        Sub::UnShare {
            name,
        } => fut(opt.socket, "stop_nbd_disk", json!({ "bdev_name": name })),
    };

    let _res = rt.block_on(fut);
    rt.shutdown_now().wait()
}
