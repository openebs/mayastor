//! A simple command line utility for calling json-rpc method with arbitrary
//! json parameters. json-rpc server must listen on unix domain socket.
//! The only transformation done on input parameters is that if there are
//! no parameters, it translates to json null value as parameters value.

extern crate clap;
extern crate futures;
extern crate jsonrpc_client_transports;
extern crate jsonrpc_core;
extern crate serde_json;
extern crate tokio;

use clap::{App, Arg};
use futures::prelude::*;
use jsonrpc_client_transports::{transports::ipc, RawClient};
use jsonrpc_core::Params;
use tokio::{reactor::Handle, runtime::Runtime};

fn main() -> Result<(), String> {
    let matches = App::new("JSON-RPC raw client")
        .arg(
            Arg::with_name("socket")
                .short("s")
                .long("socket")
                .value_name("PATH")
                .help("Unix domain socket of the server (default /var/tmp/mayastor.sock)")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("method")
                .help("Method name")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::with_name("params")
                .help("JSON parameters for the method")
                .index(2),
        )
        .get_matches();

    let socket = matches
        .value_of("socket")
        .unwrap_or("/var/tmp/mayastor.sock")
        .to_string();
    let method = matches.value_of("method").unwrap().to_string();

    let params: Params = match matches.value_of("params") {
        Some(p) => match serde_json::from_str::<serde_json::Value>(p) {
            Ok(p) => match p.as_object() {
                Some(obj) => Params::Map(obj.clone()),
                None => {
                    return Err("Parameters are not a valid JSON object".into())
                }
            },
            Err(err) => return Err(format!("Invalid JSON: {}", err)),
        },
        None => Params::None,
    };

    let mut rt = Runtime::new().unwrap();
    let handle = Handle::default();
    let fut = match ipc::connect(socket.clone(), &handle) {
        Ok(fut) => fut.map_err(|err| err.to_string()),
        Err(err) => {
            return Err(format!("Failed to connect to {}: {}", socket, err))
        }
    };
    let client: RawClient = rt.block_on(fut).unwrap();
    let fut = client
        .call_method(&method, params)
        .map(|res| println!("{}", res.to_string()))
        .map_err(|err| err.to_string());
    let res = rt.block_on(fut);
    rt.shutdown_now().wait().unwrap();
    res
}
