extern crate clap;
use clap::{App, Arg, ArgMatches};

use std::{io, str::FromStr};

fn main() {
    let matches = App::new("nvmeadm")
        .version("0.0.1")
        .author("Mayadata")
        .about("nvmeadm a la iscsiadm app")
        .arg(
            Arg::with_name("ACTION")
                .help("Select the action [connect|disconnect|discover|discoverandconnect]")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::with_name("traddr")
                .short("a")
                .long("traddr")
                .value_name("traddr")
                .help(
                    "This field specifies the network address of the Controller. This should be an IP-based address (ex. IPv4). The default is 127.0.0.1."
                )
                .takes_value(true),
        )
        .arg(
            Arg::with_name("trsvcid")
                .short("s")
                .long("trsvcid")
                .value_name("trsvcid")
                .help(
                    "This field specifies the transport service id. For transports using IP addressing (e.g. rdma) this field is the port number. By default, the IP port number is 8420."
                )
                .takes_value(true),
        )
        .arg(
            Arg::with_name("nqn")
                .short("n")
                .long("nqn")
                .value_name("nqn")
                .help("This field specifies the name for the NVMe subsystem to connect to.")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("trtype")
                .short("t")
                .long("trtype")
                .value_name("trtype")
                .help("This field specifies the network fabric being used for a NVMe-over-Fabrics network. Defaults to tcp, and only tcp is supported.")
                .takes_value(true),
        )
        .get_matches();
    println!("{:?}", perform_action(matches));
}

#[allow(dead_code)]
fn print_type_of<T>(_: &T) {
    println!("typeof {}", std::any::type_name::<T>())
}

fn perform_action(matches: ArgMatches) -> Result<String, String> {
    let ip_addr = matches.value_of("traddr").unwrap_or("127.0.0.1");
    let port =
        u32::from_str(matches.value_of("port").unwrap_or("8420")).unwrap();

    println!("Using traddr {} and trsvcid {}", ip_addr, port);
    match matches.value_of("ACTION").unwrap() {
        "connect" => {
            println!("CONNECT");
            let nqn = match matches.value_of("nqn") {
                Some(v) => v,
                None => return Err("nqn is required for connect".to_string()),
            };
            let result = nvmeadm::nvmf_discovery::connect(ip_addr, port, nqn);
            match result {
                Ok(res) => {
                    println!("{}", res);
                    println!("Connected to {}:{} {}", ip_addr, port, nqn);
                }
                Err(e) => {
                    if let Some(ioerr) = e.downcast_ref::<io::Error>() {
                        if let Some(errcode) = ioerr.raw_os_error() {
                            if errcode == 114 {
                                return Ok("In progress".to_string());
                            }
                        }
                    }
                    return Err(format!(
                        "Connect to {}:{} {} FAILED {:?}",
                        ip_addr, port, nqn, e
                    ));
                }
            }
        }
        "discoverandconnect" => {
            println!("DISCOVERANDCONNECT");
            let nqn = match matches.value_of("nqn") {
                Some(v) => v,
                None => return Err("nqn is required for connect".to_string()),
            };
            //println!("DiscoveryBuilder ip_addr={} port={}", ip_addr, port);
            let mut discovered_targets =
                nvmeadm::nvmf_discovery::DiscoveryBuilder::default()
                    .transport("tcp".to_string())
                    .traddr(ip_addr.to_string())
                    .trsvcid(port)
                    .build()
                    .unwrap();
            //println!("discovered_targets = {:?}", discovered_targets);
            match discovered_targets.discover() {
                Ok(_discovrd) => {
                    //println!("discovered {:?}", discovrd);
                }
                Err(e) => {
                    return Err(format!("discover failed {:?}", e));
                }
            }
            let result = discovered_targets.connect(nqn);
            match result {
                Ok(res) => {
                    println!("{}", res);
                    println!("Connected to {}:{} {}", ip_addr, port, nqn);
                }
                Err(e) => {
                    if let Some(ioerr) = e.downcast_ref::<io::Error>() {
                        if let Some(errcode) = ioerr.raw_os_error() {
                            if errcode == 114 {
                                return Ok("In progress".to_string());
                            }
                        }
                    }
                    return Err(format!(
                        "Connect to {}:{} {} FAILED {:?}",
                        ip_addr, port, nqn, e
                    ));
                }
            }
        }
        "disconnect" => {
            println!("DISCONNECT");
            let nqn = match matches.value_of("nqn") {
                Some(v) => v,
                None => {
                    return Err("nqn is required for disconnect".to_string())
                }
            };
            match nvmeadm::nvmf_discovery::disconnect(&nqn) {
                Ok(_) => {
                    println!("{} has been disconnected", nqn);
                }
                Err(e) => return Err(format!("{}", e)),
            }
        }
        "discover" => {
            println!("DISCOVER");
            //println!("DiscoveryBuilder ip_addr={} port={}", ip_addr, port);
            let mut discovered_targets =
                nvmeadm::nvmf_discovery::DiscoveryBuilder::default()
                    .transport("tcp".to_string())
                    .traddr(ip_addr.to_string())
                    .trsvcid(port)
                    .build()
                    .unwrap();

            let discovered = match discovered_targets.discover() {
                Ok(discovrd) => discovrd,
                Err(e) => {
                    return Err(format!("discover failed {:?}", e));
                }
            };
            for entry in discovered {
                println!("{:?}", entry);
            }
        }
        _ => {
            return Err("Unknown action".to_string());
        }
    }

    Ok("Done".to_string())
}
