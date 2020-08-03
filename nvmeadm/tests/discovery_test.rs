use nvmeadm::nvmf_discovery::{disconnect, DiscoveryBuilder};

use failure::Error;
use std::{
    fs::File,
    io::prelude::*,
    net::{SocketAddr, TcpStream},
    path::Path,
    process::Command,
    thread,
    time::Duration,
};

static CONFIG_TEXT: &str = "[Malloc]
  NumberOfLuns 1
  LunSizeInMB  64
  BlockSize    4096
[Nvmf]
  AcceptorPollRate 10000
  ConnectionScheduler RoundRobin
[Transport]
  Type TCP
  # reduce memory requirements
  NumSharedBuffers 64
[Subsystem1]
  NQN nqn.2019-05.io.openebs:disk2
  Listen TCP 127.0.0.1:NVMF_PORT
  AllowAnyHost Yes
  SN MAYASTOR0000000001
  MN NEXUSController1
  MaxNamespaces 1
  Namespace Malloc0 1
# although not used we still have to reduce mem requirements for iSCSI
[iSCSI]
  MaxSessions 1
  MaxConnectionsPerSession 1
";

const CONFIG_FILE: &str = "/tmp/nvmeadm_nvmf_target.config";

const SERVED_DISK_NQN: &str = "nqn.2019-05.io.openebs:disk2";

const TARGET_PORT: u32 = 9523;

// Writes out a config file for spdk, but with the specified port for nvmf to
// use
fn create_config_file(config_file: &str, nvmf_port: &str) {
    let path = Path::new(config_file);
    let mut config = match File::create(&path) {
        Err(reason) => panic!(
            "Unable to create {}: {}",
            path.display(),
            reason.to_string()
        ),
        Ok(config) => config,
    };

    let after = CONFIG_TEXT.replace("NVMF_PORT", nvmf_port);

    match config.write_all(after.as_bytes()) {
        Err(reason) => panic!(
            "Unable to write to {}: {}",
            path.display(),
            reason.to_string()
        ),
        Ok(_) => println!("Wrote to file"),
    }
}

// Waits for spdk to start up and accept connections on the specified port
fn wait_for_spdk_ready(listening_port: u32) -> Result<(), String> {
    let dest = format!("127.0.0.1:{}", listening_port);
    let socket_addr: SocketAddr = dest.parse().expect("Badly formed address");

    let mut bound = false;
    for _index in 1 .. 20 {
        let result = TcpStream::connect_timeout(
            &socket_addr,
            Duration::from_millis(100),
        );
        bound = result.is_ok();
        if bound {
            break;
        }
        thread::sleep(Duration::from_millis(101));
    }

    if bound {
        Ok(())
    } else {
        Err(format!(
            "spdk listening port ({}) not found",
            listening_port
        ))
    }
}

pub struct NvmfTarget {
    /// The std::process::Child for the process running spdk
    pub spdk_proc: std::process::Child,
}

impl NvmfTarget {
    pub fn new(config_file: &str, nvmf_port: &str) -> Result<Self, Error> {
        create_config_file(config_file, nvmf_port);
        let spdk_proc = Command::new("../target/debug/spdk")
            .arg("-c")
            .arg(CONFIG_FILE)
            .spawn()
            .expect("Failed to start spdk!");

        wait_for_spdk_ready(TARGET_PORT).expect("spdk not ready");

        let _ = DiscoveryBuilder::default()
            .transport("tcp".to_string())
            .traddr("127.0.0.1".to_string())
            .trsvcid(TARGET_PORT)
            .build()
            .unwrap();

        Ok(NvmfTarget {
            spdk_proc,
        })
    }
}

impl Drop for NvmfTarget {
    fn drop(&mut self) {
        // Ensure we end with no connected disk
        let _ = disconnect(SERVED_DISK_NQN);

        // Kill the spdk nvmf target
        self.spdk_proc.kill().expect("Failed to kill SPDK process");
        self.spdk_proc
            .wait()
            .expect("Failed to wait for spdk to terminate");
    }
}

#[test]
fn discovery_test() {
    let mut explorer = DiscoveryBuilder::default()
        .transport("tcp".to_string())
        .traddr("127.0.0.1".to_string())
        .trsvcid(4420)
        .build()
        .unwrap();

    // only root can discover
    let _ = explorer.discover();
}

#[test]
fn connect_test() {
    let mut explorer = DiscoveryBuilder::default()
        .transport("tcp".to_string())
        .traddr("127.0.0.1".to_string())
        .trsvcid(4420)
        .build()
        .unwrap();

    // only root can discover and connect
    let _ = explorer.discover();
    let _ = explorer.connect("mynqn");
}

#[test]
fn disconnect_test() {
    let _ = disconnect("mynqn");
}

#[test]
fn test_against_real_target() {
    // Start an SPDK-based nvmf target
    let _target = NvmfTarget::new(CONFIG_FILE, &TARGET_PORT.to_string());

    // Perform discovery
    let mut explorer = DiscoveryBuilder::default()
        .transport("tcp".to_string())
        .traddr("127.0.0.1".to_string())
        .trsvcid(TARGET_PORT)
        .build()
        .unwrap();

    let _discover = explorer.discover();

    // Check that we CANNOT connect to an NQN that does not exist
    let _not_expected_to_connect = explorer
        .connect("an nqn that does not exist")
        .expect_err("Should NOT be able to connect to invalid target");

    // Check that we CAN connect to an NQN that is served
    let _expect_to_connect = explorer
        .connect(SERVED_DISK_NQN)
        .expect("Problem connecting to valid target");

    // Check that we CAN disconnect from a served NQN
    let num_disconnects = disconnect(SERVED_DISK_NQN);
    assert_eq!(num_disconnects.unwrap(), 1);
}
