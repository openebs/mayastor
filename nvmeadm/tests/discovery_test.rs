use nvmeadm::nvmf_discovery::{disconnect, DiscoveryBuilder};

use std::{
    fs::File,
    io::prelude::*,
    net::{SocketAddr, TcpStream},
    path::Path,
    process::Command,
    thread,
    time::Duration,
};

static CONFIG_TEXT: &str = "sync_disable: true
base_bdevs:
  - uri: \"malloc:///Malloc0?size_mb=64&blk_size=4096&uuid=dbe4d7eb-118a-4d15-b789-a18d9af6ff29\"
nexus_opts:
  nvmf_nexus_port: 4422
  nvmf_replica_port: NVMF_PORT
  iscsi_enable: false
nvmf_tcp_tgt_conf:
  max_namespaces: 2
# although not used we still have to reduce mem requirements for iSCSI
iscsi_tgt_conf:
  max_sessions: 1
  max_connections_per_session: 1
implicit_share_base: true
";

const CONFIG_FILE: &str = "/tmp/nvmeadm_nvmf_target.yaml";

const SERVED_DISK_NQN: &str =
    "nqn.2019-05.io.openebs:dbe4d7eb-118a-4d15-b789-a18d9af6ff29";

const TARGET_PORT: u32 = 9523;

/// Write out a config file for Mayastor, but with the specified port for nvmf
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

/// Wait for Mayastor to start up and accept connections on the specified port
fn wait_for_mayastor_ready(listening_port: u32) -> Result<(), String> {
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
    /// The std::process::Child for the process running Mayastor
    pub spdk_proc: std::process::Child,
}

impl NvmfTarget {
    pub fn new(config_file: &str, nvmf_port: &str) -> Self {
        create_config_file(config_file, nvmf_port);
        let spdk_proc = Command::new("../target/debug/mayastor")
            .arg("-y")
            .arg(CONFIG_FILE)
            .spawn()
            .expect("Failed to start spdk!");

        wait_for_mayastor_ready(TARGET_PORT).expect("mayastor not ready");

        let _ = DiscoveryBuilder::default()
            .transport("tcp".to_string())
            .traddr("127.0.0.1".to_string())
            .trsvcid(TARGET_PORT)
            .build()
            .unwrap();

        Self {
            spdk_proc,
        }
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

    // allow the part scan to complete for most cases
    std::thread::sleep(std::time::Duration::from_secs(1));

    // Check that we CAN disconnect from a served NQN
    let num_disconnects = disconnect(SERVED_DISK_NQN);
    assert_eq!(num_disconnects.unwrap(), 1);
}
