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

static CONFIG_TEXT: &str = "nexus_opts:
  nvmf_nexus_port: 4422
  nvmf_replica_port: NVMF_PORT
nvmf_tcp_tgt_conf:
  max_namespaces: 2
";

const CONFIG_FILE: &str = "/tmp/nvmeadm_nvmf_target.yaml";

const SERVED_DISK_NQN: &str = "nqn.2019-05.io.openebs:m0";

const TARGET_PORT: u32 = 9523;

/// Write out a config file for Mayastor, but with the specified port for nvmf
fn create_config_file(config_file: &str, nvmf_port: &str) {
    let path = Path::new(config_file);
    let mut config = match File::create(&path) {
        Err(reason) => {
            panic!("Unable to create {}: {}", path.display(), reason)
        }
        Ok(config) => config,
    };

    let after = CONFIG_TEXT.replace("NVMF_PORT", nvmf_port);

    match config.write_all(after.as_bytes()) {
        Err(reason) => {
            panic!("Unable to write to {}: {}", path.display(), reason)
        }
        Ok(_) => println!("Wrote to file"),
    }
}

/// Wait for Mayastor to start up and accept connections on the specified port
fn wait_for_mayastor_ready(listening_port: u32) -> Result<(), String> {
    let dest = format!("127.0.0.1:{}", listening_port);
    let socket_addr: SocketAddr = dest.parse().expect("Badly formed address");

    for _ in 1 .. 20 {
        let result = TcpStream::connect_timeout(
            &socket_addr,
            Duration::from_millis(100),
        );

        if result.is_ok() {
            // FIXME: For some reason mayastor may still not be ready
            // at this point, and can still refuse a connection from
            // mayastor-client. Need to rethink this approach,
            // but just add an extra sleep for now.
            thread::sleep(Duration::from_secs(2));
            return Ok(());
        }

        thread::sleep(Duration::from_millis(100));
    }

    Err(format!("failed to connect to spdk port {}", listening_port))
}

pub struct NvmfTarget {
    /// The std::process::Child for the process running Mayastor
    pub spdk_proc: std::process::Child,
}

impl NvmfTarget {
    fn get_status(
        result: Result<std::process::Output, std::io::Error>,
    ) -> bool {
        match result {
            Ok(output) => {
                if output.status.success() {
                    return true;
                }
                match std::str::from_utf8(&output.stderr) {
                    Ok(s) => {
                        println!("mayastor-client failed: {}", s);
                    }
                    Err(_) => {
                        println!("mayastor-client failed");
                    }
                }
            }
            Err(error) => {
                println!("failed to execute mayastor-client: {}", error);
            }
        }
        false
    }

    pub fn new(config_file: &str, nvmf_port: &str) -> Self {
        create_config_file(config_file, nvmf_port);
        let spdk_proc = Command::new("../target/debug/mayastor")
            .arg("-y")
            .arg(CONFIG_FILE)
            .spawn()
            .expect("Failed to start spdk!");

        wait_for_mayastor_ready(TARGET_PORT).expect("mayastor not ready");

        // create base bdev
        let result = Command::new("../target/debug/mayastor-client")
            .arg("bdev")
            .arg("create")
            .arg("malloc:///m0?size_mb=64&blk_size=4096&uuid=dbe4d7eb-118a-4d15-b789-a18d9af6ff29")
            .output();

        if Self::get_status(result) {
            println!("base bdev created");

            // share bdev over nvmf
            let result = Command::new("../target/debug/mayastor-client")
                .arg("bdev")
                .arg("share")
                .arg("-p")
                .arg("nvmf")
                .arg("m0")
                .output();

            if Self::get_status(result) {
                println!("bdev shared");
            }
        }

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
        disconnect(SERVED_DISK_NQN)
            .expect("Should disconnect from the target device");

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
    disconnect("mynqn").expect("Should disconnect from the target device");
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
