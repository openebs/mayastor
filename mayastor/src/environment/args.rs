use byte_unit::{Byte, ByteUnit};
use structopt::StructOpt;

fn parse_mb(src: &str) -> Result<i32, String> {
    // For compatibility, we check to see if there are no alphabetic characters
    // passed in, if, so we interpret the value to be in MiB which is what the
    // EAL expects it to be in.

    let has_unit = src.trim_end().chars().any(|c| c.is_alphabetic());

    if let Ok(val) = Byte::from_str(src) {
        let value;
        if has_unit {
            value = val.get_adjusted_unit(ByteUnit::MiB).get_value() as i32
        } else {
            value = val.get_bytes() as i32
        }
        Ok(value)
    } else {
        Err(format!("Invalid argument {}", src))
    }
}

#[derive(Debug, StructOpt)]
#[structopt(
    name = "Mayastor",
    about = "Containerized Attached Storage (CAS) for k8s",
    version = "19.12.1",
    raw(setting = "structopt::clap::AppSettings::ColoredHelp")
)]

pub struct MayastorCliArgs {
    #[structopt(short = "j")]
    /// Path to JSON formatted config file
    pub json: Option<String>,
    #[structopt(short = "c")]
    /// Path to the configuration file if any
    pub config: Option<String>,
    #[structopt(short = "L")]
    /// Enable logging for sub components
    pub log_components: Vec<String>,
    #[structopt(short = "m", default_value = "0x1")]
    /// The reactor mask to be used for starting up the instance
    pub reactor_mask: String,
    #[structopt(
        short = "s",
        parse(try_from_str = "parse_mb"),
        default_value = "0"
    )]
    /// The maximum amount of hugepage memory we are allowed to allocate in MiB
    /// (default: all)
    pub mem_size: i32,
    #[structopt(short = "r", default_value = "/var/tmp/mayastor.sock")]
    /// Path to create the rpc socket
    pub rpc_address: String,
    #[structopt(short = "u")]
    /// Disable the use of PCIe devices
    pub no_pci: bool,
}

/// Defaults are redefined here in case of using it during tests
impl Default for MayastorCliArgs {
    fn default() -> Self {
        Self {
            reactor_mask: "0x1".into(),
            mem_size: 0,
            rpc_address: "/var/tmp/mayastor.sock".to_string(),
            no_pci: true,
            log_components: vec![],
            config: None,
            json: None,
        }
    }
}
