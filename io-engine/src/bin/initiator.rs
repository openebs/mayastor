//! Command line test utility to copy bytes to/from a replica which can be any
//! target type understood by the nexus.

use std::{
    fmt, fs,
    io::{self, Write},
    path::{Path, PathBuf},
};

use chrono::Utc;
use clap::{Parser, Subcommand};
use tracing::{error, info, warn};
use uuid::Uuid;
use version_info::version_info_string;

use io_engine::{
    bdev::{device_create, device_open},
    bdev_api::{bdev_create, BdevError},
    core::{
        mayastor_env_stop, CoreError, MayastorCliArgs, MayastorEnvironment, Reactor,
        SnapshotParams, UntypedBdev,
    },
    jsonrpc::print_error_chain,
    logger, subsys,
    subsys::Config,
};
use spdk_rs::DmaError;

unsafe extern "C" fn run_static_initializers() {
    spdk_rs::libspdk::spdk_add_subsystem(subsys::ConfigSubsystem::new().0)
}

#[used]
static INIT_ARRAY: [unsafe extern "C" fn(); 1] = [run_static_initializers];

/// The errors from this utility are not supposed to be parsable by machine,
/// so all we need is a string with unfolded error messages from all nested
/// errors, which will be printed to stderr.
struct Error {
    msg: String,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.msg)
    }
}
impl From<CoreError> for Error {
    fn from(err: CoreError) -> Self {
        Self {
            msg: print_error_chain(&err),
        }
    }
}
impl From<DmaError> for Error {
    fn from(err: DmaError) -> Self {
        Self {
            msg: print_error_chain(&err),
        }
    }
}
impl From<BdevError> for Error {
    fn from(err: BdevError) -> Self {
        Self {
            msg: print_error_chain(&err),
        }
    }
}
impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Self {
            msg: err.to_string(),
        }
    }
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Create initiator bdev.
async fn create_bdev(uri: &url::Url) -> Result<UntypedBdev> {
    let bdev_name = bdev_create(uri.as_str()).await?;
    let bdev = UntypedBdev::lookup_by_name(&bdev_name).expect("Failed to lookup the created bdev");
    Ok(bdev)
}

/// Read block of data from bdev at given offset to a file.
async fn read(uri: &url::Url, offset: u64, file: &Path) -> Result<()> {
    let bdev = device_create(uri.as_str()).await?;
    let h = device_open(&bdev, false).unwrap().into_handle().unwrap();
    let mut buf = h.dma_malloc(h.get_device().block_len()).unwrap();
    #[allow(deprecated)]
    let n = h.read_at(offset, &mut buf).await?;
    fs::write(file, buf.as_slice())?;
    info!("{} bytes read", n);
    Ok(())
}

/// Write block of data from file to bdev at given offset.
async fn write(uri: &url::Url, offset: u64, file: &Path) -> Result<()> {
    let bdev = device_create(uri.as_str()).await?;
    let bytes = fs::read(file)?;
    let h = device_open(&bdev, false).unwrap().into_handle().unwrap();
    let mut buf = h.dma_malloc(h.get_device().block_len()).unwrap();
    let n = buf.as_mut_slice().write(&bytes[..]).unwrap();
    if n < buf.len() as usize {
        warn!("Writing a buffer which was not fully initialized from a file");
    }
    #[allow(deprecated)]
    let written = h.write_at(offset, &buf).await?;
    info!("{} bytes written", written);
    Ok(())
}

/// NVMe Admin. Only works with read commands without a buffer requirement.
async fn nvme_admin(uri: &url::Url, opcode: u8) -> Result<()> {
    let bdev = device_create(uri.as_str()).await?;
    let h = device_open(&bdev, true).unwrap().into_handle().unwrap();
    h.nvme_admin_custom(opcode).await?;
    Ok(())
}

/// NVMe Admin identify controller, write output to a file.
async fn identify_ctrlr(uri: &url::Url, file: &Path) -> Result<()> {
    let bdev = device_create(uri.as_str()).await?;
    let h = device_open(&bdev, true).unwrap().into_handle().unwrap();
    let buf = h.nvme_identify_ctrlr().await.unwrap();
    fs::write(file, buf.as_slice())?;
    Ok(())
}

/// Create a snapshot.
async fn create_snapshot(uri: &url::Url) -> Result<()> {
    let bdev = device_create(uri.as_str()).await?;
    let h = device_open(&bdev, true).unwrap().into_handle().unwrap();

    // TODO: fill all the fields properly once nexus-level
    // snapshots are fully implemented.
    let snapshot = SnapshotParams::new(
        Some(bdev.to_string()),
        Some(bdev.to_string()),
        Some(Uuid::new_v4().to_string()), // unique tx id
        Some(Uuid::new_v4().to_string()), // unique snapshot name
        Some(Uuid::new_v4().to_string()), // unique snapshot uuid
        Some(Utc::now().to_string()),
        false,
    );

    let t = h.create_snapshot(snapshot).await?;
    info!("snapshot taken at {}", t);
    Ok(())
}

/// Connect to the target.
async fn connect(uri: &url::Url) -> Result<()> {
    let _bdev = create_bdev(uri).await?;
    info!("Connected!");
    Ok(())
}

/// Connect, read or write a block to a nexus replica using its URI.
#[derive(Debug, Parser)]
#[command(
    name = "Test initiator for nexus replica",
    version = version_info_string!(),
    about = "Connect, read or write a block to a nexus replica using its URI"
)]
struct Args {
    /// URI of the replica to connect to.
    uri: url::Url,

    /// Offset of IO operation on the replica in bytes.
    #[arg(short = 'o', long, value_name = "NUMBER", default_value_t = 0)]
    offset: u64,

    #[command(subcommand)]
    command: SubCommand,
}

#[derive(Debug, Subcommand)]
enum SubCommand {
    /// Connect to and disconnect from the replica.
    Connect,

    /// Read bytes from the replica.
    Read {
        /// File to write data that were read from the replica.
        file: PathBuf,
    },

    /// Write bytes to the replica.
    Write {
        /// File to read data from that will be written to the replica.
        file: PathBuf,
    },

    /// Send a custom NVMe Admin command.
    #[command(name = "nvme-admin")]
    NvmeAdmin {
        /// Admin command opcode to send.
        opcode: u8,
    },

    /// Send NVMe Admin identify controller command.
    #[command(name = "id-ctrlr")]
    IdCtrlr {
        /// File to write output of identify controller command.
        file: PathBuf,
    },

    /// Create a snapshot on the replica.
    CreateSnapshot,
}

fn main() {
    let args = Args::parse();

    logger::init("INFO");

    // This tool is just a client, so don't start NVMe-oF services.
    Config::get_or_init(|| {
        let mut cfg = Config::default();
        cfg.nexus_opts.nvmf_enable = false;
        cfg
    });

    let ms = MayastorEnvironment::new(MayastorCliArgs::default());

    ms.init();
    let fut = async move {
        let res = match &args.command {
            SubCommand::Read { file } => read(&args.uri, args.offset, file).await,
            SubCommand::Write { file } => write(&args.uri, args.offset, file).await,
            SubCommand::NvmeAdmin { opcode } => nvme_admin(&args.uri, *opcode).await,
            SubCommand::IdCtrlr { file } => identify_ctrlr(&args.uri, file).await,
            SubCommand::CreateSnapshot => create_snapshot(&args.uri).await,
            SubCommand::Connect => connect(&args.uri).await,
        };
        if let Err(err) = res {
            error!("{}", err);
            -1
        } else {
            0
        }
    };

    Reactor::block_on(async move {
        let rc = fut.await;
        info!("{}", rc);
        mayastor_env_stop(0);
        std::process::exit(rc);
    });
}
