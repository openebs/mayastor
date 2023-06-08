//! Command line test utility to copy bytes to/from a replica which can be any
//! target type understood by the nexus.

extern crate clap;
#[macro_use]
extern crate tracing;

use std::{
    fmt,
    fs,
    io::{self, Write},
};

use clap::{App, Arg, SubCommand};

use chrono::Utc;
use io_engine::{
    bdev::{device_create, device_open},
    bdev_api::{bdev_create, BdevError},
    core::{
        mayastor_env_stop,
        CoreError,
        MayastorCliArgs,
        MayastorEnvironment,
        Reactor,
        SnapshotParams,
        UntypedBdev,
    },
    jsonrpc::print_error_chain,
    logger,
    subsys,
    subsys::Config,
};
use spdk_rs::DmaError;
use uuid::Uuid;
use version_info::version_info_str;

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
async fn create_bdev(uri: &str) -> Result<UntypedBdev> {
    let bdev_name = bdev_create(uri).await?;
    let bdev = UntypedBdev::lookup_by_name(&bdev_name)
        .expect("Failed to lookup the created bdev");
    Ok(bdev)
}

/// Read block of data from bdev at given offset to a file.
async fn read(uri: &str, offset: u64, file: &str) -> Result<()> {
    let bdev = device_create(uri).await?;
    let h = device_open(&bdev, false).unwrap().into_handle().unwrap();
    let mut buf = h.dma_malloc(h.get_device().block_len()).unwrap();
    let n = h.read_at(offset, &mut buf).await?;
    fs::write(file, buf.as_slice())?;
    info!("{} bytes read", n);
    Ok(())
}

/// Write block of data from file to bdev at given offset.
async fn write(uri: &str, offset: u64, file: &str) -> Result<()> {
    let bdev = device_create(uri).await?;
    let bytes = fs::read(file)?;
    let h = device_open(&bdev, false).unwrap().into_handle().unwrap();
    let mut buf = h.dma_malloc(h.get_device().block_len()).unwrap();
    let n = buf.as_mut_slice().write(&bytes[..]).unwrap();
    if n < buf.len() as usize {
        warn!("Writing a buffer which was not fully initialized from a file");
    }
    let written = h.write_at(offset, &buf).await?;
    info!("{} bytes written", written);
    Ok(())
}

/// NVMe Admin. Only works with read commands without a buffer requirement.
async fn nvme_admin(uri: &str, opcode: u8) -> Result<()> {
    let bdev = device_create(uri).await?;
    let h = device_open(&bdev, true).unwrap().into_handle().unwrap();
    h.nvme_admin_custom(opcode).await?;
    Ok(())
}

/// NVMe Admin identify controller, write output to a file.
async fn identify_ctrlr(uri: &str, file: &str) -> Result<()> {
    let bdev = device_create(uri).await?;
    let h = device_open(&bdev, true).unwrap().into_handle().unwrap();
    let buf = h.nvme_identify_ctrlr().await.unwrap();
    fs::write(file, buf.as_slice())?;
    Ok(())
}

/// Create a snapshot.
async fn create_snapshot(uri: &str) -> Result<()> {
    let bdev = device_create(uri).await?;
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
    );

    let t = h.create_snapshot(snapshot).await?;
    info!("snapshot taken at {}", t);
    Ok(())
}

/// Connect to the target.
async fn connect(uri: &str) -> Result<()> {
    let _bdev = create_bdev(uri).await?;
    info!("Connected!");
    Ok(())
}

fn main() {
    let matches = App::new("Test initiator for nexus replica")
        .version(version_info_str!())
        .about("Connect, read or write a block to a nexus replica using its URI")
        .arg(Arg::with_name("URI")
            .help("URI of the replica to connect to")
            .required(true)
            .index(1))
        .arg(Arg::with_name("offset")
            .short("o")
            .long("offset")
            .value_name("NUMBER")
            .help("Offset of IO operation on the replica in bytes (default 0)")
            .takes_value(true))
        .subcommand(SubCommand::with_name("connect")
            .about("Connect to and disconnect from the replica"))
        .subcommand(SubCommand::with_name("read")
            .about("Read bytes from the replica")
            .arg(Arg::with_name("FILE")
                .help("File to write data that were read from the replica")
                .required(true)
                .index(1)))
        .subcommand(SubCommand::with_name("write")
            .about("Write bytes to the replica")
            .arg(Arg::with_name("FILE")
                .help("File to read data from that will be written to the replica")
                .required(true)
                .index(1)))
        .subcommand(SubCommand::with_name("nvme-admin")
            .about("Send a custom NVMe Admin command")
            .arg(Arg::with_name("opcode")
                .help("Admin command opcode to send")
                .required(true)
                .index(1)))
        .subcommand(SubCommand::with_name("id-ctrlr")
            .about("Send NVMe Admin identify controller command")
            .arg(Arg::with_name("FILE")
                .help("File to write output of identify controller command")
                .required(true)
                .index(1)))
        .subcommand(SubCommand::with_name("create-snapshot")
            .about("Create a snapshot on the replica"))
        .get_matches();

    logger::init("INFO");

    let uri = matches.value_of("URI").unwrap().to_owned();
    let offset: u64 = match matches.value_of("offset") {
        Some(val) => val.parse().expect("Offset must be a number"),
        None => 0,
    };

    // This tool is just a client, so don't start NVMe-oF services.
    Config::get_or_init(|| {
        let mut cfg = Config::default();
        cfg.nexus_opts.nvmf_enable = false;
        cfg
    });

    let ms = MayastorEnvironment::new(MayastorCliArgs::default());

    ms.init();
    let fut = async move {
        let res = if let Some(matches) = matches.subcommand_matches("read") {
            read(&uri, offset, matches.value_of("FILE").unwrap()).await
        } else if let Some(matches) = matches.subcommand_matches("write") {
            write(&uri, offset, matches.value_of("FILE").unwrap()).await
        } else if let Some(matches) = matches.subcommand_matches("nvme-admin") {
            let opcode: u8 = match matches.value_of("opcode") {
                Some(val) => val.parse().expect("Opcode must be a number"),
                None => 0,
            };
            nvme_admin(&uri, opcode).await
        } else if let Some(matches) = matches.subcommand_matches("id-ctrlr") {
            identify_ctrlr(&uri, matches.value_of("FILE").unwrap()).await
        } else if matches.subcommand_matches("create-snapshot").is_some() {
            create_snapshot(&uri).await
        } else {
            connect(&uri).await
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
