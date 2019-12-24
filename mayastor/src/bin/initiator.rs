//! Command line test utility to copy bytes to/from a replica which can be any
//! target type understood by the nexus.

extern crate clap;
#[macro_use]
extern crate log;

use clap::{App, Arg, SubCommand};
use mayastor::{
    bdev::{bdev_lookup_by_name, Bdev},
    descriptor::{DescError, Descriptor},
    dma::{DmaBuf, DmaError},
    executor,
    jsonrpc::print_error_chain,
    mayastor_logger_init,
    mayastor_start,
    mayastor_stop,
    nexus_uri::{bdev_create, BdevError},
};
use std::{
    fmt,
    fs,
    io::{self, Write},
    process,
};

/// The errors from this utility are not supposed to be parseable by machine,
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
impl From<DescError> for Error {
    fn from(err: DescError) -> Self {
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

mayastor::CPS_INIT!();

/// Create initiator bdev.
async fn create_bdev(uri: &str) -> Result<Bdev> {
    let bdev_name = bdev_create(uri).await?;
    let bdev = bdev_lookup_by_name(&bdev_name)
        .expect("Failed to lookup the created bdev");
    Ok(bdev)
}

/// Allocate IO buffer suitable for given bdev with the bdev descriptor.
fn alloc_buf(bdev: &Bdev, desc: &Descriptor) -> Result<DmaBuf> {
    let len = bdev.block_len() as usize;
    desc.dma_malloc(len).map_err(|err| err.into())
}

/// Read block of data from bdev at given offset to a file.
async fn read(uri: &str, offset: u64, file: &str) -> Result<()> {
    let bdev = create_bdev(uri).await?;
    let desc = Descriptor::open(&bdev, false)?;
    let mut buf = alloc_buf(&bdev, &desc)?;
    let n = desc.read_at(offset, &mut buf).await?;
    fs::write(file, buf.as_slice())?;
    info!("{} bytes read", n);
    Ok(())
}

/// Write block of data from file to bdev at given offset.
async fn write(uri: &str, offset: u64, file: &str) -> Result<()> {
    let bdev = create_bdev(uri).await?;
    let bytes = fs::read(file)?;
    let desc = Descriptor::open(&bdev, true)?;
    let mut buf = alloc_buf(&bdev, &desc)?;
    let mut n = buf.as_mut_slice().write(&bytes[..]).unwrap();
    if n < buf.len() {
        warn!("Writing a buffer which was not fully initialized from a file");
    }
    n = desc.write_at(offset, &buf).await?;
    info!("{} bytes written", n);
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
        .get_matches();

    mayastor_logger_init("INFO");

    let uri = matches.value_of("URI").unwrap().to_owned();
    let offset: u64 = match matches.value_of("offset") {
        Some(val) => val.parse().expect("Offset must be a number"),
        None => 0,
    };

    let rc = mayastor_start("initiator", ["-s", "128"].to_vec(), move || {
        let fut = async move {
            let res = if let Some(matches) = matches.subcommand_matches("read")
            {
                read(&uri, offset, matches.value_of("FILE").unwrap()).await
            } else if let Some(matches) = matches.subcommand_matches("write") {
                write(&uri, offset, matches.value_of("FILE").unwrap()).await
            } else {
                connect(&uri).await
            };
            let rc = if let Err(err) = res {
                error!("{}", err);
                -1
            } else {
                0
            };
            mayastor_stop(rc);
        };
        executor::spawn(fut);
    });
    info!("{}", rc);

    process::exit(rc);
}
