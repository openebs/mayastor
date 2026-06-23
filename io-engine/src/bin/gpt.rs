use clap::Parser;
use io_engine::gpt_labels::{
    GptBuffer, GptDiskOps, GptDiskProps, GptGuid, GptLabel, LabelError, ProbeError,
};
use snafu::{ResultExt, Snafu};
use std::{
    ops::{Deref, DerefMut},
    os::unix::fs::{FileExt, FileTypeExt},
};
use version_info::{package_description, version_info_string};

#[derive(Debug, Clone, Parser)]
#[clap(name = package_description!(), version = version_info_string!())]
struct CliArgs {
    /// The disk uri to use.
    /// Run as sudo if this is a real block device.
    #[clap(short, long)]
    disk_uri: url::Url,

    /// Command.
    #[clap(subcommand)]
    command: Command,
}

#[derive(clap::Subcommand, Debug, Clone)]
enum Command {
    /// Read and display the GPT label from the disk.
    Read,
    /// Write a new GPT label to the disk, overwriting any existing label.
    Write,
}

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display("{source}"))]
    Label { source: LabelError },
    #[snafu(display("{source}"))]
    Probe { source: ProbeError },
    #[snafu(display("No filepath in disk-uri"))]
    InvalidUri,
    #[snafu(display("Failed to open disk: {source}"))]
    OpenDisk { source: std::io::Error },
    #[snafu(display("Failed to write to disk: {source}"))]
    Write { source: std::io::Error },
    #[snafu(display("Failed to read {path}: {source}"))]
    ReadFile {
        path: String,
        source: std::io::Error,
    },
    #[snafu(display("Failed to parse int from sysfs {path} = {value}: {source}"))]
    ParseSysfs {
        path: String,
        value: String,
        source: std::num::ParseIntError,
    },
    #[snafu(display("Failed to read disk metadata: {source}"))]
    DiskMeta { source: std::io::Error },
    #[snafu(display("Disk not in /dev/"))]
    DiskDev,
    #[snafu(display("{source}"))]
    InvalidUuid { source: uuid::Error },
}
impl From<uuid::Error> for Error {
    fn from(source: uuid::Error) -> Self {
        Self::InvalidUuid { source }
    }
}
impl From<LabelError> for Error {
    fn from(source: LabelError) -> Self {
        Self::Label { source }
    }
}
impl From<ProbeError> for Error {
    fn from(source: ProbeError) -> Self {
        Self::Probe { source }
    }
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let cli_args = CliArgs::parse();

    match &cli_args.command {
        Command::Write => write(&cli_args),
        Command::Read => read(&cli_args).await,
    }
    .map_err(|e| format!("{e}"))
}

/// Creates a fresh GPT label on the disk and writes both the primary (LBA 1)
/// and secondary (last LBA) headers along with their partition tables.
fn write(cli_args: &CliArgs) -> Result<(), Error> {
    // Fixed GUID used as the disk identifier for CLI testing.
    let guid = GptGuid::from(uuid::Uuid::parse_str(
        "978D2F99-AA7C-4F24-AE22-1BF99137D7B1",
    )?);
    let mut gpt_disk = GptDisk::new(false, true, cli_args)?;
    let props = gpt_disk.props();

    // Create new disk label.
    let label = GptLabel::generate_label(guid, props.block_size, props.num_blocks, None)?;
    println!("{label}");

    let primary = label.primary_data(&gpt_disk)?;
    let secondary = label.secondary_data(&gpt_disk)?;

    gpt_disk.write_at(&primary.buf, primary.offset)?;
    gpt_disk.write_at(&secondary.buf, secondary.offset)?;

    Ok(())
}

/// Probes the disk for an existing GPT label and prints it to stdout.
async fn read(cli_args: &CliArgs) -> Result<(), Error> {
    let gpt_disk = GptDisk::new(true, false, cli_args)?;

    let label = gpt_disk.probe_label().await?;
    println!("{label}");

    if !GptLabel::check_partitions(&label) {
        todo!("resync the label with the partitions");
    }

    Ok(())
}

/// Wraps a disk file or block device for GPT label read/write operations.
struct GptDisk {
    file: std::fs::File,
    props: GptDiskProps,
}
impl GptDisk {
    /// Opens the disk at the path given in `cli_args.disk_uri`.
    /// `read`/`write` control the file open flags.
    fn new(read: bool, write: bool, cli_args: &CliArgs) -> Result<Self, Error> {
        let disk = cli_args
            .disk_uri
            .to_file_path()
            .map_err(|_| Error::InvalidUri)?;
        let disk = disk.to_string_lossy().to_string();

        let file = std::fs::File::options()
            .read(read)
            .write(write)
            .open(&disk)
            .context(OpenDiskSnafu)?;
        let props = Self::disk_props(&disk, &file)?;
        Ok(Self { file, props })
    }
    /// Determines block size and block count. For block devices these are read
    /// from sysfs (`/sys/block/<dev>/queue/logical_block_size` and `size`).
    /// For regular files a 512-byte block size is assumed.
    fn disk_props(disk: &str, file: &std::fs::File) -> Result<GptDiskProps, Error> {
        let metadata = file.metadata().context(DiskMetaSnafu)?;
        let file_len = metadata.len();

        let block_size;
        let num_blocks;
        if metadata.file_type().is_block_device() {
            let dev = disk.strip_prefix("/dev/").ok_or(Error::DiskDev)?;
            block_size = Self::read_block_sysfs(dev, "queue/logical_block_size")?;
            // sysfs `size` is always reported in 512-byte sectors, regardless of
            // the device's logical block size; convert into logical blocks.
            let sysfs_sectors = Self::read_block_sysfs(dev, "size")?;
            num_blocks = sysfs_sectors * 512 / block_size;
        } else {
            block_size = 512;
            num_blocks = file_len / block_size;
        }
        Ok(GptDiskProps {
            block_size,
            num_blocks,
        })
    }
    fn read_block_sysfs(dev: &str, attr: &str) -> Result<u64, Error> {
        let path = format!("/sys/block/{dev}/{attr}");
        let value = std::fs::read_to_string(&path).map_err(|source| Error::ReadFile {
            path: path.clone(),
            source,
        })?;
        value.trim().parse().map_err(|source| Error::ParseSysfs {
            path,
            value,
            source,
        })
    }
    /// Writes `buf` to the disk at the given byte `offset`.
    fn write_at(&mut self, buf: &[u8], offset: u64) -> Result<(), Error> {
        self.file.write_all_at(buf, offset).context(WriteSnafu)?;
        Ok(())
    }
    async fn probe_label(&self) -> Result<GptLabel, LabelError> {
        GptLabel::probe_label(self).await
    }
}

/// In-memory I/O buffer satisfying [`GptBuffer`] for file-based disk access.
struct FileBuffer(Vec<u8>);
impl Deref for FileBuffer {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        self.0.as_slice()
    }
}
impl DerefMut for FileBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.as_mut_slice()
    }
}

#[async_trait::async_trait(?Send)]
impl io_engine::gpt_labels::GptDiskOps for GptDisk {
    type Buffer = FileBuffer;

    fn buffer_alloc(&self, size: u64) -> Result<Self::Buffer, spdk_rs::DmaError> {
        Ok(FileBuffer(vec![0; size as usize]))
    }
    fn props(&self) -> GptDiskProps {
        self.props
    }
    async fn read_at(
        &self,
        offset: u64,
        buffer: &mut Self::Buffer,
    ) -> Result<u64, io_engine::core::CoreError> {
        let len = buffer.0.len() as u64;
        // todo: refactor DmaError&CoreError out of this interface so we can
        // propagate this io::Error directly instead of stuffing it into a
        // generic CoreError variant.
        self.file
            .read_exact_at(&mut buffer.0, offset)
            .map_err(|e| {
                let errno = e
                    .raw_os_error()
                    .map(nix::errno::Errno::from_raw)
                    .unwrap_or(nix::errno::Errno::EIO);
                io_engine::core::CoreError::ReadDispatch {
                    source: errno,
                    offset,
                    len,
                }
            })?;
        Ok(len)
    }
}
impl GptBuffer for FileBuffer {
    fn as_slice(&self) -> &[u8] {
        self.0.as_slice()
    }
    fn as_mut_slice(&mut self) -> &mut [u8] {
        self.0.as_mut_slice()
    }
}
