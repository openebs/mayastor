use bincode::serialize_into;
use clap::Parser;
use std::io::{Cursor, Seek, SeekFrom};
use std::os::unix::fs::FileExt;
use version_info::package_description;
use version_info::version_info_str;

#[derive(Debug, Clone, Parser)]
#[clap(name = package_description!(), version = version_info_str!())]
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
    Read,
    Write,
}

fn main() {
    let cli_args = CliArgs::parse();

    match &cli_args.command {
        Command::Write => {
            write(&cli_args);
        }
        Command::Read => {
            read(&cli_args);
        }
    }
}

fn write(cli_args: &CliArgs) {
    let disk = cli_args.disk_uri.to_file_path().unwrap();
    let disk = disk.to_string_lossy().to_string();

    let guid = io_engine::GptGuid::from(
        uuid::Uuid::parse_str("978D2F99-AA7C-4F24-AE22-1BF99137D7B1").unwrap(),
    );
    let file = std::fs::File::options().write(true).open(&disk).unwrap();

    let file_len = file.metadata().unwrap().len();
    let mut block_size = 512;
    let num_blocks;
    if file_len == 0 {
        let dev = disk.strip_prefix("/dev/").unwrap();
        let ss = std::fs::read_to_string(format!("/sys/block/{dev}/size")).unwrap();
        num_blocks = ss.trim().parse().unwrap();
        let ss =
            std::fs::read_to_string(format!("/sys/block/{dev}/queue/physical_block_size")).unwrap();
        block_size = ss.trim().parse().unwrap();
    } else {
        num_blocks = file_len / block_size;
    }

    // Create new disk label.
    let label = io_engine::GptLabel::generate_label(guid, block_size, num_blocks).unwrap();
    println!("{label}");

    let p = get_primary_data(&label).unwrap();
    let s = get_secondary_data(&label).unwrap();

    println!("{}", p.buf.len());

    file.write_at(&p.buf, p.offset).unwrap();
    file.write_at(&s.buf, s.offset).unwrap();
}

fn read(cli_args: &CliArgs) {
    let disk = cli_args.disk_uri.to_file_path().unwrap();
    let disk = disk.to_string_lossy().to_string();

    let file = std::fs::File::options().read(true).open(&disk).unwrap();

    let file_len = file.metadata().unwrap().len();
    let mut block_size = 512;
    let num_blocks;
    if file_len == 0 {
        let dev = disk.strip_prefix("/dev/").unwrap();
        let ss = std::fs::read_to_string(format!("/sys/block/{dev}/size")).unwrap();
        num_blocks = ss.trim().parse().unwrap();
        let ss =
            std::fs::read_to_string(format!("/sys/block/{dev}/queue/physical_block_size")).unwrap();
        block_size = ss.trim().parse().unwrap();
    } else {
        num_blocks = file_len / block_size;
    }

    // PMBR is 512B even on larget sector disks
    let mut buf = [0; 512];
    file.read_exact_at(buf.as_mut_slice(), 0).unwrap();

    let mbr = io_engine::Pmbr::from_slice(&buf.as_slice()[440..512]).unwrap();

    let mut buf = buffer(block_size);
    file.read_exact_at(buf.as_mut_slice(), block_size).unwrap();
    let primary = io_engine::GptHeader::from_slice(&buf.as_slice()).unwrap();
    io_engine::GptLabel::validate_primary_header(&primary, block_size, num_blocks).unwrap();

    file.read_exact_at(buf.as_mut_slice(), (num_blocks - 1) * block_size)
        .unwrap();
    let secondary = io_engine::GptHeader::from_slice(&buf.as_slice()).unwrap();
    io_engine::GptLabel::validate_secondary_header(&secondary, block_size, num_blocks).unwrap();

    io_engine::GptLabel::consistency_check(&primary, &secondary).unwrap();
    let active = primary;

    // Partition table
    let blocks = io_engine::Aligned::get_blocks(
        u64::from(active.entry_size * active.num_entries),
        block_size,
    );
    let mut buf = buffer(blocks * block_size);
    let offset = active.lba_table * block_size;
    file.read_exact_at(buf.as_mut_slice(), offset).unwrap();
    let partitions = io_engine::GptEntry::from_slice(buf.as_slice(), active.num_entries).unwrap();
    io_engine::GptLabel::validate_partitions(&partitions, &active).unwrap();

    let label = io_engine::GptLabel {
        status: io_engine::NexusLabelStatus::Both,
        block_size,
        mbr,
        primary,
        partitions: partitions.into_iter().filter(|p| !p.is_unused()).collect(),
        secondary,
    };
    println!("{label}");
}

fn buffer(size: u64) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.resize(size as usize, 0);
    buf
}

struct LabelData {
    offset: u64,
    buf: Vec<u8>,
}

fn get_primary_data(label: &io_engine::GptLabel) -> Result<LabelData, io_engine::LabelError> {
    let mut buf = Vec::with_capacity((label.primary.lba_start * label.block_size) as usize);

    let mut writer = Cursor::new(&mut buf);

    // Protective MBR
    writer.seek(SeekFrom::Start(440)).unwrap();
    serialize_into(&mut writer, &label.mbr).unwrap();

    // Primary GPT header
    writer
        .seek(SeekFrom::Start(label.primary.lba_self * label.block_size))
        .unwrap();
    serialize_into(&mut writer, &label.primary).unwrap();

    // Primary partition table
    writer
        .seek(SeekFrom::Start(label.primary.lba_table * label.block_size))
        .unwrap();

    for entry in label.partitions.iter() {
        serialize_into(&mut writer, &entry).unwrap();
    }

    Ok(LabelData { offset: 0, buf })
}

fn get_secondary_data(label: &io_engine::GptLabel) -> Result<LabelData, io_engine::LabelError> {
    let mut buf = Vec::with_capacity(
        ((label.secondary.lba_self - label.secondary.lba_table + 1) * label.block_size) as usize,
    );

    let mut writer = Cursor::new(&mut buf);

    // Secondary partition table
    for entry in label.partitions.iter() {
        serialize_into(&mut writer, &entry).unwrap();
    }

    // Secondary GPT header
    writer
        .seek(SeekFrom::Start(
            (label.secondary.lba_self - label.secondary.lba_table) * label.block_size,
        ))
        .unwrap();
    serialize_into(&mut writer, &label.secondary).unwrap();

    Ok(LabelData {
        offset: label.secondary.lba_table * label.block_size,
        buf,
    })
}

fn create_pool() {
    // device=/dev/xxx
    // 1. Try to import pool with the given name
    // 2. Check if disk is:
    // 2.a. it's a filesystem: abort!
    // 2.b. it's a GPT:
    // 2.b.2. table has partitions, abort?
    // 2.c. it's a GPT Partition:
    // 2.c.1. it's not our uuid, abort?
    // 2.c.2. Absorb the partition uuid as part of the create call return status?
    // Or update the partition uuid?
    // But if uri is alread by part-uuid then we must keep it?
    // 2.c.3. create anyway? part uuid will not match for sure.
    // 3. clean disk:
    // 3.a. create GPT with pool uuid
    // 3.b. create part1 with random uuid
    // 3.c. create part2 with pool uuid
}
