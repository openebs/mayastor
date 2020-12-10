use std::{
    io::{Cursor, Read, Seek, SeekFrom},
    process::Command,
};

use bincode::serialize_into;

use mayastor::{
    bdev::{nexus_create, nexus_lookup, GPTHeader, GptEntry},
    core::{
        mayastor_env_stop,
        DmaBuf,
        MayastorCliArgs,
        MayastorEnvironment,
        Reactor,
    },
};

const HDR_GUID: &str = "322974ae-5711-874b-bfbd-1a74df4dd714";
const PART0_GUID: &str = "ea2872a6-02ce-3f4b-82c4-c2147f76e3ff";
const PART1_GUID: &str = "a0ff1b47-2890-eb4c-a837-01df152f9442";

const CRC32: u32 = 0x9029_d72c;
static DISKNAME1: &str = "/tmp/disk1.img";
static BDEVNAME1: &str = "aio:///tmp/disk1.img?blk_size=512";

static DISKNAME2: &str = "/tmp/disk2.img";
static BDEVNAME2: &str = "aio:///tmp/disk2.img?blk_size=512";
pub mod common;

#[test]
fn read_label() {
    common::mayastor_test_init();
    let output = Command::new("truncate")
        .args(&["-s", "64m", DISKNAME1])
        .output()
        .expect("failed exec truncate");
    assert_eq!(output.status.success(), true);

    let output = Command::new("truncate")
        .args(&["-s", "64m", DISKNAME2])
        .output()
        .expect("failed exec truncate");

    assert_eq!(output.status.success(), true);

    let rc = MayastorEnvironment::new(MayastorCliArgs::default())
        .start(|| Reactor::block_on(start()).unwrap())
        .unwrap();
    assert_eq!(rc, 0);

    let output = Command::new("rm")
        .args(&["-rf", DISKNAME1, DISKNAME2])
        .output()
        .expect("failed delete test file");

    assert_eq!(output.status.success(), true);
}

async fn start() {
    test_known_label();
    make_nexus().await;
    label_child().await;
    mayastor_env_stop(0);
}

/// Test that we can deserialize a known good gpt label and parse it. After
/// parsing it, serialize it again, and assert the values.

fn test_known_label() {
    let mut file = std::fs::File::open("./gpt_primary_test_data.bin").unwrap();

    file.seek(SeekFrom::Start(512)).unwrap();
    let mut hdr_buf: [u8; 512] = [0; 512];
    file.read_exact(&mut hdr_buf).unwrap();

    let mut hdr: GPTHeader = GPTHeader::from_slice(&hdr_buf).unwrap();
    assert_eq!(hdr.self_checksum, CRC32);
    assert_eq!(hdr.guid.to_string(), HDR_GUID,);

    let mut part_buf: [u8; 128 * 128] = [0; 128 * 128];
    file.seek(SeekFrom::Start(1024)).unwrap();
    file.read_exact(&mut part_buf).unwrap();

    let partitions = GptEntry::from_slice(&part_buf, hdr.num_entries).unwrap();

    assert_eq!(partitions[0].ent_guid.to_string(), PART0_GUID);
    assert_eq!(partitions[1].ent_guid.to_string(), PART1_GUID);
    assert_eq!(partitions[0].ent_name.name, "nexus_meta");
    assert_eq!(partitions[1].ent_name.name, "zfs_data");

    assert_eq!(hdr.checksum(), CRC32);

    let array_checksum = GptEntry::checksum(&partitions);

    assert_eq!(array_checksum, hdr.table_crc);

    // test if we can serialize the original gpt partitions ourselves and
    // validate it matches the well-known crc32
    let mut buf = DmaBuf::new(32 * 512, 9).unwrap();

    let mut writer = Cursor::new(buf.as_mut_slice());
    for i in 0 .. hdr.num_entries {
        serialize_into(&mut writer, &partitions[i as usize]).unwrap();
    }

    let partitions =
        GptEntry::from_slice(&buf.as_slice(), hdr.num_entries).unwrap();

    let array_checksum = GptEntry::checksum(&partitions);
    assert_eq!(array_checksum, hdr.table_crc);
}

/// as replica URIs are new, so this will, implicitly, create a label on the
/// device
async fn make_nexus() {
    let ch = vec![BDEVNAME1.to_string(), BDEVNAME2.to_string()];
    nexus_create("gpt_nexus", 512 * 131_072, None, &ch)
        .await
        .unwrap();
}

// compare what is written
async fn label_child() {
    let nexus = nexus_lookup("gpt_nexus").unwrap();
    let child = &mut nexus.children[0];
    let hdl = child.handle().unwrap();

    let mut file = std::fs::File::open("./gpt_primary_test_data.bin").unwrap();
    let mut buffer = hdl.dma_malloc(34 * 512).unwrap();
    file.read_exact(&mut buffer.as_mut_slice()).unwrap();
    // write out the MBR + primary GPT header + GPT partition table
    hdl.write_at(0, &buffer).await.unwrap();

    let mut read_buffer = hdl.dma_malloc(34 * 512).unwrap();
    hdl.read_at(0, &mut read_buffer).await.unwrap();
    for (i, o) in buffer.as_slice().iter().zip(read_buffer.as_slice().iter()) {
        assert_eq!(i, o)
    }

    let mut file =
        std::fs::File::open("./gpt_secondary_test_data.bin").unwrap();
    let mut buffer = hdl.dma_malloc(33 * 512).unwrap();
    file.read_exact(&mut buffer.as_mut_slice()).unwrap();
    // write out the secondary GPT partition table + GPT header
    hdl.write_at(131_039 * 512, &buffer).await.unwrap();

    let mut read_buffer = hdl.dma_malloc(33 * 512).unwrap();
    hdl.read_at(131_039 * 512, &mut read_buffer).await.unwrap();
    for (i, o) in buffer.as_slice().iter().zip(read_buffer.as_slice().iter()) {
        assert_eq!(i, o)
    }

    let nl = child.probe_label().await.unwrap();
    assert_eq!(&nl.partitions[0].ent_guid.to_string(), &PART0_GUID);
    assert_eq!(&nl.partitions[1].ent_guid.to_string(), &PART1_GUID);
}
