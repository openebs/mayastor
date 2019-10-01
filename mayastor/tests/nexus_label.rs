#![feature(async_await)]
use mayastor::bdev::nexus::nexus_label::{GPTHeader, GptEntry};
use std::io::{Cursor, Read};

const HDR_GUID: &str = "322974ae-5711-874b-bfbd-1a74df4dd714";
const PART0_GUID: &str = "ea2872a6-02ce-3f4b-82c4-c2147f76e3ff";
const PART1_GUID: &str = "a0ff1b47-2890-eb4c-a837-01df152f9442";

const CRC32: u32 = 3_031_999_803;
use mayastor::{mayastor_start, spdk_stop};

use bincode::serialize_into;
use mayastor::{
    bdev::nexus::nexus_bdev::{nexus_create, nexus_lookup},
    descriptor::DmaBuf,
};
use std::{
    io::{Seek, SeekFrom},
    process::Command,
};

static DISKNAME1: &str = "/tmp/disk1.img";
static BDEVNAME1: &str = "aio:///tmp/disk1.img?blk_size=512";

static DISKNAME2: &str = "/tmp/disk2.img";
static BDEVNAME2: &str = "aio:///tmp/disk2.img?blk_size=512";

#[test]
fn read_label() {
    let _log = mayastor::spdklog::SpdkLog::new();
    let _l = _log.init();

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

    mayastor::CPS_INIT!();
    let rc = mayastor_start("io-testing", vec!["-L", "all"], || {
        mayastor::executor::spawn(start());
    });

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
    spdk_stop(0);
}

/// Test that we can deserialize a known good gpt label and parse it. After
/// parsing it, serialize it again, and assert the values.

fn test_known_label() {
    let mut file = std::fs::File::open("./gpt_test_data.bin").unwrap();

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
    assert_eq!(partitions[0].ent_name.as_str(), "nexus_meta");
    assert_eq!(partitions[1].ent_name.as_str(), "zfs_data");

    assert_eq!(hdr.checksum(), CRC32);

    let array_checksum = GptEntry::checksum(&partitions);

    assert_eq!(array_checksum, hdr.table_crc);

    // test if we can serialize the original gpt partitions ourselves and
    // validate it matches the the well known crc32
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
    let _nexus = nexus_create("gpt_nexus", 512, 131_072, None, &ch)
        .await
        .unwrap();
}

// compare what is written
async fn label_child() {
    let nexus = nexus_lookup("gpt_nexus").unwrap();
    let child = nexus.get_child_as_mut_ref(0).unwrap();
    let mut file = std::fs::File::open("./gpt_test_data.bin").unwrap();

    let mut buffer = child.get_buf(34 * 512).unwrap();
    file.read_exact(&mut buffer.as_mut_slice()).unwrap();
    // we also write the mbr here hence the offset is 0
    child.write_at(0, &buffer).await.unwrap();

    let mut read_buffer = child.get_buf(34 * 512).unwrap();
    child.read_at(0, &mut read_buffer).await.unwrap();

    for (i, o) in buffer.as_slice().iter().zip(read_buffer.as_slice().iter()) {
        assert_eq!(i, o)
    }

    let nl = child.probe_label().await.unwrap();
    assert_eq!(&nl.partitions[0].ent_guid.to_string(), &PART0_GUID);
    assert_eq!(&nl.partitions[1].ent_guid.to_string(), &PART1_GUID);
}
