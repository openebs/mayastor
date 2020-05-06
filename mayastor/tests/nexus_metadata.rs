use std::{process::Command, time::SystemTime};

use mayastor::{
    bdev::{
        nexus_create,
        nexus_lookup,
        NexusConfig,
        NexusConfigVersion1,
        NexusConfigVersion2,
        NexusConfigVersion3,
    },
    core::{mayastor_env_stop, MayastorCliArgs, MayastorEnvironment, Reactor},
};

const DISKNAME: &str = "/tmp/disk1.img";
const BDEVNAME: &str = "aio:///tmp/disk1.img?blk_size=512";

pub mod common;

#[test]
fn get_metadata() {
    common::mayastor_test_init();
    let output = Command::new("truncate")
        .args(&["-s", "64m", DISKNAME])
        .output()
        .expect("failed exec truncate");
    assert_eq!(output.status.success(), true);

    let status = MayastorEnvironment::new(MayastorCliArgs::default())
        .start(|| Reactor::block_on(start()).unwrap())
        .unwrap();
    assert_eq!(status, 0);

    let output = Command::new("rm")
        .args(&["-f", DISKNAME])
        .output()
        .expect("failed delete test file");
    assert_eq!(output.status.success(), true);
}

async fn start() {
    make_nexus().await;
    read_write_metadata().await;
    mayastor_env_stop(0);
}

async fn make_nexus() {
    let ch = vec![BDEVNAME.to_string()];
    nexus_create("metadata_nexus", 512 * 131_072, None, &ch)
        .await
        .unwrap();
}

async fn read_write_metadata() {
    let nexus = nexus_lookup("metadata_nexus").unwrap();
    let child = &mut nexus.children[0];

    let now = SystemTime::now();
    let mut data: Vec<NexusConfig> = Vec::new();

    data.push(NexusConfig::Version1(NexusConfigVersion1 {
        name: "Hello".to_string(),
        labels: String::from("How now brown cow")
            .split_whitespace()
            .map(String::from)
            .collect(),
        revision: 38,
        checksum: 0x3c2d38ab,
        data: String::from("Hello from v1"),
    }));

    data.push(NexusConfig::Version2(NexusConfigVersion2 {
        name: "Hello".to_string(),
        labels: String::from("How now brown cow")
            .split_whitespace()
            .map(String::from)
            .collect(),
        revision: 40,
        checksum: 0x3c2e40ab,
        count: 100,
    }));

    data.push(NexusConfig::Version3(NexusConfigVersion3 {
        name: "Hello".to_string(),
        revision: 42,
        checksum: 0x3c2f42ab,
        data: String::from("Hello from v3")
            .split_whitespace()
            .map(String::from)
            .collect(),
    }));

    // create an index and append two objects
    let mut metadata = child.create_metadata().await.unwrap();
    child
        .append_config_object(&mut metadata, &data[0_usize], &now)
        .await
        .unwrap();
    child
        .append_config_object(&mut metadata, &data[1_usize], &now)
        .await
        .unwrap();

    // read index from disk, retrieve first object, and compare with original
    metadata = child.get_metadata().await.unwrap();
    let config = child.get_config_object(&metadata, 0).await.unwrap();
    assert_eq!(config.unwrap(), data[0]);

    // re-read index from disk, delete first object, and append a third
    metadata = child.get_metadata().await.unwrap();
    child.delete_config_object(&mut metadata, 0).await.unwrap();
    child
        .append_config_object(&mut metadata, &data[2_usize], &now)
        .await
        .unwrap();

    // re-read index from disk, retrieve first object, and compare with original
    metadata = child.get_metadata().await.unwrap();
    let config = child.get_config_object(&metadata, 0).await.unwrap();
    assert_eq!(config.unwrap(), data[1]);

    // re-read index from disk, retrieve last object, and compare with original
    metadata = child.get_metadata().await.unwrap();
    let config = child.get_latest_config_object(&metadata).await.unwrap();
    assert_eq!(config.unwrap(), data[2]);
}
