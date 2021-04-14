use std::{process::Command, time::SystemTime};

use mayastor::{
    bdev::{
        nexus_create,
        nexus_lookup,
        Guid,
        MetaDataChildEntry,
        MetaDataIndex,
        MetaDataObject,
        NexusMetaData,
    },
    core::{mayastor_env_stop, MayastorCliArgs, MayastorEnvironment, Reactor},
};

const DISKNAME: &str = "/tmp/disk1.img";
const BDEVNAME: &str = "aio:///tmp/disk1.img?blk_size=512";

pub mod common;

#[test]
fn metadata_test() {
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

    let mut data: Vec<MetaDataObject> = Vec::new();

    let mut object = MetaDataObject::new();
    object.generation = 1;
    object.timestamp = 1001;
    object.children.push(MetaDataChildEntry {
        guid: Guid::new_random(),
        state: 0,
    });
    object.children.push(MetaDataChildEntry {
        guid: Guid::new_random(),
        state: 1,
    });
    data.push(object);

    let mut object = MetaDataObject::new();
    object.generation = 2;
    object.timestamp = 1002;
    object.children.push(MetaDataChildEntry {
        guid: Guid::new_random(),
        state: 0,
    });
    object.children.push(MetaDataChildEntry {
        guid: Guid::new_random(),
        state: 1,
    });
    object.children.push(MetaDataChildEntry {
        guid: Guid::new_random(),
        state: 2,
    });
    object.children.push(MetaDataChildEntry {
        guid: Guid::new_random(),
        state: 4,
    });
    data.push(object);

    let mut object = MetaDataObject::new();
    object.generation = 3;
    object.timestamp = 1003;
    for i in 0 .. 26 {
        object.children.push(MetaDataChildEntry {
            guid: Guid::new_random(),
            state: i,
        });
    }
    data.push(object);

    let mut object = MetaDataObject::new();
    object.generation = 4;
    object.timestamp = 1004;
    data.push(object);

    let mut object = MetaDataObject::new();
    object.generation = 5;
    object.timestamp = 1005;
    object.children.push(MetaDataChildEntry {
        guid: Guid::new_random(),
        state: 0,
    });
    object.children.push(MetaDataChildEntry {
        guid: Guid::new_random(),
        state: 1,
    });
    object.children.push(MetaDataChildEntry {
        guid: Guid::new_random(),
        state: 2,
    });
    object.children.push(MetaDataChildEntry {
        guid: Guid::new_random(),
        state: 4,
    });
    data.push(object);

    let mut object = MetaDataObject::new();
    object.generation = 6;
    object.timestamp = 1006;
    object.children.push(MetaDataChildEntry {
        guid: Guid::new_random(),
        state: 0,
    });
    object.children.push(MetaDataChildEntry {
        guid: Guid::new_random(),
        state: 1,
    });
    object.children.push(MetaDataChildEntry {
        guid: Guid::new_random(),
        state: 2,
    });
    object.children.push(MetaDataChildEntry {
        guid: Guid::new_random(),
        state: 4,
    });
    data.push(object);

    // check if default index was created
    assert!(NexusMetaData::get_index(&child).await.unwrap().is_some());

    // create a (new) index with a capacity of 4
    let now = SystemTime::now();
    let mut index = MetaDataIndex::new(
        Guid::new_random(),
        Guid::new_random(),
        child.metadata_index_lba,
        4,
    );
    NexusMetaData::create_index(&child, &mut index, &now)
        .await
        .unwrap();

    // verify index exists
    assert!(NexusMetaData::get_index(&child).await.unwrap().is_some());

    // append two objects
    let now = SystemTime::now();
    NexusMetaData::add(&child, &mut data[0], &now)
        .await
        .unwrap();
    NexusMetaData::add(&child, &mut data[1], &now)
        .await
        .unwrap();

    // retrieve the "last" object and compare with the original
    let object = NexusMetaData::last(&child).await.unwrap();
    assert_eq!(object.unwrap(), data[1]);

    // append two more objects
    let now = SystemTime::now();
    NexusMetaData::add(&child, &mut data[2], &now)
        .await
        .unwrap();
    NexusMetaData::add(&child, &mut data[3], &now)
        .await
        .unwrap();

    // the index should now be full - retrieve all objects
    let stored = NexusMetaData::get(&child, 10).await.unwrap();
    assert_eq!(stored.len(), 4);
    assert_eq!(data[0], stored[0]);
    assert_eq!(data[1], stored[1]);
    assert_eq!(data[2], stored[2]);
    assert_eq!(data[3], stored[3]);

    // append one more object
    let now = SystemTime::now();
    NexusMetaData::add(&child, &mut data[4], &now)
        .await
        .unwrap();

    // retrieve all objects again
    let stored = NexusMetaData::get(&child, 10).await.unwrap();
    assert_eq!(stored.len(), 4);

    // the first object should have been removed
    // to make space for the last object added
    assert_eq!(data[1], stored[0]);
    assert_eq!(data[2], stored[1]);
    assert_eq!(data[3], stored[2]);
    assert_eq!(data[4], stored[3]);

    // remove the last object
    let now = SystemTime::now();
    let object = NexusMetaData::remove(&child, &now).await.unwrap();
    assert_eq!(object.unwrap(), data[4]);

    // replace the (new) last object
    let now = SystemTime::now();
    NexusMetaData::update(&child, &mut data[5], &now)
        .await
        .unwrap();

    // retrieve last two objects
    let stored = NexusMetaData::get(&child, 2).await.unwrap();
    assert_eq!(stored.len(), 2);
    assert_eq!(data[2], stored[0]);
    assert_eq!(data[5], stored[1]);

    assert_eq!(stored[0].children.len(), 26);
    assert_eq!(stored[1].children.len(), 4);

    // purge all but the last object
    let now = SystemTime::now();
    NexusMetaData::purge(&child, 1, &now).await.unwrap();

    // retrieve all objects again
    let stored = NexusMetaData::get(&child, 10).await.unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(data[5], stored[0]);
}
