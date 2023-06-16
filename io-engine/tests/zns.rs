use once_cell::sync::OnceCell;
use std::{
    convert::TryFrom,
    sync::Mutex,
};
use lazy_static::lazy_static;

extern crate libnvme_rs;

use io_engine::{
    bdev::nexus::{
        nexus_create,
        nexus_lookup_mut,
        nexus_bdev_error::Error,
    },
    core::{MayastorCliArgs, Protocol, UntypedBdevHandle},
};

pub mod common;
use common::compose::MayastorTest;
use run_script::{self};

//TODO: Also test pcie and nvmf
//static BDEVNAME1: &str = "pcie:///0000:00:03.0";
//static BDEVNAME1: &str = "nvmf://192.168.0.1:4420/nvmet-always";
lazy_static! {
    static ref BDEVNAME1: Mutex<String> = Mutex::new(String::new());
}
fn get_bdevname1() -> String {
    BDEVNAME1.lock().unwrap().clone()
}
fn set_bdevname1(name: String) {
    *BDEVNAME1.lock().unwrap() = name;
}

static DISKNAME2: &str = "/tmp/disk2.img";
static BDEVNAME2: &str = "uring:///tmp/disk2.img?blk_size=4096";

static MAYASTOR: OnceCell<MayastorTest> = OnceCell::new();

fn prepare_storage() -> u32 {
    common::delete_file(&[DISKNAME2.into()]);
    common::truncate_file(DISKNAME2, 64 * 1024);
    let ret = common::create_zoned_nullblk_device(4096, 2048, 1077, 0, 16, 14, 14);
    let nullblk_id = ret.unwrap();
    set_bdevname1(format!("uring:///dev/nullb{}?blk_size=4096", nullblk_id));
    nullblk_id
}

fn free_storage(nullblk_id: u32) {
    common::delete_nullblk_device(nullblk_id);
}

fn get_ms() -> &'static MayastorTest<'static> {
    MAYASTOR.get_or_init(|| MayastorTest::new(MayastorCliArgs::default()))
}

async fn create_connected_nvmf_nexus(
    ms: &'static MayastorTest<'static>,
) -> (libnvme_rs::NvmeTarget, String) {
    let uri = ms
        .spawn(async {
            create_nexus().await;
            // Claim the bdev
            let hdl = UntypedBdevHandle::open(&get_bdevname1(), true, true);
            let nexus = nexus_lookup_mut("nexus").unwrap();
            let ret = nexus.share(Protocol::Nvmf, None).await.unwrap();
            drop(hdl);
            ret
        })
        .await;
    // Create and connect NVMF target.
    let target = libnvme_rs::NvmeTarget::try_from(uri)
        .unwrap()
        .with_rand_hostnqn(true);

    target.connect().unwrap();

    let devices = target.block_devices(2).unwrap();

    assert_eq!(devices.len(), 1);
    (target, devices[0].to_string())
}

fn fio_run_zoned_verify(device: &str) -> Result<String, String> {
    println!("Running fio workload ...");
    //This writes sequentially two zones, resets them, writes them again and reads from them to do the crc32 check
    let (exit, stdout, stderr) = run_script::run(
        r#"
        fio --name=zonedwrite --rw=write --ioengine=libaio --direct=1 --zonemode=zbd \
        --size=2z --io_size=4z --bs=128k --verify=crc32 --filename=$1
        "#,
        &vec![device.into()],
        &run_script::ScriptOptions::new(),
    ).unwrap();

    if exit == 0 {
        Ok(stdout)
    } else {
        Err(stderr)
    }
}

fn blkzone(device: &str, subcommand: &str) -> Result<String, String> {
    let (exit, stdout, stderr) = run_script::run(
        r#"
        blkzone $1 $2
        "#,
        &vec![subcommand.into(), device.into()],
        &run_script::ScriptOptions::new(),
    ).unwrap();

    if exit == 0 {
        Ok(stdout)
    } else {
        Err(stderr)
    }
}

#[tokio::test]
async fn zns_fio(){

    let ms = get_ms();

    let nullblk_id = prepare_storage();
    let (target, nvmf_dev) = create_connected_nvmf_nexus(ms).await;

    let fio_result = fio_run_zoned_verify(&nvmf_dev);
    match fio_result {
        Ok(ref ok) => println!("{}", ok),
        Err(ref err) => println!("{}", err),
    }

    target.disconnect().unwrap();

    ms.spawn(async move {
        let mut nexus = nexus_lookup_mut("nexus").unwrap();
        nexus.as_mut().unshare_nexus().await.unwrap();
        nexus.destroy().await.unwrap();
    }).await;

    free_storage(nullblk_id);

    assert_eq!(true, fio_result.is_ok());
}

#[tokio::test]
async fn zns_blkzone(){

    let ms = get_ms();

    let nullblk_id = prepare_storage();
    let (target, nvmf_dev) = create_connected_nvmf_nexus(ms).await;

    let blkzone_report_result = blkzone(&nvmf_dev, "report");
    match blkzone_report_result {
        Ok(ref ok) => println!("{}", ok),
        Err(ref err) => println!("{}", err),
    }

    let blkzone_reset_result = blkzone(&nvmf_dev, "reset");
    match blkzone_reset_result {
        Ok(ref ok) => println!("{}", ok),
        Err(ref err) => println!("{}", err),
    }

    target.disconnect().unwrap();

    ms.spawn(async move {
        let mut nexus = nexus_lookup_mut("nexus").unwrap();
        nexus.as_mut().unshare_nexus().await.unwrap();
        nexus.destroy().await.unwrap();
    }).await;

    free_storage(nullblk_id);

    assert_eq!(true, blkzone_report_result.is_ok());
    assert_eq!(true, blkzone_reset_result.is_ok());
}

#[tokio::test]
async fn zns_replicated(){

    let ms = get_ms();

    let nullblk_id = prepare_storage();
    let ret  = ms.spawn(async {
            create_replicated_nexus().await
        })
        .await;

    free_storage(nullblk_id);
    assert_eq!(true, ret.is_err());
}

async fn create_nexus() {
    let ch = vec![get_bdevname1()];
    //TODO: test different sizes and a splitted nexus
    nexus_create("nexus", 1024*1024*1024*32, None, &ch)
        .await
        .unwrap();
}

async fn create_replicated_nexus() -> Result<(), Error> {
    let ch = vec![get_bdevname1(), BDEVNAME2.to_string()];
    nexus_create("nexus", 1024*1024*1024*32, None, &ch).await
}

