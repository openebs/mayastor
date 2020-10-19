use std::{io, io::Write, process::Command, thread, time};

use common::{bdev_io, ms_exec::MayastorProcess};
use mayastor::{
    bdev::nexus_create,
    core::{
        mayastor_env_stop,
        BdevHandle,
        CoreError,
        MayastorCliArgs,
        MayastorEnvironment,
        Reactor,
    },
    lvs::{Lvol, Lvs},
    subsys,
    subsys::Config,
};

pub mod common;

static DISKNAME1: &str = "/tmp/disk1.img";
static DISKNAME2: &str = "/tmp/disk2.img";

static DISKSIZE_KB: u64 = 128 * 1024;

static CFGNAME1: &str = "/tmp/child1.yaml";
static CFGNAME2: &str = "/tmp/child2.yaml";
static UUID1: &str = "00000000-76b6-4fcf-864d-1027d4038756";

static NXNAME: &str = "replica_snapshot_test";
static NXNAME_SNAP: &str = "replica_snapshot_test-snap";

fn generate_config() {
    let mut config = Config::default();

    config.nexus_opts.iscsi_enable = false;
    let pool1 = subsys::Pool {
        name: "pool1".to_string(),
        disks: vec!["aio://".to_string() + &DISKNAME1.to_string()],
        replicas: Default::default(),
    };
    config.pools = Some(vec![pool1]);
    config.write(CFGNAME1).unwrap();
    config.nexus_opts.nvmf_replica_port = 8430;
    config.nexus_opts.nvmf_nexus_port = 8440;
    let pool2 = subsys::Pool {
        name: "pool2".to_string(),
        disks: vec!["aio://".to_string() + &DISKNAME2.to_string()],
        replicas: Default::default(),
    };
    config.pools = Some(vec![pool2]);
    config.write(CFGNAME2).unwrap();
}

fn start_mayastor(cfg: &str) -> MayastorProcess {
    let args = vec![
        "-s".to_string(),
        "128".to_string(),
        "-g".to_string(),
        "127.0.0.1:10125".to_string(),
        "-y".to_string(),
        cfg.to_string(),
    ];

    MayastorProcess::new(Box::from(args)).unwrap()
}

fn conf_mayastor(msc_args: &[&str]) {
    let msc = "../target/debug/mayastor-client";
    let output = Command::new(msc)
        .args(&*msc_args)
        .output()
        .expect("could not exec mayastor-client");
    if !output.status.success() {
        io::stderr().write_all(&output.stderr).unwrap();
        panic!("failed to configure mayastor");
    }
}

fn create_replica() {
    // configuration yaml does not yet support creating replicas
    conf_mayastor(&[
        "-p",
        "10125",
        "replica",
        "create",
        "--protocol",
        "nvmf",
        "pool2",
        UUID1,
        "--size",
        "64M",
    ]);
}

fn share_snapshot(t: u64) {
    conf_mayastor(&[
        "-p",
        "10125",
        "replica",
        "share",
        &Lvol::format_snapshot_name(UUID1, t),
        "nvmf",
    ]);
}

#[test]
fn replica_snapshot() {
    generate_config();

    // Start with fresh pools
    common::delete_file(&[DISKNAME1.to_string()]);
    common::truncate_file(DISKNAME1, DISKSIZE_KB);
    common::delete_file(&[DISKNAME2.to_string()]);
    common::truncate_file(DISKNAME2, DISKSIZE_KB);

    let _ms2 = start_mayastor(CFGNAME2);
    // Allow Mayastor process to start listening on NVMf port
    thread::sleep(time::Duration::from_millis(250));

    create_replica();

    test_init!(CFGNAME1);

    Reactor::block_on(async {
        let pool = Lvs::lookup("pool1").unwrap();
        pool.create_lvol(UUID1, 64 * 1024 * 1024, true)
            .await
            .unwrap();
        create_nexus(0).await;
        bdev_io::write_some(NXNAME, 0, 0xff).await.unwrap();
        // Issue an unimplemented vendor command
        custom_nvme_admin(0xc1)
            .await
            .expect_err("unexpectedly succeeded invalid nvme admin command");
        bdev_io::read_some(NXNAME, 0, 0xff).await.unwrap();
        let t = create_snapshot().await.unwrap();
        // Check that IO to the replica still works after creating a snapshot
        bdev_io::read_some(NXNAME, 0, 0xff).await.unwrap();
        bdev_io::write_some(NXNAME, 0, 0xff).await.unwrap();
        bdev_io::read_some(NXNAME, 0, 0xff).await.unwrap();
        bdev_io::write_some(NXNAME, 1024, 0xaa).await.unwrap();
        bdev_io::read_some(NXNAME, 1024, 0xaa).await.unwrap();
        // Share the snapshot and create a new nexus
        share_snapshot(t);
        create_nexus(t).await;
        bdev_io::write_some(NXNAME_SNAP, 0, 0xff)
            .await
            .expect_err("writing to snapshot should fail");
        // Verify that data read from snapshot remains unchanged
        bdev_io::write_some(NXNAME, 0, 0x55).await.unwrap();
        bdev_io::read_some(NXNAME, 0, 0x55).await.unwrap();
        bdev_io::read_some(NXNAME_SNAP, 0, 0xff).await.unwrap();
        bdev_io::read_some(NXNAME_SNAP, 1024, 0).await.unwrap();
    });
    mayastor_env_stop(0);

    common::delete_file(&[DISKNAME1.to_string()]);
    common::delete_file(&[DISKNAME2.to_string()]);
}

async fn create_nexus(t: u64) {
    let mut children = vec![
        "loopback:///".to_string() + &UUID1.to_string(),
        "nvmf://127.0.0.1:8430/nqn.2019-05.io.openebs:".to_string()
            + &UUID1.to_string(),
    ];
    let mut nexus_name = NXNAME;
    if t > 0 {
        children
            .iter_mut()
            .for_each(|c| *c = Lvol::format_snapshot_name(&c, t));
        nexus_name = NXNAME_SNAP;
    }

    nexus_create(&nexus_name, 64 * 1024 * 1024, None, &children)
        .await
        .unwrap();
}

async fn create_snapshot() -> Result<u64, CoreError> {
    let h = BdevHandle::open(NXNAME, true, false).unwrap();
    let t = h
        .create_snapshot()
        .await
        .expect("failed to create snapshot");
    Ok(t)
}

async fn custom_nvme_admin(opc: u8) -> Result<(), CoreError> {
    let h = BdevHandle::open(NXNAME, true, false).unwrap();
    h.nvme_admin_custom(opc).await?;
    Ok(())
}
