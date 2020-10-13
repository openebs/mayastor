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
    lvs::Lvol,
    subsys,
    subsys::Config,
};

pub mod common;

static DISKNAME1: &str = "/tmp/disk1.img";

static DISKSIZE_KB: u64 = 128 * 1024;

static CFGNAME1: &str = "/tmp/child1.yaml";
static UUID1: &str = "00000000-76b6-4fcf-864d-1027d4038756";

static NXNAME: &str = "replica_snapshot_test";
static NXNAME_SNAP: &str = "replica_snapshot_test-snap";

fn generate_config() {
    let mut config = Config::default();

    config.implicit_share_base = true;
    config.nexus_opts.iscsi_enable = false;
    config.nexus_opts.nvmf_replica_port = 8430;
    config.nexus_opts.nvmf_nexus_port = 8440;
    let pool = subsys::Pool {
        name: "pool0".to_string(),
        disks: vec!["aio://".to_string() + &DISKNAME1.to_string()],
        replicas: Default::default(),
    };
    config.pools = Some(vec![pool]);
    config.write(CFGNAME1).unwrap();
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

fn conf_mayastor() {
    // configuration yaml does not yet support creating replicas
    let msc = "../target/debug/mayastor-client";
    let output = Command::new(msc)
        .args(&[
            "-p",
            "10125",
            "replica",
            "create",
            "--protocol",
            "nvmf",
            "pool0",
            UUID1,
            "--size",
            "64M",
        ])
        .output()
        .expect("could not exec mayastor-client");

    if !output.status.success() {
        io::stderr().write_all(&output.stderr).unwrap();
        panic!("failed to configure mayastor");
    }
}

fn share_snapshot(t: u64) {
    let msc = "../target/debug/mayastor-client";
    let output = Command::new(msc)
        .args(&[
            "-p",
            "10125",
            "replica",
            "share",
            &Lvol::format_snapshot_name(UUID1, t),
            "nvmf",
        ])
        .output()
        .expect("could not exec mayastor-client");

    if !output.status.success() {
        io::stderr().write_all(&output.stderr).unwrap();
        panic!("failed to configure mayastor");
    }
}

#[test]
fn replica_snapshot() {
    generate_config();

    // Start with a fresh pool
    common::delete_file(&[DISKNAME1.to_string()]);
    common::truncate_file(DISKNAME1, DISKSIZE_KB);

    let _ms1 = start_mayastor(CFGNAME1);
    // Allow Mayastor process to start listening on NVMf port
    thread::sleep(time::Duration::from_millis(250));

    conf_mayastor();

    test_init!();

    Reactor::block_on(async {
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
}

async fn create_nexus(t: u64) {
    let mut child_name = "nvmf://127.0.0.1:8430/nqn.2019-05.io.openebs:"
        .to_string()
        + &UUID1.to_string();
    let mut nexus_name = NXNAME;
    if t > 0 {
        child_name = Lvol::format_snapshot_name(&child_name, t);
        nexus_name = NXNAME_SNAP;
    }
    let ch = vec![child_name];

    nexus_create(&nexus_name, 64 * 1024 * 1024, None, &ch)
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
