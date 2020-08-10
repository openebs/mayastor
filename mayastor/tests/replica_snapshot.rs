use std::{io, io::Write, process::Command, thread, time};

use common::ms_exec::MayastorProcess;
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
    subsys,
    subsys::Config,
};

pub mod common;

static DISKNAME1: &str = "/tmp/disk1.img";

static DISKSIZE_KB: u64 = 128 * 1024;

static CFGNAME1: &str = "/tmp/child1.yaml";
static UUID1: &str = "00000000-76b6-4fcf-864d-1027d4038756";

static NXNAME: &str = "replica_snapshot_test";

fn generate_config() {
    let mut config = Config::default();

    config.implicit_share_base = true;
    config.nexus_opts.iscsi_enable = false;
    config.nexus_opts.nvmf_replica_port = 8430;
    config.nexus_opts.nvmf_nexus_port = 8440;
    let pool = subsys::Pool {
        name: "pool0".to_string(),
        disks: vec![DISKNAME1.to_string()],
        blk_size: 512,
        io_if: 1, // AIO
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
        create_nexus().await;
        write_some().await.unwrap();
        custom_nvme_admin(0xc1)
            .await
            .expect_err("unexpectedly succeeded invalid nvme admin command");
        read_some().await.unwrap();
        create_snapshot().await.unwrap();
        // Check that IO to the replica still works after creating a snapshot
        // Checking the snapshot itself is tbd
        read_some().await.unwrap();
        write_some().await.unwrap();
        read_some().await.unwrap();
    });
    mayastor_env_stop(0);

    common::delete_file(&[DISKNAME1.to_string()]);
}

async fn create_nexus() {
    let ch = vec![
        "nvmf://127.0.0.1:8430/nqn.2019-05.io.openebs:".to_string()
            + &UUID1.to_string(),
    ];

    nexus_create(NXNAME, 64 * 1024 * 1024, None, &ch)
        .await
        .unwrap();
}

async fn write_some() -> Result<(), CoreError> {
    let h = BdevHandle::open(NXNAME, true, false).unwrap();
    let mut buf = h.dma_malloc(512).expect("failed to allocate buffer");
    buf.fill(0xff);

    let s = buf.as_slice();
    assert_eq!(s[0], 0xff);

    h.write_at(0, &buf).await?;
    Ok(())
}

async fn read_some() -> Result<(), CoreError> {
    let h = BdevHandle::open(NXNAME, true, false).unwrap();
    let mut buf = h.dma_malloc(1024).expect("failed to allocate buffer");
    let slice = buf.as_mut_slice();

    assert_eq!(slice[0], 0);
    slice[512] = 0xff;
    assert_eq!(slice[512], 0xff);

    let len = h.read_at(0, &mut buf).await?;
    assert_eq!(len, 1024);

    let slice = buf.as_slice();

    for &it in slice.iter().take(512) {
        assert_eq!(it, 0xff);
    }
    assert_eq!(slice[512], 0);
    Ok(())
}

async fn create_snapshot() -> Result<(), CoreError> {
    let h = BdevHandle::open(NXNAME, true, false).unwrap();
    h.create_snapshot()
        .await
        .expect("failed to create snapshot");
    Ok(())
}

async fn custom_nvme_admin(opc: u8) -> Result<(), CoreError> {
    let h = BdevHandle::open(NXNAME, true, false).unwrap();
    h.nvme_admin_custom(opc).await?;
    Ok(())
}
