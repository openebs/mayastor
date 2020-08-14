#![allow(unused_assignments)]

use std::{thread, time};

use common::{bdev_io, ms_exec::MayastorProcess};
use mayastor::{
    bdev::{nexus_create, nexus_lookup},
    core::{mayastor_env_stop, MayastorCliArgs, MayastorEnvironment, Reactor},
    subsys,
    subsys::Config,
};

pub mod common;

static DISKNAME1: &str = "/tmp/disk1.img";
static BDEVNAME1: &str = "aio:///tmp/disk1.img?blk_size=512";

static DISKNAME2: &str = "/tmp/disk2.img";
static BDEVNAME2: &str = "aio:///tmp/disk2.img?blk_size=512";

static DISKSIZE_KB: u64 = 64 * 1024;

static CFGNAME1: &str = "/tmp/child1.yaml";
static UUID1: &str = "00000000-76b6-4fcf-864d-1027d4038756";
static CFGNAME2: &str = "/tmp/child2.yaml";
static UUID2: &str = "11111111-76b6-4fcf-864d-1027d4038756";

static NXNAME: &str = "replica_timeout_test";

fn generate_config() {
    let mut config = Config::default();

    let child1_bdev = subsys::BaseBdev {
        uri: format!("{}&uuid={}", BDEVNAME1, UUID1),
    };

    let child2_bdev = subsys::BaseBdev {
        uri: format!("{}&uuid={}", BDEVNAME2, UUID2),
    };

    config.base_bdevs = Some(vec![child1_bdev]);
    config.implicit_share_base = true;
    config.nexus_opts.iscsi_enable = false;
    config.nexus_opts.nvmf_replica_port = 8430;
    config.nexus_opts.nvmf_nexus_port = 8440;
    config.write(CFGNAME1).unwrap();

    config.base_bdevs = Some(vec![child2_bdev]);
    config.nexus_opts.nvmf_replica_port = 8431;
    config.nexus_opts.nvmf_nexus_port = 8441;
    config.write(CFGNAME2).unwrap();
}

fn start_mayastor(cfg: &str) -> MayastorProcess {
    let args = vec![
        "-s".to_string(),
        "128".to_string(),
        "-y".to_string(),
        cfg.to_string(),
    ];

    MayastorProcess::new(Box::from(args)).unwrap()
}

#[test]
#[ignore]
fn replica_stop_cont() {
    generate_config();

    common::truncate_file(DISKNAME1, DISKSIZE_KB);

    let mut ms = start_mayastor(CFGNAME1);

    test_init!();

    Reactor::block_on(async {
        create_nexus(true).await;
        bdev_io::write_some(NXNAME).await.unwrap();
        bdev_io::read_some(NXNAME).await.unwrap();
        ms.sig_stop();
        let handle = thread::spawn(move || {
            // Sufficiently long to cause a controller reset
            // see NvmeBdevOpts::Defaults::timeout_us
            thread::sleep(time::Duration::from_secs(3));
            ms.sig_cont();
            ms
        });
        bdev_io::read_some(NXNAME)
            .await
            .expect_err("should fail read after controller reset");
        ms = handle.join().unwrap();
        bdev_io::read_some(NXNAME)
            .await
            .expect("should read again after Nexus child continued");
        nexus_lookup(NXNAME).unwrap().destroy().await.unwrap();
        assert!(nexus_lookup(NXNAME).is_none());
    });

    common::delete_file(&[DISKNAME1.to_string()]);
}

#[test]
#[ignore]
fn replica_term() {
    generate_config();

    common::truncate_file(DISKNAME1, DISKSIZE_KB);
    common::truncate_file(DISKNAME2, DISKSIZE_KB);

    let mut ms1 = start_mayastor(CFGNAME1);
    let mut ms2 = start_mayastor(CFGNAME2);
    // Allow Mayastor processes to start listening on NVMf port
    thread::sleep(time::Duration::from_millis(250));

    test_init!();

    Reactor::block_on(async {
        create_nexus(false).await;
        bdev_io::write_some(NXNAME).await.unwrap();
        bdev_io::read_some(NXNAME).await.unwrap();
    });
    ms1.sig_term();
    thread::sleep(time::Duration::from_secs(1));
    Reactor::block_on(async {
        bdev_io::read_some(NXNAME)
            .await
            .expect("should read with 1 Nexus child terminated");
    });
    ms2.sig_term();
    thread::sleep(time::Duration::from_secs(1));
    Reactor::block_on(async {
        bdev_io::read_some(NXNAME)
            .await
            .expect_err("should fail read with 2 Nexus children terminated");
    });
    mayastor_env_stop(0);

    common::delete_file(&[DISKNAME1.to_string(), DISKNAME2.to_string()]);
}

async fn create_nexus(single: bool) {
    let mut ch = vec![
        "nvmf://127.0.0.1:8430/nqn.2019-05.io.openebs:".to_string()
            + &UUID1.to_string(),
    ];
    if !single {
        ch.push(
            "nvmf://127.0.0.1:8431/nqn.2019-05.io.openebs:".to_string()
                + &UUID2.to_string(),
        );
    }

    nexus_create(NXNAME, DISKSIZE_KB * 1024, None, &ch)
        .await
        .unwrap();
}
