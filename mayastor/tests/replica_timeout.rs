pub mod common;
use common::ms_exec::MayastorProcess;
use mayastor::{
    bdev::nexus_create,
    core::{
        mayastor_env_stop,
        BdevHandle,
        MayastorCliArgs,
        MayastorEnvironment,
        Reactor,
    },
    subsys,
    subsys::Config,
};
use std::{thread, time};

static DISKNAME1: &str = "/tmp/disk1.img";
static BDEVNAME1: &str = "aio:///tmp/disk1.img?blk_size=512";

static DISKNAME2: &str = "/tmp/disk2.img";
static BDEVNAME2: &str = "aio:///tmp/disk2.img?blk_size=512";

static DISKSIZE_KB: u64 = 64 * 1024;

static CFGNAME1: &str = "/tmp/child1.yaml";
static UUID1: &str = "00000000-76b6-4fcf-864d-1027d4038756";
static CFGNAME2: &str = "/tmp/child2.yaml";
static UUID2: &str = "11111111-76b6-4fcf-864d-1027d4038756";

static NXNAME: &str = "replica_term_test";

fn generate_config() {
    let uri1 = BDEVNAME1.into();
    let uri2 = BDEVNAME2.into();
    let mut config = Config::default();

    let child1_bdev = subsys::BaseBdev {
        uri: uri1,
        uuid: Some(UUID1.into()),
    };

    let child2_bdev = subsys::BaseBdev {
        uri: uri2,
        uuid: Some(UUID2.into()),
    };

    config.base_bdevs = Some(vec![child1_bdev]);
    config.implicit_share_base = true;
    config.nexus_opts.iscsi_enable = false;
    config.nexus_opts.replica_port = 8430;
    config.write(CFGNAME1).unwrap();

    config.base_bdevs = Some(vec![child2_bdev]);
    config.nexus_opts.replica_port = 8431;
    config.write(CFGNAME2).unwrap();
}

fn start_mayastor(cfg: &str, port: u16) -> MayastorProcess {
    let args = vec![
        "-s".to_string(),
        "128".to_string(),
        "-y".to_string(),
        cfg.to_string(),
        "-p".into(),
        port.to_string(),
    ];

    MayastorProcess::new(Box::from(args)).unwrap()
}

#[test]
fn replica_term() {
    generate_config();

    common::truncate_file(DISKNAME1, DISKSIZE_KB);
    common::truncate_file(DISKNAME2, DISKSIZE_KB);

    let mut ms1 = start_mayastor(CFGNAME1, 10126);
    let mut ms2 = start_mayastor(CFGNAME2, 10127);

    test_init!();

    Reactor::block_on(async {
        create_nexus(false).await;
        write_some().await;
        read_some(true).await;
    });
    ms1.sig_term();
    thread::sleep(time::Duration::from_secs(1));
    Reactor::block_on(async {
        read_some(true).await;
    });
    ms2.sig_term();
    thread::sleep(time::Duration::from_secs(1));
    Reactor::block_on(async {
        read_some(false).await;
    });
    mayastor_env_stop(0);
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

async fn write_some() {
    let bdev = BdevHandle::open(NXNAME, true, false).unwrap();
    let mut buf = bdev.dma_malloc(512).expect("failed to allocate buffer");
    buf.fill(0xff);

    let s = buf.as_slice();
    assert_eq!(s[0], 0xff);

    bdev.write_at(0, &buf).await.unwrap();
}

async fn read_some(ok: bool) {
    let bdev = BdevHandle::open(NXNAME, true, false).unwrap();
    let mut buf = bdev.dma_malloc(1024).expect("failed to allocate buffer");
    let slice = buf.as_mut_slice();

    assert_eq!(slice[0], 0);
    slice[512] = 0xff;
    assert_eq!(slice[512], 0xff);

    match bdev.read_at(0, &mut buf).await {
        Ok(s) => {
            assert_eq!(ok, true);
            assert_eq!(s, 1024);
        }
        Err(_e) => {
            assert_eq!(ok, false);
            return;
        }
    };

    let slice = buf.as_slice();

    for &it in slice.iter().take(512) {
        assert_eq!(it, 0xff);
    }
    assert_eq!(slice[512], 0);
}
