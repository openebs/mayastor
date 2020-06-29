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

pub mod common;

static DISKNAME1: &str = "/tmp/disk1.img";
static BDEVNAME1: &str = "aio:///tmp/disk1.img?blk_size=512";

static DISKNAME2: &str = "/tmp/disk2.img";
static BDEVNAME2: &str = "aio:///tmp/disk2.img?blk_size=512";

fn generate_config() {
    let uri1 = BDEVNAME1.into();
    let uri2 = BDEVNAME2.into();
    let mut config = Config::default();

    let child1_bdev = subsys::BaseBdev {
        uri: uri1,
        uuid: Some("00000000-76b6-4fcf-864d-1027d4038756".into()),
    };

    let child2_bdev = subsys::BaseBdev {
        uri: uri2,
        uuid: Some("11111111-76b6-4fcf-864d-1027d4038756".into()),
    };

    config.base_bdevs = Some(vec![child1_bdev]);
    config.implicit_share_base = true;
    config.nexus_opts.iscsi_enable = false;
    config.nexus_opts.nvmf_replica_port = 8430;
    config.nexus_opts.nvmf_nexus_port = 8440;
    config.write("/tmp/child1.yaml").unwrap();

    config.base_bdevs = Some(vec![child2_bdev]);
    config.nexus_opts.nvmf_replica_port = 8431;
    config.nexus_opts.nvmf_nexus_port = 8441;
    config.write("/tmp/child2.yaml").unwrap();
}

#[test]
fn nexus_reset_mirror() {
    generate_config();

    common::truncate_file(DISKNAME1, 64 * 1024);
    common::truncate_file(DISKNAME2, 64 * 1024);

    let args = vec![
        "-s".to_string(),
        "128".to_string(),
        "-y".to_string(),
        "/tmp/child1.yaml".to_string(),
    ];

    let _ms1 = MayastorProcess::new(Box::from(args)).unwrap();

    let args = vec![
        "-s".to_string(),
        "128".to_string(),
        "-y".to_string(),
        "/tmp/child2.yaml".to_string(),
    ];

    let _ms2 = MayastorProcess::new(Box::from(args)).unwrap();

    test_init!();

    Reactor::block_on(async {
        create_nexus().await;
        reset().await;
    });
    mayastor_env_stop(0);
}

async fn create_nexus() {
    let ch = vec![
        "nvmf://127.0.0.1:8431/nqn.2019-05.io.openebs:11111111-76b6-4fcf-864d-1027d4038756".to_string(),
        "nvmf://127.0.0.1:8430/nqn.2019-05.io.openebs:00000000-76b6-4fcf-864d-1027d4038756".into()
    ];

    nexus_create("reset_test", 64 * 1024 * 1024, None, &ch)
        .await
        .unwrap();
}

async fn reset() {
    let bdev = BdevHandle::open("reset_test", true, true).unwrap();
    bdev.reset().await.unwrap();
}
