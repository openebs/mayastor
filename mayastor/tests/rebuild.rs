use mayastor::{
    aio_dev::AioBdev,
    descriptor::Descriptor,
    mayastor_start,
    mayastor_stop,
    rebuild::RebuildTask,
};

static DISKNAME1: &str = "/tmp/source.img";
static BDEVNAME1: &str = "aio:///tmp/source.img?blk_size=512";

static DISKNAME2: &str = "/tmp/target.img";
static BDEVNAME2: &str = "aio:///tmp/target.img?blk_size=512";

mod common;
#[test]
fn copy_task() {
    common::mayastor_test_init();
    let args = vec!["rebuild_task", "-m", "0x2"];

    common::dd_random_file(DISKNAME1, "4096", "16384");
    common::truncate_file(DISKNAME2, "64M");

    let rc: i32 = mayastor_start("test", args, || {
        mayastor::executor::spawn(works());
    });

    assert_eq!(rc, 0);

    common::compare_files(DISKNAME1, DISKNAME2);
    common::delete_file(&[DISKNAME1.into(), DISKNAME2.into()]);
}

async fn create_bdevs() {
    let source = AioBdev {
        name: BDEVNAME1.to_string(),
        file: DISKNAME1.to_string(),
        blk_size: 4096,
    };

    let target = AioBdev {
        name: BDEVNAME2.to_string(),
        file: DISKNAME2.to_string(),
        blk_size: 4096,
    };

    if source.create().await.is_err() {
        panic!("failed to create source device for rebuild test");
    }

    if target.create().await.is_err() {
        panic!("failed to create target device for rebuild test");
    }
}

async fn works() {
    create_bdevs().await;

    let source = Descriptor::open(BDEVNAME1, false).unwrap();
    let target = Descriptor::open(BDEVNAME2, true).unwrap();

    let copy_task = RebuildTask::new(source, target).unwrap();

    if let Ok(r) = RebuildTask::start_rebuild(copy_task) {
        let done = r.await.expect("rebuild task already gone!");
        assert_eq!(done, true);
        mayastor_stop(0);
    }
}
