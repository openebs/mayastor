use mayastor::{mayastor_start, mayastor_stop};

use mayastor::{
    descriptor::Descriptor,
    nexus_uri::{nexus_parse_uri, BdevType},
};
use std::process::Command;

static DISKNAME: &str = "/tmp/disk.img";
static BDEVNAME: &str = "aio:///tmp/disk.img?blk_size=512";

#[test]
fn io_test() {
    let _log = mayastor::spdklog::SpdkLog::new();
    let _l = _log.init();
    mayastor::CPS_INIT!();
    let output = Command::new("truncate")
        .args(&["-s", "64m", DISKNAME])
        .output()
        .expect("failed exec truncate");

    assert_eq!(output.status.success(), true);

    mayastor_start("io-testing", vec![""], || {
        mayastor::executor::spawn(start());
    });

    let output = Command::new("rm")
        .args(&["-rf", DISKNAME])
        .output()
        .expect("failed delete test file");

    assert_eq!(output.status.success(), true);
}

// The actual work here is completely driven by the futures. We
// only execute one future per reactor loop.
async fn start() {
    make_bdev().await;
    write_some().await;
    read_some().await;
    mayastor_stop(0);
}

async fn make_bdev() {
    match nexus_parse_uri(BDEVNAME) {
        Ok(BdevType::Aio(args)) => {
            let _ = args.create().await.expect("failed to create");
        }
        _ => {
            panic!("invalid test configuration");
        }
    }
}

async fn write_some() {
    let d = Descriptor::open(BDEVNAME, true).expect("failed open bdev");
    let mut buf = d.dma_zmalloc(512).expect("failed to allocate buffer");
    buf.fill(0xff);

    let s = buf.as_slice();
    assert_eq!(s[0], 0xff);

    d.write_at(0, &buf).await.unwrap();
    d.close();
}

async fn read_some() {
    let d = Descriptor::open(BDEVNAME, false);
    let d = d.unwrap();
    let mut buf = d.dma_zmalloc(1024).expect("failed to allocate buffer");
    let slice = buf.as_mut_slice();

    assert_eq!(slice[0], 0);
    slice[513] = 0xff;
    assert_eq!(slice[513], 0xff);

    d.read_at(0, &mut buf).await.unwrap();
    d.close();

    let slice = buf.as_slice();

    assert_eq!(slice[0], 0xff);
    assert_eq!(slice[513], 0);
}
