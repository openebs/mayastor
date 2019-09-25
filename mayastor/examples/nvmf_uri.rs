#![feature(async_await)]
use log::error;
use mayastor::{
    bdev::nexus::nexus_bdev::nexus_create,
    mayastor_start,
    spdk_stop,
};

fn main() {
    let log = mayastor::spdklog::SpdkLog::new();
    let _ = log.init();
    mayastor::CPS_INIT!();
    let args = vec!["-c", "../etc/test.conf"];
    mayastor_start("test", args, || {
        mayastor::executor::spawn(works());
    });
}
async fn works() {
    let children = vec![
        "aio:////disk1.img?blk_size=512".to_string(),
        "aio:////disk2.img?blk_size=512".into(),
    ];
    let name = nexus_create("hello", 512, 131_072, None, &children).await;

    if let Err(name) = name {
        error!("{:?}", name);
    }
    spdk_stop(0);
}
