use mayastor::core::{MayastorCliArgs, Mthread};

pub mod common;
use common::MayastorTest;
use mayastor::nexus_uri::bdev_create;

#[tokio::test]
async fn thread_tokio() {
    let args = MayastorCliArgs {
        reactor_mask: "0x3".into(),
        ..Default::default()
    };

    let ms = MayastorTest::new(args);

    let st = Mthread::get_init();
    let name = "malloc:///malloc0?size_mb=4";
    let rx = st.spawn_local(async move { bdev_create(name).await });
    rx.unwrap().await.unwrap().unwrap();
    drop(ms)
}
