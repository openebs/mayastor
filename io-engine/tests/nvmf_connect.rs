use std::pin::Pin;

use once_cell::sync::OnceCell;

pub mod common;

use common::MayastorTest;

use io_engine::{
    bdev::{device_create, device_destroy, device_lookup},
    bdev_api::BdevError,
    core::{CoreError, MayastorCliArgs, Share},
    lvs::{Lvs, LvsLvol},
    pool_backend::PoolArgs,
};

static MAYASTOR: OnceCell<MayastorTest> = OnceCell::new();

fn get_ms() -> &'static MayastorTest<'static> {
    MAYASTOR.get_or_init(|| MayastorTest::new(MayastorCliArgs::default()))
}

/// Runs several non-blocking qpair connections in parallel.
#[tokio::test]
async fn nvmf_connect_async() {
    common::composer_init();

    let uri = init_nvmf_share().await;

    for _ in 0 .. 20 {
        let name = spawn_device_create(&uri).await;

        let f0 = spawn_get_io_handle_nonblock(&name);
        let f1 = spawn_get_io_handle_nonblock(&name);
        let f2 = spawn_get_io_handle_nonblock(&name);

        let res = tokio::join!(f0, f1, f2);
        assert!(res.0.is_ok());
        assert!(res.1.is_ok());
        assert!(res.2.is_ok());

        spawn_device_destroy(&uri).await.unwrap();
    }

    deinit_nvmf_share().await;
}

/// Runs several non-blocking qpair connections, and a device destroy in
/// parallel.
#[tokio::test]
async fn nvmf_connect_async_drop() {
    common::composer_init();

    let uri = init_nvmf_share().await;

    for _ in 0 .. 20 {
        let name = spawn_device_create(&uri).await;

        let f0 = spawn_get_io_handle_nonblock(&name);
        let f1 = spawn_get_io_handle_nonblock(&name);
        let f2 = spawn_get_io_handle_nonblock(&name);
        let f3 = spawn_device_destroy(&uri);

        let res = tokio::join!(f0, f1, f2, f3);

        #[cfg(feature = "spdk-async-qpair-connect")]
        {
            assert!(res.0.is_err());
            assert!(res.1.is_err());
            assert!(res.2.is_err());
        }

        #[cfg(not(feature = "spdk-async-qpair-connect"))]
        {
            assert!(res.0.is_err());
            assert!(res.1.is_err());
            assert!(res.2.is_err());
        }

        assert!(res.3.is_ok());
    }

    deinit_nvmf_share().await;
}

const POOL_SIZE: u64 = 64 * 1024 * 1024;
const BDEV_NAME: &str = "malloc:///mem0?size_mb=128";
const POOL_NAME: &str = "pool_0";
const REPL_NAME: &str = "repl_0";
const REPL_UUID: &str = "65acdaac-14c4-41d8-a55e-d03bfd7185a4";

async fn init_nvmf_share() -> String {
    get_ms()
        .spawn(async {
            let pool = Lvs::create_or_import(PoolArgs {
                name: POOL_NAME.to_string(),
                disks: vec![BDEV_NAME.to_string()],
                uuid: None,
                cluster_size: None,
            })
            .await
            .unwrap();

            let mut lvol = pool
                .create_lvol(REPL_NAME, POOL_SIZE, Some(REPL_UUID), false, None)
                .await
                .unwrap();

            let mut lvol = Pin::new(&mut lvol);
            lvol.as_mut().share_nvmf(None).await.unwrap();
            lvol.as_bdev().share_uri().unwrap()
        })
        .await
}

async fn deinit_nvmf_share() {
    get_ms()
        .spawn(async {
            Lvs::lookup(POOL_NAME).unwrap().destroy().await.unwrap();
        })
        .await;
}

async fn spawn_device_create(uri: &str) -> String {
    get_ms()
        .spawn({
            let uri = uri.to_string();
            async move { device_create(&uri).await.unwrap() }
        })
        .await
}

async fn spawn_get_io_handle_nonblock(name: &str) -> Result<(), CoreError> {
    get_ms()
        .spawn({
            let name = name.to_string();
            async move {
                device_lookup(&name)
                    .unwrap()
                    .open(true)
                    .unwrap()
                    .get_io_handle_nonblock()
                    .await
                    .map(|_| ())
            }
        })
        .await
}

async fn spawn_device_destroy(uri: &str) -> Result<(), BdevError> {
    get_ms()
        .spawn({
            let uri = uri.to_string();
            async move { device_destroy(&uri).await }
        })
        .await
}
