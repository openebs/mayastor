use common::MayastorTest;
use io_engine::{
    bdev::crypto::{Cipher, EncryptionKey},
    core::{MayastorCliArgs, MayastorFeatures, ToErrno},
    lvs::Lvs,
    pool_backend::{PoolArgs, PoolBackend},
};
use once_cell::sync::OnceCell;

pub mod common;

static TESTDIR: &str = "/tmp/io-engine-tests";
static DISK_CRYPTO: &str = "/tmp/io-engine-tests/fips_crypto_disk.img";
static XTS_KEY: &str = "2b7e151628aed2a6abf7158809cf4f3c";
static XTS_KEY2: &str = "2b7e151628aed2a6abf7158809cf4f3d";

static MAYASTOR: OnceCell<MayastorTest> = OnceCell::new();

fn ms() -> &'static MayastorTest<'static> {
    MAYASTOR.get_or_init(|| {
        MayastorTest::new(MayastorCliArgs {
            fips: true,
            ..Default::default()
        })
    })
}

fn enc_pool_args(name: &str) -> PoolArgs {
    PoolArgs {
        name: name.into(),
        disks: vec![format!("aio://{DISK_CRYPTO}")],
        enc_key: Some(EncryptionKey {
            cipher: Cipher::AesXts,
            key_name: "test_key".into(),
            key: XTS_KEY.into(),
            key_len: 128,
            key2: Some(XTS_KEY2.into()),
            key2_len: Some(128),
        }),
        crypto_vbdev_name: Some(format!("crypto_{name}")),
        backend: PoolBackend::Lvs,
        ..Default::default()
    }
}

/// The crypto module we use is not FIPS validated, so with FIPS mode enabled
/// encryption must not be offered nor allowed.
#[tokio::test]
async fn lvs_pool_fips_no_encryption() {
    let _ = std::process::Command::new("mkdir")
        .args(["-p"])
        .args([TESTDIR])
        .output()
        .expect("failed to execute mkdir");

    common::delete_file(&[DISK_CRYPTO.into()]);
    common::truncate_file(DISK_CRYPTO, 128 * 1024);

    ms().spawn(async {
        let features = MayastorFeatures::get();
        assert!(features.fips(), "fips mode should be enabled");
        assert!(
            !features.diskpool_encryption(),
            "encryption must not be supported in fips mode"
        );

        // An encrypted pool can neither be created...
        let error = Lvs::create_or_import(enc_pool_args("fips_enc_pool"))
            .await
            .expect_err("encrypted pool create should fail in fips mode");
        assert_eq!(error.to_errno(), nix::errno::Errno::EOPNOTSUPP);

        // ... nor imported.
        let error = Lvs::import_from_args(enc_pool_args("fips_enc_pool"))
            .await
            .expect_err("encrypted pool import should fail in fips mode");
        assert_eq!(error.to_errno(), nix::errno::Errno::EOPNOTSUPP);

        // A plain pool is unaffected.
        let pool = Lvs::create_or_import(PoolArgs {
            name: "fips_plain_pool".into(),
            disks: vec![format!("aio://{DISK_CRYPTO}")],
            backend: PoolBackend::Lvs,
            ..Default::default()
        })
        .await
        .expect("plain pool create should succeed in fips mode");
        assert_eq!(pool.base_bdev_().driver(), "aio");
        pool.destroy().await.unwrap();
    })
    .await;

    common::delete_file(&[DISK_CRYPTO.into()]);
}
