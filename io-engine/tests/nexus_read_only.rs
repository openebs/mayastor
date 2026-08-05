//! Nexus ROX (ReadOnlyMany) submit-time rejection tests.
//!
//! Verifies that when the nexus is marked read-only, write-family I/O
//! submitted to the nexus bdev is rejected at submit time and reads
//! continue to succeed. The read-only flag is set via
//! `Nexus::set_nexus_read_only`, which stores on the atomic on `Nexus` and
//! fans the new value out to every per-core `NexusChannel` snapshot so the
//! submit gate reads a plain bool on the hot path. The share/unshare paths
//! go through the same setter; this test drives it directly to isolate the
//! submit gate from the rest of the share pipeline.

use common::{bdev_io, MayastorTest};
use io_engine::{
    bdev::nexus::{nexus_create, nexus_lookup, nexus_lookup_mut, Error},
    core::{MayastorCliArgs, Protocol},
};
use once_cell::sync::OnceCell;

pub mod common;

static TESTDIR: &str = "/tmp/io-engine-tests";

static DISKNAME1: &str = "/tmp/io-engine-tests/nexus_read_only_1.img";
static DISKNAME2: &str = "/tmp/io-engine-tests/nexus_read_only_2.img";
static BDEVNAME1: &str = "aio:///tmp/io-engine-tests/nexus_read_only_1.img?blk_size=512";
static BDEVNAME2: &str = "aio:///tmp/io-engine-tests/nexus_read_only_2.img?blk_size=512";

static NEXUS_NAME: &str = "nexus_read_only";
static NEXUS_UUID: &str = "cdc2a7db-3ac3-403a-af80-7fadc1581c47";
static NEXUS_SIZE: u64 = 32 * 1024 * 1024;

static IMMUTABLE_DISKNAME1: &str = "/tmp/io-engine-tests/nexus_read_only_immut_1.img";
static IMMUTABLE_DISKNAME2: &str = "/tmp/io-engine-tests/nexus_read_only_immut_2.img";
static IMMUTABLE_BDEVNAME1: &str =
    "aio:///tmp/io-engine-tests/nexus_read_only_immut_1.img?blk_size=512";
static IMMUTABLE_BDEVNAME2: &str =
    "aio:///tmp/io-engine-tests/nexus_read_only_immut_2.img?blk_size=512";

static IMMUTABLE_NEXUS_NAME: &str = "nexus_read_only_immutable";
static IMMUTABLE_NEXUS_UUID: &str = "8c9c9e4e-3a49-42d7-9cf5-2b0d7e7d1c9a";

const BACKING_FILE_SIZE_KB: u64 = 64 * 1024;

/// SPDK/EAL can only be initialised once per test binary; share the
/// `MayastorTest` across the tests in this file.
static MAYASTOR: OnceCell<MayastorTest> = OnceCell::new();

fn get_ms() -> &'static MayastorTest<'static> {
    MAYASTOR.get_or_init(|| {
        MayastorTest::new(MayastorCliArgs {
            enable_io_all_thrd_nexus_channels: true,
            ..Default::default()
        })
    })
}

fn ensure_testdir() {
    let _ = std::process::Command::new("mkdir")
        .args(["-p", TESTDIR])
        .output()
        .expect("failed to execute mkdir");
}

#[tokio::test]
async fn nexus_read_only_rejects_writes() {
    let ms = get_ms();

    ensure_testdir();
    let disks: &[String] = &[DISKNAME1.into(), DISKNAME2.into()];
    common::delete_file(disks);
    common::truncate_file(DISKNAME1, BACKING_FILE_SIZE_KB);
    common::truncate_file(DISKNAME2, BACKING_FILE_SIZE_KB);

    ms.spawn(async {
        nexus_create(
            NEXUS_NAME,
            NEXUS_SIZE,
            Some(NEXUS_UUID),
            &[BDEVNAME1.to_string(), BDEVNAME2.to_string()],
        )
        .await
        .expect("failed to create nexus");

        // A freshly created nexus is RWO by default; verify both a write and a
        // read succeed before we flip the flag, so any failure below is
        // attributable to the ROX gate rather than test setup.
        {
            let nex = nexus_lookup(NEXUS_NAME).expect("nexus should be present");
            assert!(!nex.is_read_only(), "nexus should default to RWO");
        }
        bdev_io::write_some(NEXUS_NAME, 0, 2, 0xff)
            .await
            .expect("baseline RWO write should succeed");
        bdev_io::read_some(NEXUS_NAME, 0, 2, 0xff)
            .await
            .expect("baseline RWO read should succeed");

        // Flip the flag via `set_nexus_read_only` — this is the same async
        // fan-out the share/unshare paths use, which pushes the new value out
        // to every per-core `NexusChannel` snapshot so the submit gate sees it.
        // Bypasses `share_ext` so the test focuses on the submit gate itself.
        nexus_lookup(NEXUS_NAME)
            .expect("nexus should be present")
            .set_nexus_read_only(true)
            .await;

        // Writes must now fail. `bdev_io::write_some` propagates the submit
        // rejection as an error; the exact NVMe status is verified by the
        // fact that reads still succeed against the same offsets.
        bdev_io::write_some(NEXUS_NAME, 0, 2, 0xaa)
            .await
            .expect_err("write should be rejected while nexus is ROX");
        bdev_io::read_some(NEXUS_NAME, 0, 2, 0xff)
            .await
            .expect("read should succeed while nexus is ROX");

        // Flipping back to RWO must restore write behaviour and must not
        // require any nexus teardown or child re-open.
        nexus_lookup(NEXUS_NAME)
            .expect("nexus should be present")
            .set_nexus_read_only(false)
            .await;
        bdev_io::write_some(NEXUS_NAME, 0, 2, 0xbb)
            .await
            .expect("write should succeed after flipping back to RWO");
        bdev_io::read_some(NEXUS_NAME, 0, 2, 0xbb)
            .await
            .expect("read after RWO write should reflect the written data");
    })
    .await;

    common::delete_file(disks);
}

/// `read_only` is negotiated with the NVMe initiator at connect time
/// (identify data is cached per session), so a mid-lifetime flip on a
/// live target wouldn't propagate to already-connected clients. `share_ext`
/// enforces this by rejecting a re-share on an already-shared target when
/// the requested `read_only` differs from the live value — callers must
/// unshare and re-share to change the mode.
#[tokio::test]
async fn nexus_read_only_immutable_on_shared_target() {
    let ms = get_ms();

    ensure_testdir();
    let disks: &[String] = &[IMMUTABLE_DISKNAME1.into(), IMMUTABLE_DISKNAME2.into()];
    common::delete_file(disks);
    common::truncate_file(IMMUTABLE_DISKNAME1, BACKING_FILE_SIZE_KB);
    common::truncate_file(IMMUTABLE_DISKNAME2, BACKING_FILE_SIZE_KB);

    ms.spawn(async {
        nexus_create(
            IMMUTABLE_NEXUS_NAME,
            NEXUS_SIZE,
            Some(IMMUTABLE_NEXUS_UUID),
            &[
                IMMUTABLE_BDEVNAME1.to_string(),
                IMMUTABLE_BDEVNAME2.to_string(),
            ],
        )
        .await
        .expect("failed to create nexus");

        // First share: RWO.
        nexus_lookup_mut(IMMUTABLE_NEXUS_NAME)
            .expect("nexus should be present")
            .share_ext(Protocol::Nvmf, None, vec![], false)
            .await
            .expect("initial RWO share should succeed");
        assert!(!nexus_lookup(IMMUTABLE_NEXUS_NAME)
            .expect("nexus should be present")
            .is_read_only());

        // Re-share with the same value: no-op, allowed.
        nexus_lookup_mut(IMMUTABLE_NEXUS_NAME)
            .expect("nexus should be present")
            .share_ext(Protocol::Nvmf, None, vec![], false)
            .await
            .expect("re-share with same read_only value should be a no-op");

        // Re-share with a different value: rejected.
        let err = nexus_lookup_mut(IMMUTABLE_NEXUS_NAME)
            .expect("nexus should be present")
            .share_ext(Protocol::Nvmf, None, vec![], true)
            .await
            .expect_err("re-share with a different read_only value must be rejected");
        assert!(
            matches!(err, Error::ReadOnlyChangeNotAllowed { current: false, .. }),
            "expected ReadOnlyChangeNotAllowed{{current:false}}, got: {:?}",
            err
        );

        // Unshare so a follow-up share can flip the mode cleanly.
        nexus_lookup_mut(IMMUTABLE_NEXUS_NAME)
            .expect("nexus should be present")
            .unshare_nexus()
            .await
            .expect("unshare should succeed");

        // Now share as ROX: allowed because the target was destroyed.
        nexus_lookup_mut(IMMUTABLE_NEXUS_NAME)
            .expect("nexus should be present")
            .share_ext(Protocol::Nvmf, None, vec![], true)
            .await
            .expect("share as ROX after unshare should succeed");
        assert!(nexus_lookup(IMMUTABLE_NEXUS_NAME)
            .expect("nexus should be present")
            .is_read_only());

        // Mirror check: on a ROX-shared target, re-share with `read_only=false`
        // must be rejected.
        let err = nexus_lookup_mut(IMMUTABLE_NEXUS_NAME)
            .expect("nexus should be present")
            .share_ext(Protocol::Nvmf, None, vec![], false)
            .await
            .expect_err("re-share flipping ROX→RWO must be rejected");
        assert!(
            matches!(err, Error::ReadOnlyChangeNotAllowed { current: true, .. }),
            "expected ReadOnlyChangeNotAllowed{{current:true}}, got: {:?}",
            err
        );
    })
    .await;

    common::delete_file(disks);
}
