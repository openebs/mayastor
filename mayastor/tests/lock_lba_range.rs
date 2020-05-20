#[macro_use]
extern crate log;

pub mod common;

use crossbeam::channel::unbounded;
use mayastor::{
    bdev::{nexus_create, nexus_lookup},
    core::{
        Bdev,
        DmaBuf,
        MayastorCliArgs,
        MayastorEnvironment,
        RangeContext,
        Reactor,
        Reactors,
    },
};
use std::sync::{Arc, Mutex};

const NEXUS_NAME: &str = "lba_range_nexus";
const NEXUS_SIZE: u64 = 10 * 1024 * 1024;
const NUM_NEXUS_CHILDREN: u64 = 2;

fn test_ini() {
    test_init!();
    for i in 0 .. NUM_NEXUS_CHILDREN {
        common::delete_file(&[get_disk(i)]);
        common::truncate_file_bytes(&get_disk(i), NEXUS_SIZE);
    }

    Reactor::block_on(async {
        create_nexus().await;
    });
}

fn test_fini() {
    for i in 0 .. NUM_NEXUS_CHILDREN {
        common::delete_file(&[get_disk(i)]);
    }

    Reactor::block_on(async {
        let nexus = nexus_lookup(NEXUS_NAME).unwrap();
        nexus.destroy().await.unwrap();
    });
}

fn get_disk(number: u64) -> String {
    format!("/tmp/disk{}.img", number)
}

fn get_dev(number: u64) -> String {
    format!("aio://{}?blk_size=512", get_disk(number))
}

async fn create_nexus() {
    let mut ch = Vec::new();
    for i in 0 .. NUM_NEXUS_CHILDREN {
        ch.push(get_dev(i));
    }

    nexus_create(NEXUS_NAME, NEXUS_SIZE, None, &ch)
        .await
        .unwrap();
}

fn get_shareable_ctx(offset: u64, len: u64) -> Arc<Mutex<RangeContext>> {
    let nexus = Bdev::open_by_name(NEXUS_NAME, true).unwrap();
    Arc::new(Mutex::new(RangeContext::new(
        offset,
        len,
        Arc::new(nexus.get_channel().unwrap()),
    )))
}

async fn lock_range(ctx: &mut RangeContext) {
    let mut nexus = Bdev::open_by_name(NEXUS_NAME, true).unwrap();
    let _ = nexus.lock_lba_range(ctx).await;
}

async fn unlock_range(ctx: &mut RangeContext) {
    let mut nexus = Bdev::open_by_name(NEXUS_NAME, true).unwrap();
    let _ = nexus.unlock_lba_range(ctx).await;
}

#[test]
// Test acquiring and releasing a lock.
fn lock_unlock() {
    test_ini();
    Reactor::block_on(async {
        let mut nexus = Bdev::open_by_name(NEXUS_NAME, true).unwrap();
        let mut ctx =
            RangeContext::new(1, 5, Arc::new(nexus.get_channel().unwrap()));
        let _ = nexus.lock_lba_range(&mut ctx).await;
        let _ = nexus.unlock_lba_range(&mut ctx).await;
    });
    test_fini();
}

#[test]
// Test that an error is received when the lock/unlock contexts don't match
fn lock_unlock_different_context() {
    test_ini();
    Reactor::block_on(async {
        let mut nexus = Bdev::open_by_name(NEXUS_NAME, true).unwrap();
        let mut ctx =
            RangeContext::new(1, 5, Arc::new(nexus.get_channel().unwrap()));
        let mut ctx1 =
            RangeContext::new(1, 5, Arc::new(nexus.get_channel().unwrap()));
        let _ = nexus.lock_lba_range(&mut ctx).await;
        if nexus.unlock_lba_range(&mut ctx1).await.is_ok() {
            panic!("Shouldn't be able to unlock with a different context");
        }
    });
    test_fini();
}

#[test]
// Test taking out multiple locks on an overlapping block range.
// The second lock should only succeeded after the first lock is released.
fn multiple_locks() {
    test_ini();
    {
        let reactor = Reactors::current();

        // First lock
        let (s, r) = unbounded::<()>();
        let ctx = get_shareable_ctx(1, 10);
        let ctx_clone = Arc::clone(&ctx);
        reactor.send_future(async move {
            lock_range(&mut ctx_clone.lock().unwrap()).await;
            let _ = s.send(());
        });
        reactor_poll!(r);

        // Second lock
        let (lock_sender, lock_receiver) = unbounded::<()>();
        let ctx1 = get_shareable_ctx(1, 5);
        let ctx1_clone = Arc::clone(&ctx1);
        reactor.send_future(async move {
            lock_range(&mut ctx1_clone.lock().unwrap()).await;
            trace!("Second lock succeeded");
            let _ = lock_sender.send(());
        });
        reactor_poll!(100);

        // First lock is held, second lock shouldn't succeed
        assert!(lock_receiver.try_recv().is_err());

        // First unlock
        let (s, r) = unbounded::<()>();
        let ctx_clone = Arc::clone(&ctx);
        reactor.send_future(async move {
            unlock_range(&mut ctx_clone.lock().unwrap()).await;
            trace!("Unlock first lock");
            let _ = s.send(());
        });
        reactor_poll!(r);

        // First lock released, second lock should succeed
        assert!(lock_receiver.try_recv().is_ok());

        // Second unlock
        let (s, r) = unbounded::<()>();
        let ctx1_clone = Arc::clone(&ctx1);
        reactor.send_future(async move {
            unlock_range(&mut ctx1_clone.lock().unwrap()).await;
            trace!("Unlock second lock");
            let _ = s.send(());
        });
        reactor_poll!(r);
    }
    test_fini();
}

#[test]
// Test locking a block range and then issuing a front-end I/O to an overlapping
// range.
// TODO: Add additional test for issuing front-end I/O then taking a lock
fn lock_then_fe_io() {
    test_ini();
    {
        let reactor = Reactors::current();

        // Lock range
        let (s, r) = unbounded::<()>();
        let ctx = get_shareable_ctx(1, 10);
        let ctx_clone = Arc::clone(&ctx);
        reactor.send_future(async move {
            lock_range(&mut ctx_clone.lock().unwrap()).await;
            trace!("Lock succeeded");
            let _ = s.send(());
        });
        reactor_poll!(r);

        // Issue front-end I/O
        let (io_sender, io_receiver) = unbounded::<()>();
        reactor.send_future(async move {
            let nexus_desc = Bdev::open_by_name(&NEXUS_NAME, true).unwrap();
            let h = nexus_desc.into_handle().unwrap();

            let blk = 2;
            let blk_size = 512;
            let buf = DmaBuf::new(blk * blk_size, 9).unwrap();

            match h.write_at((blk * blk_size) as u64, &buf).await {
                Ok(_) => trace!("Successfully wrote to nexus"),
                Err(e) => trace!("Failed to write to nexus: {}", e),
            }

            let _ = io_sender.send(());
        });
        reactor_poll!(1000);

        // Lock is held, I/O should not succeed
        assert!(io_receiver.try_recv().is_err());

        // Unlock
        let (s, r) = unbounded::<()>();
        let ctx_clone = Arc::clone(&ctx);
        reactor.send_future(async move {
            unlock_range(&mut ctx_clone.lock().unwrap()).await;
            trace!("Release lock");
            let _ = s.send(());
        });
        reactor_poll!(r);

        // Lock released, I/O should succeed
        assert!(io_receiver.try_recv().is_ok());
    }
    test_fini();
}
