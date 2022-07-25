#![allow(clippy::await_holding_refcell_ref)]
#[macro_use]
extern crate tracing;

use crossbeam::channel::{unbounded, Receiver};

use io_engine::{
    bdev::nexus::{nexus_create, nexus_lookup_mut},
    core::{
        MayastorCliArgs,
        MayastorEnvironment,
        Reactor,
        Reactors,
        UntypedBdev,
    },
};
use spdk_rs::{BdevDescError, DmaBuf, LbaRange, LbaRangeLock};
pub mod common;

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
        let nexus = nexus_lookup_mut(NEXUS_NAME).unwrap();
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

async fn lock_range(ctx: LbaRange) -> Result<LbaRangeLock<()>, BdevDescError> {
    let nexus = UntypedBdev::open_by_name(NEXUS_NAME, true).unwrap();
    nexus.lock_lba_range(ctx).await
}

async fn unlock_range(lock: LbaRangeLock<()>) -> Result<(), BdevDescError> {
    let nexus = UntypedBdev::open_by_name(NEXUS_NAME, true).unwrap();
    nexus.unlock_lba_range(lock).await
}

fn recv_from<T>(r: Receiver<T>) -> T {
    loop {
        io_engine::core::Reactors::current().poll_once();
        if let Ok(res) = r.try_recv() {
            return res;
        }
    }
}

#[test]
// Test acquiring and releasing a lock.
fn lock_unlock() {
    test_ini();
    Reactor::block_on(async {
        let nexus = UntypedBdev::open_by_name(NEXUS_NAME, true).unwrap();
        let range = LbaRange::new(1, 5);
        let lock = nexus
            .lock_lba_range(range)
            .await
            .expect("Failed to acquire lock");
        nexus
            .unlock_lba_range(lock)
            .await
            .expect("Failed to release lock");
    });
    test_fini();
}

#[test]
// Test taking out multiple locks on an overlapping block range.
// The second lock should only succeeded after the first lock is released.
fn multiple_locks() {
    test_ini();

    let reactor = Reactors::current();

    // First Lock
    let (s, r) = unbounded();
    reactor.send_future(async move {
        let lock1 = lock_range(LbaRange::new(1, 10)).await.unwrap();
        s.send(lock1).unwrap();
    });
    let lock1 = recv_from(r);

    // Second Lock
    let (lock_sender, lock_receiver) = unbounded();
    reactor.send_future(async move {
        let lock2 = lock_range(LbaRange::new(1, 5)).await.unwrap();
        lock_sender.send(lock2).unwrap();
    });
    reactor_poll!(100);

    // First lock is held, second lock shouldn't succeed
    assert!(lock_receiver.try_recv().is_err());

    // First unlock
    let (s, r) = unbounded();
    reactor.send_future(async move {
        unlock_range(lock1).await.unwrap();
        s.send(()).unwrap();
    });
    reactor_poll!(r);

    // Poll reactor to allow the second lock to be obtained
    let lock2 = recv_from(lock_receiver);

    // // First lock released, second lock should succeed.
    // assert!(lock_receiver.try_recv().is_ok());

    // Second unlock
    let (s, r) = unbounded::<()>();
    reactor.send_future(async move {
        unlock_range(lock2).await.unwrap();
        s.send(()).unwrap();
    });
    reactor_poll!(r);

    test_fini();
}

#[test]
// Test locking a block range and then issuing a front-end I/O to an overlapping
// range.
// TODO: Add additional test for issuing front-end I/O then taking a lock
fn lock_then_fe_io() {
    test_ini();

    let reactor = Reactors::current();

    // Lock range
    let (s, r) = unbounded();
    reactor.send_future(async move {
        let lock = lock_range(LbaRange::new(1, 10)).await.unwrap();
        s.send(lock).unwrap();
    });
    let lock = recv_from(r);

    // Issue front-end I/O
    let (io_sender, io_receiver) = unbounded::<()>();
    reactor.send_future(async move {
        let nexus_desc = UntypedBdev::open_by_name(NEXUS_NAME, true).unwrap();
        let h = nexus_desc.into_handle().unwrap();

        let blk = 2;
        let blk_size = 512;
        let buf = DmaBuf::new(blk * blk_size, 9).unwrap();

        match h.write_at((blk * blk_size) as u64, &buf).await {
            Ok(_) => trace!("Successfully wrote to nexus"),
            Err(e) => trace!("Failed to write to nexus: {}", e),
        }

        io_sender.send(()).unwrap();
    });
    reactor_poll!(1000);

    // Lock is held, I/O should not succeed
    assert!(io_receiver.try_recv().is_err());

    // Unlock
    let (s, r) = unbounded();
    reactor.send_future(async move {
        unlock_range(lock).await.unwrap();
        s.send(()).unwrap();
    });
    reactor_poll!(r);

    // Lock released, I/O should succeed
    assert!(io_receiver.try_recv().is_ok());

    test_fini();
}
