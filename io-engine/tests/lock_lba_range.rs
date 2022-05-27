#![allow(clippy::await_holding_refcell_ref)]
#[macro_use]
extern crate tracing;
use std::{
    cell::{Ref, RefCell, RefMut},
    ops::{Deref, DerefMut},
    rc::Rc,
};

use crossbeam::channel::unbounded;

use io_engine::{
    bdev::nexus::{nexus_create, nexus_lookup_mut},
    core::{
        IoChannel,
        MayastorCliArgs,
        MayastorEnvironment,
        RangeContext,
        Reactor,
        Reactors,
        UntypedBdev,
    },
};
use spdk_rs::DmaBuf;
pub mod common;

const NEXUS_NAME: &str = "lba_range_nexus";
const NEXUS_SIZE: u64 = 10 * 1024 * 1024;
const NUM_NEXUS_CHILDREN: u64 = 2;

/// Data structure that can be shared between futures.
///
/// The individual fields are wrapped in a Rc and RefCell such that they can be
/// shared between futures.
///
/// (Note: Wrapping the entire structure in a Rc and RefCell does not allow the
/// individual fields to be shared).
#[derive(Clone)]
struct ShareableContext {
    ctx: Rc<RefCell<RangeContext>>,
    ch: Rc<RefCell<IoChannel>>,
}

impl ShareableContext {
    /// Create a new Shareable Context
    pub fn new(offset: u64, len: u64) -> ShareableContext {
        let nexus = UntypedBdev::open_by_name(NEXUS_NAME, true).unwrap();
        Self {
            ctx: Rc::new(RefCell::new(RangeContext::new(offset, len))),
            ch: Rc::new(RefCell::new(nexus.get_channel().unwrap())),
        }
    }

    /// Mutably borrow the RangeContext
    pub fn borrow_mut_ctx(&self) -> RefMut<RangeContext> {
        self.ctx.borrow_mut()
    }

    /// Immutably borrow the IoChannel
    pub fn borrow_ch(&self) -> Ref<IoChannel> {
        self.ch.borrow()
    }
}

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

async fn lock_range(
    ctx: &mut RangeContext,
    ch: &IoChannel,
) -> Result<(), nix::errno::Errno> {
    let nexus = UntypedBdev::open_by_name(NEXUS_NAME, true).unwrap();
    nexus.lock_lba_range(ctx, ch).await
}

async fn unlock_range(
    ctx: &mut RangeContext,
    ch: &IoChannel,
) -> Result<(), nix::errno::Errno> {
    let nexus = UntypedBdev::open_by_name(NEXUS_NAME, true).unwrap();
    nexus.unlock_lba_range(ctx, ch).await
}

#[test]
// Test acquiring and releasing a lock.
fn lock_unlock() {
    test_ini();
    Reactor::block_on(async {
        let nexus = UntypedBdev::open_by_name(NEXUS_NAME, true).unwrap();
        let mut ctx = RangeContext::new(1, 5);
        let ch = nexus.get_channel().unwrap();
        nexus
            .lock_lba_range(&mut ctx, &ch)
            .await
            .expect("Failed to acquire lock");
        nexus
            .unlock_lba_range(&mut ctx, &ch)
            .await
            .expect("Failed to release lock");
    });
    test_fini();
}

#[test]
// Test that an error is received when the lock/unlock contexts don't match
fn lock_unlock_different_context() {
    test_ini();
    Reactor::block_on(async {
        let nexus = UntypedBdev::open_by_name(NEXUS_NAME, true).unwrap();

        let mut ctx = RangeContext::new(1, 5);
        let ch = nexus.get_channel().unwrap();
        nexus
            .lock_lba_range(&mut ctx, &ch)
            .await
            .expect("Failed to acquire lock");

        let mut ctx1 = RangeContext::new(1, 5);
        let ch1 = nexus.get_channel().unwrap();
        nexus
            .unlock_lba_range(&mut ctx1, &ch1)
            .await
            .expect_err("Shouldn't be able to unlock with a different context");
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
    let (s, r) = unbounded::<()>();
    let ctx1 = ShareableContext::new(1, 10);
    let ctx_clone1 = ctx1.clone();
    reactor.send_future(async move {
        lock_range(
            ctx_clone1.borrow_mut_ctx().deref_mut(),
            ctx_clone1.borrow_ch().deref(),
        )
        .await
        .unwrap();
        s.send(()).unwrap();
    });
    reactor_poll!(r);

    // Second Lock
    let (lock_sender, lock_receiver) = unbounded::<()>();
    let ctx2 = ShareableContext::new(1, 5);
    let ctx_clone2 = ctx2.clone();
    reactor.send_future(async move {
        lock_range(
            ctx_clone2.borrow_mut_ctx().deref_mut(),
            ctx_clone2.borrow_ch().deref(),
        )
        .await
        .unwrap();
        lock_sender.send(()).unwrap();
    });
    reactor_poll!(100);

    // First lock is held, second lock shouldn't succeed
    assert!(lock_receiver.try_recv().is_err());

    // First unlock
    let (s, r) = unbounded::<()>();
    reactor.send_future(async move {
        unlock_range(
            ctx1.borrow_mut_ctx().deref_mut(),
            ctx1.borrow_ch().deref(),
        )
        .await
        .unwrap();
        s.send(()).unwrap();
    });
    reactor_poll!(r);

    // Poll reactor to allow the second lock to be obtained
    reactor_poll!(100);

    // First lock released, second lock should succeed.
    assert!(lock_receiver.try_recv().is_ok());

    // Second unlock
    let (s, r) = unbounded::<()>();
    reactor.send_future(async move {
        unlock_range(
            ctx2.borrow_mut_ctx().deref_mut(),
            ctx2.borrow_ch().deref(),
        )
        .await
        .unwrap();
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
    let (s, r) = unbounded::<()>();
    let ctx = ShareableContext::new(1, 10);
    let ctx_clone = ctx.clone();
    reactor.send_future(async move {
        lock_range(
            ctx_clone.borrow_mut_ctx().deref_mut(),
            ctx_clone.borrow_ch().deref(),
        )
        .await
        .unwrap();
        s.send(()).unwrap();
    });
    reactor_poll!(r);

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
    let (s, r) = unbounded::<()>();
    reactor.send_future(async move {
        unlock_range(ctx.borrow_mut_ctx().deref_mut(), ctx.borrow_ch().deref())
            .await
            .unwrap();
        s.send(()).unwrap();
    });
    reactor_poll!(r);

    // Lock released, I/O should succeed
    assert!(io_receiver.try_recv().is_ok());

    test_fini();
}
