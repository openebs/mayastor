use std::{collections::HashMap, ffi::CString, ptr::null_mut};

use once_cell::sync::OnceCell;

use common::compose::MayastorTest;
use io_engine::core::{mempool::MemoryPool, MayastorCliArgs};

pub mod common;

static MAYASTOR: OnceCell<MayastorTest> = OnceCell::new();

fn get_ms() -> &'static MayastorTest<'static> {
    MAYASTOR.get_or_init(|| MayastorTest::new(MayastorCliArgs::default()))
}

struct TestCtx {
    id: u64,
    pos: u32,
    ctx: *const i8,
}

const POOL_SIZE: u64 = 128 * 1024 - 1;
const TEST_BULK_SIZE: usize = 32 * 1024;

#[tokio::test]
async fn test_get() {
    let ms = get_ms();
    let cname = CString::new("test").unwrap();
    let c_c = cname.as_ptr();

    ms.spawn(async move {
        // Create pool.
        let pool = MemoryPool::<TestCtx>::create("test_pool", POOL_SIZE)
            .expect("Failed to create test memory pool");

        let mut used_addrs = HashMap::<usize, *mut TestCtx>::new();

        // Allocate all available items from the pool, make sure all addresses
        // are unique.
        for i in 0 .. POOL_SIZE {
            let id: u64 = i;
            let pos: u32 = 3 * i as u32;

            let o = pool.get(TestCtx {
                id,
                pos,
                ctx: c_c,
            });

            assert!(o.is_some(), "Failed to get element from memory pool");
            let p = o.unwrap();

            let e = unsafe { p.as_ref().unwrap() };
            // Make sure element is properly initialized.
            assert_eq!(e.id, id);
            assert_eq!(e.pos, pos);
            assert_eq!(e.ctx, c_c);

            // Make sure the address is unique and not allocated to a different
            // item.
            let a = p as usize;
            assert!(
                !used_addrs.contains_key(&a),
                "Address already in use by a different pool item"
            );
            used_addrs.insert(a, p);
        }

        // Now pool is full and the following allocation must fail.
        let o = pool.get(TestCtx {
            id: 1,
            pos: 1984,
            ctx: c_c,
        });

        assert!(
            o.is_none(),
            "Successfully allocated element from fully consumed memory pool"
        );

        // Free some arbitrary elements, saving their addresses for further use.
        // null existing allocated addresses to mark them as free for further
        // checks.
        for (_, v) in used_addrs.iter_mut().take(TEST_BULK_SIZE) {
            let addr = *v;

            pool.put(addr);
            *v = null_mut();
        }

        // Now try to allocate elements - we must see the same addresses as the
        // ones we just freed.
        for _ in 0 .. TEST_BULK_SIZE {
            let o = pool.get(TestCtx {
                id: 1,
                pos: 1984,
                ctx: c_c,
            });

            // Make sure element is available.
            assert!(o.is_some(), "Failed to get element from memory pool");
            let p = o.unwrap();
            let a = p as usize;

            // Make sure address is free and not used in any existing
            // allocations.
            assert!(
                used_addrs.contains_key(&a),
                "Allocated address does not belong to pool"
            );
            assert!(used_addrs.get(&a).unwrap().is_null());
            used_addrs.insert(a, p);
        }

        // Make sure all freed addressess were reused and no new elements were
        // added.
        assert_eq!(used_addrs.len(), POOL_SIZE as usize);
        assert!(used_addrs.iter().all(|(_, v)| { !v.is_null() }));

        // Free all elements before dropping the pool.
        // Memory pools panic if being dropped whilst having any live
        // allocations - should not happen now.
        for (_, v) in used_addrs.iter() {
            pool.put(*v);
        }

        drop(pool);
    })
    .await;
}
