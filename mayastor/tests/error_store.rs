use std::time::{Duration, Instant};

use mayastor::bdev::NexusErrStore;

pub mod common;

const ALL_FLAGS: u32 = 0xffff_ffff;

#[test]
fn nexus_child_error_store_test() {
    let mut es = NexusErrStore::new(15);
    let start_inst = Instant::now();

    let mut errors = do_query(&es, ALL_FLAGS, ALL_FLAGS, start_inst, 0);
    assert_eq!(errors, 0);

    add_records(&mut es, 1, NexusErrStore::IO_TYPE_READ, start_inst, 5);
    add_records(&mut es, 1, NexusErrStore::IO_TYPE_READ, start_inst, 10);

    errors = do_query(&es, ALL_FLAGS, ALL_FLAGS, start_inst, 0);
    assert_eq!(errors, 2);

    add_records(&mut es, 2, NexusErrStore::IO_TYPE_WRITE, start_inst, 11);

    errors = do_query(&es, ALL_FLAGS, ALL_FLAGS, start_inst, 0);
    assert_eq!(errors, 4);

    add_records(&mut es, 3, NexusErrStore::IO_TYPE_UNMAP, start_inst, 12);
    errors = do_query(&es, ALL_FLAGS, ALL_FLAGS, start_inst, 0);
    assert_eq!(errors, 7);

    add_records(&mut es, 4, NexusErrStore::IO_TYPE_FLUSH, start_inst, 13);
    errors = do_query(&es, ALL_FLAGS, ALL_FLAGS, start_inst, 0);
    assert_eq!(errors, 11);

    add_records(&mut es, 5, NexusErrStore::IO_TYPE_RESET, start_inst, 14);
    errors = do_query(&es, ALL_FLAGS, ALL_FLAGS, start_inst, 0);
    // last record over-writes the first, hence 15 not 16
    assert_eq!(errors, 15);

    /////////////////// filter by time ////////////////////////////

    errors = do_query(&es, ALL_FLAGS, ALL_FLAGS, start_inst, 0);
    assert_eq!(errors, 15);

    errors = do_query(&es, ALL_FLAGS, ALL_FLAGS, start_inst, 10);
    assert_eq!(errors, 15);

    errors = do_query(&es, ALL_FLAGS, ALL_FLAGS, start_inst, 11);
    assert_eq!(errors, 14);

    errors = do_query(&es, ALL_FLAGS, ALL_FLAGS, start_inst, 12);
    assert_eq!(errors, 12);

    errors = do_query(&es, ALL_FLAGS, ALL_FLAGS, start_inst, 13);
    assert_eq!(errors, 9);

    errors = do_query(&es, ALL_FLAGS, ALL_FLAGS, start_inst, 14);
    assert_eq!(errors, 5);

    errors = do_query(&es, ALL_FLAGS, ALL_FLAGS, start_inst, 15);
    assert_eq!(errors, 0);

    errors = es.query(ALL_FLAGS, ALL_FLAGS, None); // no time specified
    assert_eq!(errors, 15);

    /////////////////////// filter by op ////////////////////////

    errors = do_query(&es, NexusErrStore::READ_FLAG, ALL_FLAGS, start_inst, 10);
    assert_eq!(errors, 1);

    errors =
        do_query(&es, NexusErrStore::WRITE_FLAG, ALL_FLAGS, start_inst, 10);
    assert_eq!(errors, 2);

    errors =
        do_query(&es, NexusErrStore::UNMAP_FLAG, ALL_FLAGS, start_inst, 10);
    assert_eq!(errors, 3);

    errors =
        do_query(&es, NexusErrStore::FLUSH_FLAG, ALL_FLAGS, start_inst, 10);
    assert_eq!(errors, 4);

    errors =
        do_query(&es, NexusErrStore::RESET_FLAG, ALL_FLAGS, start_inst, 10);
    assert_eq!(errors, 5);

    errors = do_query(&es, 0, ALL_FLAGS, start_inst, 10);
    assert_eq!(errors, 0);

    ////////////////////// filter by failure //////////////////////////

    errors = do_query(
        &es,
        ALL_FLAGS,
        NexusErrStore::IO_FAILED_FLAG,
        start_inst,
        10,
    );
    assert_eq!(errors, 15);

    errors = do_query(&es, ALL_FLAGS, 0, start_inst, 10);
    assert_eq!(errors, 0);
}

fn add_records(
    es: &mut NexusErrStore,
    how_many: usize,
    op: u32,
    start_inst: Instant,
    when: u64,
) {
    let offset: u64 = 0;
    let num_of_blocks: u64 = 1;
    for _ in 0 .. how_many {
        es.add_record(
            op,
            NexusErrStore::IO_FAILED,
            offset,
            num_of_blocks,
            start_inst + Duration::from_nanos(when),
        );
    }
}

fn do_query(
    es: &NexusErrStore,
    op_flags: u32,
    err_flags: u32,
    start_inst: Instant,
    when: u64,
) -> u32 {
    es.query(
        op_flags,
        err_flags,
        Some(start_inst + Duration::from_nanos(when)),
    )
}
