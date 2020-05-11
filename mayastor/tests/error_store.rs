pub mod common;

use mayastor::bdev::NexusErrStore;

const ALL_FLAGS: u32 = 0xffff_ffff;

#[test]
fn nexus_error_loading() {
    let mut es = NexusErrStore::new(15);

    let mut errors = es.query(ALL_FLAGS, ALL_FLAGS, 0);
    assert_eq!(errors, 0);

    add_records(&mut es, 1, NexusErrStore::IO_TYPE_READ, 5);
    add_records(&mut es, 1, NexusErrStore::IO_TYPE_READ, 10);

    errors = es.query(ALL_FLAGS, ALL_FLAGS, 0);
    assert_eq!(errors, 2);

    add_records(&mut es, 2, NexusErrStore::IO_TYPE_WRITE, 11);

    errors = es.query(ALL_FLAGS, ALL_FLAGS, 0);
    assert_eq!(errors, 4);

    add_records(&mut es, 3, NexusErrStore::IO_TYPE_UNMAP, 12);
    errors = es.query(ALL_FLAGS, ALL_FLAGS, 0);
    assert_eq!(errors, 7);

    add_records(&mut es, 4, NexusErrStore::IO_TYPE_FLUSH, 13);
    errors = es.query(ALL_FLAGS, ALL_FLAGS, 0);
    assert_eq!(errors, 11);

    add_records(&mut es, 5, NexusErrStore::IO_TYPE_RESET, 14);
    errors = es.query(ALL_FLAGS, ALL_FLAGS, 0);
    // last record over-writes the first, hence 15 not 16
    assert_eq!(errors, 15);

    /////////////////// filter by time ////////////////////////////

    errors = es.query(ALL_FLAGS, ALL_FLAGS, 0);
    assert_eq!(errors, 15);

    errors = es.query(ALL_FLAGS, ALL_FLAGS, 10);
    assert_eq!(errors, 15);

    errors = es.query(ALL_FLAGS, ALL_FLAGS, 11);
    assert_eq!(errors, 14);

    errors = es.query(ALL_FLAGS, ALL_FLAGS, 12);
    assert_eq!(errors, 12);

    errors = es.query(ALL_FLAGS, ALL_FLAGS, 13);
    assert_eq!(errors, 9);

    errors = es.query(ALL_FLAGS, ALL_FLAGS, 14);
    assert_eq!(errors, 5);

    errors = es.query(ALL_FLAGS, ALL_FLAGS, 15);
    assert_eq!(errors, 0);

    /////////////////////// filter by op ////////////////////////

    errors = es.query(NexusErrStore::READ_FLAG, ALL_FLAGS, 10);
    assert_eq!(errors, 1);

    errors = es.query(NexusErrStore::WRITE_FLAG, ALL_FLAGS, 10);
    assert_eq!(errors, 2);

    errors = es.query(NexusErrStore::UNMAP_FLAG, ALL_FLAGS, 10);
    assert_eq!(errors, 3);

    errors = es.query(NexusErrStore::FLUSH_FLAG, ALL_FLAGS, 10);
    assert_eq!(errors, 4);

    errors = es.query(NexusErrStore::RESET_FLAG, ALL_FLAGS, 10);
    assert_eq!(errors, 5);

    errors = es.query(0, ALL_FLAGS, 10);
    assert_eq!(errors, 0);

    ////////////////////// filter by failure //////////////////////////

    errors = es.query(ALL_FLAGS, NexusErrStore::IO_FAILED_FLAG, 10);
    assert_eq!(errors, 15);

    errors = es.query(ALL_FLAGS, 0, 10);
    assert_eq!(errors, 0);
}

fn add_records(
    es: &mut NexusErrStore,
    how_many: usize,
    op_flag: u32,
    when: u64,
) {
    let offset: u64 = 0;
    let num_of_blocks: u64 = 1;
    for _ in 0 .. how_many {
        es.add_record(
            op_flag,
            NexusErrStore::IO_FAILED,
            offset,
            num_of_blocks,
            when,
        );
    }
}
