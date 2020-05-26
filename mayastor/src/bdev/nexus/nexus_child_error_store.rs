use spdk_sys::spdk_bdev_io_type;
use std::fmt::{Debug, Display};

use crate::bdev::nexus::nexus_io::{io_status, io_type};
use serde::export::{fmt::Error, Formatter};

#[derive(Default, Copy, Clone)]
pub struct NexusChildErrorRecord {
    io_op: spdk_bdev_io_type,
    io_error: i32,
    io_offset: u64,
    io_num_blocks: u64,
    timestamp_nano: u64,
}

pub struct NexusErrStore {
    no_of_records: usize,
    next_record_index: usize,
    records: Vec<NexusChildErrorRecord>,
}

impl NexusErrStore {
    pub const READ_FLAG: u32 = 1;
    pub const WRITE_FLAG: u32 = 2;
    pub const UNMAP_FLAG: u32 = 4;
    pub const FLUSH_FLAG: u32 = 8;
    pub const RESET_FLAG: u32 = 16;

    pub const IO_FAILED_FLAG: u32 = 1;

    // the following definitions are for the error_store unit test
    pub const IO_TYPE_READ: u32 = io_type::READ;
    pub const IO_TYPE_WRITE: u32 = io_type::WRITE;
    pub const IO_TYPE_UNMAP: u32 = io_type::UNMAP;
    pub const IO_TYPE_FLUSH: u32 = io_type::FLUSH;
    pub const IO_TYPE_RESET: u32 = io_type::RESET;

    pub const IO_FAILED: i32 = io_status::FAILED;

    pub fn new(max_records: usize) -> Self {
        let mut es = NexusErrStore {
            no_of_records: 0,
            next_record_index: 0,
            records: Vec::with_capacity(max_records),
        };

        let er: NexusChildErrorRecord = Default::default();

        for _ in 0 .. max_records {
            es.records.push(er);
        }
        es
    }

    pub fn add_record(
        &mut self,
        io_op: spdk_bdev_io_type,
        io_error: i32,
        io_offset: u64,
        io_num_blocks: u64,
        timestamp_nano: u64,
    ) -> &mut Self {
        self.records[self.next_record_index].io_op = io_op;
        self.records[self.next_record_index].io_error = io_error;
        self.records[self.next_record_index].io_offset = io_offset;
        self.records[self.next_record_index].io_num_blocks = io_num_blocks;
        self.records[self.next_record_index].timestamp_nano = timestamp_nano;

        if self.no_of_records < self.records.len() {
            self.no_of_records += 1;
        };
        self.next_record_index =
            (self.next_record_index + 1) % self.records.len();
        self
    }

    pub fn query(
        &self,
        io_op_flags: u32,
        io_error_flags: u32,
        target_timestamp_nano: u64,
    ) -> u32 {
        let mut idx = self.next_record_index;
        let mut error_count: u32 = 0;

        for _ in 0 .. self.no_of_records {
            if idx > 0 {
                idx -= 1;
            } else {
                idx = self.records.len() - 1;
            }
            if self.records[idx].timestamp_nano < target_timestamp_nano {
                break;
            }
            let found_op = match self.records[idx].io_op {
                io_type::READ => (io_op_flags & NexusErrStore::READ_FLAG) != 0,
                io_type::WRITE => {
                    (io_op_flags & NexusErrStore::WRITE_FLAG) != 0
                }
                io_type::UNMAP => {
                    (io_op_flags & NexusErrStore::UNMAP_FLAG) != 0
                }
                io_type::FLUSH => {
                    (io_op_flags & NexusErrStore::FLUSH_FLAG) != 0
                }
                io_type::RESET => {
                    (io_op_flags & NexusErrStore::RESET_FLAG) != 0
                }
                _ => false,
            };

            let found_err = match self.records[idx].io_error {
                io_status::FAILED => {
                    (io_error_flags & NexusErrStore::IO_FAILED_FLAG) != 0
                }
                _ => false,
            };

            if found_op && found_err {
                error_count += 1;
            }
        }
        error_count
    }

    fn error_fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        let mut idx = self.next_record_index;
        write!(f, "\nErrors ({}):", self.no_of_records)
            .expect("invalid format");
        for n in 0 .. self.no_of_records {
            if idx > 0 {
                idx -= 1;
            } else {
                idx = self.records.len() - 1;
            }
            write!(
                f,
                "\n    {}: timestamp:{} op:{} error:{} offset:{} blocks:{}",
                n,
                self.records[idx].timestamp_nano,
                self.records[idx].io_op,
                self.records[idx].io_error,
                self.records[idx].io_offset,
                self.records[idx].io_num_blocks,
            )
            .expect("invalid format");
        }
        Ok(())
    }
}

impl Debug for NexusErrStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        self.error_fmt(f)
    }
}

impl Display for NexusErrStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        self.error_fmt(f)
    }
}
