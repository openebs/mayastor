use std::{
    fmt::{Debug, Display},
    time::{Duration, Instant},
};

use serde::export::{fmt::Error, Formatter};

use spdk_sys::{spdk_bdev, spdk_bdev_io_type};

use crate::{
    bdev::nexus::{
        nexus_bdev,
        nexus_bdev::{
            nexus_lookup,
            Error::{ChildMissing, ChildMissingErrStore},
            Nexus,
        },
        nexus_io::{io_status, io_type},
    },
    core::{Cores, Reactors},
    subsys::Config,
};

#[derive(Copy, Clone)]
pub struct NexusChildErrorRecord {
    io_offset: u64,
    io_num_blocks: u64,
    timestamp: Instant,
    io_error: i32,
    io_op: spdk_bdev_io_type,
}

impl Default for NexusChildErrorRecord {
    fn default() -> Self {
        Self {
            io_op: 0,
            io_error: 0,
            io_offset: 0,
            io_num_blocks: 0,
            timestamp: Instant::now(), // for lack of another suitable default
        }
    }
}

pub struct NexusErrStore {
    no_of_records: usize,
    next_record_index: usize,
    records: Vec<NexusChildErrorRecord>,
}

impl NexusErrStore {
    pub const READ_FLAG: u32 = 1 << (io_type::READ - 1);
    pub const WRITE_FLAG: u32 = 1 << (io_type::WRITE - 1);
    pub const UNMAP_FLAG: u32 = 1 << (io_type::UNMAP - 1);
    pub const FLUSH_FLAG: u32 = 1 << (io_type::FLUSH - 1);
    pub const RESET_FLAG: u32 = 1 << (io_type::RESET - 1);

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
        timestamp: Instant,
    ) {
        let new_record = NexusChildErrorRecord {
            io_op,
            io_error,
            io_offset,
            io_num_blocks,
            timestamp,
        };

        self.records[self.next_record_index] = new_record;

        if self.no_of_records < self.records.len() {
            self.no_of_records += 1;
        };
        self.next_record_index =
             // consider using a mask if we restrict the size to a power-of-2
            (self.next_record_index + 1) % self.records.len();
        // debug!("added record - buffer is {}", &self);
    }

    pub fn query(
        &self,
        io_op_flags: u32,
        io_error_flags: u32,
        target_timestamp: Option<Instant>,
    ) -> u32 {
        let mut idx = self.next_record_index;
        let mut error_count: u32 = 0;

        for _ in 0 .. self.no_of_records {
            if idx > 0 {
                idx -= 1;
            } else {
                idx = self.records.len() - 1;
            }
            if target_timestamp.is_some()
                && self.records[idx]
                    .timestamp
                    .checked_duration_since(target_timestamp.unwrap())
                    .is_none()
            {
                break; // reached a record older than the wanted timespan
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
                "\n    {}: timestamp:{:?} op:{} error:{} offset:{} blocks:{}",
                n,
                self.records[idx].timestamp,
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

impl Nexus {
    pub fn error_record_add(
        &self,
        bdev: *const spdk_bdev,
        io_op_type: spdk_bdev_io_type,
        io_error_type: i32,
        io_offset: u64,
        io_num_blocks: u64,
    ) {
        let now = Instant::now();
        let cfg = Config::get();
        if cfg.err_store_opts.enable_err_store {
            let nexus_name = self.name.clone();
            // dispatch message to management core to do this
            let mgmt_reactor = Reactors::get_by_core(Cores::first()).unwrap();
            mgmt_reactor.send_future(async move {
                Nexus::future_error_record_add(
                    nexus_name,
                    bdev,
                    io_op_type,
                    io_error_type,
                    io_offset,
                    io_num_blocks,
                    now,
                );
            });
        }
    }

    fn future_error_record_add(
        name: String,
        bdev: *const spdk_bdev,
        io_op_type: spdk_bdev_io_type,
        io_error_type: i32,
        io_offset: u64,
        io_num_blocks: u64,
        now: Instant,
    ) {
        let nexus = match nexus_lookup(&name) {
            Some(nexus) => nexus,
            None => {
                error!("Failed to find the nexus {}", name);
                return;
            }
        };
        for child in nexus.children.iter_mut() {
            if child.bdev.as_ref().unwrap().as_ptr() as *const _ == bdev {
                if child.err_store.is_some() {
                    child.err_store.as_mut().unwrap().add_record(
                        io_op_type,
                        io_error_type,
                        io_offset,
                        io_num_blocks,
                        now,
                    );
                } else {
                    error!("Failed to record error - child has no error store");
                }
                return;
            }
        }
        error!("Failed to record error - could not find child");
    }

    pub fn error_record_query(
        &self,
        child_name: &str,
        io_op_flags: u32,
        io_error_flags: u32,
        age_nano: Option<u64>, // None for any age
    ) -> Result<Option<u32>, nexus_bdev::Error> {
        let earliest_time = match age_nano {
            // can also be None if earlier than the node has been up
            Some(a) => Instant::now().checked_sub(Duration::from_nanos(a)),
            None => None,
        };
        let cfg = Config::get();
        if cfg.err_store_opts.enable_err_store {
            if let Some(child) =
                self.children.iter().find(|c| c.name == child_name)
            {
                if child.err_store.as_ref().is_some() {
                    Ok(Some(child.err_store.as_ref().unwrap().query(
                        io_op_flags,
                        io_error_flags,
                        earliest_time,
                    )))
                } else {
                    Err(ChildMissingErrStore {
                        child: child_name.to_string(),
                        name: self.name.clone(),
                    })
                }
            } else {
                Err(ChildMissing {
                    child: child_name.to_string(),
                    name: self.name.clone(),
                })
            }
        } else {
            Ok(None)
        }
    }
}
