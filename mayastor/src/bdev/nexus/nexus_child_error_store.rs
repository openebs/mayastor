use std::{
    fmt::{Debug, Display},
    time::{Duration, Instant},
};

use serde::export::{fmt::Error, Formatter};

use spdk_sys::spdk_bdev;

use crate::{
    bdev::{
        nexus::{
            nexus_bdev,
            nexus_bdev::{
                nexus_lookup,
                Error::{ChildMissing, ChildMissingErrStore},
                Nexus,
            },
            nexus_child::{ChildState, NexusChild},
            nexus_io::{IoStatus, IoType},
        },
        Reason,
    },
    core::{Cores, Reactors},
    subsys::Config,
};

#[derive(Copy, Clone)]
pub struct NexusChildErrorRecord {
    io_offset: u64,
    io_num_blocks: u64,
    timestamp: Instant,
    io_error: IoStatus,
    io_op: IoType,
}

impl Default for NexusChildErrorRecord {
    fn default() -> Self {
        Self {
            io_op: IoType::Invalid,
            io_error: IoStatus::Failed,
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

// this controls what kind of search to perform in the error store
pub enum QueryType {
    Total, /* determine the total number of errors stored
            * other types, e.g. MostAttempts may be added in future
            * revisions */
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ActionType {
    Ignore,
    Fault,
}

impl NexusErrStore {
    pub const READ_FLAG: u32 = 1 << (IoType::Read as u32 - 1);
    pub const WRITE_FLAG: u32 = 1 << (IoType::Write as u32 - 1);
    pub const UNMAP_FLAG: u32 = 1 << (IoType::Unmap as u32 - 1);
    pub const FLUSH_FLAG: u32 = 1 << (IoType::Flush as u32 - 1);
    pub const RESET_FLAG: u32 = 1 << (IoType::Reset as u32 - 1);

    pub const IO_FAILED_FLAG: u32 = 1;

    // the following definitions are for the error_store unit test
    pub const IO_TYPE_READ: u32 = IoType::Read as u32;
    pub const IO_TYPE_WRITE: u32 = IoType::Write as u32;
    pub const IO_TYPE_UNMAP: u32 = IoType::Unmap as u32;
    pub const IO_TYPE_FLUSH: u32 = IoType::Flush as u32;
    pub const IO_TYPE_RESET: u32 = IoType::Reset as u32;

    pub const IO_FAILED: i32 = IoStatus::Failed as i32;

    pub fn new(max_records: usize) -> Self {
        Self {
            no_of_records: 0,
            next_record_index: 0,
            records: vec![NexusChildErrorRecord::default(); max_records],
        }
    }

    pub fn add_record(
        &mut self,
        io_op: IoType,
        io_error: IoStatus,
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
    }

    #[inline]
    fn has_error(
        record: &NexusChildErrorRecord,
        io_op_flags: u32,
        io_error_flags: u32,
        target_timestamp: Option<Instant>,
    ) -> bool {
        match record.io_op {
            IoType::Read if (io_op_flags & NexusErrStore::READ_FLAG) != 0 => {}
            IoType::Write if (io_op_flags & NexusErrStore::WRITE_FLAG) != 0 => {
            }
            IoType::Unmap if (io_op_flags & NexusErrStore::UNMAP_FLAG) != 0 => {
            }
            IoType::Flush if (io_op_flags & NexusErrStore::FLUSH_FLAG) != 0 => {
            }
            IoType::Reset if (io_op_flags & NexusErrStore::RESET_FLAG) != 0 => {
            }
            _ => return false,
        };

        match record.io_error {
            IoStatus::Failed
                if (io_error_flags & NexusErrStore::IO_FAILED_FLAG) != 0 => {}
            _ => return false,
        };

        match target_timestamp {
            Some(ts) => record.timestamp.checked_duration_since(ts).is_some(),
            None => true,
        }
    }

    #[inline]
    fn count_errors(
        &self,
        io_op_flags: u32,
        io_error_flags: u32,
        target_timestamp: Option<Instant>,
    ) -> u32 {
        let mut idx = self.next_record_index;
        let mut err_count: u32 = 0;

        for _ in 0 .. self.no_of_records {
            if idx > 0 {
                idx -= 1;
            } else {
                idx = self.records.len() - 1;
            }
            if Self::has_error(
                &self.records[idx],
                io_op_flags,
                io_error_flags,
                target_timestamp,
            ) {
                err_count += 1;
            }
        }
        err_count
    }

    #[inline]
    pub fn query(
        &self,
        io_op_flags: u32,
        io_error_flags: u32,
        target_timestamp: Option<Instant>,
        query_type: QueryType,
    ) -> u32 {
        match query_type {
            QueryType::Total => {
                self.count_errors(io_op_flags, io_error_flags, target_timestamp)
            }
        }
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
                "\n    {}: timestamp:{:?} op:{:?} error:{:?} offset:{} blocks:{}",
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
        io_op_type: IoType,
        io_error_type: IoStatus,
        io_offset: u64,
        io_num_blocks: u64,
    ) {
        let now = Instant::now();
        let cfg = Config::get();
        if cfg.err_store_opts.enable_err_store
            && (io_op_type == IoType::Read || io_op_type == IoType::Write)
        {
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
                )
                .await;
            });
        }
    }

    async fn future_error_record_add(
        name: String,
        bdev: *const spdk_bdev,
        io_op_type: IoType,
        io_error_type: IoStatus,
        io_offset: u64,
        io_num_blocks: u64,
        now: Instant,
    ) {
        let nexus = match nexus_lookup(&name) {
            Some(nexus) => nexus,
            None => {
                error!("Failed to find the nexus >{}<", name);
                return;
            }
        };
        trace!("Adding error record {:?} bdev {:?}", io_op_type, bdev);
        for child in nexus.children.iter_mut() {
            if let Some(bdev) = child.bdev.as_ref() {
                if bdev.as_ptr() as *const _ == bdev {
                    if child.state() == ChildState::Open {
                        if child.err_store.is_some() {
                            child.err_store.as_mut().unwrap().add_record(
                                io_op_type,
                                io_error_type,
                                io_offset,
                                io_num_blocks,
                                now,
                            );
                            let cfg = Config::get();
                            if cfg.err_store_opts.action == ActionType::Fault
                                && !Self::assess_child(
                                    &child,
                                    cfg.err_store_opts.max_errors,
                                    cfg.err_store_opts.retention_ns,
                                    QueryType::Total,
                                )
                            {
                                let child_name = child.name.clone();
                                info!("Faulting child {}", child_name);
                                if nexus
                                    .fault_child(&child_name, Reason::IoError)
                                    .await
                                    .is_err()
                                {
                                    error!(
                                        "Failed to fault the child {}",
                                        child_name,
                                    );
                                }
                            }
                        } else {
                            let child_name = child.name.clone();
                            error!(
                                "Failed to record error - no error store in child {}",
                                child_name,
                            );
                        }
                        return;
                    }
                    let child_name = child.name.clone();
                    trace!("Ignoring error response sent to non-open child {}, state {:?}", child_name, child.state());
                    return;
                }
            }
            //error!("Failed to record error - could not find child");
        }
    }

    pub fn error_record_query(
        &self,
        child_name: &str,
        io_op_flags: u32,
        io_error_flags: u32,
        age_nano: Option<u64>, // None for any age
        query_type: QueryType,
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
                        query_type,
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

    // Returns false if the child is deemed faulted. This is determined by
    // by the number, type and time stamp of the errors stored in the error
    // store, filtered by the passed-in flags and compared against a
    // configurable limit.
    fn assess_child(
        child: &NexusChild,
        max_errors: u32,
        retention_ns: u64,
        query_type: QueryType,
    ) -> bool {
        let earliest_time =
            Instant::now().checked_sub(Duration::from_nanos(retention_ns));
        let res = child.err_store.as_ref().unwrap().query(
            NexusErrStore::READ_FLAG | NexusErrStore::WRITE_FLAG,
            NexusErrStore::IO_FAILED_FLAG,
            earliest_time, // can be None
            query_type,
        );
        res <= max_errors
    }
}
