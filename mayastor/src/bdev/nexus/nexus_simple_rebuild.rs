//!
//! This file implements operations for the rebuild process
//!
//! `simple_rebuild` does a very simple sequential rebuild
//! 

use crate::{
    bdev::nexus::{
        nexus_bdev::{
            Nexus,
            NexusState,
        },
        nexus_child::{ChildState},
        nexus_channel::DREvent,
    },
};

use std::cmp;

impl Nexus {
    // rebuilds only the first bad child it finds
    pub(crate) async fn simple_rebuild(&mut self) -> NexusState {
        // if failed go back to the previous state
        let state = self.state;

        self.set_state(NexusState::Remuling);

        let good_child = match self.children.iter().find(|c| !c.repairing) {
            Some(good_child) => good_child,
            None => return self.set_state(state),
        };

        let bad_child = match self.children.iter().find(|c| c.repairing) {
            Some(bad_child) => bad_child,
            None => return self.set_state(state),
        };

        let bdev_handle = match &bad_child.bdev_handle {
            Some(bdev_handle) => bdev_handle,
            None => return self.set_state(state),
        };

        let block_size = self.bdev.block_len() as u64;
        let max_rebuild_size = 10u64 * 1024 * 1024; // 10MiB
        let max_blocks = max_rebuild_size / block_size;

        // At the moment if we copy the whole bdev it causes the js tests to timeout
        // One option is to invoke the rebuild through a different command (not on the add_child)
        // TODO: fix this
        let block_count = cmp::min(self.bdev.num_blocks(), max_blocks);

        info!("Rebuilding child {} from {}, blocks: {}, blockSize: {}", bad_child.name, good_child.name, block_count, block_size);

        let mut buf = match bdev_handle.dma_malloc(block_size as usize) {
            Ok(buf) => buf,
            Err(_) => return self.set_state(state),
        };

        for blk in 0..block_count {

            let addr: u64 = (blk+self.data_ent_offset)*(block_size as u64);
            if good_child.read_at(addr, &mut buf).await.is_err() {
                return self.set_state(state)
            }

            if bad_child.write_at(addr, &buf).await.is_err() {
                return self.set_state(state)
            }
        }

        // here we drop the immutable ref in favour a mutable ref so we can mutate the child
        // not ideal but it works... 
        // alternatively we'd have to use something like "interior mutability" or AtomicBool
        let bad_name = bad_child.name.clone();
        let bad_child = match self.children.iter_mut().find(|c| c.name == bad_name) {
            Some(bad_child) => bad_child,
            None => return state,
        };
        
        bad_child.repairing = false;
        bad_child.state = ChildState::Open;

        info!("Rebuild of child {} is complete!", bad_name);

        // child can now be part of the IO path
        self.reconfigure(DREvent::ChildOnline).await;

        self.set_state(NexusState::Online)
    }
}
