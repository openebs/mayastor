use std::fmt::{Debug, Display};

/// TODO
#[derive(Debug)]
pub struct WorkQueue<T: Send + Debug + Display> {
    incoming: crossbeam::queue::SegQueue<T>,
}

impl<T: Send + Debug + Display> Default for WorkQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Send + Debug + Display> WorkQueue<T> {
    pub fn new() -> Self {
        Self {
            incoming: crossbeam::queue::SegQueue::new(),
        }
    }

    pub fn enqueue(&self, entry: T) {
        trace!("Enqueued {}", entry);
        self.incoming.push(entry)
    }

    pub fn len(&self) -> usize {
        self.incoming.len()
    }

    pub fn is_empty(&self) -> bool {
        self.incoming.len() == 0
    }

    pub fn take(&self) -> Option<T> {
        if let Some(elem) = self.incoming.pop() {
            return Some(elem);
        }
        None
    }
}
