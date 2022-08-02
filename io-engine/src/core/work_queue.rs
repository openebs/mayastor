use std::fmt::Debug;

/// TODO
#[derive(Debug)]
pub struct WorkQueue<T: Send + Debug> {
    incoming: crossbeam::queue::SegQueue<T>,
}

impl<T: Send + Debug> Default for WorkQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Send + Debug> WorkQueue<T> {
    pub fn new() -> Self {
        Self {
            incoming: crossbeam::queue::SegQueue::new(),
        }
    }

    pub fn enqueue(&self, entry: T) {
        trace!(?entry, "enqueued");
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
