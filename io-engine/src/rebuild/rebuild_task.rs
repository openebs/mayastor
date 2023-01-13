use super::RebuildError;
use futures::{channel::mpsc, StreamExt};
use spdk_rs::DmaBuf;

/// Result returned by each segment task worker
/// used to communicate with the management task indicating that the
/// segment task worker is ready to copy another segment
#[derive(Debug, Clone)]
pub(super) struct TaskResult {
    /// block that was being rebuilt
    pub(super) blk: u64,
    /// id of the task
    pub(super) id: usize,
    /// encountered error, if any
    pub(super) error: Option<RebuildError>,
}

/// Each rebuild task needs a unique buffer to read/write from source to target
/// A mpsc channel is used to communicate with the management task
#[derive(Debug)]
pub(super) struct RebuildTask {
    /// TODO
    pub(super) buffer: DmaBuf,
    /// TODO
    pub(super) sender: mpsc::Sender<TaskResult>,
    /// TODO
    pub(super) error: Option<TaskResult>,
}

/// Pool of rebuild tasks and progress tracking
/// Each task uses a clone of the sender allowing the management task to poll a
/// single receiver
pub(super) struct RebuildTasks {
    /// TODO
    pub(super) tasks: Vec<RebuildTask>,
    /// TODO
    pub(super) channel: (mpsc::Sender<TaskResult>, mpsc::Receiver<TaskResult>),
    /// TODO
    pub(super) active: usize,
    /// TODO
    pub(super) total: usize,
    /// TODO
    pub(super) segments_done: u64,
}

impl std::fmt::Debug for RebuildTasks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RebuildTasks")
            .field("active", &self.active)
            .field("total", &self.total)
            .field("segments_done", &self.segments_done)
            .finish()
    }
}

impl RebuildTasks {
    /// TODO
    pub(super) async fn await_one_task(&mut self) -> Option<TaskResult> {
        self.channel.1.next().await.map(|f| {
            self.active -= 1;
            if f.error.is_none() {
                self.segments_done += 1;
            } else {
                self.tasks[f.id].error = Some(f.clone());
            }
            f
        })
    }
}
