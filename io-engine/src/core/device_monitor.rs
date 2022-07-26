use std::{fmt::Debug, time::Duration};

use super::{work_queue::WorkQueue, Reactor};
use crate::{bdev::nexus::nexus_lookup_mut, core::VerboseError};

/// TODO
#[derive(Debug, Clone)]
pub enum DeviceCommand {
    RemoveDevice {
        nexus_name: String,
        child_device: String,
    },
}

/// TODO
static DEV_CMD_QUEUE: once_cell::sync::Lazy<WorkQueue<DeviceCommand>> =
    once_cell::sync::Lazy::new(WorkQueue::new);

/// TODO
pub fn device_cmd_queue() -> &'static WorkQueue<DeviceCommand> {
    &DEV_CMD_QUEUE
}

/// TODO
pub async fn device_monitor_loop() {
    let mut interval = tokio::time::interval(Duration::from_millis(10));
    loop {
        interval.tick().await;
        if let Some(w) = device_cmd_queue().take() {
            info!("Device monitor: executing command: {:?}", w);
            match w {
                DeviceCommand::RemoveDevice {
                    nexus_name,
                    child_device,
                } => {
                    let rx = Reactor::spawn_at_primary(async move {
                        if let Some(n) = nexus_lookup_mut(&nexus_name) {
                            if let Err(e) =
                                n.destroy_child_device(&child_device).await
                            {
                                error!(
                                    "{:?}: destroy child failed: {}",
                                    n,
                                    e.verbose()
                                );
                            }
                        }
                    });

                    match rx {
                        Err(e) => {
                            error!(
                                "Failed to schedule removal request: {}",
                                e.verbose()
                            )
                        }
                        Ok(rx) => rx.await.unwrap(),
                    }
                }
            }
        }
    }
}
