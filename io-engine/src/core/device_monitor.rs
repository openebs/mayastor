use std::{
    fmt::{Debug, Display, Formatter},
    time::Duration,
};

use super::{work_queue::WorkQueue, Reactor};
use crate::{bdev::nexus::nexus_lookup, core::VerboseError};

/// TODO
#[derive(Debug, Clone)]
pub enum DeviceCommand {
    RetireDevice {
        nexus_name: String,
        child_device: String,
    },
}

impl Display for DeviceCommand {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self {
            Self::RetireDevice {
                nexus_name,
                child_device,
            } => write!(
                f,
                "retire device '{child_device}' from nexus '{nexus_name}'",
            ),
        }
    }
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
            info!("Device monitor executing command: {}", w);
            match w {
                DeviceCommand::RetireDevice {
                    nexus_name,
                    child_device,
                } => {
                    let rx = Reactor::spawn_at_primary({
                        let nexus_name = nexus_name.clone();
                        let child_device = child_device.clone();
                        async move {
                            if let Some(n) = nexus_lookup(&nexus_name) {
                                if let Err(e) =
                                    n.close_child(&child_device).await
                                {
                                    error!(
                                    "Nexus '{nexus_name}': failed to close \
                                    retired child '{child_device}': {e}",
                                    e = e.verbose()
                                );
                                }
                            }
                        }
                    });

                    match rx {
                        Err(_) => {
                            error!(
                                "Nexus '{nexus_name}': failed to schedule \
                                removal request for '{child_device}'"
                            );
                        }
                        Ok(rx) => {
                            if let Err(e) = rx.await {
                                error!(
                                    "Nexus '{nexus_name}': failed to process \
                                    removal request for '{child_device}': {e}"
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}
