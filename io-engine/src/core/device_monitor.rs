use std::{fmt::Debug, time::Duration};

use super::{work_queue::WorkQueue, Reactor};

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
            info!(?w, "executing command");
            match w {
                DeviceCommand::RemoveDevice {
                    nexus_name,
                    child_device,
                } => {
                    let rx = Reactor::spawn_at_primary(async move {
                        if let Some(n) =
                            crate::bdev::nexus::nexus_lookup_mut(&nexus_name)
                        {
                            if let Err(e) = n.destroy_child(&child_device).await
                            {
                                error!(?e, "destroy child failed");
                            }
                        }
                    });

                    match rx {
                        Err(e) => {
                            error!(?e, "failed to equeue removal request")
                        }
                        Ok(rx) => rx.await.unwrap(),
                    }
                }
            }
        }
    }
}
