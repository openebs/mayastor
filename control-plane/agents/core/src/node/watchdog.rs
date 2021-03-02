use crate::node::service::Service;
use mbus_api::v0::NodeId;

/// Watchdog which must be pet within the deadline, otherwise
/// it triggers the `on_timeout` callback from the node `Service`
#[derive(Debug, Clone)]
pub(crate) struct Watchdog {
    node_id: NodeId,
    deadline: std::time::Duration,
    timestamp: std::time::Instant,
    pet_chan: Option<tokio::sync::mpsc::Sender<()>>,
    service: Option<Service>,
}

impl Watchdog {
    /// new empty watchdog with a deadline timeout for node `node_id`
    pub(crate) fn new(node_id: &NodeId, deadline: std::time::Duration) -> Self {
        Self {
            deadline,
            node_id: node_id.clone(),
            timestamp: std::time::Instant::now(),
            pet_chan: None,
            service: None,
        }
    }

    /// the set deadline
    pub(crate) fn deadline(&self) -> std::time::Duration {
        self.deadline
    }

    /// last time the node was seen
    pub(crate) fn timestamp(&self) -> std::time::Instant {
        self.timestamp
    }

    /// arm watchdog with self timeout and execute error callback if
    /// the deadline is not met
    pub(crate) fn arm(&mut self, service: Service) {
        tracing::debug!("Arming the watchdog for node '{}'", self.node_id);
        let (s, mut r) = tokio::sync::mpsc::channel(1);
        self.pet_chan = Some(s);
        self.service = Some(service.clone());
        let deadline = self.deadline;
        let id = self.node_id.clone();
        tokio::spawn(async move {
            loop {
                let result = tokio::time::timeout(deadline, r.recv()).await;
                match result {
                    Err(_) => Service::on_timeout(&service, &id).await,
                    Ok(None) => {
                        tracing::warn!("Stopping Watchdog for node '{}'", id);
                        break;
                    }
                    _ => (),
                }
            }
        });
    }

    /// meet the deadline
    pub(crate) async fn pet(
        &mut self,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<()>> {
        self.timestamp = std::time::Instant::now();
        if let Some(chan) = &mut self.pet_chan {
            chan.send(()).await
        } else {
            // if the watchdog was stopped, then rearm it
            if let Some(service) = self.service.clone() {
                self.arm(service);
            }
            Ok(())
        }
    }
    /// stop the watchdog
    pub(crate) fn disarm(&mut self) {
        tracing::debug!("Disarming the watchdog for node '{}'", self.node_id);
        if let Some(chan) = &mut self.pet_chan {
            let _ = chan.disarm();
        }
        self.pet_chan = None;
    }
}
