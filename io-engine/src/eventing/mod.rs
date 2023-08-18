mod nexus_events;
mod pool_events;
use events_api::event::{EventAction, EventMessage};

/// Event trait definition for creating events.
pub(crate) trait Event {
    /// Create event message.
    fn event(&self, event_action: EventAction) -> EventMessage;
}

/// Rebuild event trait definition for creating rebuild events.
pub(crate) trait RebuildEvent {
    /// Create rebuild event message.
    fn rebuild_event(
        &self,
        event_action: EventAction,
        source: Option<&str>,
        destination: &str,
        error: Option<&str>,
    ) -> EventMessage;
}
