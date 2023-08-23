pub mod pool_events;
use events_api::event::{EventAction, EventMessage};

/// Event trait definition for creating events.
pub(crate) trait Event {
    /// Create event message.
    fn event(&self, event_action: Option<EventAction>) -> EventMessage;
}
