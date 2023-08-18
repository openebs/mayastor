use events_api::event::{
    EventAction,
    EventCategory,
    EventMessage,
    EventMeta,
    EventSource,
};

use crate::{
    bdev::nexus,
    eventing::{Event, RebuildEvent},
    grpc::node_name,
};

// Nexus event messages from Nexus data.
impl<'n> Event for nexus::Nexus<'n> {
    fn event(&self, event_action: EventAction) -> EventMessage {
        let event_source = EventSource::new(node_name(&None));
        EventMessage {
            category: EventCategory::Nexus as i32,
            action: event_action as i32,
            target: self.uuid().to_string(),
            metadata: Some(EventMeta::from_source(event_source)),
        }
    }
}

// Rebuild event messages from Nexus data.
impl<'n> RebuildEvent for nexus::Nexus<'n> {
    fn rebuild_event(
        &self,
        event_action: EventAction,
        source: Option<&str>,
        destination: &str,
        error: Option<&str>,
    ) -> EventMessage {
        let event_source = EventSource::new(node_name(&None)).add_rebuild_data(
            source,
            destination,
            error,
        );
        EventMessage {
            category: EventCategory::Nexus as i32,
            action: event_action as i32,
            target: self.uuid().to_string(),
            metadata: Some(EventMeta::from_source(event_source)),
        }
    }
}
