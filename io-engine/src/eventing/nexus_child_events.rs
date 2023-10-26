use events_api::event::{
    EventAction,
    EventCategory,
    EventMessage,
    EventMeta,
    EventSource,
};

use io_engine_api::v1::nexus::{AddChildNexusRequest, RemoveChildNexusRequest};

use crate::{core::MayastorEnvironment, eventing::Event};

// Nexus child added event message.
impl Event for AddChildNexusRequest {
    fn event(&self, event_action: EventAction) -> EventMessage {
        let event_source = EventSource::new(
            MayastorEnvironment::global_or_default().node_name,
        )
        .with_nexus_child_data(&self.uri);
        EventMessage {
            category: EventCategory::Nexus as i32,
            action: event_action as i32,
            target: self.uuid.clone(),
            metadata: Some(EventMeta::from_source(event_source)),
        }
    }
}

// Nexus child removed event message.
impl Event for RemoveChildNexusRequest {
    fn event(&self, event_action: EventAction) -> EventMessage {
        let event_source = EventSource::new(
            MayastorEnvironment::global_or_default().node_name,
        )
        .with_nexus_child_data(&self.uri);
        EventMessage {
            category: EventCategory::Nexus as i32,
            action: event_action as i32,
            target: self.uuid.clone(),
            metadata: Some(EventMeta::from_source(event_source)),
        }
    }
}
