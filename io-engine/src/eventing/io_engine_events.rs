use events_api::event::{
    EventAction,
    EventCategory,
    EventMessage,
    EventMeta,
    EventSource,
};

use crate::{
    core::{MayastorEnvironment, Reactor},
    eventing::Event,
};

// Io-engine event message from Mayastor env data.
impl Event for MayastorEnvironment {
    fn event(&self, event_action: EventAction) -> EventMessage {
        let event_source = EventSource::new(self.node_name.clone());
        EventMessage {
            category: EventCategory::IoEngineCategory as i32,
            action: event_action as i32,
            target: self.name.clone(),
            metadata: Some(EventMeta::from_source(event_source)),
        }
    }
}

// Reactor event message from Reactor data.
impl Event for Reactor {
    fn event(&self, event_action: EventAction) -> EventMessage {
        let event_source = EventSource::new(
            MayastorEnvironment::global_or_default().node_name,
        )
        .with_reactor_details(
            self.core().into(),
            &self.get_state().to_string(),
        );
        EventMessage {
            category: EventCategory::IoEngineCategory as i32,
            action: event_action as i32,
            target: self.tid().to_string(),
            metadata: Some(EventMeta::from_source(event_source)),
        }
    }
}
