use events_api::event::{
    EventAction,
    EventCategory,
    EventMessage,
    EventMeta,
    EventSource,
};

use crate::{eventing::Event, grpc::node_name, lvs::Lvs};

// Pool event messages from Lvs data.
impl Event for Lvs {
    fn event(&self, event_action: EventAction) -> EventMessage {
        let event_source = EventSource::new(node_name(&None));
        EventMessage {
            category: EventCategory::Pool as i32,
            action: event_action as i32,
            target: self.name().to_string(),
            metadata: Some(EventMeta::from_source(event_source)),
        }
    }
}
