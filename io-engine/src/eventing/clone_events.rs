use events_api::event::{
    EventAction,
    EventCategory,
    EventMessage,
    EventMeta,
    EventSource,
};

use crate::{
    core::{snapshot::CloneParams, MayastorEnvironment},
    eventing::Event,
};

impl Event for CloneParams {
    fn event(&self, event_action: EventAction) -> EventMessage {
        let event_source = EventSource::new(
            MayastorEnvironment::global_or_default().node_name,
        )
        .with_clone_data(
            self.source_uuid().unwrap_or_default(),
            self.clone_create_time().unwrap_or_default(),
        );

        EventMessage {
            category: EventCategory::Clone as i32,
            action: event_action as i32,
            target: self.clone_uuid().unwrap_or_default(),
            metadata: Some(EventMeta::from_source(event_source)),
        }
    }
}
