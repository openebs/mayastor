use events_api::event::{
    EventAction,
    EventCategory,
    EventMessage,
    EventMeta,
    EventSource,
};

use crate::{
    core::{
        snapshot::ISnapshotDescriptor,
        MayastorEnvironment,
        SnapshotParams,
    },
    eventing::Event,
};

impl Event for SnapshotParams {
    fn event(&self, event_action: EventAction) -> EventMessage {
        let event_source = EventSource::new(
            MayastorEnvironment::global_or_default().node_name,
        )
        .with_snapshot_data(
            self.parent_id().unwrap_or_default(),
            self.create_time().unwrap_or_default(),
            self.entity_id().unwrap_or_default(),
        );

        EventMessage {
            category: EventCategory::Snapshot as i32,
            action: event_action as i32,
            target: self.snapshot_uuid().unwrap_or_default(),
            metadata: Some(EventMeta::from_source(event_source)),
        }
    }
}
