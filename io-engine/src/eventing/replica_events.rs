use events_api::event::{
    EventAction,
    EventCategory,
    EventMessage,
    EventMeta,
    EventSource,
};

use crate::{
    core::{logical_volume::LogicalVolume, MayastorEnvironment},
    lvs::lvs_lvol::{Lvol, LvsLvol},
};

use crate::{
    bdev::nexus::{ChildState, NexusChild},
    eventing::{Event, EventWithMeta},
};

// Replica event messages from Lvol data.
impl Event for Lvol {
    fn event(&self, event_action: EventAction) -> EventMessage {
        let event_source = EventSource::new(
            MayastorEnvironment::global_or_default().node_name,
        )
        .with_replica_data(
            self.lvs().name(),
            &self.lvs().uuid(),
            &self.name(),
        );

        EventMessage {
            category: EventCategory::Replica as i32,
            action: event_action as i32,
            target: self.uuid(),
            metadata: Some(EventMeta::from_source(event_source)),
        }
    }
}

/// Replica state change event.
pub(crate) fn state_change_event_meta(
    previous: ChildState,
    next: ChildState,
) -> EventMeta {
    let event_source =
        EventSource::new(MayastorEnvironment::global_or_default().node_name)
            .with_state_change_data(previous.to_string(), next.to_string());
    EventMeta::from_source(event_source)
}

impl<'n> EventWithMeta for NexusChild<'n> {
    fn event(
        &self,
        event_action: EventAction,
        meta: EventMeta,
    ) -> EventMessage {
        EventMessage {
            category: EventCategory::Replica as i32,
            action: event_action as i32,
            target: self.get_uuid().unwrap_or_default(),
            metadata: Some(meta),
        }
    }
}
