use events_api::event::{
    EventAction,
    EventCategory,
    EventMessage,
    EventMeta,
    EventSource,
};

use crate::{
    core::{logical_volume::LogicalVolume, MayastorEnvironment},
    eventing::Event,
    lvs::lvs_lvol::{Lvol, LvsLvol},
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
