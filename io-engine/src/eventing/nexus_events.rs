use events_api::event::{
    EventAction,
    EventCategory,
    EventMessage,
    EventMeta,
    EventSource,
    RebuildStatus,
};

use crate::{
    bdev::nexus,
    core::{MayastorEnvironment, VerboseError},
    eventing::{Event, EventMetaGen, EventWithMeta},
    rebuild::{RebuildJob, RebuildState},
};

impl EventMetaGen for RebuildJob {
    fn meta(&self) -> EventMeta {
        let rebuild_status = match self.state() {
            RebuildState::Init | RebuildState::Running => {
                RebuildStatus::Started
            }
            RebuildState::Stopped => RebuildStatus::Stopped,
            RebuildState::Failed => RebuildStatus::Failed,
            RebuildState::Completed => RebuildStatus::Completed,
            _ => RebuildStatus::Unknown,
        };

        let event_source = EventSource::new(
            MayastorEnvironment::global_or_default().node_name,
        )
        .with_rebuild_data(
            rebuild_status,
            self.src_uri(),
            self.dst_uri(),
            self.error().map(|e| e.verbose()),
        );

        EventMeta::from_source(event_source)
    }
}

impl<'n> Event for nexus::Nexus<'n> {
    fn event(&self, event_action: EventAction) -> EventMessage {
        let event_source = EventSource::new(
            MayastorEnvironment::global_or_default().node_name,
        );
        EventMessage {
            category: EventCategory::Nexus as i32,
            action: event_action as i32,
            target: self.uuid().to_string(),
            metadata: Some(EventMeta::from_source(event_source)),
        }
    }
}

impl<'n> EventWithMeta for nexus::Nexus<'n> {
    fn event(
        &self,
        event_action: EventAction,
        meta: EventMeta,
    ) -> EventMessage {
        EventMessage {
            category: EventCategory::Nexus as i32,
            action: event_action as i32,
            target: self.uuid().to_string(),
            metadata: Some(meta),
        }
    }
}
