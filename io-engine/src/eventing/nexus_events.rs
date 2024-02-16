use events_api::event::{
    EventAction,
    EventCategory,
    EventMessage,
    EventMeta,
    EventSource,
    RebuildStatus,
};

use std::time::Duration;

use crate::{
    bdev::{
        nexus,
        nexus::{Error, NexusChild, NexusPauseState, NexusState},
    },
    core::{MayastorEnvironment, VerboseError},
    eventing::{Event, EventMetaGen, EventWithMeta},
    rebuild::{NexusRebuildJob, RebuildState},
};

impl EventMetaGen for NexusRebuildJob {
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

impl<'n> EventMetaGen for NexusChild<'n> {
    fn meta(&self) -> EventMeta {
        let event_source = EventSource::new(
            MayastorEnvironment::global_or_default().node_name,
        )
        .with_nexus_child_data(self.uri());
        EventMeta::from_source(event_source)
    }
}

/// Nexus state change event meta.
pub(crate) fn state_change_event_meta(
    previous: NexusState,
    next: NexusState,
) -> EventMeta {
    let event_source =
        EventSource::new(MayastorEnvironment::global_or_default().node_name)
            .with_state_change_data(previous.to_string(), next.to_string());
    EventMeta::from_source(event_source)
}

/// Subsystem pause event meta.
pub(crate) fn subsystem_pause_event_meta(
    nexus_pause_status: Option<NexusPauseState>,
    total_time_option: Option<Duration>,
    error_option: Option<&Error>,
) -> EventMeta {
    let nexus_pause_state = match nexus_pause_status {
        Some(pause_state) => pause_state.to_string(),
        None => String::default(),
    };
    let mut event_source =
        EventSource::new(MayastorEnvironment::global_or_default().node_name)
            .with_subsystem_pause_details(nexus_pause_state);
    if let Some(total_time) = total_time_option {
        event_source =
            event_source.with_event_action_duration_details(total_time);
    }
    if let Some(err) = error_option {
        event_source = event_source.with_error_details(err.to_string());
    };
    EventMeta::from_source(event_source)
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

impl EventMetaGen for Error {
    fn meta(&self) -> EventMeta {
        let event_source = EventSource::new(
            MayastorEnvironment::global_or_default().node_name,
        )
        .with_error_details(self.to_string());
        EventMeta::from_source(event_source)
    }
}
