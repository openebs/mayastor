use events_api::event::{
    EventAction,
    EventCategory,
    EventMessage,
    EventMeta,
    EventSource,
};

use crate::{
    core::MayastorEnvironment,
    eventing::{EventMetaGen, EventWithMeta},
    subsys::NvmfSubsystem,
};
use spdk_rs::NvmfController;

impl EventMetaGen for NvmfSubsystem {
    fn meta(&self) -> EventMeta {
        let event_source = EventSource::new(
            MayastorEnvironment::global_or_default().node_name,
        )
        .with_subsystem_data(&self.get_nqn());

        EventMeta::from_source(event_source)
    }
}

impl EventWithMeta for NvmfController {
    fn event(
        &self,
        event_action: EventAction,
        mut meta: EventMeta,
    ) -> EventMessage {
        if let Some(source) = meta.source {
            let event_source = source.with_host_initiator_data(&self.hostnqn());
            meta.source = Some(event_source);
        }

        EventMessage {
            category: EventCategory::HostInitiator as i32,
            action: event_action as i32,
            target: "".to_string(),
            metadata: Some(meta),
        }
    }
}
