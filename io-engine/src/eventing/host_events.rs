use events_api::event::{
    EventAction,
    EventCategory,
    EventMessage,
    EventMeta,
    EventSource,
};

use crate::{
    bdev::Nexus,
    core::{LogicalVolume, MayastorEnvironment},
    eventing::{EventMetaGen, EventWithMeta},
    lvs::Lvol,
    subsys::NvmfSubsystem,
};
use spdk_rs::NvmfController;

/// A trait definition to include target details in host events meta data
pub(crate) trait HostTargetMeta {
    /// Add target detaails to host event meta
    fn host_target_meta(&self, meta: EventMeta) -> EventMeta;
}

impl<'n> HostTargetMeta for Nexus<'n> {
    fn host_target_meta(&self, mut meta: EventMeta) -> EventMeta {
        if let Some(source) = meta.source {
            let event_source =
                source.with_target_data("nexus", &self.uuid().to_string());
            meta.source = Some(event_source);
        }
        meta
    }
}

impl HostTargetMeta for Lvol {
    fn host_target_meta(&self, mut meta: EventMeta) -> EventMeta {
        if let Some(source) = meta.source {
            let event_source = source.with_target_data("replica", &self.uuid());
            meta.source = Some(event_source);
        }
        meta
    }
}

impl EventMetaGen for NvmfSubsystem {
    fn meta(&self) -> EventMeta {
        let nqn = self.get_nqn();
        let event_source = EventSource::new(
            MayastorEnvironment::global_or_default().node_name,
        )
        .with_subsystem_data(&nqn);

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
