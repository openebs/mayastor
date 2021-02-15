//! Converts rpc messages to message bus messages and vice versa.

use mbus_api::{
    v0 as mbus,
    v0::{ChildState, NexusState, Protocol},
};
use rpc::mayastor as rpc;

/// Trait for converting rpc messages to message bus messages.
pub trait RpcToMessageBus {
    /// Message bus message type.
    type BusMessage;
    /// Conversion of rpc message to message bus message.
    fn to_mbus(&self) -> Self::BusMessage;
}

impl RpcToMessageBus for rpc::block_device::Partition {
    type BusMessage = mbus::Partition;
    fn to_mbus(&self) -> Self::BusMessage {
        Self::BusMessage {
            parent: self.parent.clone(),
            number: self.number,
            name: self.name.clone(),
            scheme: self.scheme.clone(),
            typeid: self.typeid.clone(),
            uuid: self.uuid.clone(),
        }
    }
}

impl RpcToMessageBus for rpc::block_device::Filesystem {
    type BusMessage = mbus::Filesystem;
    fn to_mbus(&self) -> Self::BusMessage {
        Self::BusMessage {
            fstype: self.fstype.clone(),
            label: self.label.clone(),
            uuid: self.uuid.clone(),
            mountpoint: self.mountpoint.clone(),
        }
    }
}

/// Node Agent Conversions

impl RpcToMessageBus for rpc::BlockDevice {
    type BusMessage = mbus::BlockDevice;
    fn to_mbus(&self) -> Self::BusMessage {
        Self::BusMessage {
            devname: self.devname.clone(),
            devtype: self.devtype.clone(),
            devmajor: self.devmajor,
            devminor: self.devminor,
            model: self.model.clone(),
            devpath: self.devpath.clone(),
            devlinks: self.devlinks.clone(),
            size: self.size,
            partition: match &self.partition {
                Some(partition) => partition.to_mbus(),
                None => mbus::Partition {
                    ..Default::default()
                },
            },
            filesystem: match &self.filesystem {
                Some(filesystem) => filesystem.to_mbus(),
                None => mbus::Filesystem {
                    ..Default::default()
                },
            },
            available: self.available,
        }
    }
}

///  Pool Agent conversions

impl RpcToMessageBus for rpc::Pool {
    type BusMessage = mbus::Pool;
    fn to_mbus(&self) -> Self::BusMessage {
        Self::BusMessage {
            node: Default::default(),
            id: self.name.clone().into(),
            disks: self.disks.clone(),
            state: self.state.into(),
            capacity: self.capacity,
            used: self.used,
        }
    }
}

impl RpcToMessageBus for rpc::Replica {
    type BusMessage = mbus::Replica;
    fn to_mbus(&self) -> Self::BusMessage {
        Self::BusMessage {
            node: Default::default(),
            uuid: self.uuid.clone().into(),
            pool: self.pool.clone().into(),
            thin: self.thin,
            size: self.size,
            share: self.share.into(),
            uri: self.uri.clone(),
        }
    }
}

/// Volume Agent conversions

impl RpcToMessageBus for rpc::Nexus {
    type BusMessage = mbus::Nexus;

    fn to_mbus(&self) -> Self::BusMessage {
        Self::BusMessage {
            node: Default::default(),
            uuid: self.uuid.clone().into(),
            size: self.size,
            state: NexusState::from(self.state),
            children: self.children.iter().map(|c| c.to_mbus()).collect(),
            device_uri: self.device_uri.clone(),
            rebuilds: self.rebuilds,
        }
    }
}

impl RpcToMessageBus for rpc::Child {
    type BusMessage = mbus::Child;

    fn to_mbus(&self) -> Self::BusMessage {
        Self::BusMessage {
            uri: self.uri.clone().into(),
            state: ChildState::from(self.state),
            rebuild_progress: if self.rebuild_progress >= 0 {
                Some(self.rebuild_progress)
            } else {
                None
            },
        }
    }
}

/// Trait for converting message bus messages to rpc messages.
pub trait MessageBusToRpc {
    /// RPC message type.
    type RpcMessage;
    /// Conversion of message bus message to rpc message.
    fn to_rpc(&self) -> Self::RpcMessage;
}

/// Pool Agent Conversions

impl MessageBusToRpc for mbus::CreateReplica {
    type RpcMessage = rpc::CreateReplicaRequest;
    fn to_rpc(&self) -> Self::RpcMessage {
        Self::RpcMessage {
            uuid: self.uuid.clone().into(),
            pool: self.pool.clone().into(),
            thin: self.thin,
            size: self.size,
            share: self.share.clone() as i32,
        }
    }
}

impl MessageBusToRpc for mbus::ShareReplica {
    type RpcMessage = rpc::ShareReplicaRequest;
    fn to_rpc(&self) -> Self::RpcMessage {
        Self::RpcMessage {
            uuid: self.uuid.clone().into(),
            share: self.protocol.clone() as i32,
        }
    }
}

impl MessageBusToRpc for mbus::UnshareReplica {
    type RpcMessage = rpc::ShareReplicaRequest;
    fn to_rpc(&self) -> Self::RpcMessage {
        Self::RpcMessage {
            uuid: self.uuid.clone().into(),
            share: Protocol::Off as i32,
        }
    }
}

impl MessageBusToRpc for mbus::CreatePool {
    type RpcMessage = rpc::CreatePoolRequest;
    fn to_rpc(&self) -> Self::RpcMessage {
        Self::RpcMessage {
            name: self.id.clone().into(),
            disks: self.disks.clone(),
        }
    }
}

impl MessageBusToRpc for mbus::DestroyReplica {
    type RpcMessage = rpc::DestroyReplicaRequest;
    fn to_rpc(&self) -> Self::RpcMessage {
        Self::RpcMessage {
            uuid: self.uuid.clone().into(),
        }
    }
}

impl MessageBusToRpc for mbus::DestroyPool {
    type RpcMessage = rpc::DestroyPoolRequest;
    fn to_rpc(&self) -> Self::RpcMessage {
        Self::RpcMessage {
            name: self.id.clone().into(),
        }
    }
}

/// Volume Agent Conversions

impl MessageBusToRpc for mbus::CreateNexus {
    type RpcMessage = rpc::CreateNexusRequest;
    fn to_rpc(&self) -> Self::RpcMessage {
        Self::RpcMessage {
            uuid: self.uuid.clone().into(),
            size: self.size,
            children: self.children.iter().map(|c| c.to_string()).collect(),
        }
    }
}

impl MessageBusToRpc for mbus::ShareNexus {
    type RpcMessage = rpc::PublishNexusRequest;
    fn to_rpc(&self) -> Self::RpcMessage {
        Self::RpcMessage {
            uuid: self.uuid.clone().into(),
            key: self.key.clone().unwrap_or_default(),
            share: self.protocol.clone() as i32,
        }
    }
}

impl MessageBusToRpc for mbus::UnshareNexus {
    type RpcMessage = rpc::UnpublishNexusRequest;
    fn to_rpc(&self) -> Self::RpcMessage {
        Self::RpcMessage {
            uuid: self.uuid.clone().into(),
        }
    }
}

impl MessageBusToRpc for mbus::DestroyNexus {
    type RpcMessage = rpc::DestroyNexusRequest;
    fn to_rpc(&self) -> Self::RpcMessage {
        Self::RpcMessage {
            uuid: self.uuid.clone().into(),
        }
    }
}

impl MessageBusToRpc for mbus::AddNexusChild {
    type RpcMessage = rpc::AddChildNexusRequest;
    fn to_rpc(&self) -> Self::RpcMessage {
        Self::RpcMessage {
            uuid: self.nexus.clone().into(),
            uri: self.uri.clone().into(),
            norebuild: !self.auto_rebuild,
        }
    }
}

impl MessageBusToRpc for mbus::RemoveNexusChild {
    type RpcMessage = rpc::RemoveChildNexusRequest;
    fn to_rpc(&self) -> Self::RpcMessage {
        Self::RpcMessage {
            uuid: self.nexus.clone().into(),
            uri: self.uri.clone().into(),
        }
    }
}
