// Volume manager implementation.

import events = require('events');
import { grpcCode, GrpcError } from './grpc_client';
import { Volume, VolumeSpec, VolumeState } from './volume';
import { Workq } from './workq';
import { VolumeStatus } from './volume_operator';
import { EventStream } from './event_stream';
import { Logger } from './logger';

const log = Logger('volumes');

// Type used in "create volume" workq
type CreateArgs = {
  uuid: string;
  spec: VolumeSpec;
}

// Volume manager that emit events for new/modified/deleted volumes.
export class Volumes extends events.EventEmitter {
  private registry: any;
  private events: any; // stream of events from registry
  private volumes: Record<string, Volume>; // volumes indexed by uuid

  constructor (registry: any) {
    super();
    this.registry = registry;
    this.events = null;
    this.volumes = {};
  }

  start() {
    const self = this;
    this.events = new EventStream({ registry: this.registry });
    this.events.on('data', async function (ev: any) {
      if (ev.kind === 'pool' && ev.eventType === 'new') {
        // New pool was added and perhaps we have volumes waiting to schedule
        // their replicas on it.
        Object.values(self.volumes)
          .filter((v) => v.state === VolumeState.Degraded)
          .forEach((v) => v.fsa());
      } else if (ev.kind === 'replica' || ev.kind === 'nexus') {
        const uuid: string = ev.object.uuid;
        const volume = self.volumes[uuid];
        if (!volume) {
          // Ignore events for volumes that do not exist. Those might be events
          // related to a volume that is being destroyed.
          log.debug(`${ev.eventType} event for unknown volume "${uuid}"`);
          return;
        }
        if (ev.kind === 'replica') {
          if (ev.eventType === 'new') {
            volume.newReplica(ev.object);
          } else if (ev.eventType === 'mod') {
            volume.modReplica(ev.object);
          } else if (ev.eventType === 'del') {
            volume.delReplica(ev.object);
          }
        } else if (ev.kind === 'nexus') {
          if (ev.eventType === 'new') {
            volume.newNexus(ev.object);
          } else if (ev.eventType === 'mod') {
            volume.modNexus(ev.object);
          } else if (ev.eventType === 'del') {
            volume.delNexus(ev.object);
          }
        }
      } else if (ev.kind === 'node' && ev.object.isSynced()) {
        // Create nexus for volumes that should have one on the node
        Object.values(self.volumes)
          .filter((v) => v.getNodeName() === ev.object.name)
          .forEach((v) => v.fsa());
      }
    });
  }

  stop() {
    this.events.destroy();
    this.events.removeAllListeners();
    this.events = null;
    Object.values(this.volumes).forEach((vol) => {
      vol.deactivate();
    })
    this.volumes = {};
  }

  // Return a volume with specified uuid.
  //
  // @param   uuid   ID of the volume.
  // @returns Matching volume or undefined if not found.
  //
  get(uuid: string): Volume | undefined {
    return this.volumes[uuid];
  }

  // Return all volumes.
  list(): Volume[] {
    return Object.values(this.volumes);
  }

  // Create volume object (just the object) and add it to the internal list
  // of volumes. The method is idempotent. If a volume with the same uuid
  // already exists, then update its parameters.
  //
  // @param   {string}   uuid                 ID of the volume.
  // @param   {object}   spec                 Properties of the volume.
  // @params  {number}   spec.replicaCount    Number of desired replicas.
  // @params  {string[]} spec.preferredNodes  Nodes to prefer for scheduling replicas.
  // @params  {string[]} spec.requiredNodes   Replicas must be on these nodes.
  // @params  {number}   spec.requiredBytes   The volume must have at least this size.
  // @params  {number}   spec.limitBytes      The volume should not be bigger than this.
  // @params  {string}   spec.protocol        The share protocol for the nexus.
  // @returns {object}   New volume object.
  async createVolume(uuid: string, spec: VolumeSpec): Promise<Volume> {
    if (!spec.requiredBytes || spec.requiredBytes < 0) {
      throw new GrpcError(
        grpcCode.INVALID_ARGUMENT,
        'Required bytes must be greater than zero'
      );
    }
    let volume = this.volumes[uuid];
    if (volume) {
      if (volume.isSpecUpdatable())
        volume.update(spec);
      else {
        // note: if the volume is destroyed but still in the list, it may never get deleted again and so
        // subsequent calls to create volume will keep failing.
        log.error(`Failing createVolume for volume ${uuid} because its state is "${volume.state}"`);
        throw new GrpcError(
          grpcCode.UNAVAILABLE,
          `Volume cannot be updated, its state is "${volume.state}"`
        );
      }
    } else {
      // The volume starts to exist before it is created because we must receive
      // events for it and we want to show to user that it is being created.
      this.volumes[uuid] = new Volume(uuid, this.registry, this, spec);
      volume = this.volumes[uuid];
      this.emit('volume', {
        eventType: 'new',
        object: volume
      });

      try {
        await volume.create();
      } catch (err) {
        // Undo the pending state and whatever has been created
        volume.state = VolumeState.Unknown;
        try {
          this.destroyVolume(uuid);
        } catch (err) {
          log.error(`Failed to destroy "${volume}": ${err}`);
        }
        throw err;
      }
      volume.fsa();
    }
    return volume;
  }

  // Destroy the volume.
  //
  // The method is idempotent - if the volume does not exist it does not return
  // an error.
  //
  // @param   uuid            ID of the volume.
  //
  async destroyVolume(uuid: string) {
    const volume = this.volumes[uuid];
    if (!volume) return;

    await volume.destroy();
    volume.deactivate();
    delete this.volumes[uuid];
  }

  // Import the volume object (just the object) and add it to the internal list
  // of volumes. The method is idempotent. If a volume with the same uuid
  // already exists, then update its parameters.
  //
  // @param   {string}    uuid                 ID of the volume.
  // @param   {object}    spec                 Properties of the volume.
  // @params  {number}    spec.replicaCount    Number of desired replicas.
  // @params  {string[]}  spec.preferredNodes  Nodes to prefer for scheduling replicas.
  // @params  {string[]}  spec.requiredNodes   Replicas must be on these nodes.
  // @params  {number}    spec.requiredBytes   The volume must have at least this size.
  // @params  {number}    spec.limitBytes      The volume should not be bigger than this.
  // @params  {string}    spec.protocol        The share protocol for the nexus.
  // @params  {object}    status               Current properties of the volume
  // @params  {string}    status.state         Last known state of the volume.
  // @params  {number}    status.size          Size of the volume.
  // @params  {string}    status.targetNodes   Node(s) where the volume is published.
  // @returns {object} New volume object.
  //
  importVolume(uuid: string, spec: VolumeSpec, status?: VolumeStatus): Volume {
    let volume = this.volumes[uuid];

    if (!volume) {
      let state = status?.state;
      let size = status?.size;
      // We don't support multiple nexuses yet so take the first one
      let publishedOn = (status?.targetNodes || []).pop();
      // If for some strange reason the status is "pending" change it to unknown
      // because fsa would refuse to act on it otherwise.
      if (!state || state === VolumeState.Pending) {
        state = VolumeState.Unknown;
      }
      this.volumes[uuid] = new Volume(
        uuid,
        this.registry,
        this,
        spec,
        state,
        size,
        publishedOn,
      );
      volume = this.volumes[uuid];
      volume.attach();
      volume.fsa();
    }
    return volume;
  }
}