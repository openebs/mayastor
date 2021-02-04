// Volume manager implementation.

import assert from 'assert';
import { Volume, VolumeState } from './volume';
import { Workq } from './workq';

const EventEmitter = require('events');
const EventStream = require('./event_stream');
const { GrpcCode, GrpcError } = require('./grpc_client');
const log = require('./logger').Logger('volumes');

// Type used in "create volume" workq
type CreateArgs = {
  uuid: string;
  spec: any;
}

// Volume manager that emit events for new/modified/deleted volumes.
export class Volumes extends EventEmitter {
  private registry: any;
  private events: any; // stream of events from registry
  private volumes: Record<string, Volume>; // volumes indexed by uuid
  private createWorkq: Workq;

  constructor (registry: any) {
    super();
    this.registry = registry;
    this.events = null;
    this.volumes = {};
    this.createWorkq = new Workq('create volume');
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

  // We have to serialize create volume requests because concurrent creates
  // can create havoc in space accounting and contribute to overall mess.
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
  async createVolume(uuid: string, spec: any): Promise<Volume> {
    return await this.createWorkq.push({uuid, spec}, (args: CreateArgs) => {
      return this._createVolume(args.uuid, args.spec);
    });
  }

  // Create volume object (just the object) and add it to the internal list
  // of volumes. The method is idempotent. If a volume with the same uuid
  // already exists, then update its parameters.
  //
  async _createVolume(uuid: string, spec: any): Promise<Volume> {
    if (!spec.requiredBytes || spec.requiredBytes < 0) {
      throw new GrpcError(
        GrpcCode.INVALID_ARGUMENT,
        'Required bytes must be greater than zero'
      );
    }
    let volume = this.volumes[uuid];
    if (volume) {
      volume.update(spec);
    } else {
      volume = new Volume(uuid, this.registry, (type: string) => {
        assert(volume);
        this.emit('volume', {
          eventType: type,
          object: volume
        });
      }, spec);
      // The volume starts to exist before it is created because we must receive
      // events for it and we want to show to user that it is being created.
      this.volumes[uuid] = volume;
      this.emit('volume', {
        eventType: 'new',
        object: volume
      });

      try {
        await volume.create();
      } catch (err) {
        // undo the pending state
        delete this.volumes[uuid];
        try {
          await volume.destroy();
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
    delete this.volumes[uuid];
  }

  scheduleDestroyVolume(uuid: string) {
    const volume = this.volumes[uuid];
    if (!volume) return;
    volume.scheduleDestroy()
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
  importVolume(uuid: string, spec: any, status: any): Volume {
    let volume = this.volumes[uuid];

    if (volume) {
      volume.update(spec);
    } else {
      // We don't support multiple nexuses yet so take the first one
      let publishedOn = (status.targetNodes || []).pop();
      volume = new Volume(uuid, this.registry, (type: string) => {
        assert(volume);
        this.emit('volume', {
          eventType: type,
          object: volume
        });
      }, spec, status.state, status.size, publishedOn);
      volume.attach();
      volume.state = VolumeState.Unknown;
      this.volumes[uuid] = volume;
      volume.fsa();
    }
    return volume;
  }
}
