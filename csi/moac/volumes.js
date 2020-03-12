// Volume manager implementation.

'use strict';

const assert = require('assert');
const EventEmitter = require('events');
const EventStream = require('./event_stream');
const Volume = require('./volume');
const { GrpcCode, GrpcError } = require('./grpc_client');
const log = require('./logger').Logger('volumes');

// Volume manager that emit events for new/modified/deleted volumes.
class Volumes extends EventEmitter {
  constructor(registry) {
    super();
    this.registry = registry;
    this.events = null; // stream of events from registry
    this.volumes = {}; // volumes indexed by uuid
  }

  start() {
    var self = this;
    this.events = new EventStream({ registry: this.registry });
    this.events.on('data', async function(ev) {
      if (ev.kind != 'replica' && ev.kind != 'nexus') {
        // not interesed in node and pool events
        return;
      }
      let uuid = ev.object.uuid;
      let volume = self.volumes[uuid];
      if (!volume) {
        // Ignore events for volumes that do not exist. Those might be events
        // related to a volume that is being destroyed.
        log.debug(`${ev.eventType} event for unknown volume "${uuid}"`);
        return;
      }
      if (ev.kind == 'replica') {
        if (ev.eventType == 'new') {
          volume.newReplica(ev.object);
        } else if (ev.eventType == 'mod') {
          volume.modReplica(ev.object);
        } else if (ev.eventType == 'del') {
          volume.delReplica(ev.object);
        }
      } else if (ev.kind == 'nexus') {
        if (ev.eventType == 'new') {
          volume.newNexus(ev.object);
        } else if (ev.eventType == 'mod') {
          volume.modNexus(ev.object);
        } else if (ev.eventType == 'del') {
          volume.delNexus(ev.object);
        }
      }
      self.emit('volume', {
        eventType: 'mod',
        object: volume,
      });
    });
  }

  stop() {
    this.events.destroy();
    this.events.removeAllListeners();
    this.events = null;
  }

  // Return a volume with specified uuid or all volumes if called without
  // an argument.
  //
  // @param   {string}          uuid   ID of the volume.
  // @returns {object|object[]} Matching volume (or null if not found) or all volumes.
  //
  get(uuid) {
    if (uuid) return this.volumes[uuid] || null;
    else return Object.values(this.volumes);
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
  // @returns {object}   New volume object.
  //
  async createVolume(uuid, spec) {
    let created = false;

    if (!spec.requiredBytes || spec.requiredBytes < 0) {
      throw new GrpcError(
        GrpcCode.INVALID_ARGUMENT,
        'Required bytes must be greater than zero'
      );
    }
    let volume = this.volumes[uuid];
    if (volume) {
      if (!volume.update(spec)) {
        // exactly the same volume exists - no work
        return volume;
      } else {
        // TODO: What to do if size changes and is incompatible?
        this.emit('volume', {
          eventType: 'mod',
          object: volume,
        });
      }
    } else {
      created = true;
      volume = new Volume(uuid, this.registry, spec);
      // The volume starts to exist before it is created because we must receive
      // events for it and we want to show to user that it is being created.
      this.volumes[uuid] = volume;
      this.emit('volume', {
        eventType: 'new',
        object: volume,
      });
      // check for components that already exist and assign them to the volume
      var self = this;
      this.registry.getReplicaSet(uuid).forEach(r => volume.newReplica(r));
      let nexus = this.registry.getNexus(uuid);
      if (nexus) {
        volume.newNexus(nexus);
      }
    }
    try {
      await volume.ensure();
    } catch (err) {
      // undo the PENDING state
      if (created) {
        delete this.volumes[uuid];
        this.emit('volume', {
          eventType: 'del',
          object: volume,
        });
      }
      throw err;
    }
    return volume;
  }

  // Destroy the volume.
  //
  // The method is idempotent - if the volume does not exist it does not return
  // an error.
  //
  // @param   {string}   uuid            ID of the volume.
  //
  async destroyVolume(uuid) {
    let volume = this.volumes[uuid];
    if (!volume) return;

    await volume.destroy();
    delete this.volumes[uuid];
    this.emit('volume', {
      eventType: 'del',
      object: volume,
    });
  }
}

module.exports = Volumes;
