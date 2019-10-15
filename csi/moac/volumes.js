// Operations on volumes (create, destroy, list) and cache layer used by the
// csi plugin.

'use strict';

const assert = require('assert');
const EventEmitter = require('events');
const grpc = require('grpc-uds');
const grpc_promise = require('grpc-promise');
const _ = require('lodash');
const { mayastor, GrpcError } = require('./common');
const log = require('./logger').Logger('volumes');

// Volume cache with create, destroy and list methods.
class VolumeOperator {
  constructor(nodeOperator) {
    this.replicas = {};
    this.nexus = {};
    this.nodes = nodeOperator;
    this.addNodeListener = null;
    this.modPoolListener = null;
    // used to serialize syncs not to have more than one in progress
    this.pendingSync = {};
    // timers for sync retries in case of failures
    this.retrySync = {};
  }

  _createClient(node) {
    let client = new mayastor.Mayastor(
      node.endpoint,
      grpc.credentials.createInsecure()
    );
    grpc_promise.promisifyAll(client);
    return client;
  }

  // Get grpc mayastor service client for particular storage node.
  // Return null if there is not a node with such a name.
  _getNodeClient(nodeName) {
    let node = this.nodes.get(nodeName);

    if (!node) {
      return null;
    }
    return this._createClient(node);
  }

  // Do a complete sync of all volumes (replicas and nexus's) on all nodes.
  // We do not remove any volume from cache, because just because the storage
  // node disappeared does not mean the volume stopped to exist. It may rejoin
  // the cluster later and from k8s perspective the PV is still there until
  // explicitly deleted.
  async start() {
    var self = this;
    var nodes = self.nodes.get();
    assert(nodes);

    self.addNodeListener = function(ev) {
      self.syncNode(ev.node);
    };
    self.removeNodeListener = function(ev) {
      if (self.retrySync[ev.node]) {
        clearTimeout(self.retrySync[ev.node]);
        delete self.retrySync[ev.node];
      }
      if (self.pendingSync[ev.node]) {
        delete self.pendingSync[ev.node];
      }
    };
    self.nodes.on('add', self.addNodeListener);
    self.nodes.on('remove', self.removeNodeListener);

    for (let i = 0; i < nodes.length; i++) {
      await self.syncNode(nodes[i].node);
    }
  }

  // Stop listening for node add events and reset the cache
  async stop() {
    if (this.addNodeListener) {
      this.nodes.removeListener('add', this.addNodeListener);
    }
    if (this.removeNodeListener) {
      this.nodes.removeListener('remove', this.removeNodeListener);
    }
    for (let i in this.retrySync) {
      clearTimeout(this.retrySync[i]);
    }
    this.retrySync = {};
    this.replicas = {};
    this.nexus = {};
  }

  // Add replicas and nexus's from a particular storage node to the cache.
  // Return false if the sync failed, otherwise true.
  //
  // TODO: Implement node sync retry. So when a list of volumes on node fails,
  // it is retried in the future. This requires using 'remove' event from node
  // operator too so that we know when to stop retries for the node.
  async _syncNode(nodeName) {
    log.debug(`Sync of volumes on node "${nodeName}"`);
    let client = this._getNodeClient(nodeName);
    if (!client) {
      log.error(`Failed to get client for node "${nodeName}"`);
      return false;
    }
    var replicas;
    var nexus;
    try {
      replicas = await client.listReplicas().sendMessage({});
      nexus = await client.listNexus().sendMessage({});
    } catch (err) {
      log.error(`Failed to list volumes on node "${nodeName}": ` + err);
      return false;
    } finally {
      client.close();
    }
    for (let i = 0; i < replicas.replicas.length; i++) {
      let r = replicas.replicas[i];
      if (this.replicas[r.uuid]) {
        log.debug(`Updating replica ${r.uuid} in the cache`);
      } else {
        log.debug(`Adding replica ${r.uuid} to the cache`);
      }
      this.replicas[r.uuid] = {
        uuid: r.uuid,
        pool: r.pool,
        node: nodeName,
        size: r.size,
      };
    }
    for (let i = 0; i < nexus.nexusList.length; i++) {
      let n = nexus.nexusList[i];
      if (this.nexus[n.name]) {
        log.debug(`Updating nexus ${n.uuid} in the cache`);
      } else {
        log.debug(`Adding nexus ${n.uuid} to the cache`);
      }
      this.nexus[n.uuid] = {
        uuid: n.uuid,
        state: n.state,
        children: _.map(n.children, 'uri'),
        size: n.size,
        node: nodeName,
        devicePath: n.devicePath || null,
      };
    }
    return true;
  }

  // This wrapper ensures that there is just one sync for given node running
  // at any given time.
  async syncNode(nodeName) {
    var self = this;

    if (this.pendingSync[nodeName]) {
      // tell sync executor to run another sync when it is done
      if (this.pendingSync[nodeName] == 1) this.pendingSync[nodeName]++;
      return;
    }

    this.pendingSync[nodeName] = 1;
    let ok = await this._syncNode(nodeName);
    if (!this.pendingSync[nodeName]) {
      // the node has been removed while syncing
      return;
    }
    this.pendingSync[nodeName]--;

    if (this.pendingSync[nodeName] > 0) {
      setTimeout(() => self._syncNode(nodeName), 0);
      return;
    }

    if (!ok) {
      // retry the sync after timeout
      this.retrySync[nodeName] = setTimeout(() => {
        delete self.retrySync[nodeName];
        self.syncNode(nodeName);
      }, exports.retrySyncInterval);
    }
  }

  // Destroy replica on storage node and remove it from the cache.
  // Throws grpc error if error.
  async destroyReplica(nodeName, uuid) {
    let client = this._getNodeClient(nodeName);
    if (!client) {
      throw new GrpcError(
        grpc.status.INTERNAL,
        `Failed to obtain grpc client for node "${nodeName}"`
      );
    }

    try {
      await client.destroyReplica().sendMessage({ uuid: uuid });
    } catch (err) {
      if (err.code != grpc.status.NOT_FOUND) {
        throw new GrpcError(
          grpc.status.INTERNAL,
          'Failed to destroy replica: ' + err
        );
      }
    } finally {
      client.close();
    }

    // remove it from the cache
    delete this.replicas[uuid];
  }

  // Destroy nexus on storage node and remove it from the cache.
  // Throws grpc error if error.
  async destroyNexus(nodeName, uuid) {
    let client = this._getNodeClient(nodeName);
    if (!client) {
      throw new GrpcError(
        grpc.status.INTERNAL,
        `Failed to obtain grpc client for node "${nodeName}"`
      );
    }

    try {
      await client.destroyNexus().sendMessage({ name: uuid });
    } catch (err) {
      if (err.code != grpc.status.NOT_FOUND) {
        throw new GrpcError(
          grpc.status.INTERNAL,
          'Failed to destroy nexus: ' + err
        );
      }
    } finally {
      client.close();
    }

    // remove it from the cache
    delete this.nexus[uuid];
  }

  // Create replica and add it to the cache.
  // Throws a string (error message) if error.
  async createReplica(nodeName, poolName, uuid, size) {
    let client = this._getNodeClient(nodeName);
    if (!client) {
      throw `Failed to obtain grpc client for node "${nodeName}"`;
    }

    try {
      await client.createReplica().sendMessage({
        uuid: uuid,
        pool: poolName,
        size: size,
        thin: false,
        share: 'NONE',
      });
    } catch (err) {
      throw new GrpcError(
        grpc.status.INTERNAL,
        `Failed to create replica ${uuid} on pool "${poolName}": ` + err
      );
    } finally {
      client.close();
    }

    // add it to the cache
    this.replicas[uuid] = {
      uuid: uuid,
      pool: poolName,
      node: nodeName,
      size: size,
    };
  }

  // Create nexus and add it to the cache.
  // Throws a string (error message) if error.
  async createNexus(nodeName, uuid, size, children) {
    let client = this._getNodeClient(nodeName);
    if (!client) {
      throw `Failed to obtain grpc client for node "${nodeName}"`;
    }

    try {
      await client.createNexus().sendMessage({
        uuid: uuid,
        size: size,
        children: children,
      });
    } catch (err) {
      throw new GrpcError(
        grpc.status.INTERNAL,
        `Failed to create nexus ${uuid} on node "${nodeName}": ` + err
      );
    } finally {
      client.close();
    }

    // add it to the cache
    this.nexus[uuid] = {
      uuid: uuid,
      node: nodeName,
      children: children,
      size: size,
      state: 'online', // XXX is the state correct?
      devicePath: null,
    };
  }

  // Get internal representation of replica with given uuid or all replicas
  // if uuid is not specified.
  // NOTE: The returned value must not be modified!
  getReplica(uuid) {
    if (uuid) {
      return this.replicas[uuid];
    } else {
      return Object.values(this.replicas);
    }
  }

  // Get internal representation of nexus with given uuid or all nexus's
  // if uuid is not specified.
  // NOTE: The returned value must not be modified!
  getNexus(uuid) {
    if (uuid) {
      return this.nexus[uuid];
    } else {
      return Object.values(this.nexus);
    }
  }

  // TODO: should return stats for nexus rather than for replica
  async getStats() {
    var self = this;
    var vols = [];
    var nodes = self.nodes.get();

    for (let i in nodes) {
      let client = self._createClient(nodes[i]);
      let res;

      log.debug('Retrieving volume stats from node ' + nodes[i].node);

      try {
        res = await client.statReplicas().sendMessage({});
      } catch (err) {
        log.error(
          `Failed to retrieve stats from node "${nodes[i].node}": ` + err
        );
        continue;
      } finally {
        client.close();
      }

      // jshint ignore:start
      vols = vols.concat(
        res.replicas
          .filter(r => !!self.replicas[r.uuid])
          .map(r => {
            return {
              volume: r.uuid,
              pool: r.pool,
              stats: {
                num_read_ops: r.stats.numReadOps,
                num_write_ops: r.stats.numWriteOps,
                bytes_read: r.stats.bytesRead,
                bytes_written: r.stats.bytesWritten,
              },
            };
          })
      );
      // jshint ignore:end
    }

    return vols;
  }

  // Publish nexus and return the device path under which it got shared
  async publishNexus(uuid) {
    let nexus = this.nexus[uuid];
    if (!nexus) {
      throw new GrpcError(
        grpc.status.INTERNAL,
        `Nexus "${uuid}" to be published does not exist`
      );
    }
    let client = this._getNodeClient(nexus.node);
    if (!client) {
      throw new GrpcError(
        grpc.status.INTERNAL,
        `Failed to obtain grpc client handle for node "${nexus.node}"`
      );
    }
    var res;
    try {
      res = await client.publishNexus().sendMessage({ uuid: uuid , key : '' });
    } catch (err) {
      throw new GrpcError(
        grpc.status.INTERNAL,
        `Failed to publish nexus ${uuid}: ` + err
      );
    } finally {
      client.close();
    }
    // we got switched off the cpu and the nexus might be gone now
    nexus = this.nexus[uuid];
    if (!nexus) {
      throw new GrpcError(
        grpc.status.INTERNAL,
        `Nexus ${uuid} was destroyed before it got shared`
      );
    }
    nexus.devicePath = res.devicePath;
    return res.devicePath;
  }

  // Unpublish nexus
  async unpublishNexus(uuid) {
    let nexus = this.nexus[uuid];
    if (!nexus) {
      throw new GrpcError(
        grpc.status.INTERNAL,
        `Nexus "${uuid}" to be unpublished does not exist`
      );
    }
    let client = this._getNodeClient(nexus.node);
    if (!client) {
      throw new GrpcError(
        grpc.status.INTERNAL,
        `Failed to obtain grpc client handle for node "${nexus.node}"`
      );
    }
    try {
      await client.unpublishNexus().sendMessage({ uuid: uuid });
    } catch (err) {
      throw new GrpcError(
        grpc.status.INTERNAL,
        `Failed to unpublish nexus ${uuid}: ` + err
      );
    } finally {
      client.close();
    }
    // we got switched off the cpu and the nexus might be gone now
    nexus = this.nexus[uuid];
    if (nexus) {
      nexus.devicePath = null;
    }
  }
}

// Mock class used in tests where volume operator is required and must be faked
class VolumeOperatorMock extends EventEmitter {
  // The first arg is list of volumes which will be put into the cache
  // Second arg is stat value returned by getStats call.
  constructor(nexus, replicas, stat) {
    super();
    this.nexus = nexus || [];
    this.replicas = replicas || [];
    this.errors = [];
    this.stat = stat || 0;
  }

  getReplica(uuid) {
    if (uuid) {
      return this.replicas.find(r => r.uuid == uuid);
    } else {
      return this.replicas;
    }
  }

  getNexus(uuid) {
    if (uuid) {
      return this.nexus.find(n => n.uuid == uuid);
    } else {
      return this.nexus;
    }
  }

  async createReplica(nodeName, poolName, uuid, size) {
    let err = this.errors.shift();
    if (err) {
      throw err;
    }
    let obj = {
      uuid: uuid,
      pool: poolName,
      node: nodeName,
      size: size,
    };
    let idx = this.replicas.findIndex(r => r.uuid == uuid);
    if (idx >= 0) {
      this.replicas[idx] = obj;
    } else {
      this.replicas.push(obj);
    }
  }

  async createNexus(nodeName, uuid, size, children) {
    let err = this.errors.shift();
    if (err) {
      throw err;
    }
    let obj = {
      uuid: uuid,
      node: nodeName,
      size: size,
      children: children,
      devicePath: null,
    };
    let idx = this.nexus.findIndex(n => n.uuid == uuid);
    if (idx >= 0) {
      this.nexus[idx] = obj;
    } else {
      this.nexus.push(obj);
    }
  }

  async destroyReplica(nodeName, uuid) {
    let err = this.errors.shift();
    if (err) {
      throw err;
    }
    this.replicas = this.replicas.filter(r => r.uuid != uuid);
  }

  async destroyNexus(nodeName, uuid) {
    let err = this.errors.shift();
    if (err) {
      throw err;
    }
    this.nexus = this.nexus.filter(n => n.uuid != uuid);
  }

  async getStats() {
    var self = this;
    return this.replicas.map(r => {
      return {
        uuid: r.uuid,
        pool: r.pool,
        stats: {
          num_read_ops: self.stat,
          num_write_ops: self.stat,
          bytes_read: self.stat,
          bytes_written: self.stat,
        },
      };
    });
  }

  async publishNexus(uuid) {
    let nexus = this.nexus.find(n => n.uuid == uuid);
    if (!nexus) {
      throw new GrpcError(
        grpc.status.INTERNAL,
        `Nexus "${uuid}" to be published does not exist`
      );
    }
    nexus.devicePath = '/dev/nbd0';
  }

  async unpublishNexus(uuid) {
    let nexus = this.nexus.find(n => n.uuid == uuid);
    if (!nexus) {
      throw new GrpcError(
        grpc.status.INTERNAL,
        `Nexus "${uuid}" to be unpublished does not exist`
      );
    }
    nexus.devicePath = null;
  }

  injectError(err) {
    this.errors.push(err);
  }
}

var exports = {
  VolumeOperator,
  // the rest is for testing
  VolumeOperatorMock,
  retrySyncInterval: 60000, // retry after 1 min
};

module.exports = exports;
