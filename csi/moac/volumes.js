// Operations on volumes (create, destroy, list) and cache layer used by the
// csi plugin.

'use strict';

const assert = require('assert');
const EventEmitter = require('events');
const grpc = require('grpc-uds');
const grpc_promise = require('grpc-promise');
const { mayastor, GrpcError } = require('./common');
const log = require('./logger').Logger('volumes');

// Create k8s volume object as returned by CSI list volumes method.
function createK8sVolumeObject(obj) {
  if (!obj) return obj;
  return {
    volumeId: obj.uuid,
    capacityBytes: obj.size,
    accessibleTopology: [
      {
        segments: { 'kubernetes.io/hostname': obj.node },
      },
    ],
  };
}

// Volume cache with create, destroy and list methods.
class VolumeOperator {
  constructor(nodeOperator) {
    this.volumes = {};
    this.nodes = nodeOperator;
    this.addNodeListener = null;
    this.modPoolListener = null;
    // used to serialize syncs not to have more than one in progress
    this.pendingSync = {};
    // timers for sync retries in case of failures
    this.retrySync = {};
  }

  // TODO: We use Mayastor for v0.1 but later moac will have to use ingress
  // service for handling multiple replicas
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

  // Do a complete sync of all volumes on all nodes.
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
    this.volumes = [];
  }

  // Add volumes from a particular storage node to the cache.
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
    var res;
    try {
      res = await client.listReplicas().sendMessage({});
    } catch (err) {
      log.error(`Failed to list replicas on node "${nodeName}": ` + err);
      return false;
    } finally {
      client.close();
    }
    for (let i = 0; i < res.replicas.length; i++) {
      let r = res.replicas[i];
      if (this.volumes[r.uuid]) {
        log.debug(`Adding volume ${r.uuid} to the cache`);
      } else {
        log.debug(`Updating volume ${r.uuid} in the cache`);
      }
      this.volumes[r.uuid] = {
        uuid: r.uuid,
        pool: r.pool,
        node: nodeName,
        size: r.size,
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

  // Destroy volume on storage node and remove it from the cache.
  // Throws grpc error if error.
  async destroy(nodeName, uuid) {
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
    delete this.volumes[uuid];
  }

  // Create volume and add it to the cache.
  // Throws a string (error message) if error.
  async create(nodeName, poolName, uuid, size) {
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
        `Failed to create volume ${uuid} on pool "${poolName}": ` + err
      );
    } finally {
      client.close();
    }

    // add it to the cache
    this.volumes[uuid] = {
      uuid: uuid,
      pool: poolName,
      node: nodeName,
      size: size,
    };
  }

  async listReplicas(nodeName) {
    let client = this._getNodeClient(nodeName);
    let volumes = await client.listReplicas().sendMessage({});
    if (!client) {
      throw `Failed to obtain grpc client for node "${nodeName}"`;
    }
    try {
    } catch (err) {
      throw new GrpcError(
        grpc.status.INTERNAL,
        `Failed to list replicas for  node "${nodeName}"`
      );
    } finally {
      client.close();
    }
    return volumes;
  }

  // Get volume (internal representation) with given uuid or all if uuid
  // is not specified.
  // NOTE: The returned value must not be modified!
  get(uuid) {
    if (uuid) {
      return this.volumes[uuid];
    } else {
      return Object.values(this.volumes);
    }
  }

  // Return snapshot (shallow-copy) of volume k8s objects
  snapshot() {
    return Object.values(this.volumes).map(createK8sVolumeObject);
  }

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
          .filter(r => !!self.volumes[r.uuid])
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

  async createBlkdev(nodeName, uuid) {
    let client = this._getNodeClient(nodeName);
    if (!client) {
      throw `Failed to obtain grpc client handle for node  "${nodeName}"`;
    }
    try {
      await client.createBlkdev().sendMessage({
        replica: uuid,
      });
    } catch (err) {
      throw new GrpcError(
        grpc.status.INTERNAL,
        `Failed to create blkdev for volume ${uuid}: ` + err
      );
    }
  }

  async destroyBlkdev(nodeName, uuid) {
    let client = this._getNodeClient(nodeName);
    if (!client) {
      throw `Failed to obtain grpc client handle for node  "${nodeName}"`;
    }
    try {
      await client.destroyBlkdev().sendMessage({
        replica: uuid,
      });
    } catch (err) {
      throw new GrpcError(
        grpc.status.INTERNAL,
        `Failed to destroy blkdev for volume ${uuid}: ` + err
      );
    }
  }
}

// Mock class used in tests where volume operator is required and must be faked
class VolumeOperatorMock extends EventEmitter {
  // The first arg is list of volumes which will be put into the cache
  // Second arg is stat value returned by getStats call.
  constructor(volumes, stat) {
    super();
    this.volumes = volumes || [];
    this.errors = [];
    this.stat = stat || 0;
  }

  // Get volume with given name
  get(uuid) {
    if (uuid) {
      return this.volumes.find(v => v.uuid == uuid);
    } else {
      return this.volumes;
    }
  }

  // Return snapshot (shallow-copy) of volume list
  snapshot() {
    return this.volumes.map(createK8sVolumeObject);
  }

  async create(nodeName, poolName, uuid, size) {
    let err = this.errors.shift();
    if (err) {
      throw err;
    }
    let obj = {
      uuid: uuid,
      pool: poolName,
      node: nodeName,
      size: size,
      dev: null, // a field only present in mock for testing (un)publish
    };
    let idx = this.volumes.findIndex(v => v.uuid == uuid);
    if (idx >= 0) {
      this.volumes[idx] = obj;
    } else {
      this.volumes.push(obj);
    }
  }

  async destroy(nodeName, uuid) {
    let err = this.errors.shift();
    if (err) {
      throw err;
    }
    this.volumes = this.volumes.filter(v => v.uuid != uuid);
  }

  async getStats() {
    var self = this;
    return this.volumes.map(v => {
      return {
        volume: v.uuid,
        pool: v.pool,
        stats: {
          num_read_ops: self.stat,
          num_write_ops: self.stat,
          bytes_read: self.stat,
          bytes_written: self.stat,
        },
      };
    });
  }

  async createBlkdev(noneName, uuid) {
    let vol = this.volumes.find(v => v.uuid == uuid);
    assert(vol);
    assert(!vol.dev);
    vol.dev = '/dev/nbd0';
  }

  async destroyBlkdev(noneName, uuid) {
    let vol = this.volumes.find(v => v.uuid == uuid);
    assert(vol);
    assert(vol.dev);
    delete vol.dev;
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
