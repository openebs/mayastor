// Operations on volumes (create, destroy, list) and cache layer used by the
// higher level code (i.e. csi methods).

'use strict';

const _ = require('lodash');
const assert = require('assert');
const EventEmitter = require('events');
const grpc = require('grpc-uds');
const { GrpcClient, GrpcError } = require('./grpc_client');
const log = require('./logger').Logger('volumes');
const sleep = require('sleep-promise');

// Volume cache with create, destroy and list methods.
class VolumeOperator {
  constructor(nodeOperator) {
    // Nexus objects indexed by uuid
    this.nexus = {};
    // Replica objects indexed by uuid. Note that replica's unique key is
    // (uuid, node) pair, so each entry is an array instead of an object.
    this.replicas = {};
    this.nodes = nodeOperator;
    this.client = new GrpcClient(nodeOperator);
    this.addNodeListener = null;
    this.modPoolListener = null;
    // used to serialize syncs not to have more than one in progress
    this.pendingSync = {};
    // timers for sync retries in case of failures
    this.retrySync = {};
  }

  // Add replica object to the cache. Replica is stored in a list
  // of replicas with the same uuid but different node.
  _addReplica(uuid, pool, node, size, share, uri) {
    let r = {
      uuid,
      pool,
      node,
      size,
      share,
      uri,
    };
    let replicaSet = this.replicas[uuid] || [];
    let idx = replicaSet.findIndex(ent => ent.node === node);
    if (idx >= 0) {
      log.debug(`Updating replica ${r.uuid}@${node} in the cache`);
      replicaSet[idx] = r;
    } else {
      log.debug(`Adding replica ${r.uuid}@${node} to the cache`);
      replicaSet.push(r);
    }
    this.replicas[uuid] = replicaSet;
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
    var self = this;

    log.debug(`Sync of volumes on node "${nodeName}"`);

    try {
      return await self.client.with_handle(nodeName, async handle => {
        var rlist;
        var nlist;

        try {
          rlist = await handle.call('listReplicas', {});
          nlist = await handle.call('listNexus', {});
        } catch (err) {
          log.error(`Failed to list volumes on node "${nodeName}": ` + err);
          return false;
        }
        for (let i = 0; i < rlist.replicas.length; i++) {
          let r = rlist.replicas[i];
          self._addReplica(r.uuid, r.pool, nodeName, r.size, r.share, r.uri);
        }
        for (let i = 0; i < nlist.nexusList.length; i++) {
          let n = nlist.nexusList[i];
          if (self.nexus[n.name]) {
            log.debug(`Updating nexus ${n.uuid} in the cache`);
          } else {
            log.debug(`Adding nexus ${n.uuid} to the cache`);
          }
          self.nexus[n.uuid] = {
            uuid: n.uuid,
            state: n.state,
            children: _.map(n.children, 'uri'),
            size: n.size,
            node: nodeName,
            devicePath: n.devicePath || null,
          };
        }
        return true;
      });
    } catch (err) {
      log.error('Sync of nodes failed: ' + err.toString());
      return false;
    }
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
    log.debug(`Destroying replica "${uuid}@${nodeName}" ...`);

    await this.client.with_handle(nodeName, async handle => {
      try {
        await handle.call('destroyReplica', { uuid: uuid });
      } catch (err) {
        if (err.code != grpc.status.NOT_FOUND) {
          log.error(`Failed to destroy replica "${uuid}@${nodeName}": ` + err);
          throw new GrpcError(
            grpc.status.INTERNAL,
            'Failed to destroy replica: ' + err
          );
        }
      }
    });

    // remove it from the cache
    let replicaSet = this.replicas[uuid];
    if (replicaSet) {
      let idx = replicaSet.findIndex(ent => ent.node == nodeName);
      if (idx >= 0) {
        replicaSet.splice(idx, 1);
        if (replicaSet.length == 0) {
          delete this.replicas[uuid];
        }
        log.info(`Replica "${uuid}@${nodeName}" was destroyed`);
        return;
      }
    }
    log.warn(`Destroyed replica "${uuid}@${nodeName}" was not in the cache`);
  }

  // Destroy nexus on storage node and remove it from the cache.
  // Throws grpc error if error.
  async destroyNexus(nodeName, uuid) {
    log.debug(`Destroying nexus "${uuid}@${nodeName}" ...`);

    await this.client.with_handle(nodeName, async handle => {
      try {
        await handle.call('destroyNexus', { uuid });
      } catch (err) {
        if (err.code != grpc.status.NOT_FOUND) {
          throw new GrpcError(
            grpc.status.INTERNAL,
            `Failed to destroy nexus "${uuid}@${nodeName}": ` + err
          );
        }
      }
    });

    if (this.nexus[uuid]) {
      // remove it from the cache
      delete this.nexus[uuid];
      log.info(`Nexus "${uuid}@${nodeName}" was destroyed`);
    } else {
      log.warn(`Destroyed nexus "${uuid}@${nodeName}" was not in the cache`);
    }
  }

  // Create replica and add it to the cache.
  // Throws a string (error message) if error.
  async createReplica(nodeName, poolName, uuid, size) {
    log.debug(`Creating replica "${uuid}@${nodeName}" ...`);

    let resp = await this.client.with_handle(nodeName, async handle => {
      try {
        return await handle.call('createReplica', {
          uuid: uuid,
          pool: poolName,
          size: size,
          thin: false,
          share: 'NONE',
        });
      } catch (err) {
        throw new GrpcError(
          grpc.status.INTERNAL,
          `Failed to create replica "${uuid}@${nodeName}": ` + err
        );
      }
    });

    // add it to the cache
    this._addReplica(uuid, poolName, nodeName, size, 'NONE', resp.uri);
    log.info(`Created replica "${uuid}@${nodeName}"`);
  }

  // Create nexus and add it to the cache.
  // Throws a string (error message) if error.
  async createNexus(nodeName, uuid, size, children) {
    log.debug(`Creating nexus "${uuid}@${nodeName}" ...`);

    await this.client.with_handle(nodeName, async handle => {
      try {
        await handle.call('createNexus', {
          uuid: uuid,
          size: size,
          children: children,
        });
      } catch (err) {
        throw new GrpcError(
          grpc.status.INTERNAL,
          `Failed to create nexus "${uuid}@${nodeName}": ` + err
        );
      }
    });

    // add it to the cache
    let nexus = {
      uuid: uuid,
      node: nodeName,
      children: children,
      size: size,
      state: 'online', // XXX is the state correct?
      devicePath: null,
    };
    this.nexus[uuid] = nexus;
    log.info(`Created nexus "${uuid}@${nodeName}"`);
    return nexus;
  }

  // Get internal representation of replica(s) with given uuid or all replicas
  // if uuid is not specified.
  // NOTE: The returned replicas must not be modified!
  getReplicaSet(uuid) {
    if (uuid) {
      let rs = this.replicas[uuid] || [];
      // concat is used to make a shallow copy before we return the list
      return rs.concat();
    } else {
      return _.flatten(Object.values(this.replicas));
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
      let nodeName = nodes[i].node;
      let timestamp = new Date().toISOString();

      // Lint does not like using for-loop variable in a function defined
      // in the loop. But we know it's ok in this case.
      // jshint ignore:start
      let res = await this.client.with_handle(nodeName, async handle => {
        log.debug('Retrieving volume stats from node ' + nodeName);
        try {
          return await handle.call('statReplicas', {});
        } catch (err) {
          log.error(`Failed to retrieve stats from node "${nodeName}": ` + err);
          return null;
        }
      });
      if (!res) continue;

      vols = vols.concat(
        res.replicas
          // ignore replicas which we don't know about (yet)
          .filter(r => {
            let replicaSet = self.replicas[r.uuid];
            if (replicaSet) {
              return !!replicaSet.find(ent => ent.node == nodeName);
            }
            return false;
          })
          .map(r => {
            return {
              timestamp,
              // tags
              uuid: r.uuid,
              node: nodeName,
              pool: r.pool,
              // counters
              num_read_ops: r.stats.numReadOps,
              num_write_ops: r.stats.numWriteOps,
              bytes_read: r.stats.bytesRead,
              bytes_written: r.stats.bytesWritten,
            };
          })
      );
      // jshint ignore:end
    }

    return vols;
  }

  async shareReplica(nodeName, uuid, share) {
    assert(['NONE', 'ISCSI', 'NVMF'].indexOf(share) >= 0);
    log.debug(`Setting share protocol for replica "${uuid}@${nodeName}" ...`);
    let r = (this.replicas[uuid] || []).find(r => r.node == nodeName);
    if (!r) {
      throw new GrpcError(
        grpc.status.NOT_FOUND,
        `Replica "${uuid}@${nodeName}" to be shared does not exist`
      );
    }
    let res = await this.client.with_handle(nodeName, async handle => {
      try {
        return await handle.call('shareReplica', { uuid, share });
      } catch (err) {
        throw new GrpcError(
          grpc.status.INTERNAL,
          `Failed to set share pcol for replica "${uuid}@${nodeName}": ` + err
        );
      }
    });
    log.info(
      `Share pcol for replica "${uuid}@${nodeName}" set to ${share} (${res.uri})`
    );
    r.share = share;
    r.uri = res.uri;
  }

  // Publish nexus and return the device path under which it got shared
  async publishNexus(uuid) {
    log.debug(`Publishing nexus "${uuid}" ...`);
    let nexus = this.nexus[uuid];
    if (!nexus) {
      throw new GrpcError(
        grpc.status.NOT_FOUND,
        `Nexus "${uuid}" to be published does not exist`
      );
    }
    let nodeName = nexus.node;
    let res = await this.client.with_handle(nodeName, async handle => {
      try {
        return await handle.call('publishNexus', { uuid: uuid, key: '' });
      } catch (err) {
        throw new GrpcError(
          grpc.status.INTERNAL,
          `Failed to publish nexus ${uuid}@${nodeName}: ` + err
        );
      }
    });
    // TODO: we got switched off the cpu and the nexus might be gone now.
    // Make sure that it cannot happen - problem for all methods here.
    nexus.devicePath = res.devicePath;
    log.info(`Nexus "${uuid}@${nodeName}" was published at ${res.devicePath}`);
    return res.devicePath;
  }

  // Unpublish nexus
  async unpublishNexus(uuid) {
    log.debug(`Unpublishing nexus "${uuid}" ...`);
    let nexus = this.nexus[uuid];
    if (!nexus) {
      throw new GrpcError(
        grpc.status.NOT_FOUND,
        `Nexus "${uuid}" to be unpublished does not exist`
      );
    }
    let nodeName = nexus.node;
    await this.client.with_handle(nodeName, async handle => {
      try {
        await handle.call('unpublishNexus', { uuid });
      } catch (err) {
        throw new GrpcError(
          grpc.status.INTERNAL,
          `Failed to unpublish nexus "${uuid}@${nodeName}": ` + err
        );
      }
    });
    nexus.devicePath = null;
    log.info(`Nexus "${uuid}@${nodeName}" was unpublished`);
  }

  async addChildNexus(uuid, uri) {
    log.debug(`Adding child "${uri}" of nexus "${uuid}" ...`);
    let nexus = this.nexus[uuid];
    if (!nexus) {
      throw new GrpcError(
        grpc.status.NOT_FOUND,
        `Nexus "${uuid}" does not exist`
      );
    }
    await this.client.with_handle(nexus.node, async handle => {
      try {
        await handle.call('addChildNexus', { uuid, uri });
      } catch (err) {
        throw new GrpcError(
          grpc.status.INTERNAL,
          `Failed to add child "${uri}" to nexus "${uuid}": ` + err
        );
      }
    });
    assert(nexus.children.indexOf(uri) == -1);
    nexus.children.push(uri);
  }

  async removeChildNexus(uuid, uri) {
    log.debug(`Removing child "${uri}" of nexus "${uuid}" ...`);
    let nexus = this.nexus[uuid];
    if (!nexus) {
      throw new GrpcError(
        grpc.status.NOT_FOUND,
        `Nexus "${uuid}" does not exist`
      );
    }
    await this.client.with_handle(nexus.node, async handle => {
      try {
        await handle.call('removeChildNexus', { uuid, uri });
      } catch (err) {
        throw new GrpcError(
          grpc.status.INTERNAL,
          `Failed to remove child "${uri}" from nexus "${uuid}": ` + err
        );
      }
    });
    let idx = nexus.children.indexOf(uri);
    if (idx >= 0) {
      nexus.children.splice(idx, 1);
    }
  }
}

// Mock class used in tests where volume operator is required and must be faked.
// We use sleep(1) in async methods to mimic async behaviour of real methods.
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

  getReplicaSet(uuid) {
    if (uuid) {
      return this.replicas.filter(r => r.uuid == uuid);
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
      uuid,
      pool: poolName,
      node: nodeName,
      size,
      share: 'NONE',
      uri: 'bdev:///' + uuid,
    };
    let idx = this.replicas.findIndex(
      r => r.uuid == uuid && r.node == nodeName
    );
    if (idx >= 0) {
      this.replicas[idx] = obj;
    } else {
      this.replicas.push(obj);
    }
  }

  async createNexus(nodeName, uuid, size, children) {
    await sleep(1);
    let err = this.errors.shift();
    if (err) {
      throw err;
    }
    let obj = {
      uuid: uuid,
      node: nodeName,
      size: size,
      state: 'online',
      children: children,
      devicePath: null,
    };
    let idx = this.nexus.findIndex(n => n.uuid == uuid);
    if (idx >= 0) {
      this.nexus[idx] = obj;
    } else {
      this.nexus.push(obj);
    }
    return obj;
  }

  async destroyReplica(nodeName, uuid) {
    await sleep(1);
    let err = this.errors.shift();
    if (err) {
      throw err;
    }
    // modify the list in place to mimic real behaviour
    let idx = this.replicas.findIndex(
      r => r.uuid == uuid && r.node == nodeName
    );
    if (idx >= 0) {
      this.replicas.splice(idx, 1);
    }
  }

  async destroyNexus(nodeName, uuid) {
    await sleep(1);
    let err = this.errors.shift();
    if (err) {
      throw err;
    }
    // modify the list in place to mimic real behaviour
    let idx = this.nexus.findIndex(n => n.uuid == uuid);
    if (idx >= 0) {
      this.nexus.splice(idx, 1);
    }
  }

  async getStats() {
    await sleep(1);
    var self = this;
    return this.replicas.map(r => {
      return {
        timestamp: new Date().toISOString(),
        uuid: r.uuid,
        node: r.node,
        pool: r.pool,
        num_read_ops: self.stat,
        num_write_ops: self.stat,
        bytes_read: self.stat,
        bytes_written: self.stat,
      };
    });
  }

  async shareReplica(node, uuid, share) {
    assert(['NONE', 'ISCSI', 'NVMF'].indexOf(share) >= 0);
    await sleep(1);
    let err = this.errors.shift();
    if (err) {
      throw err;
    }
    let replica = this.replicas.find(r => r.uuid == uuid && r.node == node);
    if (!replica) {
      throw new GrpcError(grpc.status.NOT_FOUND, `Replica "${uuid}" not found`);
    }
    replica.share = share;
    if (share == 'NONE') {
      replica.uri = 'bdev:///' + replica.uuid;
    } else if (share == 'NVMF') {
      replica.uri = 'nvmf://192.168.0.1:4020/nqn.bla.' + replica.uuid;
    } else if (share == 'ISCSI') {
      replica.uri = 'iscsi://192.168.0.1:3800/iqn.bla.' + replica.uuid;
    }
  }

  async publishNexus(uuid) {
    await sleep(1);
    let nexus = this.nexus.find(n => n.uuid == uuid);
    if (!nexus) {
      throw new GrpcError(
        grpc.status.NOT_FOUND,
        `Nexus "${uuid}" to be published does not exist`
      );
    }
    nexus.devicePath = '/dev/nbd0';
  }

  async unpublishNexus(uuid) {
    await sleep(1);
    let nexus = this.nexus.find(n => n.uuid == uuid);
    if (!nexus) {
      throw new GrpcError(
        grpc.status.NOT_FOUND,
        `Nexus "${uuid}" to be unpublished does not exist`
      );
    }
    nexus.devicePath = null;
  }

  async addChildNexus(uuid, uri) {
    await sleep(1);
    let nexus = this.nexus.find(n => n.uuid == uuid);
    if (!nexus) {
      throw new GrpcError(
        grpc.status.NOT_FOUND,
        `Nexus "${uuid}" does not exist`
      );
    }
    nexus.children.push(uri);
  }

  async removeChildNexus(uuid, uri) {
    await sleep(1);
    let nexus = this.nexus.find(n => n.uuid == uuid);
    if (!nexus) {
      throw new GrpcError(
        grpc.status.NOT_FOUND,
        `Nexus "${uuid}" does not exist`
      );
    }
    // modify the list in place to mimic real behaviour
    let idx = nexus.children.findIndex(ch => ch == uri);
    if (idx >= 0) {
      nexus.children.splice(idx, 1);
    }
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
