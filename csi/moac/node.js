// Abstraction representing a storage node with its objects (nexus, pools,
// replicas). Consumers can use it to receive information about the storage
// objects and notifications about the changes.

'use strict';

const assert = require('assert');
const EventEmitter = require('events');
const Workq = require('./workq');
const Nexus = require('./nexus');
const Pool = require('./pool');
const Replica = require('./replica');
const log = require('./logger').Logger('node');
const { GrpcClient, GrpcCode, GrpcError } = require('./grpc_client');

// Object represents mayastor storage node.
//
// Node emits following events:
// "node": node related events with payload { eventType: "sync", object: node }
//         when the node is sync'd after previous sync failure(s).
// "pool", "replica", "nexus": with eventType "new", "mod", "del".
class Node extends EventEmitter {
  // Create a storage node object.
  //
  // @param {string} name              Node name.
  // @param {Object} [opts]            Options
  // @param {number} opts.syncPeriod   How often to sync healthy node (in ms).
  // @param {number} opts.syncRetry    How often to retry sync if it failed (in ms).
  // @param {number} opts.syncBadLimit Flip the node to offline state after this many retries have failed.
  constructor(name, opts) {
    opts = opts || {};

    super();
    this.name = name;
    this.syncPeriod = opts.syncPeriod || 60000;
    this.syncRetry = opts.syncRetry || 10000;
    this.syncBadLimit = opts.syncBadLimit || 0;

    this.endpoint = null;
    this.client = null; // grpc client handle
    this.workq = new Workq(); // work queue for serializing grpc calls
    // We don't want to switch all objects to offline state when moac starts
    // just because a node is not reachable from the beginning. That's why we
    // set syncFailed to syncBadLimit + 1.
    this.syncFailed = this.syncBadLimit + 1; // 0 if last sync was successful
    this.syncTimer = null; // timer for periodic sync of the node

    // cache of objects from storage node
    this.nexus = [];
    this.pools = [];
  }

  // Stringify node object.
  toString() {
    return this.name;
  }

  // Create grpc connection to the mayastor server
  connect(endpoint) {
    if (this.client) {
      if (this.endpoint == endpoint) {
        // nothing changed
        return;
      } else {
        log.info(
          `mayastor endpoint on node "${this.name}" changed from "${this.endpoint}" to "${endpoint}"`
        );
        this.client.close();
        clearTimeout(this.syncTimer);
      }
    } else {
      log.info(`new mayastor node "${this.name}" with endpoint "${endpoint}"`);
    }
    this.endpoint = endpoint;
    this.client = new GrpcClient(endpoint);
    this.sync();
  }

  // Close the grpc connection
  disconnect() {
    log.info(`mayastor on node "${this.name}" is gone`);
    assert(this.client);
    this.client.close();
    this.client = null;
    clearTimeout(this.syncTimer);
    this.syncTimer = null;
    this.syncFailed = this.syncBadLimit + 1;
    this._offline();
  }

  // The node is considered broken, emit offline events on all objects
  // that are present on the node.
  _offline() {
    this.pools.forEach(pool => pool.offline());
    this.nexus.forEach(nexus => nexus.offline());
  }

  // Call grpc method on storage node. The calls are serialized in order
  // to prevent race conditions and inconsistencies.
  //
  // @param {string} method  gRPC method name.
  // @param {object} args    Arguments for gRPC method.
  // @returns {object} A promise that evals to return value of gRPC method.
  //
  async call(method, args) {
    return await this.workq.push({ method, args }, this._call.bind(this));
  }

  async _call(ctx) {
    if (!this.client) {
      throw new GrpcError(
        GrpcCode.INTERNAL,
        `Broken connection to mayastor on node "${this.name}"`
      );
    }
    return await this.client.call(ctx.method, ctx.args);
  }

  // Sync triggered by the timer. It ensures that the sync does run in
  // parallel with any other rpc call or another sync.
  async sync() {
    var nextSync;
    this.syncTimer = null;

    try {
      await this.workq.push({}, this._sync.bind(this));
      nextSync = this.syncPeriod;
    } catch (err) {
      // We don't want to cover up unexpected errors. But it's hard to
      // differenciate between expected and unexpected errors. At least we try.
      if (!(err instanceof GrpcError) && !err.code) {
        throw err;
      }
      nextSync = this.syncRetry;
      if (this.syncFailed++ == this.syncBadLimit) {
        log.error(`The node "${this.name}" is out of sync: ${err}`);
        this._offline();
      } else if (this.syncFailed <= this.syncBadLimit) {
        log.warn(`Failed to sync the node "${this.name}": ${err}`);
      }
    }

    // if still connected then schedule next sync
    if (!this.syncTimer && this.client) {
      this.syncTimer = setTimeout(this.sync.bind(this), nextSync);
    }
  }

  // Synchronize nexus, replicas and pools. Called from work queue so it cannot
  // interfere with other grpc calls.
  async _sync() {
    var reply;
    var pools, nexus, replicas;

    log.debug(`Syncing the node "${this.name}"`);

    reply = await this._call({
      method: 'listNexus',
      args: {},
    });
    nexus = reply.nexusList;
    reply = await this._call({
      method: 'listPools',
      args: {},
    });
    pools = reply.pools;
    reply = await this._call({
      method: 'listReplicas',
      args: {},
    });
    replicas = reply.replicas;

    // merge pools and replicas
    this._mergePoolsAndReplicas(pools, replicas);
    // merge nexus
    this._mergeNexus(nexus);

    log.debug(`The node "${this.name}" was successfully synced`);

    if (this.syncFailed > 0) {
      this.syncFailed = 0;
      this.emit('node', {
        eventType: 'sync',
        object: this,
      });
    }
  }

  // Merge information about pools and replicas obtained from storage node
  // with the information we knew before. Add, remove and update existing
  // objects as necessary.
  //
  // @param {object[]} pools    New pools with properties.
  // @param {object[]} replicas New replicas with properties.
  //
  _mergePoolsAndReplicas(pools, replicas) {
    var self = this;
    // detect modified and new pools
    pools.forEach(props => {
      let poolReplicas = replicas.filter(r => r.pool == props.name);
      let pool = self.pools.find(p => p.name == props.name);
      if (pool) {
        // the pool already exists - update it
        pool.merge(props, poolReplicas);
      } else {
        // it is a new pool
        self._registerPool(new Pool(props), poolReplicas);
      }
    });
    // remove pools that no longer exist
    self.pools
      .filter(p => !pools.find(ent => ent.name == p.name))
      .forEach(p => p.unbind());
  }

  // Compare list of existing nexus with nexus properties obtained from
  // storage node and:
  //
  // 1. call merge nexus method if the nexus was found
  // 2. create a new nexus based on the properties if not found
  // 3. remove the nexus if it no longer exists
  //
  // These actions will further emit new/mod/del events to inform other
  // components about the changes.
  //
  // @param {object[]} nexusList  List of nexus obtained from storage node.
  //
  _mergeNexus(nexusList) {
    var self = this;
    // detect modified and new pools
    nexusList.forEach(props => {
      let nexus = self.nexus.find(n => n.uuid == props.uuid);
      if (nexus) {
        // the nexus already exists - update it
        nexus.merge(props);
      } else {
        // it is a new nexus
        self._registerNexus(new Nexus(props, []));
      }
    });
    // remove nexus that no longer exist
    let removedNexus = self.nexus.filter(
      n => !nexusList.find(ent => ent.uuid == n.uuid)
    );
    removedNexus.forEach(n => n.destroy());
  }

  // Push the new pool to a list of pools of this node.
  //
  // @param {object}   pool        New pool object.
  // @param {object[]} [replicas]  New replicas on the pool.
  //
  _registerPool(pool, replicas) {
    assert(!this.pools.find(p => p.name == pool.name));
    this.pools.push(pool);
    pool.bind(this);
    replicas = replicas || [];
    replicas.forEach(r => pool.registerReplica(new Replica(r)));
  }

  // Remove the pool from list of pools of this node.
  //
  // @param {object} pool  The pool to be deregistered from the node.
  //
  unregisterPool(pool) {
    let idx = this.pools.indexOf(pool);
    if (idx >= 0) {
      this.pools.splice(idx, 1);
    } else {
      log.warn(
        `Pool "${pool}" is being deregistered and not assigned to the node "${this.name}"`
      );
    }
  }

  // Push the new nexus to a nexus list of this node.
  //
  // @param {object} nexus      New nexus object.
  //
  _registerNexus(nexus) {
    assert(!this.nexus.find(p => p.uuid == nexus.uuid));
    this.nexus.push(nexus);
    nexus.bind(this);
  }

  // Remove the nexus from list of nexus's for the node.
  //
  // @param {object} nexus  The nexus to be deregistered from the node.
  //
  unregisterNexus(nexus) {
    let idx = this.nexus.indexOf(nexus);
    if (idx >= 0) {
      this.nexus.splice(idx, 1);
    } else {
      log.warn(
        `Nexus "${nexus}" is being deregistered and not assigned to the node "${this.name}"`
      );
    }
  }

  // Get all replicas across all pools on this node.
  //
  // @returns {object[]}  All replicas on this node.
  getReplicas() {
    return this.pools.reduce((acc, pool) => acc.concat(pool.replicas), []);
  }

  // Return true if the node is considered healthy which means that its state
  // is synchronized with the state maintained on behalf of this node object.
  //
  // @returns {boolean} True if the node is healthy, false otherwise.
  //
  isSynced() {
    return this.syncFailed <= this.syncBadLimit;
  }

  // Create storage pool on this node.
  //
  // @param {string}   name   Name of the new pool.
  // @param {string[]} disks  List of disk devices for the pool.
  // @returns {object} New pool object.
  //
  async createPool(name, disks) {
    log.debug(`Creating pool "${name}@${this.name}" ...`);

    try {
      await this.call('createPool', { name, disks });
      log.info(`Created pool "${name}@${this.name}"`);
    } catch (err) {
      // TODO: Make rpc idempotent
      if (err.code != GrpcCode.ALREADY_EXISTS) {
        throw err;
      }
    }

    // it's not done yet, we have to get properties of the pool to
    // obtain its state, capacity, etc.
    var resp;
    try {
      resp = await this.call('listPools', {});
    } catch (err) {
      throw new GrpcError(
        GrpcCode.INTERNAL,
        `Failed to list new pool "${name}": ${err}`
      );
    }
    var poolInfo = resp.pools.filter(p => p.name == name)[0];
    if (!poolInfo) {
      throw new GrpcError(GrpcCode.INTERNAL, `New pool "${name}" not found`);
    }
    poolInfo.disks.sort();

    let newPool = new Pool(poolInfo);
    this._registerPool(newPool);
    return newPool;
  }

  // Create nexus on this node.
  //
  // @param {string}   uuid      ID of the new nexus.
  // @param {number}   size      Size of nexus in bytes.
  // @param {object[]} replicas  Replica objects comprising the nexus.
  // @returns {object} New nexus object.
  async createNexus(uuid, size, replicas) {
    let children = replicas.map(r => r.uri);
    log.debug(`Creating nexus "${uuid}@${this.name}"`);

    try {
      await this.call('createNexus', { uuid, size, children });
      log.info(`Created nexus "${uuid}@${this.name}"`);
    } catch (err) {
      // TODO: Make rpc idempotent
      if (err.code != GrpcCode.ALREADY_EXISTS) {
        throw err;
      }
    }

    // it's not done yet, we have to get properties of the nexus to
    // obtain its state and perhaps other volatile properties.
    var resp;
    try {
      resp = await this.call('listNexus', {});
    } catch (err) {
      throw new GrpcError(
        GrpcCode.INTERNAL,
        `Failed to list new nexus "${uuid}": ${err}`
      );
    }
    var nexusInfo = resp.nexusList.filter(n => n.uuid == uuid)[0];
    if (!nexusInfo) {
      throw new GrpcError(GrpcCode.INTERNAL, `New nexus "${uuid}" not found`);
    }
    nexusInfo.children.sort((a, b) => (a.uri > b.uri ? 1 : -1));

    let newNexus = new Nexus(nexusInfo, replicas);
    this._registerNexus(newNexus);
    return newNexus;
  }

  // Get IO statistics for all replicas on the node.
  //
  // @returns {object[]} Array of stats where each object is for a different replica and keys are stats names and values stats values.
  async getStats() {
    log.debug(`Retrieving volume stats from node "${this}"`);
    let reply = await this.call('statReplicas', {});
    return reply.replicas;
  }
}

module.exports = Node;
