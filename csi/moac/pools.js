'use strict';

const assert = require('assert');
const fs = require('fs');
const EventEmitter = require('events');
const grpc = require('grpc-uds');
const grpc_promise = require('grpc-promise');
const yaml = require('js-yaml');
const log = require('./logger').Logger('pool-operator');
const Watcher = require('./watcher').Watcher;
const { mayastor, isPoolAccessible } = require('./common');

const crdPool = yaml.safeLoad(
  fs.readFileSync(__dirname + '/crds/mayastorpool.yaml', 'utf8')
);

// How often (more or less) pools on nodes are synchronized (in secs)
var exports = {
  syncInterval: 60,
  checkInterval: 20,
};

// Pool operator tries to bring the real state of storage pools on mayastor
// nodes in sync with the state recorded in mayastorpool custom resources.
// It depends on node operator to get actual information about the mayastor
// nodes and uses mayastor grpc service on storage nodes to query, create and
// destroy storage pools on mayastor nodes as needed.
//
// Operator does not distinguish between transient and permanent failures.
// If the list, destroy or create operation fails for any reason, there isn't
// immediate recover logic. Though we sync nodes every couple of minutes so
// the operation will be retried eventually.
//
class PoolOperator extends EventEmitter {
  constructor() {
    super();
    this.client = null; // k8s client
    this.nodes = null; // node operator managing mayastor nodes
    this.pools = {}; // List of mayastor pools indexed by pool name.
    // Reflects the state on the storage nodes - not k8s state.
    this.watcher = null; // pool resource watcher
    this.pendingNodeEvents = null; // queued node events during the initial sync
    this.wq = null; // work queue for serializing calls to create/destroy pool
    // Time from time we update pool status in pool CRs (which includes used bytes).
    // Here we keep track of which node was sync'd when.
    this.nodeSyncs = {};
    this.syncTimer = null;
  }

  // Create pool CRD if it doesn't exist and augment client object so that CRD
  // can be manipulated as any other standard k8s api object.
  // Bind node operator to pool operator through events.
  async init(client, nodeOperator) {
    log.info('Initializing pool operator');

    try {
      await client.apis[
        'apiextensions.k8s.io'
      ].v1beta1.customresourcedefinitions.post({ body: crdPool });
      log.info('Created CRD ' + crdPool.spec.names.kind);
    } catch (err) {
      // API returns a 409 Conflict if CRD already exists.
      if (err.statusCode !== 409) throw err;
    }
    client.addCustomResourceDefinition(crdPool);

    this.client = client; // k8s client
    this.nodes = nodeOperator;

    // Initialize watcher with all callbacks for new/mod/del events
    this.watcher = new Watcher(
      'pool',
      this.client.apis['openebs.io'].v1alpha1.mayastorpools,
      this.client.apis['openebs.io'].v1alpha1.watch.mayastorpools,
      this.filterMayastorPool
    );
  }

  // Bind the watcher to pool operator's callbacks for new/mod/del events.
  bindWatcher(watcher) {
    watcher.on('new', this._qwork.bind(this, 'create'));
    watcher.on('del', this._qwork.bind(this, 'destroy'));
    watcher.on('mod', this._qwork.bind(this, 'modify'));
  }

  // Convert k8s mayastor pool object to internal representation.
  filterMayastorPool(msp) {
    let poolStatus = msp.status || {};

    // This defines the internal representation of the pool
    let pool = {
      name: msp.metadata.name,
      node: msp.spec.node,
      disks: msp.spec.disks,
      state: poolStatus.state,
      reason: poolStatus.reason,
    };
    // sort the disks for easy string to string comparison
    pool.disks.sort();

    return pool;
  }

  // Start pool operator's watcher loop.
  // The node operator should be in ready state when calling this function.
  //
  // NOTE: Not getting the start sequence right can have catastrophic
  // consequence leading to unintended pool destruction and data loss (i.e. when
  // node record is available before the pool record is).
  //
  // The steps are:
  //   1. Init the pool CR cache
  //   2. Inspect all storage nodes and update the state (create, destroy pools)
  //   3. Process pool CRs and create missing pools
  async start() {
    var self = this;

    // this will init cache with all the pool CRs and sync them
    self.pendingNodeEvents = [];
    await self.watcher.start();

    // take a snapshot of pool CRs for initial synchronization of nodes
    var pools = self.watcher.list();
    for (let i = 0; i < pools.length; i++) {
      let pool = pools[i];
      // we trust only real status obtained from storage nodes
      delete pool.state;
      delete pool.reason;
      self.pools[pool.name] = pool;
    }

    log.info(`Starting to sync pools on the nodes`);

    var nodes = self.nodes.get();
    assert(nodes);
    self.nodes.on('add', ev => {
      if (self.pendingNodeEvents != null) {
        self.pendingNodeEvents.push({
          type: 'add',
          ev: ev,
        });
      } else {
        self._qwork('sync', ev);
      }
    });

    self.nodes.on('remove', ev => {
      if (self.pendingNodeEvents != null) {
        self.pendingNodeEvents.push({
          type: 'remove',
          ev: ev,
        });
      } else {
        self._qwork('remove', ev);
      }
    });

    for (let i = 0; i < nodes.length; i++) {
      await self._qwork('sync', nodes[i]);
    }

    // replay all events which we missed while syncing pools on the nodes
    var ent;
    while ((ent = self.pendingNodeEvents.shift())) {
      if (ent.type == 'add') {
        await self._qwork('sync', ent.ev);
      } else {
        assert(ent.type == 'remove');
        await self._qwork('remove', ent.ev);
      }
    }
    // stop postponing events and resume normal operation
    self.pendingNodeEvents = null;
    log.info(`Sync of the pools on the nodes done`);

    // Catch up with pool k8s state changes which happened since the initial
    // snapshot. This will start a stream of pool CR events
    self.bindWatcher(self.watcher);
    pools = self.watcher.list();
    for (let i = 0; i < pools.length; i++) {
      let pool = pools[i];
      // do not block so that we queue the work before the real events
      // from the watcher start to come
      if (self.pools[pool.name]) {
        self._qwork('modify', pool);
      } else {
        self._qwork('create', pool);
      }
    }

    self.syncTimer = setInterval(
      this._syncNodes.bind(this),
      exports.checkInterval * 1000
    );
  }

  // Sync nodes which were not sync'd for a long time
  _syncNodes() {
    let now = Date.now() / 1000;

    for (let nodeName in this.nodeSyncs) {
      let synced = this.nodeSyncs[nodeName];
      let node = this.nodes.get(nodeName);

      if (node && synced + exports.syncInterval <= now) {
        this._qwork('sync', node);
      }
    }
  }

  async stop() {
    if (this.syncTimer) {
      clearInterval(this.syncTimer);
      this.syncTimer = null;
    }
    this.watcher.removeAllListeners();
    await this.watcher.stop();
  }

  // Get either specified pool or all pools if name is not specified.
  get(name) {
    if (name) {
      return this.pools[name];
    } else {
      return Object.values(this.pools);
    }
  }

  // Synchronize all pools for a particular storage node or all nodes if node
  // name is not specified.
  // It is a wrapper around internal work queue which calls _syncNode()
  // to really do the job.
  async syncNode(nodeName) {
    var nodes;

    if (nodeName) {
      let node = this.nodes.get(nodeName);
      if (!node) return;
      nodes = [node];
    } else {
      nodes = this.nodes.get();
    }
    for (let i = 0; i < nodes.length; i++) {
      await this._qwork('sync', nodes[i]);
    }
  }

  // Put the job to work queue and process it in order
  async _qwork(type, object) {
    var resolveCb;
    var promise = new Promise((resolve, reject) => {
      resolveCb = resolve;
    });
    var w = { type, object, resolveCb };
    if (this.wq != null) {
      this.wq.push(w);
      return promise;
    }
    this.wq = [];

    while (w) {
      switch (w.type) {
        case 'create':
          await this._createPool(w.object);
          break;
        case 'destroy':
          await this._destroyPool(w.object.name);
          break;
        case 'modify':
          let obj = w.object;
          let old = this.pools[obj.name];
          if (old) {
            if (JSON.stringify(old.disks) !== JSON.stringify(obj.disks)) {
              // TODO: It should be possible to add a new disk to RAID-0.
              // Though it is currently unsupported.
              log.error(
                `Changing disks of the pool "${old.name}" is not supported`
              );
            }
            // Changing node implies destroying the pool on the old node
            // and recreating it on the new node => destructive action.
            // It's unlikely to happen often.
            if (old.node !== obj.node) {
              await this._destroyPool(old.name);
              await this._createPool(obj);
            }
          } else {
            // the pool might not have been created due to an error
            log.error(
              `Ignoring modification of pool "${obj.name}": does not exist`
            );
          }
          break;
        case 'sync':
          await this._syncNode(w.object);
          break;
        case 'remove':
          await this._removeNode(w.object);
          break;
        default:
          assert(false, 'Invalid work type');
      }
      w.resolveCb();
      w = this.wq.shift();
    }
    this.wq = null;
  }

  // Helper function to lookup node, call create pool grpc method, update CR
  // status and update the internal state.
  async _createPool(pool) {
    pool.status = {}; // disregard status from k8s - we know better
    this.pools[pool.name] = pool;

    // TODO: Support NDM disk IDs
    if (
      !pool.disks.every(
        ent => ent.startsWith('/dev/') && ent.indexOf('..') == -1
      )
    ) {
      let msg = 'All disks must be absolute paths beginning with /dev';
      log.error(`Cannot create pool "${pool.name}": ` + msg);
      await this._updateStatus(pool.name, {
        state: 'PENDING',
        reason: msg,
      });
      return;
    }
    let client = this._getNodeClient(pool.node);

    if (!client) {
      // the pool will get created later when the node joins the cluster
      let msg = `mayastor on node "${pool.node}" is not running`;
      log.error(`Cannot create pool "${pool.name}": ` + msg);
      await this._updateStatus(pool.name, {
        state: 'PENDING',
        reason: msg,
      });
      return;
    }

    try {
      await this._createPoolWithClient(client, pool);
    } finally {
      client.close();
    }
  }

  // Helper function to lookup node, call destroy pool grpc method and
  // remove the pool from internal state.
  async _destroyPool(poolName) {
    var pool = this.pools[poolName];
    if (!pool) {
      return; // nothing to destroy
    }
    delete this.pools[poolName];

    let client = this._getNodeClient(pool.node);
    if (!client) {
      // the pool will be destroyed if the node later joins the cluster
      let msg = `mayastor on node "${pool.node}" is not running`;
      log.error(`Cannot destroy pool "${poolName}": ` + msg);
      return;
    }

    try {
      await this._destroyPoolWithClient(client, poolName, pool.node);
    } finally {
      client.close();
    }
  }

  // Create a pool if we already have mayastor client handle.
  // This function does not throw and takes care of updating pool status if
  // the create fails.
  async _createPoolWithClient(client, pool) {
    log.debug(`Creating pool "${pool.name}" on node "${pool.node}"`);
    try {
      await client.CreatePool().sendMessage({
        name: pool.name,
        disks: pool.disks,
      });
      log.info(`Created pool "${pool.name}" on node "${pool.node}"`);
    } catch (err) {
      if (err.code != grpc.status.ALREADY_EXISTS) {
        log.error(
          `Failed to create pool "${pool.name}" on node "${pool.node}": ${err}`
        );
        await this._updateStatus(pool.name, {
          state: 'PENDING',
          reason: err.toString(),
        });
        return;
      }
    }

    var poolInfo;
    var poolStatus;

    try {
      let resp = await client.ListPools().sendMessage({});
      poolInfo = resp.pools.filter(p => p.name == pool.name)[0];
    } catch (err) {
      log.error(`Failed to list pools on node "${pool.node}": ${err}`);
    }
    if (poolInfo) {
      poolInfo.disks.sort();
      // check that the pool parameters are the same
      if (JSON.stringify(poolInfo.disks) == JSON.stringify(pool.disks)) {
        poolStatus = {
          state: poolInfo.state,
          reason: '',
          capacity: poolInfo.capacity,
          used: poolInfo.used,
        };
      } else {
        poolStatus = {
          state: 'PENDING',
          reason: 'A different pool with the same name already exists',
        };
      }
    } else {
      poolStatus = {
        state: 'OFFLINE',
        reason: 'Failed to list storage pools on the node',
      };
    }

    await this._updateStatus(pool.name, poolStatus);
  }

  // Create a pool if we already have mayastor client handle.
  // This function does not throw and takes care of updating pool status if
  // the create fails.
  async _destroyPoolWithClient(client, poolName, nodeName) {
    log.debug(`Destroying pool "${poolName}" on node "${nodeName}"`);
    try {
      await client.DestroyPool().sendMessage({ name: poolName });
      log.info(`Destroyed pool "${poolName}" on node "${nodeName}"`);
      // event used by the tests
      process.nextTick(this.emit.bind(this), 'destroy', poolName);
    } catch (err) {
      if (err.code != grpc.status.NOT_FOUND) {
        log.error(
          `Failed to destroy pool "${poolName}" on node "${nodeName}": ${err}`
        );
      }
    }
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

  // Update status of a pool resource (ONLINE, DEGRADED, OFFLINE or PENDING).
  //
  // NOTE: This method does not throw as there is nothing we can do if it fails
  // except logging an error message.
  async _updateStatus(name, stat) {
    var pool = this.pools[name];
    var stateChange = pool.state != stat.state;
    pool.state = stat.state;
    pool.reason = stat.reason;
    if (stat.capacity != null) pool.capacity = stat.capacity;
    if (stat.used != null) pool.used = stat.used;

    // For update of pool status we need real k8s pool object and just change
    // its status to the new content. Another reason for grabbing latest vers
    // of object from watcher cache even if this.pools contains an older vers
    // is that k8s refuses to update the object unless the object's
    // resourceVersion is the latest one.
    var k8sPool = this.watcher.getRaw(name);

    if (!k8sPool) {
      // it could happen that the object was deleted in the meantime
      log.error(`Pool ${name} was deleted before its status could be updated`);
      return;
    }
    k8sPool.status = k8sPool.status || {};
    if (
      k8sPool.status.state == stat.state &&
      k8sPool.status.reason == stat.reason &&
      (stat.capacity == null || k8sPool.status.capacity == stat.capacity) &&
      (stat.used == null || k8sPool.status.used == stat.used)
    ) {
      // no change to be done
      return;
    }
    k8sPool.status = stat;

    try {
      await this.client.apis['openebs.io'].v1alpha1
        .mayastorpools(name)
        .status.put({ body: k8sPool });
    } catch (err) {
      log.error(`Failed to set ${stat.state} state on pool "${name}": ${err}`);
    }

    if (stateChange) {
      var reasonSuffix = '';

      if (stat.reason) {
        reasonSuffix = ': ' + stat.reason;
      }
      log.info(`Pool "${name}" is ${stat.state}` + reasonSuffix);
    }
  }

  // Sync storage node involves:
  // 1. List storage pools on a the storage node
  // 2. update pool CRs based on the information obtained from the node.
  // 3. Remove storage pools from the node which no longer have associated CR.
  async _syncNode(ev) {
    var client = this._createClient(ev);

    // record the last sync of the node
    this.nodeSyncs[ev.node] = Date.now() / 1000;

    log.info(`Syncing pools on node "${ev.node}"`);

    // list pools
    var resp;
    try {
      resp = await client.listPools().sendMessage({});
    } catch (err) {
      log.error(
        `Failed to sync node "${ev.node}": failed to list pools: ${err}`
      );
      client.close();

      // Offline pools which are supposedly on the now unreachable node.
      // The pools might function properly but we cannot tell for sure.
      for (let name in this.pools) {
        let pool = this.pools[name];

        if (pool.node == ev.node) {
          await this._updateStatus(name, {
            state: 'OFFLINE',
            reason: err.toString(),
          });
        }
      }
      return;
    }

    // convert list of pools to hash table
    var pools = {};
    for (let i = 0; i < resp.pools.length; i++) {
      let ent = resp.pools[i];
      ent.disks.sort();
      pools[ent.name] = ent;
    }

    // fist destroy the pools which should no longer be there
    for (let name in pools) {
      if (!(name in this.pools)) {
        await this._destroyPoolWithClient(client, name, ev.node);
      }
    }

    // create pools which are missing on the node and update info about
    // existing pools
    for (let name in this.pools) {
      let pool = this.pools[name];

      if (pool.node != ev.node) {
        continue;
      }

      if (name in pools) {
        await this._updateStatus(name, {
          state: pools[name].state,
          reason: '',
          capacity: pools[name].capacity,
          used: pools[name].used,
        });
        if (JSON.stringify(pool.disks) != JSON.stringify(pools[name].disks)) {
          log.error(`Inconsistent disk list of pool "${name}"`);
          pool.disks = pools[name].disks;
        }
      } else {
        await this._createPoolWithClient(client, pool);
      }
    }
    client.close();
  }

  // Update status of all pools on removed node to offline
  async _removeNode(ev) {
    // stop syncing the node
    delete this.nodeSyncs[ev.node];

    for (let name in this.pools) {
      if (this.pools[name].node == ev.node) {
        let msg = `mayastor on node "${ev.node}" is not running`;
        await this._updateStatus(name, {
          state: 'OFFLINE',
          reason: msg,
        });
      }
    }
  }
}

exports.PoolOperator = PoolOperator;
module.exports = exports;
