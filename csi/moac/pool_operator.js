// Pool operator monitors k8s pool resources (desired state). It creates
// and destroys pools on storage nodes to reflect the desired state.

'use strict';

const _ = require('lodash');
const path = require('path');
const fs = require('fs');
const yaml = require('js-yaml');
const log = require('./logger').Logger('pool-operator');
const Watcher = require('./watcher');
const EventStream = require('./event_stream');
const Workq = require('./workq');
const { FinalizerHelper } = require('./finalizer_helper');
const poolFinalizerValue = 'finalizer.mayastor.openebs.io';

// Load custom resource definition
const crdPool = yaml.safeLoad(
  fs.readFileSync(path.join(__dirname, '/crds/mayastorpool.yaml'), 'utf8')
);

// Pool operator tries to bring the real state of storage pools on mayastor
// nodes in sync with mayastorpool custom resources in k8s.
class PoolOperator {
  constructor (namespace) {
    this.namespace = namespace;
    this.k8sClient = null; // k8s client
    this.registry = null; // registry containing info about mayastor nodes
    this.eventStream = null; // A stream of node and pool events.
    this.resource = {}; // List of storage pool resources indexed by name.
    this.watcher = null; // pool CRD watcher.
    this.workq = new Workq(); // for serializing pool operations
    this.finalizerHelper = new FinalizerHelper(
      this.namespace,
      crdPool.spec.group,
      crdPool.spec.version,
      crdPool.spec.names.plural
    );
  }

  // Create pool CRD if it doesn't exist and augment client object so that CRD
  // can be manipulated as any other standard k8s api object.
  // Bind node operator to pool operator through events.
  //
  // @param {object} k8sClient   Client for k8s api server.
  // @param {object} registry    Registry with node and pool information.
  //
  async init (k8sClient, registry) {
    log.info('Initializing pool operator');

    try {
      await k8sClient.apis[
        'apiextensions.k8s.io'
      ].v1beta1.customresourcedefinitions.post({ body: crdPool });
      log.info('Created CRD ' + crdPool.spec.names.kind);
    } catch (err) {
      // API returns a 409 Conflict if CRD already exists.
      if (err.statusCode !== 409) throw err;
    }
    k8sClient.addCustomResourceDefinition(crdPool);

    this.k8sClient = k8sClient;
    this.registry = registry;
    this.watcher = new Watcher(
      'pool',
      this.k8sClient.apis['openebs.io'].v1alpha1.namespaces(
        this.namespace
      ).mayastorpools,
      this.k8sClient.apis['openebs.io'].v1alpha1.watch.namespaces(
        this.namespace
      ).mayastorpools,
      this._filterMayastorPool
    );
  }

  // Convert pool CRD to an object with specification of the pool.
  //
  // @param   {object} msp   MayaStor pool custom resource.
  // @returns {object} Pool properties defining a pool.
  //
  _filterMayastorPool (msp) {
    const props = {
      name: msp.metadata.name,
      node: msp.spec.node,
      disks: msp.spec.disks
    };
    // sort the disks for easy string to string comparison
    props.disks.sort();
    return props;
  }

  // Start pool operator's watcher loop.
  //
  // NOTE: Not getting the start sequence right can have catastrophic
  // consequence leading to unintended pool destruction and data loss
  // (i.e. when node info is available before the pool CRD is).
  //
  // The right order of steps is:
  //   1. Get pool resources
  //   2. Get info about pools on storage nodes
  async start () {
    var self = this;

    // get pool k8s resources for initial synchronization and install
    // event handlers to follow changes to them.
    await self.watcher.start();
    self._bindWatcher(self.watcher);
    self.watcher.list().forEach((r) => {
      const poolName = r.name;
      log.debug(`Reading pool custom resource "${poolName}"`);
      self.resource[poolName] = r;
    });

    // this will start async processing of node and pool events
    self.eventStream = new EventStream({ registry: self.registry });
    self.eventStream.on('data', async (ev) => {
      if (ev.kind === 'pool') {
        await self.workq.push(ev, self._onPoolEvent.bind(self));
      } else if (ev.kind === 'node' && (ev.eventType === 'sync' || ev.eventType === 'mod')) {
        await self.workq.push(ev.object.name, self._onNodeSyncEvent.bind(self));
      } else if (ev.kind === 'replica' && (ev.eventType === 'new' || ev.eventType === 'del')) {
        await self.workq.push(ev, self._onReplicaEvent.bind(self));
      }
    });
  }

  // Handler for new/mod/del pool events
  //
  // @param {object} ev       Pool event as received from event stream.
  //
  async _onPoolEvent (ev) {
    const name = ev.object.name;
    const resource = this.resource[name];

    log.debug(`Received "${ev.eventType}" event for pool "${name}"`);

    if (ev.eventType === 'new') {
      if (!resource) {
        log.warn(`Unknown pool "${name}" will be destroyed`);
        await this._destroyPool(name);
      } else {
        await this._updateResource(ev.object);
      }
    } else if (ev.eventType === 'mod') {
      await this._updateResource(ev.object);
    } else if (ev.eventType === 'del' && resource) {
      log.warn(`Recreating destroyed pool "${name}"`);
      await this._createPool(resource);
    }
  }

  // Handler for node sync event.
  //
  // Either the node is new or came up after an outage - check that we
  // don't have any pending pools waiting to be created on it.
  //
  // @param {string} nodeName    Name of the new node.
  //
  async _onNodeSyncEvent (nodeName) {
    log.debug(`Syncing pool records for node "${nodeName}"`);

    const resources = Object.values(this.resource).filter(
      (ent) => ent.node === nodeName
    );
    for (let i = 0; i < resources.length; i++) {
      await this._createPool(resources[i]);
    }
  }

  // Handler for new/del replica events
  //
  // @param {object} ev       Replica event as received from event stream.
  //
  async _onReplicaEvent (ev) {
    const replica = ev.object;

    log.debug(`Received "${ev.eventType}" event for replica "${replica.name}"`);

    if (replica.pool === undefined) {
      log.warn(`not processing for finalizers: pool not defined for replica ${replica.name}.`);
      return;
    }

    const pool = this.registry.getPool(replica.pool.name);
    if (pool == null) {
      log.warn(`not processing for finalizers: failed to retrieve pool ${replica.pool.name}`);
      return;
    }

    log.debug(`On "${ev.eventType}" event for replica "${replica.name}", replica count=${pool.replicas.length}`);

    if (pool.replicas.length > 0) {
      this.finalizerHelper.addFinalizerToCR(replica.pool.name, poolFinalizerValue);
    } else {
      this.finalizerHelper.removeFinalizerFromCR(replica.pool.name, poolFinalizerValue);
    }
  }

  // Stop the watcher, destroy event stream and reset resource cache.
  async stop () {
    this.watcher.removeAllListeners();
    await this.watcher.stop();
    this.eventStream.destroy();
    this.eventStream = null;
    this.resource = {};
  }

  // Bind watcher's new/mod/del events to pool operator's callbacks.
  //
  // @param {object} watcher   k8s pool resource watcher.
  //
  _bindWatcher (watcher) {
    var self = this;
    watcher.on('new', (resource) => {
      self.workq.push(resource, self._createPool.bind(self));
    });
    watcher.on('mod', (resource) => {
      self.workq.push(resource, self._modifyPool.bind(self));
    });
    watcher.on('del', (resource) => {
      self.workq.push(resource.name, self._destroyPool.bind(self));
    });
  }

  // Create a pool according to the specification.
  // That includes parameters checks, node lookup and a call to registry
  // to create the pool.
  //
  // @param {object}   resource       Pool resource properties.
  // @param {string}   resource.name  Pool name.
  // @param {string}   resource.node  Node name for the pool.
  // @param {string[]} resource.disks Disks comprising the pool.
  //
  async _createPool (resource) {
    const name = resource.name;
    const nodeName = resource.node;
    this.resource[name] = resource;

    let pool = this.registry.getPool(name);
    if (pool) {
      // the pool already exists, just update its properties in k8s
      await this._updateResource(pool);
      return;
    }

    const node = this.registry.getNode(nodeName);
    if (!node) {
      const msg = `mayastor does not run on node "${nodeName}"`;
      log.error(`Cannot create pool "${name}": ${msg}`);
      await this._updateResourceProps(name, 'pending', msg);
      return;
    }
    if (!node.isSynced()) {
      log.debug(
        `The pool "${name}" will be synced when the node "${nodeName}" is synced`
      );
      return;
    }

    // We will update the pool status once the pool is created, but
    // that can take a time, so set reasonable default now.
    await this._updateResourceProps(name, 'pending', 'Creating the pool');

    try {
      // pool resource props will be updated when "new" pool event is emitted
      pool = await node.createPool(name, resource.disks);
    } catch (err) {
      log.error(`Failed to create pool "${name}": ${err}`);
      await this._updateResourceProps(name, 'pending', err.toString());
    }
  }

  // Remove the pool from internal state and if it exists destroy it.
  // Does not throw - only logs an error.
  //
  // @param {string} name   Name of the pool to destroy.
  //
  async _destroyPool (name) {
    var resource = this.resource[name];
    var pool = this.registry.getPool(name);

    if (resource) {
      delete this.resource[name];
    }
    if (pool) {
      try {
        await pool.destroy();
      } catch (err) {
        log.error(`Failed to destroy pool "${name}@${pool.node.name}": ${err}`);
      }
    }
  }

  // Changing pool parameters is actually not supported. However the pool
  // operator's state should reflect the k8s state, so we make the change
  // only at operator level and log a warning message.
  //
  // @param {string} newPool   New pool parameters.
  //
  async _modifyPool (newProps) {
    const name = newProps.name;
    const curProps = this.resource[name];
    if (!curProps) {
      log.warn(`Ignoring modification to unknown pool "${name}"`);
      return;
    }
    if (!_.isEqual(curProps.disks, newProps.disks)) {
      // TODO: Growing pools, mirrors, etc. is currently unsupported.
      log.error(`Changing disks of the pool "${name}" is not supported`);
      curProps.disks = newProps.disks;
    }
    // Changing node implies destroying the pool on the old node and recreating
    // it on the new node that is destructive action -> unsupported.
    if (curProps.node !== newProps.node) {
      log.error(`Moving pool "${name}" between nodes is not supported`);
      curProps.node = newProps.node;
    }
  }

  // Update status properties of k8s resource to be aligned with pool object
  // properties.
  //
  // NOTE: This method does not throw if the update fails as there is nothing
  // we can do if it fails. Though it logs an error message.
  //
  // @param {object} pool      Pool object.
  //
  async _updateResource (pool) {
    var name = pool.name;
    var resource = this.resource[name];

    // we don't track this pool so we cannot update the CRD
    if (!resource) {
      log.warn(`State of unknown pool "${name}" has changed`);
      return;
    }
    var state = pool.state.replace(/^POOL_/, '').toLowerCase();
    var reason = '';
    if (state === 'offline') {
      reason = `mayastor does not run on the node "${pool.node}"`;
    }

    await this._updateResourceProps(
      name,
      state,
      reason,
      pool.disks,
      pool.capacity,
      pool.used,
      pool.replicas.length
    );
  }

  // Update status properties of k8s CRD object.
  //
  // Parameters "name" and "state" are required, the rest is optional.
  //
  // NOTE: This method does not throw if the update fails as there is nothing
  // we can do if it fails. Though we log an error message in such a case.
  //
  // @param {string} name       Name of the pool.
  // @param {string} state      State of the pool.
  // @param {string} [reason]   Reason describing the root cause of the state.
  // @param {string[]} [disks]  Disk URIs.
  // @param {number} [capacity] Capacity of the pool in bytes.
  // @param {number} [used]     Used bytes in the pool.
  // @param {number} [replicacount] Count of replicas using the pool.
  //
  async _updateResourceProps (name, state, reason, disks, capacity, used, replicacount) {
    // For the update of CRD status we need a real k8s pool object, change the
    // status in it and store it back. Another reason for grabbing the latest
    // version of CRD from watcher cache (even if this.resource contains an older
    // version than the one fetched from watcher cache) is that k8s refuses to
    // update CRD unless the object's resourceVersion is the latest.
    var k8sPool = this.watcher.getRaw(name);

    // it could happen that the object was deleted in the meantime
    if (!k8sPool) {
      log.warn(
        `Pool resource "${name}" was deleted before its status could be updated`
      );
      return;
    }
    const status = k8sPool.status || {};
    // avoid the update if the object has not changed
    if (
      state === status.state &&
      reason === status.reason &&
      capacity === status.capacity &&
      used === status.used &&
      _.isEqual(disks, status.disks)
    ) {
      return;
    }

    log.debug(`Updating properties of pool resource "${name}"`);
    status.state = state;
    status.reason = reason || '';
    status.disks = disks || [];
    if (capacity != null) {
      status.capacity = capacity;
    }
    if (used != null) {
      status.used = used;
    }

    try {
      await this.k8sClient.apis['openebs.io'].v1alpha1
        .namespaces(this.namespace)
        .mayastorpools(name)
        .status.put({ body: k8sPool });
    } catch (err) {
      log.error(`Failed to update status of pool "${name}": ${err}`);
    }
    k8sPool.status = status;

    if (replicacount != null) {
      if (replicacount === 0) {
        this.finalizerHelper.removeFinalizer(k8sPool, name, poolFinalizerValue);
      } else {
        this.finalizerHelper.addFinalizer(k8sPool, name, poolFinalizerValue);
      }
    }
  }
}

module.exports = PoolOperator;
