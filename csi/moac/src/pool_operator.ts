// Pool operator monitors k8s pool resources (desired state). It creates
// and destroys pools on storage nodes to reflect the desired state.

import * as fs from 'fs';
import * as _ from 'lodash';
import * as path from 'path';
import {
  ApiextensionsV1Api,
  KubeConfig,
} from '@kubernetes/client-node';
import {
  CustomResource,
  CustomResourceCache,
  CustomResourceMeta,
} from './watcher';
import { EventStream } from './event_stream';
import { Workq } from './workq';
import { Logger } from './logger';

const log = Logger('pool-operator');

const yaml = require('js-yaml');

const RESOURCE_NAME: string = 'mayastorpool';
const POOL_FINALIZER = 'finalizer.mayastor.openebs.io';

// Load custom resource definition
const crdPool = yaml.load(
  fs.readFileSync(path.join(__dirname, '../crds/mayastorpool.yaml'), 'utf8')
);

// Set of possible pool states. Some of them come from mayastor and
// offline, pending and error are deduced in the control plane itself.
enum PoolState {
  Unknown = "unknown",
  Online = "online",
  Degraded = "degraded",
  Faulted = "faulted",
  Offline = "offline",
  Pending = "pending",
  Error = "error",
}

function poolStateFromString(val: string): PoolState {
  if (val === PoolState.Online) {
    return PoolState.Online;
  } else if (val === PoolState.Degraded) {
    return PoolState.Degraded;
  } else if (val === PoolState.Faulted) {
    return PoolState.Faulted;
  } else if (val === PoolState.Offline) {
    return PoolState.Offline;
  } else if (val === PoolState.Pending) {
    return PoolState.Pending;
  } else if (val === PoolState.Error) {
    return PoolState.Error;
  } else {
    return PoolState.Unknown;
  }
}

// Object defines spec properties of a pool resource.
export class PoolSpec {
  node: string;
  disks: string[];

  // Create and validate pool custom resource.
  constructor(node: string, disks: string[]) {
    this.node = node;
    this.disks = disks;
  }
}

// Object defines properties of pool resource.
export class PoolResource extends CustomResource {
  apiVersion?: string;
  kind?: string;
  metadata: CustomResourceMeta;
  spec: PoolSpec;
  status: {
    spec?: PoolSpec,
    state: PoolState,
    reason?: string,
    disks?: string[],
    capacity?: number,
    used?: number
  };

  // Create and validate pool custom resource.
  constructor(cr: CustomResource) {
    super();
    this.apiVersion = cr.apiVersion;
    this.kind = cr.kind;
    if (cr.metadata === undefined) {
      throw new Error('missing metadata');
    } else {
      this.metadata = cr.metadata;
    }
    if (cr.spec === undefined) {
      throw new Error('missing spec');
    } else {
      let node = (cr.spec as any).node;
      if (typeof node !== 'string') {
        throw new Error('missing or invalid node in spec');
      }
      let disks = (cr.spec as any).disks;
      if (!Array.isArray(disks)) {
        throw new Error('missing or invalid disks in spec');
      }
      disks = disks.slice(0).sort();
      //if (typeof disks !== 'string') {
      this.spec = { node, disks };
    }
    this.status = {
      state: poolStateFromString(cr.status?.state),
      spec: cr.status?.spec,
      reason: cr.status?.reason,
      disks: cr.status?.disks,
      capacity: cr.status?.capacity,
      used: cr.status?.used,
    };
  }

  // Extract name of the pool from the resource metadata.
  getName(): string {
    if (this.metadata.name === undefined) {
      throw Error("Resource object does not have a name")
    } else {
      return this.metadata.name;
    }
  }

  // Get the pool spec
  // If the pool has not been created yet, the user spec is returned
  // If the pool has already been created, then the initial spec (cached in the status) is returned
  getSpec(): PoolSpec {
    if (this.status.spec !== undefined) {
      return this.status.spec
    } else {
      return this.spec
    }
  }

  // Get the pool disk device
  // If the pool has been created once already, then it's the initial URI returned by mayastor
  // Otherwise, it's the disk device from the SPEC
  getDisks(): string[] {
    if (this.status.disks !== undefined) {
      return this.status.disks
    } else {
      return this.getSpec().disks
    }
  }
}

// Pool operator tries to bring the real state of storage pools on mayastor
// nodes in sync with mayastorpool custom resources in k8s.
export class PoolOperator {
  namespace: string;
  watcher: CustomResourceCache<PoolResource>; // k8s resource watcher for pools
  registry: any; // registry containing info about mayastor nodes
  eventStream: any; // A stream of node and pool events.
  workq: Workq; // for serializing pool operations

  // Create pool operator.
  //
  // @param namespace     Namespace the operator should operate on.
  // @param kubeConfig    KubeConfig.
  // @param registry      Registry with node objects.
  // @param [idleTimeout] Timeout for restarting watcher connection when idle.
  constructor (
    namespace: string,
    kubeConfig: KubeConfig,
    registry: any,
    idleTimeout: number | undefined,
  ) {
    this.namespace = namespace;
    this.registry = registry; // registry containing info about mayastor nodes
    this.eventStream = null; // A stream of node and pool events.
    this.workq = new Workq('mayastorpool'); // for serializing pool operations
    this.watcher = new CustomResourceCache(
      this.namespace,
      RESOURCE_NAME,
      kubeConfig,
      PoolResource,
      { idleTimeout }
    );
  }

  // Create pool CRD if it doesn't exist.
  //
  // @param kubeConfig  KubeConfig.
  async init (kubeConfig: KubeConfig) {
    log.info('Initializing pool operator');
    let k8sExtApi = kubeConfig.makeApiClient(ApiextensionsV1Api);
    try {
      await k8sExtApi.createCustomResourceDefinition(crdPool);
      log.info(`Created CRD ${RESOURCE_NAME}`);
    } catch (err) {
      // API returns a 409 Conflict if CRD already exists.
      if (err.statusCode !== 409) throw err;
    }
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
    self._bindWatcher(self.watcher);
    await self.watcher.start();

    // this will start async processing of node and pool events
    self.eventStream = new EventStream({ registry: self.registry });
    self.eventStream.on('data', async (ev: any) => {
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
  // @param ev       Pool event as received from event stream.
  //
  async _onPoolEvent (ev: any) {
    const name: string = ev.object.name;
    const resource = this.watcher.get(name);

    log.debug(`Received "${ev.eventType}" event for pool "${name}"`);

    if (ev.eventType === 'new') {
      if (resource === undefined) {
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
  // @param nodeName    Name of the new node.
  //
  async _onNodeSyncEvent (nodeName: string) {
    log.debug(`Syncing pool records for node "${nodeName}"`);

    const resources = this.watcher.list().filter(
      (ent) => ent.spec.node === nodeName
    );
    for (let i = 0; i < resources.length; i++) {
      await this._createPool(resources[i]);
    }
  }

  // Handler for new/del replica events
  //
  // @param ev       Replica event as received from event stream.
  //
  async _onReplicaEvent (ev: any) {
    const pool = ev.object.pool;
    if (!pool) {
      // can happen if the node goes away (replica will shortly disappear too)
      return;
    }
    await this._updateFinalizer(pool.name, pool.replicas.length > 0);
  }

  // Stop the events, destroy event stream and reset resource cache.
  stop () {
    this.watcher.stop();
    this.watcher.removeAllListeners();
    if (this.eventStream) {
      this.eventStream.destroy();
      this.eventStream = null;
    }
  }

  // Bind watcher's new/mod/del events to pool operator's callbacks.
  //
  // @param watcher   k8s pool resource watcher.
  //
  _bindWatcher (watcher: CustomResourceCache<PoolResource>) {
    watcher.on('new', (resource: PoolResource) => {
      this.workq.push(resource, this._createPool.bind(this));
    });
    watcher.on('mod', (resource: PoolResource) => {
      this.workq.push(resource, this._modifyPool.bind(this));
    });
    watcher.on('del', (resource: PoolResource) => {
      this.workq.push(resource, async (arg: PoolResource) => {
        await this._destroyPool(arg.getName());
      });
    });
  }

  // Create a pool according to the specification.
  // That includes parameters checks, node lookup and a call to registry
  // to create the pool.
  //
  // @param resource       Pool resource properties.
  //
  async _createPool (resource: PoolResource) {
    const name: string = resource.getName();
    const nodeName = resource.getSpec().node;

    // Nothing prevents the user from modifying the spec part of the CRD, which could trick MOAC into recreating
    // the pool on a different node, for example.
    // So, store the initial spec in the status section of the CRD so that we may ignore any CRD edits from the user.
    if (resource.status.spec === undefined) {
      resource.status.spec = resource.spec;
      await this._updateResourceProps(
        name,
        resource.status.state,
        undefined,
        undefined,
        undefined,
        undefined,
        resource.status.spec
      );
    }

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
      await this._updateResourceProps(name, PoolState.Pending, msg);
      return;
    }
    if (!node.isSynced()) {
      const msg = `mayastor on node "${nodeName}" is offline`;
      log.error(`Cannot sync pool "${name}": ${msg}`);
      await this._updateResourceProps(name, PoolState.Pending, msg);
      return;
    }

    // We will update the pool status once the pool is created, but
    // that can take a time, so set reasonable default now.
    await this._updateResourceProps(name, PoolState.Pending, 'Creating the pool');
    try {
      // pool resource props will be updated when "new" pool event is emitted
      pool = await node.createPool(name, resource.getDisks());
    } catch (err) {
      log.error(`Failed to create pool "${name}": ${err}`);
      await this._updateResourceProps(name, PoolState.Error, err.toString());
    }
  }

  // Remove the pool from internal state and if it exists destroy it.
  // Does not throw - only logs an error.
  //
  // @param name   Name of the pool to destroy.
  //
  async _destroyPool (name: string) {
    var pool = this.registry.getPool(name);

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
  // @param newPool   New pool parameters.
  //
  async _modifyPool (resource: PoolResource) {
    const name = resource.getName();
    const ignoreMessage = "SPEC Modification ignored since that is not currently supported. ";
    let reason = resource.status.reason;

    // Pool SPEC modifications are ignored, add a reason to the CRD to make the user aware of this
    if (!_.isEqual(resource.spec, resource.getSpec())) {
      log.error(`Ignoring modification to pool "${name}" since that is not currently supported.`);

      if (!reason?.includes(ignoreMessage) || reason === undefined) {
        await this._updateResourceProps(
          name,
          resource.status.state,
          ignoreMessage + reason,
        );
      }
    } else if (reason?.includes(ignoreMessage)) {
      await this._updateResourceProps(
        name,
        resource.status.state,
        reason.replace(ignoreMessage, "") || "",
      );
    }
  }

  // Update status properties of k8s resource to be aligned with pool object
  // properties.
  //
  // NOTE: This method does not throw if the update fails as there is nothing
  // we can do if it fails. Though it logs an error message.
  //
  // @param pool      Pool object.
  //
  async _updateResource (pool: any) {
    var name = pool.name;
    var resource = this.watcher.get(name);

    // we don't track this pool so we cannot update the CRD
    if (!resource) {
      log.warn(`State of unknown pool "${name}" has changed`);
      return;
    }
    var state = poolStateFromString(
      pool.state.replace(/^POOL_/, '').toLowerCase()
    );
    var reason;
    if (state === PoolState.Offline) {
      reason = `mayastor does not run on the node "${pool.node}"`;
    }

    await this._updateResourceProps(
      name,
      state,
      reason,
      pool.disks,
      pool.capacity,
      pool.used,
      resource.status.spec || resource.spec
    );
  }

  // Update status properties of k8s CRD object.
  //
  // Parameters "name" and "state" are required, the rest is optional.
  //
  // NOTE: This method does not throw if the update fails as there is nothing
  // we can do if it fails. Though we log an error message in such a case.
  //
  // @param name       Name of the pool.
  // @param state      State of the pool.
  // @param [reason]   Reason describing the root cause of the state.
  // @param [disks]    Disk URIs.
  // @param [capacity] Capacity of the pool in bytes.
  // @param [used]     Used bytes in the pool.
  async _updateResourceProps (
    name: string,
    state: PoolState,
    reason?: string,
    disks?: string[],
    capacity?: number,
    used?: number,
    specInStatus?: PoolSpec,
  ) {
    try {
      await this.watcher.updateStatus(name, (orig: PoolResource) => {
        // avoid the update if the object has not changed
        if (
          state === orig.status.state &&
          (reason === orig.status.reason || (!reason && !orig.status.reason)) &&
          (capacity === undefined || capacity === orig.status.capacity) &&
          (used === undefined || used === orig.status.used) &&
          (disks === undefined || _.isEqual(disks, orig.status.disks)) &&
          (specInStatus === undefined || specInStatus === orig.status.spec)
        ) {
          return;
        }

        log.debug(`Updating properties of pool resource "${name}"`);
        let resource: PoolResource = _.cloneDeep(orig);
        resource.status = {
          state: state,
          reason: reason || '',
          spec: specInStatus || resource.status.spec,
          disks: resource.status.disks
        };
        if (disks != null) {
          resource.status.disks = disks;
        }
        if (capacity != null) {
          resource.status.capacity = capacity;
        }
        if (used != null) {
          resource.status.used = used;
        }
        return resource;
      });
    } catch (err) {
      log.error(`Failed to update status of pool "${name}": ${err}`);
    }
  }

  // Place or remove finalizer from pool resource.
  //
  // @param name       Name of the pool.
  // @param [busy]     At least one replica on it.
  async _updateFinalizer(name: string, busy: boolean) {
    try {
      if (busy) {
        this.watcher.addFinalizer(name, POOL_FINALIZER);
      } else {
        this.watcher.removeFinalizer(name, POOL_FINALIZER);
      }
    } catch (err) {
      log.error(`Failed to update finalizer on pool "${name}": ${err}`);
    }
  }
}
