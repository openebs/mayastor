// Abstraction representing a storage node with its objects (nexus, pools,
// replicas). Consumers can use it to receive information about the storage
// objects and notifications about the changes.

import assert from 'assert';
import events = require('events');

import { grpcCode, GrpcError, GrpcClient } from './grpc_client';
import { Pool } from './pool';
import { Nexus } from './nexus';
import { Replica } from './replica';
import { Workq } from './workq';
import { Logger } from './logger';

const log = Logger('node');

// We increase timeout value to nexus create method because it involves
// updating etcd state in mayastor. Mayastor itself uses 30s timeout for etcd.
const NEXUS_CREATE_TIMEOUT_MS = 60000;

// Type returned by stats grpc call
export type ReplicaStat = {
  timestamp: number,
  // tags
  uuid: string,
  pool: string,
  // counters
  num_read_ops: number,
  num_write_ops: number,
  bytes_read: number,
  bytes_written: number,
}

// Node options when created.
export type NodeOpts = {
  // How often to sync healthy node (in ms).
  syncPeriod?: number;
  // How often to retry sync if it failed (in ms).
  syncRetry?: number;
  // Flip the node to offline state after this many retries have failed.
  syncBadLimit?: number;
}

// Object represents mayastor storage node.
//
// Node emits following events:
// "node": node related events with payload { eventType: "sync", object: node }
//         when the node is sync'd after previous sync failure(s).
// "pool", "replica", "nexus": with eventType "new", "mod", "del".
export class Node extends events.EventEmitter {
  name: string;
  syncPeriod: number;
  syncRetry: number;
  syncBadLimit: number;
  endpoint: string | null;
  client: any;
  workq: Workq;
  syncFailed: number;
  syncTimer: NodeJS.Timeout | null;
  nexus: Nexus[];
  pools: Pool[];

  // Create a storage node object.
  //
  // @param {string} name              Node name.
  // @param {Object} [opts]            Options
  constructor (name: string, opts?: NodeOpts) {
    opts = opts || {};

    super();
    this.name = name;
    this.syncPeriod = opts.syncPeriod || 60000;
    this.syncRetry = opts.syncRetry || 10000;
    this.syncBadLimit = opts.syncBadLimit || 0;

    this.endpoint = null;
    this.client = null; // grpc client handle
    this.workq = new Workq('grpc call'); // work queue for serializing grpc calls
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
  toString(): string {
    return this.name;
  }

  // Create grpc connection to the mayastor server
  connect(endpoint: string) {
    if (this.client) {
      if (this.endpoint === endpoint) {
        // nothing changed
        return;
      } else {
        log.info(
          `mayastor endpoint on node "${this.name}" changed from "${this.endpoint}" to "${endpoint}"`
        );
        this.emit('node', {
          eventType: 'mod',
          object: this
        });
        this.client.close();
        if (this.syncTimer) {
          clearTimeout(this.syncTimer);
          this.syncTimer = null;
        }
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
    if (!this.client) return;
    log.info(`mayastor on node "${this.name}" is gone`);
    this.client.close();
    this.client = null;
    this.endpoint = null;
    if (this.syncTimer) {
      clearTimeout(this.syncTimer);
      this.syncTimer = null;
    }
    this.syncFailed = this.syncBadLimit + 1;
    this._offline();
  }

  unbind() {
    // todo: on user explicit removal should we destroy the pools as well?
    this.pools.forEach((pool) => pool.unbind());
    this.nexus.forEach((nexus) => nexus.unbind());
  }

  // The node is considered broken, emit offline events on all objects
  // that are present on the node.
  _offline() {
    this.emit('node', {
      eventType: 'mod',
      object: this
    });
    this.pools.forEach((pool) => pool.offline());
    this.nexus.forEach((nexus) => nexus.offline());
  }

  // Call grpc method on storage node. The calls are serialized in order
  // to prevent race conditions and inconsistencies.
  //
  // @param method    gRPC method name.
  // @param args      Arguments for gRPC method.
  // @param [timeout] Optional timeout in ms.
  // @returns A promise that evals to return value of gRPC method.
  //
  async call(method: string, args: any, timeout?: number): Promise<any> {
    return this.workq.push({ method, args, timeout }, ({method, args, timeout}) => {
      return this._call(method, args, timeout);
    });
  }

  async _call(method: string, args: any, timeout?: number): Promise<any> {
    if (!this.client) {
      throw new GrpcError(
        grpcCode.INTERNAL,
        `Broken connection to mayastor on node "${this.name}"`
      );
    }
    return this.client.call(method, args, timeout);
  }

  // Sync triggered by the timer. It ensures that the sync does run in
  // parallel with any other rpc call or another sync.
  async sync() {
    let nextSync;
    this.syncTimer = null;

    try {
      await this.workq.push(null, () => {
        return this._sync();
      });
      nextSync = this.syncPeriod;
    } catch (err) {
      // We don't want to cover up unexpected errors. But it's hard to
      // differenciate between expected and unexpected errors. At least we try.
      if (!(err instanceof GrpcError) && !err.code) {
        throw err;
      }
      nextSync = this.syncRetry;
      if (this.syncFailed++ === this.syncBadLimit) {
        log.error(`The node "${this.name}" is out of sync: ${err}`);
        this._offline();
      } else if (this.syncFailed <= this.syncBadLimit) {
        log.warn(`Failed to sync the node "${this.name}": ${err}`);
      } else {
        log.debug(`Failed to sync the node "${this.name}": ${err}`);
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
    log.debug(`Syncing the node "${this.name}"`);

    // TODO: Harden checking of outputs of the methods below
    let reply = await this._call('listNexus', {});
    const nexus = reply.nexusList;
    reply = await this._call('listPools', {});
    const pools = reply.pools;
    reply = await this._call('listReplicas', {});
    const replicas = reply.replicas;

    // Move the the node to online state before we attempt to merge objects
    // because they might need to invoke rpc methods on the node.
    const wasOffline = this.syncFailed > 0;
    if (wasOffline) {
      this.syncFailed = 0;
    }
    // merge pools and replicas
    this._mergePoolsAndReplicas(pools, replicas);
    // merge nexus
    this._mergeNexus(nexus);

    log.debug(`The node "${this.name}" was successfully synced`);

    if (wasOffline) {
      this.emit('node', {
        eventType: 'mod',
        object: this
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
  _mergePoolsAndReplicas(pools: any[], replicas: any[]) {
    // detect modified and new pools
    pools.forEach((props) => {
      const poolReplicas = replicas.filter((r) => r.pool === props.name);
      const pool = this.pools.find((p) => p.name === props.name);
      if (pool) {
        // the pool already exists - update it
        pool.merge(props, poolReplicas);
      } else {
        // it is a new pool
        this._registerPool(new Pool(props), poolReplicas);
      }
    });
    // remove pools that no longer exist
    this.pools
      .filter((p) => !pools.find((ent) => ent.name === p.name))
      .forEach((p) => p.unbind());
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
  _mergeNexus(nexusList: any[]) {
    // detect modified and new pools
    nexusList.forEach((props) => {
      const nexus = this.nexus.find((n) => n.uuid === props.uuid);
      if (nexus) {
        // the nexus already exists - update it
        nexus.merge(props);
      } else {
        // it is a new nexus
        this._registerNexus(new Nexus(props));
      }
    });
    // remove nexus that no longer exist
    const removedNexus = this.nexus.filter(
      (n) => !nexusList.find((ent) => ent.uuid === n.uuid)
    );
    removedNexus.forEach((n) => n.destroy());
  }

  // Push the new pool to a list of pools of this node.
  //
  // @param {object}   pool        New pool object.
  // @param {object[]} [replicas]  New replicas on the pool.
  //
  _registerPool(pool: Pool, replicas: any) {
    assert(!this.pools.find((p) => p.name === pool.name));
    this.pools.push(pool);
    pool.bind(this);
    replicas = replicas || [];
    replicas.forEach((r: any) => pool.registerReplica(new Replica(r)));
  }

  // Remove the pool from list of pools of this node.
  //
  // @param {object} pool  The pool to be deregistered from the node.
  //
  unregisterPool(pool: Pool) {
    const idx = this.pools.indexOf(pool);
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
  _registerNexus(nexus: Nexus) {
    assert(!this.nexus.find((p) => p.uuid === nexus.uuid));
    this.nexus.push(nexus);
    nexus.bind(this);
  }

  // Remove the nexus from list of nexus's for the node.
  //
  // @param {object} nexus  The nexus to be deregistered from the node.
  //
  unregisterNexus(nexus: Nexus) {
    const idx = this.nexus.indexOf(nexus);
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
  // @returns All replicas on this node.
  getReplicas(): Replica[] {
    return this.pools.reduce(
      (acc: Replica[], pool: Pool) => acc.concat(pool.replicas), []);
  }

  // Return true if the node is considered healthy which means that its state
  // is synchronized with the state maintained on behalf of this node object.
  //
  // @returns True if the node is healthy, false otherwise.
  //
  isSynced(): boolean {
    return this.syncFailed <= this.syncBadLimit;
  }

  // Create storage pool on this node.
  //
  // @param name   Name of the new pool.
  // @param disks  List of disk devices for the pool.
  // @returns New pool object.
  //
  async createPool(name: string, disks: string[]): Promise<Pool> {
    log.debug(`Creating pool "${name}@${this.name}" ...`);

    const poolInfo = await this.call('createPool', { name, disks });
    log.info(`Created pool "${name}@${this.name}"`);

    const newPool = new Pool(poolInfo);
    this._registerPool(newPool, []);
    return newPool;
  }

  // Create nexus on this node.
  //
  // @param uuid      ID of the new nexus.
  // @param size      Size of nexus in bytes.
  // @param replicas  Replica objects comprising the nexus.
  // @returns New nexus object.
  async createNexus(uuid: string, size: number, replicas: Replica[]): Promise<Nexus> {
    const children = replicas.map((r) => r.uri);
    log.debug(`Creating nexus "${uuid}@${this.name}"`);

    const nexusInfo = await this.call(
      'createNexus',
      { uuid, size, children },
      NEXUS_CREATE_TIMEOUT_MS,
    );
    log.info(`Created nexus "${uuid}@${this.name}"`);

    const newNexus = new Nexus(nexusInfo);
    this._registerNexus(newNexus);
    return newNexus;
  }

  // Get IO statistics for all replicas on the node.
  //
  // @returns Array of stats - one entry for each replica on the node.
  async getStats(): Promise<ReplicaStat[]> {
    log.debug(`Retrieving replica stats from node "${this}"`);
    const reply = await this.call('statReplicas', {});
    const timestamp = new Date().toISOString();
    return reply.replicas.map((r: any) => {
      return {
        timestamp,
        // tags
        uuid: r.uuid,
        node: this.name,
        pool: r.pool,
        // counters
        num_read_ops: r.stats.numReadOps,
        num_write_ops: r.stats.numWriteOps,
        bytes_read: r.stats.bytesRead,
        bytes_written: r.stats.bytesWritten
      };
    });
  }
}
