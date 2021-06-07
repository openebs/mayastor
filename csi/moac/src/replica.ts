// Replica object implementation.

import assert from 'assert';
import * as _ from 'lodash';

import { grpcCode, GrpcError } from './grpc_client';
import { Logger } from './logger';
import { Pool } from './pool';

const log = Logger('replica');
const parse = require('url-parse');

// Replica destruction on mayastor node can be very slow. Until the problem
// is fixed in mayastor we use 1 hour timeout for destroy calls.
const DESTROY_TIMEOUT_MS = 60 * 60 * 1000;

export class Replica {
  pool?: Pool;
  uuid: string;
  size: number;
  // TODO: define an enum
  share: string;
  uri: string;
  isDown: boolean;
  realUuid?: string;

  // Create replica object.
  //
  // @param {object} props  Replica properties obtained from storage node.
  constructor(props: any) {
    this.pool = undefined; // set by pool object during registration
    this.uuid = props.uuid;
    this.size = props.size;
    this.share = props.share;
    this.uri = props.uri;
    this.isDown = false;
    this.realUuid = parse(this.uri, true).query['uuid'];
  }

  // Stringify replica.
  toString() {
    return this.uuid + '@' + (this.pool ? this.pool.name : 'nowhere');
  }

  // Update object based on fresh properties obtained from mayastor storage node.
  //
  // @param {object}   props          Properties defining the replica.
  // @param {string}   props.uuid     ID of replica.
  // @param {number}   props.size     Capacity of the replica in bytes.
  // @param {string}   props.share    Share protocol of replica.
  // @param {string}   props.uri      URI to be used by nexus to access it.
  //
  merge(props: any) {
    if (!this.pool) {
      throw new Error('Cannot merge replica that has not been bound');
    }
    let changed = false;

    if (this.size !== props.size) {
      this.size = props.size;
      changed = true;
    }
    if (this.share !== props.share) {
      this.share = props.share;
      changed = true;
    }
    if (this.uri !== props.uri) {
      this.uri = props.uri;
      changed = true;
    }
    if (this.isDown) {
      this.isDown = false;
      changed = true;
    }
    if (changed && this.pool.node) {
      this.pool.node.emit('replica', {
        eventType: 'mod',
        object: this
      });
    }
  }

  // Set state of the replica to offline.
  // This is typically called when mayastor stops running on the node and
  // the replicas become inaccessible.
  offline() {
    if (!this.pool) {
      throw new Error('Cannot offline a replica that has not been bound');
    }
    log.warn(`Replica "${this}" got offline`);
    this.isDown = true;
    if (this.pool.node) {
      this.pool.node.emit('replica', {
        eventType: 'mod',
        object: this
      });
    }
  }

  // Return true if replica is offline otherwise false.
  isOffline() {
    return this.isDown;
  }

  // Export replica over given storage protocol for IO (NONE, ISCSI or NVMF).
  // NONE means that the replica can be accessed only locally in SPDK process.
  //
  // @param   {string} share    Name of the share protocol or "NONE" to unshare it.
  // @returns {string} URI used to reach replica from nexus.
  //
  async setShare(share: string) {
    var res;

    assert(
      ['REPLICA_NONE', 'REPLICA_ISCSI', 'REPLICA_NVMF'].indexOf(share) >= 0
    );
    if (!this.pool) {
      throw new Error('Cannot set share protocol when replica is not bound');
    }
    if (!this.pool.node) {
      throw new Error('Cannot set share protocol when pool is not bound');
    }
    log.debug(`Setting share protocol "${share}" for replica "${this}" ...`);

    try {
      res = await this.pool.node.call('shareReplica', {
        uuid: this.uuid,
        share
      });
    } catch (err) {
      throw new GrpcError(
        grpcCode.INTERNAL,
        `Failed to set share pcol for replica "${this}": ` + err
      );
    }
    log.info(`Share pcol for replica "${this}" was set: ${res.uri}`);
    this.share = share;
    this.uri = res.uri;
    this.pool.node.emit('replica', {
      eventType: 'mod',
      object: this
    });
    return res.uri;
  }

  // Destroy replica on storage node.
  //
  // This must be called after the replica is removed from nexus.
  async destroy() {
    log.debug(`Destroying replica "${this}" ...`);
    if (!this.pool) {
      throw new Error('Cannot destroy a replica that has not been bound');
    }
    if (!this.pool?.node?.isSynced()) {
      // We don't want to block the volume life-cycle in case that the node
      // is down - it may never come back online.
      log.warn(`Faking the destroy of "${this}" because it is unreachable`);
    } else {
      await this.pool.node.call(
        'destroyReplica',
        { uuid: this.uuid },
        DESTROY_TIMEOUT_MS,
      );
      log.info(`Destroyed replica "${this}"`);
    }
    this.unbind();
  }

  // Associate replica with a pool.
  //
  // @param {object} pool   Pool object to associate the replica with.
  //
  bind(pool: Pool) {
    this.pool = pool;
    log.debug(
      `Adding "${this.uuid}" to the list of replicas for the pool "${pool}"`
    );
    if (this.pool.node) {
      this.pool.node.emit('replica', {
        eventType: 'new',
        object: this
      });
    }
  }

  // Remove the replica reference from pool
  unbind() {
    if (!this.pool) return;
    log.debug(`Removing replica "${this}" from the list of replicas`);
    this.pool.unregisterReplica(this);
    if (this.pool.node) {
      this.pool.node.emit('replica', {
        eventType: 'del',
        object: this
      });
    }
    this.pool = undefined;
  }
}