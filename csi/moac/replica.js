// Replica object implementation.

'use strict';

const _ = require('lodash');
const assert = require('assert');
const { GrpcCode, GrpcError } = require('./grpc_client');
const log = require('./logger').Logger('replica');

class Replica {
  // Create replica object.
  //
  // @param {object} props  Replica properties obtained from storage node.
  constructor(props) {
    this.pool = null; // set by pool object during registration
    this.uuid = props.uuid;
    this.size = props.size;
    this.share = props.share;
    this.uri = props.uri;
    this.state = props.state;
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
  // @param {string}   props.state    State of the replica.
  //
  merge(props) {
    let changed = false;

    if (this.size != props.size) {
      this.size = props.size;
      changed = true;
    }
    if (this.share != props.share) {
      this.share = props.share;
      changed = true;
    }
    if (this.uri != props.uri) {
      this.uri = props.uri;
      changed = true;
    }
    if (this.state != props.state) {
      this.state = props.state;
      changed = true;
    }
    if (changed) {
      this.pool.node.emit('replica', {
        eventType: 'mod',
        object: this,
      });
    }
  }

  // Set state of the pool to offline and the same for all replicas on the pool.
  // This is typically called when mayastor stops running on the node and
  // the pool becomes inaccessible.
  offline() {
    log.warn(`Replica "${this}" got offline`);
    this.state = 'OFFLINE';
    this.pool.node.emit('replica', {
      eventType: 'mod',
      object: this,
    });
  }

  // Export replica over given storage protocol for IO (NONE, ISCSI or NVMF).
  // NONE means that the replica can be accessed only locally in SPDK process.
  //
  // @param   {string} share    Name of the share protocol or "NONE" to unshare it.
  // @returns {string} URI used to reach replica from nexus.
  //
  async setShare(share) {
    var res;

    assert(
      ['REPLICA_NONE', 'REPLICA_ISCSI', 'REPLICA_NVMF'].indexOf(share) >= 0
    );
    log.debug(`Setting share protocol for replica "${this}" ...`);

    try {
      res = await this.pool.node.call('shareReplica', {
        uuid: this.uuid,
        share,
      });
    } catch (err) {
      throw new GrpcError(
        GrpcCode.INTERNAL,
        `Failed to set share pcol for replica "${this}": ` + err
      );
    }
    log.info(`Share pcol for replica "${this}" set to ${share} (${res.uri})`);
    this.share = share;
    this.uri = res.uri;
    this.pool.node.emit('replica', {
      eventType: 'mod',
      object: this,
    });
    return res.uri;
  }

  // Destroy replica on storage node.
  //
  // This must be called after the replica is removed from nexus.
  async destroy() {
    log.debug(`Destroying replica "${this}" ...`);

    try {
      await this.pool.node.call('destroyReplica', { uuid: this.uuid });
      log.info(`Destroyed replica "${this}"`);
    } catch (err) {
      // TODO: make destroyReplica idempotent
      if (err.code != GrpcCode.NOT_FOUND) {
        throw err;
      }
      log.warn(`Destroyed replica "${this}" does not exist`);
    }

    this.unbind();
  }

  // Associate replica with given pool.
  //
  // @param {object} pool   Pool object to associate the replica with.
  //
  bind(pool) {
    assert(!this.pool);
    this.pool = pool;
    log.info(`Adding replica "${this}" to a list`);
    this.pool.node.emit('replica', {
      eventType: 'new',
      object: this,
    });
  }

  // Remove the replica reference from pool
  unbind() {
    log.info(`Removing replica "${this}" from a list`);
    this.pool.unregisterReplica(this);
    this.pool.node.emit('replica', {
      eventType: 'del',
      object: this,
    });
    this.pool = null;
  }
}

module.exports = Replica;
