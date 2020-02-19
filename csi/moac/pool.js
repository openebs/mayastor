// Pool object implementation.

'use strict';

const _ = require('lodash');
const assert = require('assert');
const { GrpcCode, GrpcError } = require('./grpc_client');
const log = require('./logger').Logger('pool');
const Replica = require('./replica');

class Pool {
  // Build pool object from JSON object received from mayastor storage node.
  //
  // @param {object}   props          Pool properties defining the pool.
  // @param {string}   props.name     Pool name.
  // @param {string[]} props.disks    List of disks comprising the pool.
  // @param {string}   props.state    State of the pool.
  // @param {number}   props.capacity Capacity of the pool in bytes.
  // @param {number}   props.used     How many bytes are used in the pool.
  constructor(props) {
    this.node = null; // set by registerPool method on node
    this.name = props.name;
    this.disks = props.disks.sort();
    this.state = props.state;
    this.reason = '';
    this.capacity = props.capacity;
    this.used = props.used;
    this.replicas = [];
  }

  toString() {
    return this.name + '@' + (this.node ? this.node.name : 'nowhere');
  }

  // Update object based on fresh properties obtained from mayastor storage node.
  //
  // @param {object}   props          Pool properties defining the pool.
  // @param {string}   props.name     Pool name.
  // @param {string[]} props.disks    List of disks comprising the pool.
  // @param {string}   props.state    State of the pool.
  // @param {number}   props.capacity Capacity of the pool in bytes.
  // @param {number}   props.used     How many bytes are used in the pool.
  // @param {object[]} replicas       Replicas on the pool.
  merge(props, replicas) {
    let changed = false;

    // The first case should not normally happen. We log a warning,
    // record the change to the object but refrain from propagating the
    // information further as it is not clear what the higher level code
    // should do in such case or how to recover.
    if (!_.isEqual(this.disks, props.disks.sort())) {
      log.warn(
        `Unexpected disk change of the pool "${this}" from ${this.disks} to ${props.disks}`
      );
    }
    if (this.state != props.state) {
      this.state = props.state;
      changed = true;
    }
    if (this.capacity != props.capacity) {
      this.capacity = props.capacity;
      changed = true;
    }
    if (this.used != props.used) {
      this.used = props.used;
      changed = true;
    }
    if (changed) {
      this.node.emit('pool', {
        eventType: 'mod',
        object: this,
      });
    }

    this._mergeReplicas(replicas);
  }

  // Merge old and new list of replicas.
  //
  // @param {object[]} replicas   New list of replicas properties for the pool.
  //
  _mergeReplicas(replicas) {
    var self = this;
    // detect modified and new replicas
    replicas.forEach(props => {
      let replica = self.replicas.find(r => r.uuid == props.uuid);
      if (replica) {
        // the replica already exists - update it
        replica.merge(props);
      } else {
        // it is a new replica
        self.registerReplica(new Replica(props));
      }
    });
    // remove replicas that no longer exist
    let removedReplicas = self.replicas.filter(
      r => !replicas.find(ent => ent.uuid == r.uuid)
    );
    removedReplicas.forEach(r => r.unbind());
  }

  // Add new replica to a list of replicas for this pool and emit new event
  // for the replica.
  //
  // @param {object} replica      New replica object.
  //
  registerReplica(replica) {
    assert(!this.replicas.find(r => r.uuid == replica.uuid));
    this.replicas.push(replica);
    replica.bind(this);
  }

  // Remove replica from the list of replicas for this pool.
  //
  // @param {object} replica      Replica object to remove.
  //
  unregisterReplica(replica) {
    let idx = this.replicas.indexOf(replica);
    if (idx >= 0) {
      this.replicas.splice(idx, 1);
    } else {
      log.warn(
        `Replica "${replica}" is being deregistered and not assigned to the pool "${this}"`
      );
    }
  }

  // Assign the pool to a node. It should be done right after creating
  // the pool object.
  //
  // @param {object} node   Node object to assign the pool to.
  //
  bind(node) {
    assert(!this.node);
    this.node = node;
    log.info(`Adding pool "${this}" to a list`);
    this.node.emit('pool', {
      eventType: 'new',
      object: this,
    });
  }

  // Unbind the previously bound pool from the node.
  unbind() {
    log.info(`Removing pool "${this}" from a list`);
    this.replicas.forEach(r => r.unbind());
    this.node.unregisterPool(this);

    this.node.emit('pool', {
      eventType: 'del',
      object: this,
    });
    this.node = null;
  }

  // Return amount of free space in the storage pool.
  //
  // @returns {number} Free space in bytes.
  freeBytes() {
    return this.capacity - this.used;
  }

  // Destroy the pool and remove it from the list of pools on the node.
  async destroy() {
    log.debug(`Destroying pool "${this}" ...`);

    try {
      await this.node.call('destroyPool', { name: this.name });
      log.info(`Destroyed pool "${this}"`);
    } catch (err) {
      // TODO: make destroyPool idempotent
      if (err.code != GrpcCode.NOT_FOUND) {
        throw err;
      }
      log.warn(`Removed pool "${this}" does not exist`);
    }
    this.unbind();
  }

  // Set state of the pool to offline and the same for all replicas on the pool.
  // This is typically called when mayastor stops running on the node and
  // the pool becomes inaccessible.
  offline() {
    log.warn(`Pool "${this}" got offline`);
    this.replicas.forEach(r => r.offline());
    this.state = 'OFFLINE';
    this.reason = `mayastor does not run on the node "${this.node.name}"`;
    this.node.emit('pool', {
      eventType: 'mod',
      object: this,
    });
  }

  // Update "state" and "reason" of the pool. The reason is used to further
  // explain a cause of the state. It is the only information that should
  // ever be set from the pool operator. The rest of information is either
  // set during pool creation and is immutable or obtained from storage node
  // through gRPC.
  //
  // @param {string} state   New state of the pool.
  // @param {string} reason  Reason for the new state.
  //
  setState(state, reason) {
    assert(['ONLINE', 'DEGRADED', 'PENDING', 'OFFLINE'].indexOf(state) >= 0);

    if (this.state != state) {
      let reasonSuffix = '';
      if (reason) {
        reasonSuffix = ': ' + reason;
      }
      log.info(`Pool "${this}" got ${state}` + reasonSuffix);
    }

    this.state = state;
    this.reason = reason || '';
  }

  // Return true if pool exists and is accessible, otherwise false.
  isAccessible() {
    return this.state == 'ONLINE' || this.state == 'DEGRADED';
  }

  // Create replica in this storage pool.
  //
  // @param {string} uuid   ID of the new replica.
  // @param {number} size   Size of the replica in bytes.
  //
  async createReplica(uuid, size) {
    let pool = this.name;
    const thin = false;
    const share = 'REPLICA_NONE';

    log.debug(`Creating replica "${uuid}" on the pool "${this}" ...`);

    try {
      await this.node.call('createReplica', { uuid, pool, size, thin, share });
      log.info(`Created replica "${uuid}" on the pool "${this}"`);
    } catch (err) {
      // TODO: Make rpc idempotent
      if (err.code != GrpcCode.ALREADY_EXISTS) {
        throw err;
      }
    }

    // it's not done yet, we have to get properties of the replica to
    // obtain its state, used bytes, etc.
    var resp;
    try {
      resp = await this.node.call('listReplicas', {});
    } catch (err) {
      throw new GrpcError(
        GrpcCode.INTERNAL,
        `Failed to list new replica "${uuid}" on pool "${this}": ${err}`
      );
    }
    var replicaInfo = resp.replicas.filter(r => r.uuid == uuid)[0];
    if (!replicaInfo) {
      throw new GrpcError(
        GrpcCode.INTERNAL,
        `New replica "${uuid}" on pool "${this}" not found`
      );
    }
    let newReplica = new Replica(replicaInfo);
    this.registerReplica(newReplica);
    return newReplica;
  }
}

module.exports = Pool;
