// Pool object implementation.

import assert from 'assert';
import * as _ from 'lodash';

import { grpcCode, GrpcError } from './grpc_client';
import { Node } from './node';
import { Replica } from './replica';
import { Logger } from './logger';

const log = Logger('pool');

const URI_REGEX = /^([^:]+):\/\/(.+)$/;

// Utility function to strip URI prefix from a string.
//
// Normally we should not be stripping URIs but because mayastor gRPC does
// not support URIs when creating a pool yet, we have to.
function _stripUri(str: string) {
  const match = URI_REGEX.exec(str);
  return match ? match[2] : str;
}

export class Pool {
  node?: Node;
  name: string;
  disks: [string];
  // TODO: define an enum
  state: string;
  capacity: number;
  used: number;
  replicas: Replica[];

  // Build pool object from JSON object received from mayastor storage node.
  //
  // @param {object}   props          Pool properties defining the pool.
  // @param {string}   props.name     Pool name.
  // @param {string[]} props.disks    List of disks comprising the pool.
  // @param {string}   props.state    State of the pool.
  // @param {number}   props.capacity Capacity of the pool in bytes.
  // @param {number}   props.used     How many bytes are used in the pool.
  constructor(props: any) {
    this.node = undefined; // set by registerPool method on node
    this.name = props.name;
    this.disks = props.disks.sort();
    this.state = props.state;
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
  merge(props: any, replicas: any[]) {
    let changed = false;

    // If access protocol to the disk has changed, it is ok and allowed.
    // Though if device has changed then it is at least unusual and we log
    // a warning message.
    props.disks.sort();
    if (!_.isEqual(this.disks, props.disks)) {
      let oldDisks = this.disks.map(_stripUri).sort();
      let newDisks = props.disks.map(_stripUri).sort();
      if (!_.isEqual(oldDisks, newDisks)) {
        log.warn(
          `Unexpected disk change in the pool "${this}" from ${oldDisks} to ${newDisks}`
        );
      }
      this.disks = props.disks;
      changed = true;
    }
    if (this.state !== props.state) {
      this.state = props.state;
      changed = true;
    }
    if (this.capacity !== props.capacity) {
      this.capacity = props.capacity;
      changed = true;
    }
    if (this.used !== props.used) {
      this.used = props.used;
      changed = true;
    }
    if (changed && this.node) {
      this.node.emit('pool', {
        eventType: 'mod',
        object: this
      });
    }

    this._mergeReplicas(replicas);
  }

  // Merge old and new list of replicas.
  //
  // @param {object[]} replicas   New list of replicas properties for the pool.
  //
  _mergeReplicas(replicas: any[]) {
    var self = this;
    // detect modified and new replicas
    replicas.forEach((props) => {
      const replica = self.replicas.find((r) => r.uuid === props.uuid);
      if (replica) {
        // the replica already exists - update it
        replica.merge(props);
      } else {
        // it is a new replica
        self.registerReplica(new Replica(props));
      }
    });
    // remove replicas that no longer exist
    const removedReplicas = self.replicas.filter(
      (r) => !replicas.find((ent) => ent.uuid === r.uuid)
    );
    removedReplicas.forEach((r) => r.unbind());
  }

  // Add new replica to a list of replicas for this pool and emit new event
  // for the replica.
  //
  // @param {object} replica      New replica object.
  //
  registerReplica(replica: Replica) {
    assert(!this.replicas.find((r) => r.uuid === replica.uuid));
    assert(replica.realUuid !== undefined);
    this.replicas.push(replica);
    replica.bind(this);
  }

  // Remove replica from the list of replicas for this pool.
  //
  // @param {object} replica      Replica object to remove.
  //
  unregisterReplica(replica: Replica) {
    const idx = this.replicas.indexOf(replica);
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
  // @param node   Node object to assign the pool to.
  //
  bind(node: Node) {
    this.node = node;
    log.debug(`Adding pool "${this.name}" to the list of pools on "${node}"`);
    this.node.emit('pool', {
      eventType: 'new',
      object: this
    });
  }

  // Unbind the previously bound pool from the node.
  unbind() {
    if (!this.node) return;
    log.debug(`Removing pool "${this}" from the list of pools`);
    this.replicas.forEach((r) => r.unbind());
    this.node.unregisterPool(this);

    this.node.emit('pool', {
      eventType: 'del',
      object: this
    });
    this.node = undefined;
  }

  // Return amount of free space in the storage pool.
  //
  // @returns {number} Free space in bytes.
  freeBytes() {
    return this.capacity - this.used;
  }

  // Destroy the pool and remove it from the list of pools on the node.
  async destroy() {
    if (!this.node) {
      throw new GrpcError(
        grpcCode.INTERNAL,
        `Cannot destroy disassociated pool "${this}"`,
      );
    }
    log.debug(`Destroying pool "${this}" ...`);
    await this.node.call('destroyPool', { name: this.name });
    log.info(`Destroyed pool "${this}"`);
    this.unbind();
  }

  // Set state of the pool to offline and the same for all replicas on the pool.
  // This is typically called when mayastor stops running on the node and
  // the pool becomes inaccessible.
  offline() {
    log.warn(`Pool "${this}" got offline`);
    this.replicas.forEach((r) => r.offline());
    // artificial state that does not appear in grpc protocol
    this.state = 'POOL_OFFLINE';
    if (this.node) {
      this.node.emit('pool', {
        eventType: 'mod',
        object: this
      });
    }
  }

  // Return true if pool exists and is accessible, otherwise false.
  isAccessible() {
    return this.state === 'POOL_ONLINE' || this.state === 'POOL_DEGRADED';
  }

  // Create replica in this storage pool.
  //
  // @param {string} uuid   ID of the new replica.
  // @param {number} size   Size of the replica in bytes.
  //
  async createReplica(uuid: string, size: number) {
    if (!this.node) {
      throw new GrpcError(
        grpcCode.INTERNAL,
        `Cannot create replica on disassociated pool "${this}"`,
      );
    }
    const pool = this.name;
    const thin = false;
    const share = 'REPLICA_NONE';

    log.debug(`Creating replica "${uuid}" on the pool "${this}" ...`);

    var replicaInfo = await this.node.call('createReplica', { uuid, pool, size, thin, share });
    log.info(`Created replica "${uuid}" on the pool "${this}"`);

    const newReplica = new Replica(replicaInfo);
    this.registerReplica(newReplica);
    return newReplica;
  }
}