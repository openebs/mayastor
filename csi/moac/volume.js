// Volume object abstracts user from volume components nexus and
// replicas and implements algorithms for volume recovery.

'use strict';

const _ = require('lodash');
const assert = require('assert');
const log = require('./logger').Logger('volume');
const { GrpcCode, GrpcError } = require('./grpc_client');

// Abstraction of the volume. It is an abstract object which consists of
// physical entities nexus and replicas. It provides high level methods
// for doing operations on the volume as well as recovery algorithms for
// maintaining desired redundancy.
class Volume {
  // Construct a volume object with given uuid.
  //
  // @params {string}   uuid                 ID of the volume.
  // @params {object}   registry             Registry object.
  // @params {object}   spec                 Volume parameters.
  // @params {number}   spec.replicaCount    Number of desired replicas.
  // @params {string[]} spec.preferredNodes  Nodes to prefer for scheduling replicas.
  // @params {string[]} spec.requiredNodes   Replicas must be on these nodes.
  // @params {number}   spec.requiredBytes   The volume must have at least this size.
  // @params {number}   spec.limitBytes      The volume should not be bigger than this.
  // @params {string}   spec.protocol        The share protocol for the nexus.
  // @params {object}   [size=0]             Current properties of the volume.
  //
  constructor (uuid, registry, spec, size = 0) {
    assert(spec);
    // specification of the volume
    this.uuid = uuid;
    this.registry = registry;
    this.replicaCount = spec.replicaCount || 1;
    this.preferredNodes = _.clone(spec.preferredNodes || []).sort();
    this.requiredNodes = _.clone(spec.requiredNodes || []).sort();
    this.requiredBytes = spec.requiredBytes;
    this.limitBytes = spec.limitBytes;
    this.protocol = spec.protocol;
    this.size = size;
    // state variables of the volume
    this.nexus = null;
    this.replicas = {}; // replicas indexed by node name
    this.state = 'pending';
    this.runFsa = 0; // number of requests to run FSA
  }

  // Stringify volume
  toString () {
    return this.uuid;
  }

  // Get the size of the volume.
  getSize () {
    return this.size;
  }

  // Get the node which the volume is accessible from
  // (currently that is where the nexus is).
  getNodeName () {
    return this.nexus ? this.nexus.node.name : '';
  }

  // Publish the volume. That means make it accessible through a block device.
  // @params {string}   protocol      The nexus share protocol.
  // @return {string}   uri           The URI to access the nexus.
  async publish (protocol) {
    if (this.nexus) {
      const uri = await this.nexus.publish(protocol);
      return uri;
    } else {
      throw new GrpcError(
        GrpcCode.INTERNAL,
        'Cannot publish a volume without nexus'
      );
    }
  }

  // Undo publish operation on the volume.
  async unpublish () {
    if (this.nexus) {
      await this.nexus.unpublish();
    } else {
      throw new GrpcError(
        GrpcCode.INTERNAL,
        'Cannot unpublish a volume without nexus'
      );
    }
  }

  // Delete nexus and destroy all replicas of the volume.
  async destroy () {
    if (this.nexus) {
      await this.nexus.destroy();
    }
    const promises = Object.values(this.replicas).map((replica) =>
      replica.destroy()
    );
    await Promise.all(promises);
  }

  // Trigger the run of FSA. It will either run immediately or if it is already
  // running, it will start again when the current run finishes.
  //
  // Why critical section on fsa? Certain operations done by fsa are async. If
  // we allow another process to enter fsa before the async operation is done
  // and the state of volume updated we risk that the second process repeats
  // exactly the same action (because from its point of view it hasn't been
  // done yet).
  fsa () {
    if (this.runFsa++ === 0) {
      this._fsa().finally(() => {
        const runAgain = this.runFsa > 1;
        this.runFsa = 0;
        if (runAgain) this.fsa();
      });
    }
  }

  // Implementation of finite state automaton (FSA) that moves the volume
  // through states: pending, degraded, faulted, healthy - trying to preserve
  // data on volume "no matter what".
  async _fsa () {
    if (!this.nexus) {
      // nexus does not exist yet - nothing to do
      assert.strictEqual(this.state, 'pending');
      return;
    }
    log.debug(`Volume "${this}" enters FSA in ${this.state} state`);

    if (this.nexus.state === 'NEXUS_OFFLINE') {
      // if nexus is not accessible then the information about children is stale
      // and we cannot make any reasonable decisions, so bail out.
      this._setState('offline');
      return;
    }

    // check that replicas are shared as they should be
    for (const nodeName in this.replicas) {
      const replica = this.replicas[nodeName];
      if (!replica.isOffline()) {
        let share;
        const local = replica.pool.node === this.nexus.node;
        // make sure that replica that is local to the nexus is accessed locally
        if (local && replica.share !== 'REPLICA_NONE') {
          share = 'REPLICA_NONE';
        } else if (!local && replica.share === 'REPLICA_NONE') {
          // make sure that replica that is remote to nexus can be accessed
          share = 'REPLICA_NVMF';
        }
        if (share) {
          try {
            await replica.setShare(share);
            // fsa will get called again because the replica was modified
            return;
          } catch (err) {
            throw new GrpcError(
              GrpcCode.INTERNAL,
              `Failed to set share protocol to ${share} for replica "${replica}": ${err}`
            );
          }
        }
      }
    }
    // pair nexus children with replica objects to get the full picture
    var self = this;
    const children = this.nexus.children.map((ch) => {
      return {
        uri: ch.uri,
        state: ch.state,
        replica: Object.values(self.replicas).find((r) => r.uri === ch.uri)
      };
    });
    // add newly found replicas to the nexus (one by one)
    const newReplica = Object.values(this.replicas).filter(
      (r) => !r.isOffline() && !children.find((ch) => ch.replica === r)
    )[0];
    if (newReplica) {
      try {
        await this.nexus.addReplica(newReplica);
      } catch (err) {
        log.error(err.toString());
      }
      return;
    }

    // If there is not a single replica that is online then there is no hope
    // that we could rebuild anything.
    var onlineCount = children.filter((ch) => ch.state === 'CHILD_ONLINE')
      .length;
    if (onlineCount === 0) {
      this._setState('faulted');
      return;
    }

    // If we don't have sufficient number of sound replicas (sound means online
    // , under rebuild or pending) then add a new one.
    var soundCount = children.filter((ch) => {
      return ['CHILD_ONLINE', 'CHILD_DEGRADED'].indexOf(ch.state) >= 0;
    }).length;
    if (this.replicaCount > soundCount) {
      this._setState('degraded');
      // add new replica
      try {
        await this._createReplicas(this.replicaCount - soundCount);
      } catch (err) {
        log.error(err.toString());
      }
      // The replicas will be added to nexus when the fsa is run next time
      // which happens immediately after we exit.
      return;
    }

    // The condition for later actions is that volume must not be rebuilding or
    // waiting for a child add. So check that and return if that's the case.
    var rebuildCount = children.filter((ch) => ch.state === 'CHILD_DEGRADED').length;
    if (rebuildCount > 0) {
      this._setState('degraded');
      return;
    }

    assert(onlineCount >= this.replicaCount);
    this._setState('healthy');

    // If we have more online replicas then we need to, then remove one.
    // Child that is broken and without a replica is a good fit for removal.
    let rmChild = children.find(
      (ch) => !ch.replica && ch.state === 'CHILD_FAULTED'
    );
    if (!rmChild) {
      rmChild = children.find((ch) => ch.state === 'CHILD_FAULTED');
      if (!rmChild) {
        // A child that is unknown to us (without replica object)
        rmChild = children.find((ch) => !ch.replica);
        // If all replicas are online, then continue searching for a candidate
        // only if there are more online replicas than it needs to be.
        if (!rmChild && onlineCount > this.replicaCount) {
          // The replica with the lowest score must go away
          const rmReplica = this._prioritizeReplicas(
            children.map((ch) => ch.replica)
          ).pop();
          if (rmReplica) {
            rmChild = children.find((ch) => ch.replica === rmReplica);
          }
        }
      }
    }
    if (rmChild) {
      try {
        await this.nexus.removeReplica(rmChild.uri);
      } catch (err) {
        log.error(err.toString());
        return;
      }
      if (rmChild.replica) {
        try {
          await rmChild.replica.destroy();
        } catch (err) {
          log.error(err.toString());
        }
      }
      return;
    }

    // If a replica should run on a different node then move it
    var moveChild = children.find((ch) => {
      if (
        ch.replica &&
        ch.state === 'CHILD_ONLINE' &&
        self.requiredNodes.length > 0 &&
        self.requiredNodes.indexOf(ch.replica.pool.node.name) < 0
      ) {
        if (self.requiredNodes.indexOf(ch.replica.pool.node.name) < 0) {
          return true;
        }
      }
      return false;
    });
    if (moveChild) {
      // We add a new replica and the old one will be removed when both are
      // online since there will be more of them than needed. We do one by one
      // not to trigger too many changes.
      try {
        await this._createReplicas(1);
      } catch (err) {
        log.error(err.toString());
      }
    }
  }

  // Change the volume state to given state. If the state is not the same as
  // previous one, we should emit a volume mod event.
  //
  // TODO: we should emit but we don't because currently we don't have reference
  // to the volumes object. Instead we rely that every state transition is
  // triggered by another event (i.e. new replica) so the volume operator will
  // be notified about the change anyway. It would be nice to fix this when we
  // replace our ad-hoc message bus by something better what allows us to store
  // the reference to message channel in every volume.
  //
  // @param {string} newState   New state to set on volume.
  _setState (newState) {
    if (this.state !== newState) {
      if (newState === 'healthy') {
        log.info(`Volume "${this}" is ${newState}`);
      } else {
        log.warn(`Volume "${this}" is ${newState}`);
      }
      this.state = newState;
    }
  }

  // Create the volume in accordance with requirements specified during the
  // object creation. Create whatever component is missing (note that we
  // might not be creating it from the scratch).
  //
  // NOTE: Until we create a nexus at the end, the volume is not acted upon by FSA.
  // When "new nexus" event comes in, that moves it from pending state and kicks
  // off FSA. Exactly what we want, because the async events produced by this
  // function do not interfere with execution of the "create".
  async create () {
    log.debug(`Creating the volume "${this}"`);
    assert(!this.nexus);

    // Ensure there is sufficient number of replicas for the volume.
    // TODO: take replica state into account
    const newReplicaCount = this.replicaCount - Object.keys(this.replicas).length;
    if (newReplicaCount > 0) {
      // create more replicas if higher replication factor is desired
      await this._createReplicas(newReplicaCount);
    }

    // Ensure replicas can be accessed from nexus. Set share protocols.
    const replicaSet = await this._ensureReplicaShareProtocols();

    // If the nexus poped up while we were creating replicas pick it up now.
    // Though it's an unsual situation so we log a warning if it happens.
    const nexus = this.registry.getNexus(this.uuid);
    if (nexus) {
      log.warn(
        `The nexus "${nexus}" appeared while creating replicas - using it`
      );
      this.newNexus(nexus);
      return;
    }
    if (!this.size) {
      // the size will be the smallest replica
      this.size = Object.values(this.replicas)
        .map((r) => r.size)
        .reduce((acc, cur) => (cur < acc ? cur : acc), Number.MAX_SAFE_INTEGER);
    }
    // create a new nexus with children (replicas) created in previous steps
    this.nexus = await this._createNexus(replicaSet);
    log.info(`Volume "${this}" with size ${this.size} was created`);
  }

  // Update child devices of existing nexus or create a new nexus if it does not
  // exist.
  //
  // @param {object[]} replicas   Replicas that should be used for child bdevs of nexus.
  // @returns {object} Created nexus object.
  //
  async _createNexus (replicas) {
    // create a new nexus
    const localReplica = Object.values(this.replicas).find(
      (r) => r.share === 'REPLICA_NONE'
    );
    if (!localReplica) {
      // should not happen but who knows ..
      throw new GrpcError(
        GrpcCode.INTERNAL,
        'Cannot create nexus if none of the replicas is local'
      );
    }
    return localReplica.pool.node.createNexus(
      this.uuid,
      this.size,
      Object.values(replicas)
    );
  }

  // Adjust replica count for the volume to required count.
  //
  // TODO: Take into account state of replicas.
  //
  // @param {number} newCount   Number of new replicas to create.
  //
  async _createReplicas (count) {
    let pools = this.registry.choosePools(
      this.requiredBytes,
      this.requiredNodes,
      this.preferredNodes
    );
    // remove pools that are already used by existing replicas
    const usedNodes = Object.keys(this.replicas);
    pools = pools.filter((p) => usedNodes.indexOf(p.node.name) < 0);
    if (pools.length < count) {
      log.error(
        `No suitable pool(s) for volume "${this}" with capacity ` +
          `${this.requiredBytes} and replica count ${this.replicaCount}`
      );
      throw new GrpcError(
        GrpcCode.RESOURCE_EXHAUSTED,
        'Cannot find suitable storage pool(s) for the volume'
      );
    }

    // Calculate the size of the volume if not given precisely.
    //
    // TODO: Size of the smallest pool is a safe choice though too conservative.
    if (!this.size) {
      this.size = Math.min(
        pools.reduce(
          (acc, pool) => Math.min(acc, pool.freeBytes()),
          Number.MAX_SAFE_INTEGER
        ),
        this.limitBytes || this.requiredBytes
      );
    }

    // We record all failures as we try to create the replica on available
    // pools to return them to the user at the end if we ultimately fail.
    const errors = [];
    // try one pool after another until success
    for (let i = 0; i < pools.length && count > 0; i++) {
      const pool = pools[i];

      try {
        // this will add the replica to the cache if successful
        await pool.createReplica(this.uuid, this.size);
      } catch (err) {
        log.error(err.message);
        errors.push(err.message);
        continue;
      }
      count--;
    }
    // check if we created enough replicas
    if (count > 0) {
      let msg = `Failed to create required number of replicas for volume "${this}": `;
      msg += errors.join('. ');
      throw new GrpcError(GrpcCode.INTERNAL, msg);
    }
  }

  // Get list of replicas for this volume sorted from the most to the
  // least preferred.
  //
  // @returns {object[]}  List of replicas sorted by preference (the most first).
  //
  _prioritizeReplicas (replicas) {
    const self = this;
    return Object.values(replicas).sort(
      (a, b) => self._scoreReplica(b) - self._scoreReplica(a)
    );
  }

  // Assign score to a replica based on certain criteria. The higher the better.
  //
  // @param   {object} replica  Replica object.
  // @returns {number} Score from 0 to 18.
  //
  _scoreReplica (replica) {
    let score = 0;
    const node = replica.pool.node;

    // criteria #1: must be on the required nodes if set
    if (
      this.requiredNodes.length > 0 &&
      this.requiredNodes.indexOf(node.name) >= 0
    ) {
      score += 10;
    }
    // criteria #2: replica should be online
    if (!replica.isOffline()) {
      score += 5;
    }
    // criteria #2: would be nice to run on preferred node
    if (
      this.preferredNodes.length > 0 &&
      this.preferredNodes.indexOf(node.name) >= 0
    ) {
      score += 2;
    }
    // criteria #3: local IO from nexus is certainly an advantage
    if (this.nexus && node === this.nexus.node) {
      score += 1;
    }

    // TODO: Score the replica based on the pool parameters.
    //   I.e. the replica on a less busy pool would have higher score.
    return score;
  }

  // Share replicas as appropriate to allow access from the nexus and return
  // just replicas that should be used for the nexus (excessive replicas will
  // be trimmed).
  //
  // @returns {object[]} Replicas that should be used for nexus sorted by preference.
  //
  async _ensureReplicaShareProtocols () {
    // If nexus does not exist it will be created on the same node as the most
    // preferred replica.
    const replicaSet = this._prioritizeReplicas(Object.values(this.replicas));
    if (replicaSet.length === 0) {
      throw new GrpcError(
        GrpcCode.INTERNAL,
        `There are no replicas for volume "${this}"`
      );
    }
    replicaSet.splice(this.replicaCount);

    const nexusNode = this.nexus ? this.nexus.node : replicaSet[0].pool.node;

    for (let i = 0; i < replicaSet.length; i++) {
      const replica = replicaSet[i];
      let share;
      const local = replica.pool.node === nexusNode;
      // make sure that replica which is local to the nexus is accessed locally
      if (local && replica.share !== 'REPLICA_NONE') {
        share = 'REPLICA_NONE';
      } else if (!local && replica.share === 'REPLICA_NONE') {
        // make sure that replica which is remote to nexus can be accessed
        share = 'REPLICA_NVMF';
      }
      if (share) {
        try {
          await replica.setShare(share);
        } catch (err) {
          throw new GrpcError(
            GrpcCode.INTERNAL,
            `Failed to set share protocol to ${share} for replica "${replica}": ${err}`
          );
        }
      }
    }
    return replicaSet;
  }

  // Update parameters of the volume.
  //
  // Throw exception if size of volume is changed in an incompatible way
  // (unsupported).
  //
  // @params {object}   spec                 Volume parameters.
  // @params {number}   spec.replicaCount    Number of desired replicas.
  // @params {string[]} spec.preferredNodes  Nodes to prefer for scheduling replicas.
  // @params {string[]} spec.requiredNodes   Replicas must be on these nodes.
  // @params {number}   spec.requiredBytes   The volume must have at least this size.
  // @params {number}   spec.limitBytes      The volume should not be bigger than this.
  // @params {string}   spec.protocol        The share protocol for the nexus.
  // @returns {boolean} True if the volume spec has changed, false otherwise.
  //
  update (spec) {
    var changed = false;

    if (this.size < spec.requiredBytes) {
      throw new GrpcError(
        GrpcCode.INVALID_ARGUMENT,
        `Extending the volume "${this}" is not supported`
      );
    }
    if (spec.limitBytes && this.size > spec.limitBytes) {
      throw new GrpcError(
        GrpcCode.INVALID_ARGUMENT,
        `Shrinking the volume "${this}" is not supported`
      );
    }
    if (this.protocol !== spec.protocol) {
      throw new GrpcError(
        GrpcCode.INVALID_ARGUMENT,
        `Changing the protocol for volume "${this}" is not supported`
      );
    }

    if (this.replicaCount !== spec.replicaCount) {
      this.replicaCount = spec.replicaCount;
      changed = true;
    }
    const preferredNodes = _.clone(spec.preferredNodes || []).sort();
    if (!_.isEqual(this.preferredNodes, preferredNodes)) {
      this.preferredNodes = preferredNodes;
      changed = true;
    }
    const requiredNodes = _.clone(spec.requiredNodes || []).sort();
    if (!_.isEqual(this.requiredNodes, requiredNodes)) {
      this.requiredNodes = requiredNodes;
      changed = true;
    }
    if (this.requiredBytes !== spec.requiredBytes) {
      this.requiredBytes = spec.requiredBytes;
      changed = true;
    }
    if (this.limitBytes !== spec.limitBytes) {
      this.limitBytes = spec.limitBytes;
      changed = true;
    }
    return changed;
  }

  //
  // Handlers for the events from node registry follow
  //

  // Add new replica to the volume.
  //
  // @param {object} replica   New replica object.
  newReplica (replica) {
    assert(replica.uuid === this.uuid);
    const nodeName = replica.pool.node.name;
    if (this.replicas[nodeName]) {
      log.warn(
        `Trying to add the same replica "${replica}" to the volume twice`
      );
    } else {
      log.debug(`Replica "${replica}" attached to the volume`);
      this.replicas[nodeName] = replica;
      this.fsa();
    }
  }

  // Modify replica in the volume.
  //
  // @param {object} replica   Modified replica object.
  modReplica (replica) {
    assert.strictEqual(replica.uuid, this.uuid);
    const nodeName = replica.pool.node.name;
    if (!this.replicas[nodeName]) {
      log.warn(`Modified replica "${replica}" does not belong to the volume`);
    } else {
      assert(this.replicas[nodeName] === replica);
      // the share protocol or uri could have changed
      this.fsa();
    }
  }

  // Delete replica in the volume.
  //
  // @param {object} replica   Deleted replica object.
  delReplica (replica) {
    assert.strictEqual(replica.uuid, this.uuid);
    const nodeName = replica.pool.node.name;
    if (!this.replicas[nodeName]) {
      log.warn(`Deleted replica "${replica}" does not belong to the volume`);
    } else {
      log.debug(`Replica "${replica}" detached from the volume`);
      assert(this.replicas[nodeName] === replica);
      delete this.replicas[nodeName];
      this.fsa();
    }
  }

  // Assign nexus to the volume.
  //
  // @param {object} nexus   New nexus object.
  newNexus (nexus) {
    assert.strictEqual(nexus.uuid, this.uuid);
    if (this.nexus) {
      log.warn(`Trying to add nexus "${nexus}" to the volume twice`);
    } else {
      log.debug(`Nexus "${nexus}" attached to the volume`);
      assert.strictEqual(this.state, 'pending');
      this.nexus = nexus;
      if (!this.size) this.size = nexus.size;
      this.fsa();
    }
  }

  // Nexus has been modified.
  //
  // @param {object} nexus   Modified nexus object.
  modNexus (nexus) {
    assert.strictEqual(nexus.uuid, this.uuid);
    if (!this.nexus) {
      log.warn(`Modified nexus "${nexus}" does not belong to the volume`);
    } else {
      assert.strictEqual(this.nexus, nexus);
      this.fsa();
    }
  }

  // Delete nexus in the volume.
  //
  // @param {object} nexus   Deleted nexus object.
  delNexus (nexus) {
    assert.strictEqual(nexus.uuid, this.uuid);
    if (!this.nexus) {
      log.warn(`Deleted nexus "${nexus}" does not belong to the volume`);
    } else {
      log.debug(`Nexus "${nexus}" detached from the volume`);
      assert.strictEqual(this.nexus, nexus);
      this.nexus = null;
      // this brings up back to a starting state. No FSA transitions are
      // possible after this point unless we receive new nexus event again.
      this._setState('pending');
    }
  }
}

module.exports = Volume;
