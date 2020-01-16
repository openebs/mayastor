// This is a stormtrooper of MayaStor! It is an intermediate layer between
// high-level CSI methods and lowlevel mayastor RPC calls and caching.
// It deals with scheduling of nexus's and replicas, monitoring them and
// taking recovery actions. In fact it is the brain of the CSI plugin.

'use strict';

const _ = require('lodash');
const assert = require('assert');
const EventEmitter = require('events');
const grpc = require('grpc-uds');
const log = require('./logger').Logger('commander');
const { isPoolAccessible } = require('./common');
const { GrpcError } = require('./grpc_client');

// Commander provisions volumes according to user requirements and
// contributes to recovery from failures.
class Commander extends EventEmitter {
  // Rescan period is in seconds.
  constructor(poolOperator, volumeOperator) {
    super();
    this.rescanTimer = null;
    this.pools = poolOperator;
    this.volumes = volumeOperator;
    this.ensureQ = []; // queue of ensure volume requests
    this.ensureInProg = false; // ensure operation is in progress
  }

  // Start a periodic timer which triggers rescan of all volumes.
  //
  // @param period  Scan period in seconds.
  start(period) {
    assert(!this.rescanTimer);
    this.rescanTimer = setInterval(this.rescan.bind(this), 1000 * period);
  }

  // Stop periodic timer which triggers rescan of all volumes.
  stop() {
    assert(this.rescanTimer);
    clearInterval(this.rescanTimer);
    this.rescanTimer = null;
  }

  // Iterate over all volumes bringing them in sync with the desired state.
  rescan() {
    var self = this;
    log.debug('Periodic scan of volumes started');
    // TODO: Now we iterate over nexus records but when we maintain state
    // in etcd for each volume we will iterate over those entries.
    self.volumes.getNexus().forEach(n => {
      // do only volumes for which there has not been already a request
      if (!self.ensureQ.find(ent => ent.uuid == n.uuid)) {
        self.ensureVolume(n.uuid).then(
          () => log.debug(`Volume "${n.uuid}" has been checked`),
          err => log.error(`Check of volume "${n.uuid}" failed: ${err}`)
        );
      }
    });
    // this event is for the test suite
    self.emit('rescan-done');
  }

  // Assign score to a replica based on certain criteria. The higher the better.
  //
  // @param replica  Replica object.
  // @param reqs     Requirements parameter from ensureVolume().
  _replicaScore(replica, reqs) {
    // criteria #1: does it run on the same node as nexus (and app)?
    if (replica.node == reqs.node) return 10;
    // The rest of rules is not useful right now because mustNodes and
    // shouldNodes are not known when removing superfluous replica, but
    // that is likely to change when we introduce our own CRD for describing
    // a volume.
    if (reqs.mustNodes.indexOf(replica.node) >= 0) return 5;
    if (reqs.shouldNodes.indexOf(replica.node) >= 0) return 2;
    // TODO: Score the replica based on the pool parameters. I.e. the replica
    // on a less busy pool would have higher score.
    return 0;
  }

  // Get list of replicas for particular volume sorted from the most to the
  // least preferred.
  _prioReplicaSet(uuid, reqs) {
    let replicaSet = this.volumes.getReplicaSet(uuid);
    let self = this;
    replicaSet.sort(
      (a, b) => self._replicaScore(b, reqs) - self._replicaScore(a, reqs)
    );
    return replicaSet;
  }

  // Return list of storage pools sorted by preference where a new volume
  // can be provisioned (only one pool from each node).
  //
  // The rules are simple:
  //   1) must be online (or degraded if there are no online pools)
  //   2) must have sufficient space
  //   3) least busy pools first
  _choosePools(requiredBytes, mustNodes, shouldNodes) {
    let replicas = this.volumes.getReplicaSet();
    let pools = this.pools.get().filter(p => {
      return (
        isPoolAccessible(p) &&
        p.capacity - p.used >= requiredBytes &&
        (mustNodes.length == 0 || mustNodes.indexOf(p.node) >= 0)
      );
    });
    // construct a map of how many volumes has each pool (how busy it is)
    let busy = {};
    pools.forEach(p => (busy[p.name] = 0));
    replicas.forEach(r => {
      if (busy[r.pool] != null) {
        busy[r.pool]++;
      }
    });

    pools.sort((a, b) => {
      // Rule #1: User preference
      if (shouldNodes.length > 0) {
        if (
          shouldNodes.indexOf(a.node) >= 0 &&
          shouldNodes.indexOf(b.node) < 0
        ) {
          return -1;
        } else if (
          shouldNodes.indexOf(a.node) < 0 &&
          shouldNodes.indexOf(b.node) >= 0
        ) {
          return 1;
        }
      }

      // Rule #2: Avoid degraded pools whenever possible
      if (a.state == 'ONLINE' && b.state == 'DEGRADED') {
        return -1;
      } else if (a.state == 'DEGRADED' && b.state == 'ONLINE') {
        return 1;
      }

      // Rule #3: Use the least busy pool in terms of number of volumes
      if (busy[a.name] < busy[b.name]) {
        return -1;
      } else if (busy[a.name] > busy[b.name]) {
        return 1;
      }

      // Rule #4: Pools with more free space take precedence
      let aFree = a.capacity - a.used;
      let bFree = b.capacity - b.used;
      return bFree - aFree;
    });

    // only one pool from each node
    let nodes = [];
    pools = pools.filter(p => {
      if (nodes.indexOf(p.node) < 0) {
        nodes.push(p.node);
        return true;
      } else {
        return false;
      }
    });

    return pools;
  }

  // Ensure that configuration of a volume is as it should be. Create whatever
  // component is missing and try to fix all discrepancies between desired
  // state and reality.
  //
  // @param uuid               ID of the k8s volume.
  // @param reqs               Requirements (cloberred in this func).
  // @param reqs.limitBytes    The size of the volume should be at most this.
  // @param reqs.requiredBytes The size of the volume (can be bigger).
  // @param reqs.mustNodes     The replicas must be on these nodes (no other).
  // @param reqs.shouldNodes   If possible prefer these nodes for replicas.
  // @param reqs.count         Number of desired replicas of the volume.
  // @returns nexus            The nexus info.
  ensureVolume(uuid, reqs) {
    var self = this;
    if (reqs) {
      // if the requirements are changing for the volume then skip rescan
      // for that volume
      let idx = self.ensureQ.findIndex(ent => ent.uuid == uuid);
      if (idx >= 0) {
        self.ensureQ.splice(idx, 1);
      }
    }
    return new Promise(function(resolve, reject) {
      self.ensureQ.push({
        uuid,
        reqs,
        resolve,
        reject,
      });
      if (self.ensureInProg) return;
      self.ensureInProg = true;
      self._ensureWork();
    });
  }

  // Process one item from the queue and at the end call recursively itself
  // until there is no more work to do.
  _ensureWork() {
    assert(this.ensureInProg);
    var ent = this.ensureQ.shift();
    if (!ent) {
      this.ensureInProg = false;
      return;
    }
    var self = this;
    self
      ._ensureVolume(ent.uuid, ent.reqs)
      .then(
        nexus => ent.resolve(nexus),
        err => ent.reject(err)
      )
      .finally(() => self._ensureWork());
  }

  // This does the actual ensure volume work without having to worry that
  // there is another ensure running in parallel. The parameters are the
  // same as for the ensureVolume method.
  async _ensureVolume(uuid, reqs) {
    if (!reqs) {
      reqs = {
        requiredBytes: 0,
        limitBytes: 0,
        mustNodes: [],
        shouldNodes: [],
        count: 0,
      };
    } else {
      assert.equal(typeof reqs.requiredBytes, 'number');
      assert.equal(typeof reqs.limitBytes, 'number');
      assert.equal(typeof reqs.count, 'number');
    }
    log.debug(`Ensuring state of volume "${uuid}"`);

    // check if the nexus already exists
    let nexus = this.volumes.getNexus(uuid);
    if (nexus) {
      this._deriveFromNexus(nexus, reqs);
    }
    let replicaSet = this._prioReplicaSet(uuid, reqs);
    this._deriveFromReplicas(replicaSet, reqs);
    if (reqs.count == 0) {
      // This can only happen when nexus exists but has no replicas
      throw new GrpcError(
        grpc.status.INTERNAL,
        `Cannot ensure volume "${uuid}" without any replica`
      );
    }
    // limitBytes is 0 if not set, so fix it to be at least what is required
    if (reqs.requiredBytes > reqs.limitBytes) {
      reqs.limitBytes = reqs.requiredBytes;
    }
    if (reqs.requiredBytes == 0) {
      throw new GrpcError(
        grpc.status.INVALID_ARGUMENT,
        `Cannot create zero sized volume "${uuid}"`
      );
    }

    // ensure there is the right count of replicas for the volume
    await this._ensureReplicaCount(uuid, replicaSet, reqs);

    // get fresh prioritized list of replicas
    replicaSet = this._prioReplicaSet(uuid, reqs);

    // If the nexus does not exist it will be created on the same node as
    // is the replica with local access or the most preferred replica.
    if (nexus) {
      reqs.node = nexus.node;
    } else {
      let localReplica = replicaSet.find(r => r.share == 'NONE');
      reqs.node = localReplica ? localReplica.node : replicaSet[0].node;
    }

    // ensure replicas can be accessed from nexus
    await this._ensureReplicaShareProtocols(reqs.node, replicaSet);

    // Update child devices of existing nexus or create the nexus if it
    // was missing.
    if (nexus) {
      await this._ensureNexus(nexus, replicaSet);
    } else {
      // size will be the smallest of the replicas
      let size = Math.min(..._.map(replicaSet, 'size'));
      nexus = await this.volumes.createNexus(
        reqs.node,
        uuid,
        size,
        _.map(replicaSet, 'uri')
      );
      log.info(`Volume "${uuid}" with size ${size} created`);
    }

    return nexus;
  }

  // Derive missing requirements based on what we know about nexus.
  // Note: reqs is modified in place.
  _deriveFromNexus(nexus, reqs) {
    // see if the volume is compatible in which case it is ok (be idempotent)
    if (
      nexus.size < reqs.requiredBytes ||
      (reqs.limitBytes != 0 && nexus.size > reqs.limitBytes)
    ) {
      throw new GrpcError(
        grpc.status.ALREADY_EXISTS,
        `An incompatible volume "${nexus.uuid}" already exists`
      );
    }
    // fill in requirements based on what we know about the nexus
    reqs.requiredBytes = nexus.size;
    reqs.limitBytes = reqs.limitBytes || 0;
    reqs.node = nexus.node;
    reqs.count = nexus.children.length;
  }

  // Derive missing requirements based on what we know about replicas.
  // Note: reqs is modified in place.
  _deriveFromReplicas(replicaSet, reqs) {
    if (replicaSet.length == 0) return;

    // try to deduce requiredBytes and limitBytes from existing replicas if
    // not given explicitly in requirements.
    if (!reqs.requiredBytes) {
      let minSize;
      for (let i = 0; i < replicaSet.length; i++) {
        let r = replicaSet[i];
        minSize = minSize != null ? Math.min(minSize, r.size) : r.size;
      }
      reqs.requiredBytes = minSize;
    }
    if (!reqs.limitBytes) {
      let maxSize;
      for (let i = 0; i < replicaSet.length; i++) {
        let r = replicaSet[i];
        maxSize = maxSize != null ? Math.max(maxSize, r.size) : r.size;
      }
      reqs.limitBytes = maxSize;
    }
    // Check that all replicas satisfy size requirements
    for (let i = 0; i < replicaSet.length; i++) {
      let r = replicaSet[i];
      if (r.size < reqs.requiredBytes || r.size > reqs.limitBytes) {
        throw new GrpcError(
          grpc.status.ALREADY_EXISTS,
          `Replica "${r.uuid}@${r.node}" has incompatible size ${r.size}`
        );
      }
    }
    if (!reqs.count) {
      reqs.count = replicaSet.length;
    }
  }

  // Adjust replica count for the volume with uuid to requirements in reqs.
  // Note that parameters in reqs may be updated in this method if missing by
  // deducing the values from existing replicas.
  async _ensureReplicaCount(uuid, replicaSet, reqs) {
    // create more replicas if higher replication factor is desired
    let newReplicaCount = reqs.count - replicaSet.length;
    if (newReplicaCount > 0) {
      if (reqs.requiredBytes <= 0) {
        throw new GrpcError(
          grpc.status.INVALID_ARGUMENT,
          'Cannot create zero sized volume'
        );
      }

      // sync used and capacity pool properties before making the decision
      // of where to provision the volume
      await this.pools.syncNode();

      let pools = this._choosePools(
        reqs.requiredBytes,
        reqs.mustNodes,
        reqs.shouldNodes
      );
      // remove pools which already have the replica from the selection
      let usedNodes = replicaSet.map(r => r.node);
      pools = pools.filter(p => usedNodes.indexOf(p.node) == -1);
      if (pools.length < newReplicaCount) {
        log.error(
          `No suitable pool(s) for the volume "${uuid}" with capacity ` +
            `range ${reqs.requiredBytes} - ${reqs.limitBytes} and ` +
            `replica count ${reqs.count}`
        );
        throw new GrpcError(
          grpc.status.RESOURCE_EXHAUSTED,
          'Cannot find suitable storage pool(s) for the volume'
        );
      }

      // calculate the size of the volume if not given precisely
      let size = reqs.limitBytes;
      if (reqs.requiredBytes != reqs.limitBytes) {
        for (let i = 0; i < pools.length; i++) {
          let p = pools[i];
          let free = p.capacity - p.used;
          let thisSize = free > reqs.limitBytes ? reqs.limitBytes : free;
          size = Math.min(thisSize, size);
        }
        reqs.requiredBytes = reqs.limitBytes = size;
      }

      // we record all failures as we try to create the replica on pools
      // to return them to user at the end if we fail
      let errors = [];
      // try one pool after another until success
      for (let i = 0; i < pools.length && newReplicaCount > 0; i++) {
        let p = pools[i];

        try {
          // this will add the replica to the cache if successful
          await this.volumes.createReplica(p.node, p.name, uuid, size);
        } catch (err) {
          log.error(err.message);
          errors.push(err.message);
          continue;
        }
        newReplicaCount--;
      }
      // check if we created enough replicas
      if (newReplicaCount > 0) {
        // destroy or not to destroy already created replicas?
        let msg = `Failed to create sufficient # of replicas for volume "${uuid}": `;
        msg += errors.join('. ');
        throw new GrpcError(grpc.status.INTERNAL, msg);
      }
    } else if (newReplicaCount < 0) {
      // Delete excesive replicas (those which are the least preferred)
      for (
        let i = replicaSet.length - 1;
        i > replicaSet.length - 1 + newReplicaCount;
        i--
      ) {
        let r = replicaSet[i];
        try {
          await this.volumes.destroyReplica(r.node, r.uuid);
        } catch (err) {
          // we don't treat the error as fatal
          log.error(
            `Failed to destroy redundant replica "${r.uuid}@${r.node}"`
          );
        }
      }
    }
  }

  // Share replicas as appropriate given which node the nexus is running on.
  async _ensureReplicaShareProtocols(nexusNode, replicaSet) {
    for (let i = 0; i < replicaSet.length; i++) {
      let r = replicaSet[i];
      let share;
      // make sure that replica which is local to nexus is accessed locally
      if (r.node == nexusNode && r.share != 'NONE') {
        share = 'NONE';
        // make sure that replica which is remote to nexus can be accessed
      } else if (r.node != nexusNode && r.share == 'NONE') {
        share = 'NVMF';
      }
      if (share) {
        log.info(
          `Share protocol for replica "${r.uuid}@${r.node}" set to ${share}`
        );
        try {
          await this.volumes.shareReplica(r.node, r.uuid, share);
        } catch (err) {
          throw new GrpcError(
            grpc.status.INTERNAL,
            `Failed to set share pcol to ${share} for replica ` +
              `"${r.uuid}@${r.node}": ${err}`
          );
        }
      }
    }
  }

  // Update nexus children
  async _ensureNexus(nexus, replicaSet) {
    let oldUris = nexus.children.concat();
    let newUris = _.map(replicaSet, 'uri');
    // remove children which should not be in the nexus
    for (let i = 0; i < oldUris.length; i++) {
      let ch = oldUris[i];
      let idx = newUris.indexOf(ch);
      if (idx < 0) {
        try {
          await this.volumes.removeChildNexus(nexus.uuid, ch);
        } catch (err) {
          // TODO: when we maintain recovery state in etcd we can treat this
          // as a non-fatal failure. But for now stay on the safe side.
          throw new GrpcError(
            grpc.status.INTERNAL,
            `Failed to remove child "${ch}" of nexus "${nexus.uuid}": ${err}`
          );
        }
      } else {
        newUris.splice(idx, 1);
      }
    }
    // add children which are not there yet
    for (let i = 0; i < newUris.length; i++) {
      let uri = newUris[i];
      try {
        await this.volumes.addChildNexus(nexus.uuid, uri);
      } catch (err) {
        throw new GrpcError(
          grpc.status.INTERNAL,
          `Failed to add child "${uri}" to nexus "${nexus.uuid}": ${err}`
        );
      }
    }
  }
}

module.exports = {
  Commander,
};
