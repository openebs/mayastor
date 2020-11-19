// Volume object abstracts user from volume components nexus and
// replicas and implements algorithms for volume recovery.

import assert from 'assert';
import * as _ from 'lodash';
import { Replica } from './replica';
import { Child, Nexus, Protocol } from './nexus';
import { Pool } from './pool';
import { Node } from './node';

const log = require('./logger').Logger('volume');
const { GrpcCode, GrpcError } = require('./grpc_client');

// State of the volume
export enum VolumeState {
  Unknown = 'unknown',
  Pending = 'pending',
  Healthy = 'healthy',
  Degraded = 'degraded',
  Faulted = 'faulted',
  Destroyed = 'destroyed',
  Error = 'error',  // used by the volume operator
}

export function volumeStateFromString(val: string): VolumeState {
  if (val == VolumeState.Healthy) {
    return VolumeState.Healthy;
  } else if (val == VolumeState.Degraded) {
    return VolumeState.Degraded;
  } else if (val == VolumeState.Faulted) {
    return VolumeState.Faulted;
  } else if (val == VolumeState.Destroyed) {
    return VolumeState.Destroyed;
  } else if (val == VolumeState.Error) {
    return VolumeState.Error;
  } else if (val == VolumeState.Pending) {
    return VolumeState.Pending;
  } else {
    return VolumeState.Unknown;
  }
}

// Abstraction of the volume. It is an abstract object which consists of
// physical entities nexus and replicas. It provides high level methods
// for doing operations on the volume as well as recovery algorithms for
// maintaining desired redundancy.
export class Volume {
  // volume spec properties
  private uuid: string;
  private replicaCount: number;
  private preferredNodes: string[];
  private requiredNodes: string[];
  private requiredBytes: number;
  private limitBytes: number;
  private protocol: Protocol;
  // volume status properties
  private size: number;
  private nexus: Nexus | null;
  private replicas: Record<string, Replica>; // replicas indexed by node name
  public state: VolumeState;
  private publishedOn: string | undefined;
  // internal properties
  private emitEvent: (type: string) => void;
  private registry: any;
  private runFsa: number; // number of requests to run FSA
  private nodeBlackList: Record<string, boolean>; // replicas on these nodes should be avoided

  // Construct a volume object with given uuid.
  //
  // @params uuid                 ID of the volume.
  // @params registry             Registry object.
  // @params emitEvent            Callback that should be called anytime volume state changes.
  // @params spec                 Volume parameters.
  // @params spec.replicaCount    Number of desired replicas.
  // @params spec.preferredNodes  Nodes to prefer for scheduling replicas.
  // @params spec.requiredNodes   Replicas must be on these nodes.
  // @params spec.requiredBytes   The volume must have at least this size.
  // @params spec.limitBytes      The volume should not be bigger than this.
  // @params spec.protocol        The share protocol for the nexus.
  // @params [size]               Current properties of the volume.
  // @params [publishedOn]        Node name where this volume is published.
  //
  constructor(
    uuid: string,
    registry: any,
    emitEvent: (type: string) => void,
    spec: any,
    state?: VolumeState,
    size?: number,
    publishedOn?: string,
  ) {
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
    // state variables of the volume
    this.size = size || 0;
    this.publishedOn = publishedOn;
    this.nexus = null;
    this.replicas = {};
    this.state = state || VolumeState.Pending;
    // other properties
    this.runFsa = 0;
    this.nodeBlackList = {};
    this.emitEvent = emitEvent;
  }

  // Stringify volume
  toString(): string {
    return this.uuid;
  }

  // Get the size of the volume.
  getSize(): number {
    return this.size;
  }

  // Get the node where the volume is accessible from (that is the node with
  // the nexus) or undefined when nexus does not exist (unpublished/published).
  getNodeName(): string | undefined {
    return this.publishedOn;
  }

  // Publish the volume. That means, make it accessible through a block device.
  //
  // @params protocol      The nexus share protocol.
  // @return uri           The URI to access the nexus.
  async publish(protocol: Protocol): Promise<string> {
    if (this.state !== VolumeState.Degraded && this.state !== VolumeState.Healthy) {
      throw new GrpcError(
        GrpcCode.INTERNAL,
        'Cannot publish a volume that is neither healthy nor degraded'
      );
    }
    let nexus = this.nexus;
    if (!nexus) {
      // Ensure replicas can be accessed from nexus. Set share protocols.
      const [nexusNode, replicaSet] = await this._ensureReplicaShareProtocols();

      if (!this.size) {
        // the size will be the smallest replica
        this.size = Object.values(this.replicas)
          .map((r) => r.size)
          .reduce((acc, cur) => (cur < acc ? cur : acc), Number.MAX_SAFE_INTEGER);
      }
      // create a new nexus with children (replicas) created in previous steps
      nexus = await this._createNexus(nexusNode, replicaSet);
    } else {
      log.debug(`Publishing volume ${this} that already has a nexus`)
    }
    let uri = nexus.getUri();
    if (!uri) {
      uri = await nexus.publish(protocol);
    } else {
      log.debug(`Publishing volume ${this} that has been already published`)
    }
    this.publishedOn = nexus.node.name;
    log.info(`Published "${this}" at ${uri}`);
    this.emitEvent('mod');
    return uri;
  }

  // Undo publish operation on the volume.
  async unpublish() {
    if (this.publishedOn) {
      this.publishedOn = undefined;
      if (this.nexus) {
        // We can directly destroy the nexus without unsharing it first
        // but later we will use this block of code in case we cannot
        // destroy the nexus immediately because it is rebuilding
        //if (this.nexus.getUri()) {
        //  try {
        //    await this.nexus.unpublish();
        //  } catch (err) {
        //    log.error(`Defering nexus unpublish for volume ${this}: ${err}`)
        //  }
        //}
        try {
          // TODO: defer destruction in case that the volume is rebuilding
          await this.nexus.destroy();
        } catch (err) {
          // We let unpublish to always succeed and rely on FSA to remove
          // the nexus later when it's possible to do.
          log.error(`Defering nexus destroy for volume ${this}: ${err}`)
        }
      }
      this.emitEvent('mod');
      this.fsa();
    }
  }

  // Delete nexus and destroy all replicas of the volume.
  async destroy() {
    this.publishedOn = undefined;
    this._setState(VolumeState.Destroyed);
    if (this.nexus) {
      await this.nexus.destroy();
    }
    const promises = Object.values(this.replicas).map((replica) =>
      replica.destroy()
    );
    await Promise.all(promises);
    this.emitEvent('del');
  }

  // Trigger the run of FSA. It will either run immediately or if it is already
  // running, it will start again when the current run finishes.
  //
  // Why critical section on fsa? Certain operations done by fsa are async. If
  // we allow another process to enter fsa before the async operation is done
  // and the state of volume updated we risk that the second process repeats
  // exactly the same action (because from its point of view it hasn't been
  // done yet).
  fsa() {
    if (this.runFsa++ === 0) {
      this._fsa().finally(() => {
        const runAgain = this.runFsa > 1;
        this.runFsa = 0;
        if (runAgain) this.fsa();
      });
    }
  }

  // Implementation of finite state automaton (FSA) that moves the volume
  // through the states: degraded, faulted, healthy, ... - trying to preserve
  // data on volume "no matter what".
  async _fsa() {
    // If the volume is being created, FSA should not interfere.
    if (this.state === VolumeState.Pending || this.state === VolumeState.Destroyed) {
      return;
    }
    log.debug(`Volume "${this}" enters FSA in ${this.state} state`);

    if (!this.nexus) {
      // if none of the replicas is usable then there is nothing we can do
      if (Object.values(this.replicas).filter((r) => !r.isOffline()).length == 0) {
        this._setState(VolumeState.Faulted);
        return;
      }

      // check number of replicas for the volume
      const newReplicaCount = this.replicaCount - Object.values(this.replicas).length;
      if (newReplicaCount > 0) {
        this._setState(VolumeState.Degraded);
        try {
          await this._createReplicas(newReplicaCount);
        } catch (err) {
          logError(err);
        }
        // New replicas will be added to the volume through events. On next fsa
        // enter they will be there and we may continue beyound this point then.
        return;
      }

      if (!this.publishedOn) {
        // If the volume hasn't been published we can't do anything more than what
        // we have done (that is maintain required # of replicas). When we create
        // the nexus, it may find out that some of the replicas are unusable, but
        // we don't know that now.
        this._setState(VolumeState.Healthy);
        return;
      }
    }

    // check that replicas are shared in the way they should be
    let localNode: string = (this.nexus) ? this.nexus.node.name : this.publishedOn;
    for (const nodeName in this.replicas) {
      const replica = this.replicas[nodeName];
      if (replica.isOffline()) {
        continue;
      }
      let share;
      const isLocal = replica.pool!.node.name === localNode;
      if (isLocal && replica.share !== 'REPLICA_NONE') {
        // make sure that replica that is local to the nexus is accessed locally
        share = 'REPLICA_NONE';
      } else if (!isLocal && replica.share === 'REPLICA_NONE') {
        // make sure that replica that is remote to nexus can be accessed
        share = 'REPLICA_NVMF';
      }
      if (share) {
        try {
          await replica.setShare(share);
          delete this.nodeBlackList[nodeName];
          // fsa will get called again because the replica was modified
          return;
        } catch (err) {
          this.nodeBlackList[nodeName] = true;
          log.error(
            `Failed to set share protocol to ${share} for replica "${replica}": ${err}`
          );
        }
      }
    }

    // If we don't have a nexus and the volume is published, then try to create one
    if (!this.nexus) {
      assert(this.publishedOn);
      let nexusNode = this.registry.getNode(this.publishedOn);
      if (nexusNode && nexusNode.isSynced()) {
        let replicas = [];
        for (let nodeName in this.replicas) {
          if (!this.replicas[nodeName].isOffline() && !this.nodeBlackList[nodeName]) {
            replicas.push(this.replicas[nodeName]);
          }
        }
        if (replicas.length === 0) {
          log.warn(`Cannot create nexus for ${this} because all replicas are bad`);
          return;
        }
        try {
          await this._createNexus(nexusNode, replicas);
        } catch (err) {
          log.error(`Failed to create nexus for ${this} on "${this.publishedOn}"`);
          this._setState(VolumeState.Faulted);
        }
      } else {
        log.warn(`Cannot create nexus for ${this} because "${this.publishedOn}" is down`)
        this._setState(VolumeState.Faulted);
      }
      // fsa will get called again when event about created nexus arrives
      return;
    }

    // pair nexus children with replica objects to get the full picture
    const childReplicaPairs: {ch: Child, r: Replica | undefined}[] = this.nexus.children.map((ch) => {
      const r = Object.values(this.replicas).find((r) => r.uri === ch.uri);
      return {ch, r};
    });
    // add newly found replicas to the nexus (one by one)
    const newReplicas = Object.values(this.replicas).filter((r) => {
      return (!r.isOffline() &&
        !childReplicaPairs.find((pair) => pair.r === r) &&
        !this.nodeBlackList[r.pool!.node!.name]);
    });
    for (let i = 0; i < newReplicas.length; i++) {
      try {
        await this.nexus.addReplica(newReplicas[i]);
        return;
      } catch (err) {
        // XXX what should we do with the replica? Destroy it?
        this.nodeBlackList[newReplicas[i].pool!.node!.name] = true;
        logError(err);
      }
    }

    // If there is not a single child that is online then there is no hope
    // that we could rebuild anything.
    var onlineCount = childReplicaPairs
      .filter((pair) => pair.ch.state === 'CHILD_ONLINE')
      .length;
    if (onlineCount === 0) {
      this._setState(VolumeState.Faulted);
      return;
    }
    if (this.nexus.state === 'NEXUS_OFFLINE') {
      this._setState(VolumeState.Faulted);
      return;
    }

    // publish the nexus if it is not and should be
    let uri = this.nexus.getUri();
    if (!uri && this.publishedOn) {
      try {
        uri = await this.nexus.publish(this.protocol);
      } catch (err) {
        logError(err);
        return;
      }
    }

    // If we don't have sufficient number of sound replicas (sound means online
    // or under rebuild) then add a new one.
    var soundCount = childReplicaPairs.filter((pair) => {
      return ['CHILD_ONLINE', 'CHILD_DEGRADED'].indexOf(pair.ch.state) >= 0;
    }).length;
    if (this.replicaCount > soundCount) {
      this._setState(VolumeState.Degraded);
      // add new replica
      try {
        await this._createReplicas(this.replicaCount - soundCount);
      } catch (err) {
        logError(err);
      }
      // The replicas will be added to nexus when the fsa is run next time
      // which happens immediately after we exit.
      return;
    }

    // The condition for later actions is that volume must not be rebuilding or
    // waiting for a child add. So check that and return if that's the case.
    var rebuildCount = childReplicaPairs
      .filter((pair) => pair.ch.state === 'CHILD_DEGRADED')
      .length;
    if (rebuildCount > 0) {
      this._setState(VolumeState.Degraded);
      return;
    }

    assert(onlineCount >= this.replicaCount);
    this._setState(VolumeState.Healthy);

    // If we have more online replicas then we need to, then remove one.
    // Child that is broken or without a replica goes first.
    let rmPair = childReplicaPairs.find(
      (pair) => !pair.r && pair.ch.state === 'CHILD_FAULTED'
    );
    if (!rmPair) {
      rmPair = childReplicaPairs.find((pair) => pair.ch.state === 'CHILD_FAULTED');
      if (!rmPair) {
        // A child that is unknown to us (without replica object)
        rmPair = childReplicaPairs.find((pair) => !pair.r);
        // If all replicas are online, then continue searching for a candidate
        // only if there are more online replicas than it needs to be.
        if (!rmPair && onlineCount > this.replicaCount) {
          // The replica with the lowest score must go away
          const rmReplica = this._prioritizeReplicas(
            <Replica[]> childReplicaPairs
              .map((pair) => pair.r)
              .filter((r) => r !== undefined)
          ).pop();
          if (rmReplica) {
            rmPair = childReplicaPairs.find((pair) => pair.r === rmReplica);
          }
        }
      }
    }
    if (rmPair) {
      try {
        await this.nexus.removeReplica(rmPair.ch.uri);
      } catch (err) {
        logError(err);
        return;
      }
      if (rmPair.r) {
        try {
          await rmPair.r.destroy();
        } catch (err) {
          logError(err);
        }
      }
      return;
    }

    // If a replica should run on a different node then move it
    var moveChild = childReplicaPairs.find((pair) => {
      if (
        pair.r &&
        pair.ch.state === 'CHILD_ONLINE' &&
        this.requiredNodes.length > 0 &&
        this.requiredNodes.indexOf(pair.r.pool!.node.name) < 0
      ) {
        if (this.requiredNodes.indexOf(pair.r.pool!.node.name) < 0) {
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
        logError(err);
      }
    }
  }

  // Change the volume state to given state. If the state is not the same as
  // previous one, we should emit a volume mod event.
  //
  // @param newState   New state to set on volume.
  _setState(newState: VolumeState) {
    if (this.state !== newState) {
      if (newState === VolumeState.Healthy || newState === VolumeState.Destroyed) {
        log.info(`Volume state of "${this}" is ${newState}`);
      } else {
        log.warn(`Volume state of "${this}" is ${newState}`);
      }
      this.state = newState;
      this.emitEvent('mod');
    }
  }

  // Create the volume in accordance with requirements specified during the
  // object creation. Create whatever component is missing (note that we
  // might not be creating it from the scratch).
  //
  // NOTE: Until we switch state from "pending" at the end, the volume is not
  // acted upon by FSA. That's exactly what we want, because the async events
  // produced by this function do not interfere with execution of the "create".
  async create() {
    log.debug(`Creating the volume "${this}"`);

    // Ensure there is sufficient number of replicas for the volume.
    const newReplicaCount = this.replicaCount - Object.keys(this.replicas).length;
    if (newReplicaCount > 0) {
      // create more replicas if higher replication factor is desired
      await this._createReplicas(newReplicaCount);
    }
    this._setState(VolumeState.Healthy);
    log.info(`Volume "${this}" with ${this.replicaCount} replica(s) and size ${this.size} was created`);
  }

  // Update child devices of existing nexus or create a new nexus if it does not
  // exist.
  //
  // @param node       Node where the nexus should be created.
  // @param replicas   Replicas that should be used for child bdevs of nexus.
  // @returns Created nexus object.
  //
  async _createNexus(node: Node, replicas: Replica[]): Promise<Nexus> {
    return node.createNexus(
      this.uuid,
      this.size,
      Object.values(replicas)
    );
  }

  // Adjust replica count for the volume to required count.
  //
  // @param count   Number of new replicas to create.
  //
  async _createReplicas(count: number) {
    let pools: Pool[] = this.registry.choosePools(
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
  _prioritizeReplicas(replicas: Replica[]): Replica[] {
    // Object.values clones the array so that we don't modify the original value
    return Object.values(replicas).sort(
      (a, b) => this._scoreReplica(b) - this._scoreReplica(a)
    );
  }

  // Assign score to a replica based on certain criteria. The higher the better.
  //
  // @param   {object} replica  Replica object.
  // @returns {number} Score from 0 to 18.
  //
  _scoreReplica (replica: Replica) {
    let score = 0;
    const node = replica.pool!.node;

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
  // @returns Node where nexus should be and list of replicas that should be
  //          used for nexus sorted by preference.
  //
  async _ensureReplicaShareProtocols(): Promise<[Node, Replica[]]> {
    // sort replicas and remove replicas that aren't online
    const replicaSet = this
      ._prioritizeReplicas(Object.values(this.replicas))
      .filter((r) => !r.isOffline());
    if (replicaSet.length === 0) {
      throw new GrpcError(
        GrpcCode.INTERNAL,
        `There are no replicas for volume "${this}"`
      );
    }
    replicaSet.splice(this.replicaCount);

    // If nexus does not exist it will be created on the same node as the most
    // preferred replica.
    const nexusNode = this.nexus ? this.nexus.node : replicaSet[0].pool!.node;

    for (let i = 0; i < replicaSet.length; i++) {
      const replica = replicaSet[i];
      let share;
      const local = replica.pool!.node === nexusNode;
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
    return [nexusNode, replicaSet];
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
  //
  update(spec: any) {
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
    if (changed) {
      this.emitEvent('mod');
      this.fsa();
    }
  }

  //
  // Handlers for the events from node registry follow
  //

  // Add new replica to the volume.
  //
  // @param {object} replica   New replica object.
  newReplica(replica: Replica) {
    assert.strictEqual(replica.uuid, this.uuid);
    const nodeName = replica.pool!.node.name;
    if (this.replicas[nodeName]) {
      log.warn(
        `Trying to add the same replica "${replica}" to the volume twice`
      );
    } else {
      log.debug(`Replica "${replica}" attached to the volume`);
      this.replicas[nodeName] = replica;
      this.emitEvent('mod');
      this.fsa();
    }
  }

  // Modify replica in the volume.
  //
  // @param {object} replica   Modified replica object.
  modReplica(replica: Replica) {
    assert.strictEqual(replica.uuid, this.uuid);
    const nodeName = replica.pool!.node.name;
    if (!this.replicas[nodeName]) {
      log.warn(`Modified replica "${replica}" does not belong to the volume`);
    } else {
      assert(this.replicas[nodeName] === replica);
      this.emitEvent('mod');
      // the share protocol or uri could have changed
      this.fsa();
    }
  }

  // Delete replica in the volume.
  //
  // @param {object} replica   Deleted replica object.
  delReplica(replica: Replica) {
    assert.strictEqual(replica.uuid, this.uuid);
    const nodeName = replica.pool!.node.name;
    if (!this.replicas[nodeName]) {
      log.warn(`Deleted replica "${replica}" does not belong to the volume`);
    } else {
      log.debug(`Replica "${replica}" detached from the volume`);
      assert(this.replicas[nodeName] === replica);
      delete this.replicas[nodeName];
      this.emitEvent('mod');
      this.fsa();
    }
  }

  // Assign nexus to the volume.
  //
  // @param {object} nexus   New nexus object.
  newNexus(nexus: Nexus) {
    assert.strictEqual(nexus.uuid, this.uuid);
    if (!this.nexus) {
      // If there is no nexus then accept any. This is to support rebuild when
      // volume is not published.
      log.debug(`Nexus "${nexus}" attached to the volume`);
      this.nexus = nexus;
      if (!this.size) this.size = nexus.size;
      this.emitEvent('mod');
      this.fsa();
    } else if (this.nexus === nexus) {
      log.warn(`Trying to add the same nexus "${nexus}" to the volume twice`);
    } else if (!this.publishedOn) {
      log.warn(`Trying to add another nexus "${nexus}" to unpublished volume`);
      nexus.destroy().catch((err) => {
        log.error(`Failed to destroy duplicated nexus ${nexus}: ${err}`);
      });
    } else if (this.publishedOn === nexus.node?.name) {
      log.warn(`Replacing nexus "${this.nexus}" by "${nexus}" in the volume`);
      const oldNexus = this.nexus;
      this.nexus = nexus;
      oldNexus.destroy().catch((err) => {
        log.error(`Failed to destroy stale nexus "${oldNexus}": ${err}`);
      });
    } else {
      log.warn(`Destroying new nexus "${nexus}" on the wrong node`);
      nexus.destroy().catch((err) => {
        log.error(`Failed to destroy wrong nexus "${nexus}": ${err}`);
      });
    }
  }

  // Nexus has been modified.
  //
  // @param {object} nexus   Modified nexus object.
  modNexus(nexus: Nexus) {
    assert.strictEqual(nexus.uuid, this.uuid);
    if (!this.nexus) {
      log.warn(`Modified nexus "${nexus}" does not belong to the volume`);
    } else if (this.nexus === nexus) {
      this.emitEvent('mod');
      this.fsa();
    }
  }

  // Delete nexus in the volume.
  //
  // @param {object} nexus   Deleted nexus object.
  delNexus(nexus: Nexus) {
    assert.strictEqual(nexus.uuid, this.uuid);
    if (!this.nexus) {
      log.warn(`Deleted nexus "${nexus}" does not belong to the volume`);
    } else if (this.nexus === nexus) {
      log.debug(`Nexus "${nexus}" detached from the volume`);
      assert.strictEqual(this.nexus, nexus);
      this.emitEvent('mod');
      this.nexus = null;
      this.fsa();
    } else {
      // if this is a different nexus than ours, ignore it
    }
  }
}

// When debugging unexpected errors in try-catch it is easy to modify
// this function to print a stack as well, which is handy.
function logError(err: any) {
  log.error(err.toString());
}