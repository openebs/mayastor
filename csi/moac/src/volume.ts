// Volume object abstracts user from volume components nexus and
// replicas and implements algorithms for volume recovery.

import assert from 'assert';
import events = require('events');
import * as _ from 'lodash';
import { grpcCode, GrpcError } from './grpc_client';
import { Replica } from './replica';
import { Child, Nexus, Protocol } from './nexus';
import { Pool } from './pool';
import { Node } from './node';
import { Registry } from './registry';
import { Logger } from './logger';

const log = Logger('volume');

// If state transition in FSA fails due to an error and there is no consumer
// for the error, we set a retry timer to retry the state transition.
const RETRY_TIMEOUT_MS = 30000;

type DoneCallback = (err?: Error, res?: unknown) => void;

// ID of the operation delegated to fsa() to perform.
enum DelegatedOp {
  Publish,
  Unpublish,
  Destroy,
}

// State of the volume
export enum VolumeState {
  Unknown = 'unknown',
  Pending = 'pending',
  Healthy = 'healthy',
  Degraded = 'degraded',
  Offline = 'offline', // target (nexus) is down
  Faulted = 'faulted', // data cannot be recovered
  Destroyed = 'destroyed', // destroy in progress
  Error = 'error', // used by the volume operator
}

export function volumeStateFromString(val: string): VolumeState {
  if (val == VolumeState.Healthy) {
    return VolumeState.Healthy;
  } else if (val == VolumeState.Degraded) {
    return VolumeState.Degraded;
  } else if (val == VolumeState.Faulted) {
    return VolumeState.Faulted;
  } else if (val == VolumeState.Offline) {
    return VolumeState.Offline;
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

// Specification describing the desired state of the volume.
export type VolumeSpec = {
  // Number of desired replicas.
  replicaCount: number,
  // If the application should run on the same node as the nexus.
  local: boolean,
  // Nodes to prefer for scheduling replicas.
  // There is one quirk following from k8s implementation of CSI. The first
  // node in the list is the node that k8s wants to schedule the app for.
  // The ordering of the rest does not have any significance.
  preferredNodes: string[],
  // Replicas must be on a subset of these nodes.
  requiredNodes: string[],
  // The volume must have at least this size.
  requiredBytes: number,
  // The volume should not be bigger than this.
  limitBytes: number,
  // The share protocol for the nexus.
  protocol: Protocol,
};

// Abstraction of the volume. It is an abstract object which consists of
// physical entities nexus and replicas. It provides high level methods
// for doing operations on the volume as well as recovery algorithms for
// maintaining desired redundancy.
export class Volume {
  // volume spec properties
  uuid: string;
  spec: VolumeSpec;
  // volume status properties
  private size: number;
  private nexus: Nexus | null;
  private replicas: Record<string, Replica>; // replicas indexed by node name
  public state: VolumeState;
  private publishedOn: string | undefined;
  // internal properties
  private emitter: events.EventEmitter;
  private registry: Registry;
  private runFsa: number; // number of requests to run FSA
  private waiting: Record<DelegatedOp, DoneCallback[]>; // ops waiting for completion
  private retry_fsa: NodeJS.Timeout | undefined;
  private pendingDestroy: boolean;

  // Construct a volume object with given uuid.
  //
  // @params uuid                 ID of the volume.
  // @params registry             Registry object.
  // @params emitEvent            Callback that should be called anytime volume state changes.
  // @params spec                 Volume parameters.
  // @params [size]               Current properties of the volume.
  // @params [publishedOn]        Node name where this volume is published.
  //
  constructor(
    uuid: string,
    registry: Registry,
    emitter: events.EventEmitter,
    spec: VolumeSpec,
    state?: VolumeState,
    size?: number,
    publishedOn?: string,
  ) {
    // specification of the volume
    this.uuid = uuid;
    this.spec = _.clone(spec);
    this.registry = registry;
    // state variables of the volume
    this.size = size || 0;
    this.publishedOn = publishedOn;
    this.nexus = null;
    this.replicas = {};
    this.state = state || VolumeState.Pending;
    this.pendingDestroy = false;
    // other properties
    this.runFsa = 0;
    this.emitter = emitter;
    this.waiting = <Record<DelegatedOp, DoneCallback[]>> {};
    this.waiting[DelegatedOp.Publish] = [];
    this.waiting[DelegatedOp.Unpublish] = [];
    this.waiting[DelegatedOp.Destroy] = [];
  }

  // Clear the timer on the volume to prevent it from keeping nodejs loop alive.
  deactivate() {
    this.runFsa = 0;
    if (this.retry_fsa) {
      clearTimeout(this.retry_fsa);
      this.retry_fsa = undefined;
    }
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

  // Return volume replicas.
  getReplicas(): Replica[] {
    return Object.values(this.replicas);
  }

  // Return volume nexus.
  getNexus(): Nexus | undefined {
    return this.nexus || undefined;
  }

  // Return whether the volume can still be used and is updatable.
  isSpecUpdatable (): boolean {
    return ([
      VolumeState.Unknown,
      VolumeState.Pending,
      VolumeState.Destroyed,
      VolumeState.Faulted,
    ].indexOf(this.state) < 0);
  }

  // Publish the volume. That means, make it accessible through a target.
  //
  // @params nodeId        ID of the node where the volume will be mounted.
  // @return uri           The URI to access the nexus.
  async publish(nodeId: String): Promise<string> {
    if ([
      VolumeState.Degraded,
      VolumeState.Healthy,
      VolumeState.Offline,
    ].indexOf(this.state) < 0) {
      throw new GrpcError(
        grpcCode.INTERNAL,
        `Cannot publish "${this}" that is neither healthy, degraded nor offline`
      );
    }

    let uri = this.nexus && this.nexus.getUri();

    let nexusNode = this._desiredNexusNode(this._activeReplicas(), nodeId);
    if (!nexusNode) {
      // If we get here it means that nexus is supposed to be already published
      // but on a node that is not part of the cluster (has been deregistered).
      if (!uri) {
        throw new GrpcError(
          grpcCode.INTERNAL,
          `Cannot publish "${this}" because the node does not exist`
        );
      }
      return uri;
    // If the publish has been already done on the desired node then return.
    } else if (uri && this.nexus?.node?.name === nexusNode.name) {
      return uri;
    }

    // Set the new desired state
    this.publishedOn = nexusNode.name;

    // Cancel any unpublish that might be in progress
    this._delegatedOpCancel([DelegatedOp.Unpublish], new GrpcError(
      grpcCode.INTERNAL,
      `Volume ${this} has been re-published`,
    ));

    let res = await this._delegate(DelegatedOp.Publish);
    assert(typeof res === 'string');
    return res;
  }

  // Undo publish operation on the volume.
  async unpublish() {
    // Set the new desired state
    this.publishedOn = undefined;

    // If the volume has been already unpublished then return
    if (!this.nexus || !this.nexus.getUri()) {
      return;
    }

    // Cancel any publish that might be in progress
    this._delegatedOpCancel([DelegatedOp.Publish], new GrpcError(
      grpcCode.INTERNAL,
      `Volume ${this} has been unpublished`,
    ));

    await this._delegate(DelegatedOp.Unpublish);
  }

  // Delete nexus and destroy all replicas of the volume.
  async destroy() {
    // If the volume is still being created then we cannot change the state
    // because fsa would immediately start to act on it.
    if (this.state === VolumeState.Pending) {
      this.pendingDestroy = true;
      await this._delegate(DelegatedOp.Destroy);
      return;
    }
    // Set the new desired state
    this.publishedOn = undefined;
    this._setState(VolumeState.Destroyed);

    // Cancel all other types of operations that might be in progress
    this._delegatedOpCancel([
      DelegatedOp.Publish,
      DelegatedOp.Unpublish,
    ], new GrpcError(
      grpcCode.INTERNAL,
      `Volume ${this} has been destroyed`,
    ));

    await this._delegate(DelegatedOp.Destroy);
  }

  // Trigger the run of FSA. It will always run asynchronously to give caller
  // a chance to perform other changes to the volume before everything is
  // checked by FSA. If it is already running, it will start again when the
  // current run finishes.
  //
  // Why critical section on fsa? Certain operations done by fsa are async. If
  // we allow another process to enter fsa before the async operation is done,
  // we risk that the second process repeats exactly the same actions (because
  // the state hasn't been fully updated).
  fsa() {
    if (this.runFsa++ === 0) {
      setImmediate(() => {
        if (this.retry_fsa) {
          clearTimeout(this.retry_fsa);
          this.retry_fsa = undefined;
        }
        this._fsa().finally(() => {
          const runAgain = this.runFsa > 1;
          this.runFsa = 0;
          if (runAgain) this.fsa();
        });
      })
    }
  }

  // Implementation of the Finite State Automaton (FSA) that moves the volume
  // through the states: degraded, faulted, healthy, ... It tries to reflect
  // the desired state as recorded in spec properties and some other internal
  // properties that change in response to create, publish, unpublish and
  // destroy volume operations. Since these operations delegate their tasks
  // onto FSA (not to interfere with other state transitions happening in FSA),
  // it is also responsible for notifying delegators when certain state
  // transitions complete or fail.
  async _fsa() {
    // If the volume is being created, FSA should not interfere with the
    // creation process.
    if (this.state === VolumeState.Pending) {
      return;
    }
    log.debug(`Volume ${this} enters FSA in ${this.state} state`);

    // Destroy all components of the volume if it should be destroyed
    if (this.state === VolumeState.Destroyed) {
      if (this.nexus) {
        try {
          await this.nexus.destroy();
        } catch (err) {
          this._delegatedOpFailed([
            DelegatedOp.Unpublish,
            DelegatedOp.Destroy,
          ], new GrpcError(
            grpcCode.INTERNAL,
            `Failed to destroy nexus ${this.nexus}: ${err}`,
          ));
          return;
        }
      }
      const promises = Object.values(this.replicas).map((replica) =>
        replica.destroy()
      );
      try {
        await Promise.all(promises);
      } catch (err) {
        this._delegatedOpFailed([DelegatedOp.Destroy], new GrpcError(
          grpcCode.INTERNAL,
          `Failed to destroy a replica of ${this}: ${err}`,
        ));
        return;
      }
      try {
        await this.registry.getPersistentStore().destroyNexus(this.uuid);
      } catch (err) {
        this._delegatedOpFailed([DelegatedOp.Destroy], new GrpcError(
          grpcCode.INTERNAL,
          `Failed to destroy entry from the persistent store of ${this}: ${err}`,
        ));
        return;
      }

      this._delegatedOpSuccess(DelegatedOp.Destroy);
      if (this.retry_fsa) {
        clearTimeout(this.retry_fsa);
        this.retry_fsa = undefined;
      }
      this._changed('del');
      return;
    }

    if (this.nexus && !this.publishedOn) {
      // Try to unpublish the nexus if it should not be published.
      if (this.nexus.getUri()) {
        try {
          await this.nexus.unpublish();
        } catch (err) {
          this._delegatedOpFailed([DelegatedOp.Unpublish], new GrpcError(
            grpcCode.INTERNAL,
            `Cannot unpublish ${this.nexus}: ${err}`,
          ));
          return;
        }
        this._delegatedOpSuccess(DelegatedOp.Unpublish);
        return;
      } else if (this.nexus.isOffline()) {
        // The nexus is not used and it is offline so "forget it".
        try {
          await this.nexus.destroy();
        } catch (err) {
          this._delegatedOpFailed([DelegatedOp.Unpublish], new GrpcError(
            grpcCode.INTERNAL,
            `Failed to forget nexus ${this.nexus}: ${err}`,
          ));
          return;
        }
        this._delegatedOpSuccess(DelegatedOp.Unpublish);
        return;
      }
    }

    let replicaSet: Replica[] = [];
    try {
      replicaSet = this._activeReplicas();
    } catch (err) {
      this._setState(VolumeState.Faulted);
      this._delegatedOpFailed([ DelegatedOp.Publish ], new GrpcError(
        grpcCode.INTERNAL,
        err.toString(),
      ));
      // No point in continuing if there isn't a single usable replica.
      // We might need to revisit this decision in the future, because nexus
      // might have children that we are not aware of.
      if (!this.nexus) return;
    }
    let nexusNode = this._desiredNexusNode(replicaSet);

    // If we don't have a nexus and we should have one then create it
    if (!this.nexus) {
      if (
        this.publishedOn ||
        replicaSet.length !== this.spec.replicaCount
      ) {
        if (nexusNode && nexusNode.isSynced()) {
          try {
            replicaSet = await this._ensureReplicaShareProtocols(nexusNode, replicaSet);
          } catch (err) {
            this._setState(VolumeState.Offline);
            this._delegatedOpFailed([ DelegatedOp.Publish ], new GrpcError(
              grpcCode.INTERNAL,
              err.toString(),
            ));
            return;
          }
          try {
            await this._createNexus(nexusNode, replicaSet);
          } catch (err) {
            this._setState(VolumeState.Offline);
            this._delegatedOpFailed([ DelegatedOp.Publish ], new GrpcError(
              grpcCode.INTERNAL,
              `Failed to create nexus for ${this} on "${this.publishedOn}": ${err}`,
            ));
          }
        } else {
          this._setState(VolumeState.Offline);
          this._delegatedOpFailed([ DelegatedOp.Publish ], new GrpcError(
            grpcCode.INTERNAL,
            `Cannot create nexus for ${this} because "${this.publishedOn}" is down`,
          ));
        }
      } else {
        // we have just the right # of replicas and we don't need a nexus
        this._setState(VolumeState.Healthy);
      }
      // fsa will get called again when event about created nexus arrives
      return;
    }

    if (this.publishedOn && this.nexus.node?.name !== this.publishedOn) {
      // Respawn the nexus on the desired node.
      log.info(`Recreating the nexus "${this.nexus}" on the desired node "${this.publishedOn}"`);
      try {
        await this.nexus.destroy();
      } catch (err) {
        this._delegatedOpFailed([DelegatedOp.Publish], new GrpcError(
          grpcCode.INTERNAL,
          `Failed to destroy nexus for ${this}: ${err}`,
        ));
      }
      return;
    }
    if (this.nexus.isOffline()) {
      this._setState(VolumeState.Offline);
      return;
    }

    // From now on the assumption is that the nexus exists and is reachable
    assert(nexusNode);

    // Check that the replicas are shared as they should be
    try {
      replicaSet = await this._ensureReplicaShareProtocols(nexusNode, replicaSet);
    } catch (err) {
      this._delegatedOpFailed([
        DelegatedOp.Publish,
      ], new GrpcError(
        grpcCode.INTERNAL,
        err.toString(),
      ));
      return;
    }

    // pair nexus children with replica objects to get the full picture
    const childReplicaPairs: { ch: Child, r: Replica | undefined }[] = this.nexus.children.map((ch) => {
      const r = Object.values(replicaSet).find((r) => r.uri === ch.uri);
      return { ch, r };
    });
    // add newly found replicas to the nexus (one by one)
    const newReplicas = Object.values(replicaSet).filter((r) => {
      return (!r.isOffline() && !childReplicaPairs.find((pair) => pair.r === r));
    });
    for (let i = 0; i < newReplicas.length; i++) {
      try {
        childReplicaPairs.push({
          ch: await this.nexus.addReplica(newReplicas[i]),
          r: newReplicas[i],
        })
      } catch (err) {
        log.error(err.toString());
      }
    }

    // If there is not a single child that is online then there is no hope
    // that we could rebuild anything.
    var onlineCount = childReplicaPairs
      .filter((pair) => pair.ch.state === 'CHILD_ONLINE')
      .length;
    if (onlineCount === 0) {
      this._setState(VolumeState.Faulted);
      this._delegatedOpFailed([
        DelegatedOp.Publish,
      ], new GrpcError(
        grpcCode.INTERNAL,
        `The volume ${this} has no healthy replicas`
      ));
      return;
    }

    // publish the nexus if it is not and should be
    let uri = this.nexus.getUri();
    if (!uri && this.publishedOn) {
      try {
        uri = await this.nexus.publish(this.spec.protocol);
      } catch (err) {
        this._delegatedOpFailed([DelegatedOp.Publish], new GrpcError(
          grpcCode.INTERNAL,
          err.toString(),
        ));
        return;
      }
      this._delegatedOpSuccess(DelegatedOp.Publish, uri);
    }

    // If we don't have sufficient number of sound replicas (sound means online
    // or under rebuild) then add a new one.
    var soundCount = childReplicaPairs.filter((pair) => {
      return ['CHILD_ONLINE', 'CHILD_DEGRADED'].indexOf(pair.ch.state) >= 0;
    }).length;
    if (this.spec.replicaCount > soundCount) {
      this._setState(VolumeState.Degraded);
      // add new replica
      try {
        await this._createReplicas(this.spec.replicaCount - soundCount);
      } catch (err) {
        log.error(err.toString());
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
      log.info(`The volume ${this} is rebuilding`);
      this._setState(VolumeState.Degraded);
      return;
    }

    assert(onlineCount >= this.spec.replicaCount);
    this._setState(VolumeState.Healthy);

    // If we have more online replicas than we need to, then remove one.
    // Child that is broken or without a replica goes first.
    let rmPair = childReplicaPairs.find(
      (pair) => !pair.r && pair.ch.state === 'CHILD_FAULTED'
    );
    if (!rmPair) {
      rmPair = childReplicaPairs.find((pair) => pair.ch.state === 'CHILD_FAULTED');
      // Continue searching for a candidate for removal only if there are more
      // online replicas than required.
      if (!rmPair && onlineCount > this.spec.replicaCount) {
        // A child that is unknown to us (without replica object)
        rmPair = childReplicaPairs.find((pair) => !pair.r);
        if (!rmPair) {
          // The replica with the lowest score must go away
          const rmReplica = this._prioritizeReplicas(
            <Replica[]>childReplicaPairs
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
        log.error(`Failed to remove excessive replica "${rmPair.ch.uri}" from nexus: ${err}`);
        return;
      }
      if (rmPair.r) {
        try {
          await rmPair.r.destroy();
        } catch (err) {
          log.error(`Failed to destroy excessive replica "${rmPair.r}": ${err}`);
        }
      }
      return;
    }

    // If a replica should run on a different node then move it
    var moveChild = childReplicaPairs.find((pair) => {
      if (
        pair.r &&
        pair.ch.state === 'CHILD_ONLINE' &&
        this.spec.requiredNodes.length > 0 &&
        pair.r.pool?.node &&
        this.spec.requiredNodes.indexOf(pair.r.pool.node.name) < 0
      ) {
        if (this.spec.requiredNodes.indexOf(pair.r.pool.node.name) < 0) {
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
        log.error(`Failed to move replica of the volume ${this}: ${err}`);
      }
      return;
    }

    // Finally if everything is ok and volume isn't published, destroy the
    // nexus. Leaving it around eats cpu cycles and induces network traffic
    // between nexus and replicas.
    if (!this.publishedOn) {
      try {
        await this.nexus.destroy();
      } catch (err) {
        this._delegatedOpFailed([DelegatedOp.Destroy], new GrpcError(
          grpcCode.INTERNAL,
          `Failed to destroy nexus ${this.nexus}: ${err}`,
        ));
      }
    }
  }

  // Wait for the operation that was delegated to FSA to complete (either
  // with success or failure).
  async _delegate(op: DelegatedOp): Promise<unknown> {
    return new Promise((resolve: (res: unknown) => void, reject: (err: any) => void) => {
      this.waiting[op].push((err: any, res: unknown) => {
        if (err) {
          reject(err);
        } else {
          resolve(res);
        }
      });
      this.fsa();
    });
  }

  // A state transition corresponding to certain finished operation on the
  // volume has been done. Inform registered consumer about it.
  _delegatedOpSuccess(op: DelegatedOp, result?: unknown) {
    this.waiting[op]
      .splice(0, this.waiting[op].length)
      .forEach((cb) => cb(undefined, result));
  }

  // An error has been encountered while making a state transition to desired
  // state. Inform registered consumer otherwise it could be waiting for ever
  // for the state transition to happen.
  // If there is no consumer for the information then log the error and
  // schedule a retry for the state transition.
  _delegatedOpFailed(ops: DelegatedOp[], err: Error) {
    let reported = false;
    ops.forEach((op) => {
      this.waiting[op]
        .splice(0, this.waiting[op].length)
        .forEach((cb) => {
          reported = true;
          cb(err);
        });
    });
    if (!reported) {
      let msg;
      if (err instanceof GrpcError) {
        msg = err.message;
      } else {
        // These are sort of unexpected errors so print a stack trace as well
        msg = err.stack;
      }
      log.error(msg);
      this.retry_fsa = setTimeout(this.fsa.bind(this), RETRY_TIMEOUT_MS);
    }
  }

  // Cancel given operation that is in progress (if any) by unblocking it and
  // returning specified error.
  _delegatedOpCancel(ops: DelegatedOp[], err: Error) {
    ops.forEach((op) => {
      this.waiting[op]
        .splice(0, this.waiting[op].length)
        .forEach((cb) => cb(err));
    });
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
      this._changed();
    }
  }

  // Create the volume in accordance with requirements specified during the
  // object creation. Create whatever component is missing (note that we
  // might not be creating it from the scratch).
  //
  // NOTE: Until we switch state from "pending" at the end, the volume is not
  // acted upon by FSA. That's exactly what we want, because the async events
  // produced by this function do not interfere with execution of the "create".
  //
  // We have to check pending destroy flag after each async step in case that
  // someone destroyed the volume before it was fully created.
  async create() {
    log.debug(`Creating the volume "${this}"`);

    this.attach();

    // Ensure there is sufficient number of replicas for the volume.
    const newReplicaCount = this.spec.replicaCount - Object.keys(this.replicas).length;
    if (newReplicaCount > 0) {
      // create more replicas if higher replication factor is desired
      await this._createReplicas(newReplicaCount);
      if (this.pendingDestroy) {
        throw new GrpcError(
          grpcCode.INTERNAL,
          `The volume ${this} was destroyed before it was created`,
        );
      }
    }
    this._setState(VolumeState.Healthy);
    log.info(`Volume "${this}" with ${this.spec.replicaCount} replica(s) and size ${this.size} was created`);
  }

  // Attach whatever objects belong to the volume and can be found in the
  // registry.
  attach() {
    this.registry.getReplicaSet(this.uuid).forEach((r: Replica) => this.newReplica(r));
    const nexus = this.registry.getNexus(this.uuid);
    if (nexus) {
      this.newNexus(nexus);
    }
  }

  // Update child devices of existing nexus or create a new nexus if it does not
  // exist.
  //
  // @param node       Node where the nexus should be created.
  // @param replicas   Replicas that should be used for child bdevs of nexus.
  // @returns Created nexus object.
  //
  async _createNexus(node: Node, replicas: Replica[]): Promise<Nexus> {
    if (!this.size) {
      // the size will be the smallest replica
      this.size = Object.values(replicas)
        .map((r) => r.size)
        .reduce((acc, cur) => (cur < acc ? cur : acc), Number.MAX_SAFE_INTEGER);
    }

    // filter out unhealthy replicas (they don't have the latest data) from the create call
    replicas = await this.registry.getPersistentStore().filterReplicas(this.uuid, replicas);

    if (replicas.length == 0) {
      // what could we really do in this case?
      throw `No healthy children are available so nexus "${this.uuid}" creation is not allowed at this time`;
    } else {
      return node.createNexus(
        this.uuid,
        this.size,
        Object.values(replicas)
      );
    }
  }

  // Adjust replica count for the volume to required count.
  //
  // @param count   Number of new replicas to create.
  //
  async _createReplicas(count: number) {
    let pools: Pool[] = this.registry.choosePools(
      this.spec.requiredBytes,
      this.spec.requiredNodes,
      this.spec.preferredNodes
    );
    // remove pools that are already used by existing replicas
    const usedNodes = Object.keys(this.replicas);
    pools = pools.filter((p) => p.node && usedNodes.indexOf(p.node.name) < 0);
    if (pools.length < count) {
      log.error(
        `Not enough suitable pool(s) for volume "${this}" with capacity ` +
        `${this.spec.requiredBytes} and replica count ${this.spec.replicaCount}`
      );
      throw new GrpcError(
        grpcCode.RESOURCE_EXHAUSTED,
        `Volume ${this.uuid} with capacity ${this.spec.requiredBytes} requires ${count} storage pool(s). Only ${pools.length} suitable storage pool(s) found.`
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
        this.spec.limitBytes || this.spec.requiredBytes
      );
    }

    // For local volumes, local pool should have the max priority.
    if (this.spec.local && this.spec.preferredNodes[0]) {
      let idx = pools.findIndex((p) => p.node && p.node.name === this.spec.preferredNodes[0]);
      if (idx >= 0) {
        let localPool = pools.splice(idx, 1)[0];
        pools.unshift(localPool);
      }
    }

    // We record all failures as we try to create the replica on available
    // pools to return them to the user at the end if we ultimately fail.
    const errors = [];
    const requestedReplicas = count;
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
      let msg = `Failed to create ${count} out of ${requestedReplicas} requested replicas for volume "${this}": `;
      msg += errors.join('. ');
      throw new GrpcError(grpcCode.INTERNAL, msg);
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
  _scoreReplica(replica: Replica) {
    let score = 0;
    const node = replica.pool?.node;
    if (!node) {
      return 0;
    }

    // The idea is that the sum of less important scores should never overrule
    // the more important criteria.

    // criteria #1: must be on the required nodes if set
    if (
      this.spec.requiredNodes.length > 0 &&
      this.spec.requiredNodes.indexOf(node.name) >= 0
    ) {
      score += 100;
    }
    // criteria #2: replica should be online
    if (!replica.isOffline()) {
      score += 50;
    }
    // criteria #3: would be nice to run on preferred node
    if (
      this.spec.preferredNodes.length > 0 &&
      this.spec.preferredNodes.indexOf(node.name) >= 0
    ) {
      score += 20;
    }
    // criteria #4: if "local" is set then running on the same node as app is desired
    if (
      this.spec.local &&
      this.spec.preferredNodes.length > 0 &&
      this.spec.preferredNodes[0] === node.name
    ) {
      score += 9;
    }
    // criteria #4: local IO from nexus is certainly an advantage
    if (this.nexus && node === this.nexus.node) {
      score += 1;
    }

    // TODO: Score the replica based on the pool parameters.
    //   I.e. the replica on a less busy pool would have higher score.
    return score;
  }

  // Sort replicas according to their value and remove those that aren't online.
  _activeReplicas(): Replica[] {
    const replicaSet = this
      ._prioritizeReplicas(Object.values(this.replicas))
      .filter((r) => !r.isOffline());
    if (replicaSet.length === 0) {
      throw new GrpcError(
        grpcCode.INTERNAL,
        `There are no good replicas for volume "${this}"`
      );
    }
    return replicaSet;
  }

  // Return the node where the nexus for volume is located or where it should
  // be located if it hasn't been created so far. If the nexus should be
  // located on a node that does not exist then return undefined.
  //
  // @param replicaSet List of replicas sorted by preferrence.
  // @param appNode    Name of the node where the volume will be mounted if known.
  // @returns Node object or undefined if not schedulable.
  //
  _desiredNexusNode(replicaSet: Replica[], appNode?: String): Node | undefined {
    if (this.publishedOn) {
      return this.registry.getNode(this.publishedOn);
    }
    let nexusNode: Node | undefined;
    if (appNode) {
      nexusNode = this.registry.getNode(appNode.toString());
    }
    if (!nexusNode && this.nexus) {
      nexusNode = this.nexus.node;
    }
    // If nexus does not exist it will be created on one of the replica nodes
    // with the least # of nexuses.
    if (!nexusNode) {
      nexusNode = replicaSet
        .filter((r: Replica) => !!(r.pool && r.pool.node))
        .map((r: Replica) => r.pool!.node!)
        .sort((a: Node, b: Node) => a.nexus.length - b.nexus.length)[0];
    }
    assert(nexusNode);
    return nexusNode;
  }

  // Share replicas as appropriate to allow access from the nexus.
  // It does not throw unless none of the replicas can be shared.
  // Returns list of replicas that can be accessed by the nexus.
  async _ensureReplicaShareProtocols(nexusNode: Node, replicaSet: Replica[]): Promise<Replica[]> {
    let accessibleReplicas: Replica[] = [];

    for (let i = 0; i < replicaSet.length; i++) {
      const replica: Replica = replicaSet[i];
      if (replica.pool?.node === undefined) continue;
      const replicaNode: Node = replica.pool.node;
      let share;
      const local = replicaNode === nexusNode;
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
          accessibleReplicas.push(replica);
        } catch (err) {
          log.error(err.toString());
        }
      } else {
        accessibleReplicas.push(replica);
      }
    }
    if (accessibleReplicas.length === 0) {
      throw new GrpcError(
        grpcCode.INTERNAL,
        `None of the replicas of ${this} can be accessed by nexus`,
      );
    }
    return accessibleReplicas;
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

    if (this.size && this.size < spec.requiredBytes) {
      throw new GrpcError(
        grpcCode.INVALID_ARGUMENT,
        `Extending the volume "${this}" is not supported`
      );
    }
    if (spec.limitBytes && this.size > spec.limitBytes) {
      throw new GrpcError(
        grpcCode.INVALID_ARGUMENT,
        `Shrinking the volume "${this}" is not supported`
      );
    }
    if (this.spec.protocol !== spec.protocol) {
      throw new GrpcError(
        grpcCode.INVALID_ARGUMENT,
        `Changing the protocol for volume "${this}" is not supported`
      );
    }

    if (this.spec.replicaCount !== spec.replicaCount) {
      this.spec.replicaCount = spec.replicaCount;
      changed = true;
    }
    if (this.spec.local !== spec.local) {
      this.spec.local = spec.local;
      changed = true;
    }
    if (!_.isEqual(this.spec.preferredNodes, spec.preferredNodes)) {
      this.spec.preferredNodes = spec.preferredNodes;
      changed = true;
    }
    if (!_.isEqual(this.spec.requiredNodes, spec.requiredNodes)) {
      this.spec.requiredNodes = spec.requiredNodes;
      changed = true;
    }
    if (this.spec.requiredBytes !== spec.requiredBytes) {
      this.spec.requiredBytes = spec.requiredBytes;
      changed = true;
    }
    if (this.spec.limitBytes !== spec.limitBytes) {
      this.spec.limitBytes = spec.limitBytes;
      changed = true;
    }
    if (changed) {
      this._changed();
      this.fsa();
    }
  }

  // Should be called whenever the volume changes.
  //
  // @param [eventType] The eventType is either new, mod or del and be default
  //                    we assume "mod" which is the most common emitted event.
  _changed(eventType?: string) {
    this.emitter.emit('volume', {
      eventType: eventType || 'mod',
      object: this
    });
  }

  //
  // Handlers for the events from node registry follow
  //

  // Add new replica to the volume.
  //
  // @param replica   New replica object.
  newReplica(replica: Replica) {
    assert.strictEqual(replica.uuid, this.uuid);
    const nodeName = replica.pool?.node?.name;
    if (!nodeName) {
      log.warn(
        `Cannot add replica "${replica}" without a node to the volume`
      );
      return;
    }
    if (this.replicas[nodeName]) {
      log.warn(
        `Trying to add the same replica "${replica}" to the volume twice`
      );
    } else {
      log.debug(`Replica "${replica}" attached to the volume`);
      this.replicas[nodeName] = replica;
      this._changed();
      this.fsa();
    }
  }

  // Modify replica in the volume.
  //
  // @param replica   Modified replica object.
  modReplica(replica: Replica) {
    assert.strictEqual(replica.uuid, this.uuid);
    const nodeName = replica.pool?.node?.name;
    if (!nodeName) {
      log.warn(
        `Cannot update volume by replica "${replica}" without a node`
      );
      return;
    }
    if (!this.replicas[nodeName]) {
      log.warn(`Modified replica "${replica}" does not belong to the volume`);
    } else {
      assert(this.replicas[nodeName] === replica);
      this._changed();
      // the share protocol or uri could have changed
      this.fsa();
    }
  }

  // Delete replica in the volume.
  //
  // @param replica   Deleted replica object.
  delReplica(replica: Replica) {
    assert.strictEqual(replica.uuid, this.uuid);
    const nodeName = replica.pool?.node?.name;
    if (!nodeName) {
      log.warn(
        `Cannot delete replica "${replica}" without a node from the volume`
      );
      return;
    }
    if (!this.replicas[nodeName]) {
      log.warn(`Deleted replica "${replica}" does not belong to the volume`);
    } else {
      log.debug(`Replica "${replica}" detached from the volume`);
      assert(this.replicas[nodeName] === replica);
      delete this.replicas[nodeName];
      this._changed();
      this.fsa();
    }
  }

  // Assign nexus to the volume.
  //
  // @param nexus   New nexus object.
  newNexus(nexus: Nexus) {
    assert.strictEqual(nexus.uuid, this.uuid);
    if (!this.nexus) {
      // If there is no nexus then accept any. This is to support rebuild when
      // volume is not published.
      log.debug(`Nexus "${nexus}" attached to the volume`);
      this.nexus = nexus;
      if (!this.size) this.size = nexus.size;
      this._changed();
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
  // @param nexus   Modified nexus object.
  modNexus(nexus: Nexus) {
    assert.strictEqual(nexus.uuid, this.uuid);
    if (!this.nexus) {
      log.warn(`Modified nexus "${nexus}" does not belong to the volume`);
    } else if (this.nexus === nexus) {
      this._changed();
      this.fsa();
    }
  }

  // Delete nexus in the volume.
  //
  // @param nexus   Deleted nexus object.
  delNexus(nexus: Nexus) {
    assert.strictEqual(nexus.uuid, this.uuid);
    if (!this.nexus) {
      log.warn(`Deleted nexus "${nexus}" does not belong to the volume`);
    } else if (this.nexus === nexus) {
      log.debug(`Nexus "${nexus}" detached from the volume`);
      assert.strictEqual(this.nexus, nexus);
      this.nexus = null;
      this._changed();
      this.fsa();
    } else {
      // if this is a different nexus than ours, ignore it
    }
  }
}