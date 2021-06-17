// Nexus object implementation.

import assert from 'assert';
import * as _ from 'lodash';

import { grpcCode, GrpcError, mayastor } from './grpc_client';
import { Node } from './node';
import { Replica } from './replica';
import { Logger } from './logger';

const log = Logger('nexus');

// We increase timeout value to nexus destroy method because it involves
// updating etcd state in mayastor. Mayastor itself uses 30s timeout for etcd.
const NEXUS_DESTROY_TIMEOUT_MS = 60000;

// Protocol used to export nexus (volume)
export enum Protocol {
  Unknown = 'unknown',
  Iscsi = 'iscsi',
  Nvmf = 'nvmf',
}

export function protocolFromString(val: string): Protocol {
  if (val == Protocol.Iscsi) {
    return Protocol.Iscsi;
  } else if (val == Protocol.Nvmf) {
    return Protocol.Nvmf;
  } else {
    return Protocol.Unknown;
  }
}

// Represents a child with uri and state properties.
// TODO: define state as enum.
export class Child {
  constructor(public uri: string, public state: string) {
    assert(uri);
    assert(state);
  }
  isEqual(ch: Child) {
    return (ch.uri === this.uri && ch.state === this.state);
  }
}

// Used with .sort() method to enforce deterministic order of children.
function compareChildren(a: Child, b: Child) {
  return a.uri.localeCompare(b.uri);
}

export class Nexus {
  node?: Node;
  uuid: string;
  size: number;
  deviceUri: string;
  state: string;
  children: Child[];

  // Construct new nexus object.
  //
  // @param {object}   props    Nexus properties as obtained from the storage node.
  // @param {string}   props.uuid       ID of the nexus.
  // @param {number}   props.size       Capacity of the nexus in bytes.
  // @param {string}   props.deviceUri  Block device path to the nexus.
  // @param {string}   props.state      State of the nexus.
  // @param {object[]} props.children   Replicas comprising the nexus (uri and state).
  //
  constructor(props: any) {
    this.node = undefined; // set by registerNexus method on node
    this.uuid = props.uuid;
    this.size = props.size;
    this.deviceUri = props.deviceUri;
    this.state = props.state;
    // children of the nexus (replica URIs and their state)
    this.children = (props.children || [])
      .map((ch: any) => new Child(ch.uri, ch.state))
      .sort(compareChildren);
  }

  // Stringify the nexus
  toString() {
    return this.uuid + '@' + (this.node ? this.node.name : 'nowhere');
  }

  // Update object based on fresh properties obtained from mayastor storage node.
  //
  // @param {object}   props            Properties defining the nexus.
  // @param {string}   props.uuid       ID of the nexus.
  // @param {number}   props.size       Capacity of the nexus in bytes.
  // @param {string}   props.deviceUri  Block device URI of the nexus.
  // @param {string}   props.state      State of the nexus.
  // @param {object[]} props.children   Replicas comprising the nexus (uri and state).
  //
  merge(props: any) {
    let changed = false;

    if (this.size !== props.size) {
      this.size = props.size;
      changed = true;
    }
    if (this.deviceUri !== props.deviceUri) {
      this.deviceUri = props.deviceUri;
      changed = true;
    }
    if (this.state !== props.state) {
      this.state = props.state;
      changed = true;
    }
    const children = props.children
      .map((ch: any) => new Child(ch.uri, ch.state))
      .sort(compareChildren);
    let childrenChanged = false;
    if (this.children.length !== children.length) {
      childrenChanged = true;
    } else {
      for (let i = 0; i < this.children.length; i++) {
        if (!this.children[i].isEqual(children[i])) {
          childrenChanged = true;
          break;
        }
      }
    }
    if (childrenChanged) {
      this.children = children;
      changed = true;
    }
    if (changed) {
      this._emitMod();
    }
  }

  // When anything in nexus changes, this can be called to emit mod event
  // (a shortcut for frequently used code).
  _emitMod() {
    this.node!.emit('nexus', {
      eventType: 'mod',
      object: this
    });
  }

  // Bind nexus to the node.
  //
  // @param {object} node   Node to bind the nexus to.
  //
  bind(node: any) {
    this.node = node;
    log.debug(`Adding "${this.uuid}" to the nexus list of node "${node}"`);
    this.node!.emit('nexus', {
      eventType: 'new',
      object: this
    });
  }

  // Unbind the previously bound nexus from the node.
  unbind() {
    log.debug(`Removing "${this}" from the nexus list`);
    this.node!.unregisterNexus(this);
    this.node!.emit('nexus', {
      eventType: 'del',
      object: this
    });
    this.node = undefined;
  }

  // Set state of the nexus to offline.
  // This is typically called when mayastor stops running on the node and
  // the pool becomes inaccessible.
  offline() {
    log.warn(`Nexus "${this}" got offline`);
    this.state = 'NEXUS_OFFLINE';
    this._emitMod();
  }

  // Return true if the nexus is down (unreachable).
  isOffline() {
    return !(this.node && this.node.isSynced());
  }

  // Publish the nexus to make accessible for IO.
  // @params protocol      The nexus share protocol.
  // @returns The device path of nexus block device.
  //
  async publish(protocol: Protocol): Promise<string> {
    var res;

    if (this.deviceUri) {
      throw new GrpcError(
        grpcCode.ALREADY_EXISTS,
        `Nexus ${this} has been already published`
      );
    }

    const nexusProtocol = 'NEXUS_'.concat(protocol.toUpperCase());
    var shareNumber = mayastor.enums[nexusProtocol];
    if (shareNumber === undefined) {
      throw new GrpcError(
        grpcCode.NOT_FOUND,
        `Cannot find protocol "${protocol}" for Nexus ${this}`
      );
    }
    log.info(`Publishing nexus "${this}" with protocol=${protocol} ...`);
    try {
      res = await this.node!.call('publishNexus', {
        uuid: this.uuid,
        key: '',
        share: shareNumber
      });
    } catch (err) {
      throw new GrpcError(
        grpcCode.INTERNAL,
        `Failed to publish nexus "${this}": ${err}`
      );
    }
    log.info(`Nexus "${this}" is published at "${res.deviceUri}"`);
    this.deviceUri = res.deviceUri;
    this._emitMod();
    return res.deviceUri;
  }

  // Unpublish nexus.
  async unpublish() {
    log.debug(`Unpublishing nexus "${this}" ...`);

    if (!this.node!.isSynced()) {
      // We don't want to block the volume life-cycle in case that the node
      // is down - it may never come back online.
      log.warn(`Faking the unpublish of "${this}" because it is unreachable`);
    } else {
      try {
        await this.node!.call('unpublishNexus', { uuid: this.uuid });
      } catch (err) {
        if (err.code === grpcCode.NOT_FOUND) {
          log.warn(`The nexus "${this}" does not exist`);
        } else {
          throw new GrpcError(
            grpcCode.INTERNAL,
            `Failed to unpublish nexus "${this}": ${err}`
          );
        }
      }
      log.info(`Nexus "${this}" was unpublished`);
    }
    this.deviceUri = '';
    this._emitMod();
  }

  // Get URI under which the nexus is published or "undefined" if it hasn't been
  // published.
  getUri(): string | undefined {
    return this.deviceUri || undefined;
  }

  // Add replica to the nexus.
  //
  // @param {object} replica   Replica object to add to the nexus.
  //
  async addReplica(replica: Replica): Promise<Child> {
    const uri = replica.uri;
    let ch = this.children.find((ch) => ch.uri === uri);
    if (ch) {
      return ch;
    }
    log.debug(`Adding uri "${uri}" to nexus "${this}" ...`);

    var childInfo;
    try {
      // TODO: validate the output
      childInfo = await this.node!.call('addChildNexus', {
        uuid: this.uuid,
        uri: uri,
        norebuild: false
      });
    } catch (err) {
      throw new GrpcError(
        grpcCode.INTERNAL,
        `Failed to add uri "${uri}" to nexus "${this}": ${err}`
      );
    }
    // The child will need to be rebuilt when added, but until we get
    // confirmation back from the nexus, set it as pending
    ch = new Child(childInfo.uri, childInfo.state);
    this.children.push(ch);
    this.children.sort(compareChildren);
    this.state = "NEXUS_DEGRADED"
    log.info(`Replica uri "${uri}" added to the nexus "${this}"`);
    this._emitMod();
    return ch;
  }

  // Remove replica from nexus.
  //
  // @param {string} uri   URI of the replica to be removed from the nexus.
  //
  async removeReplica(uri: string) {
    if (!this.children.find((ch) => ch.uri === uri)) {
      return;
    }

    log.debug(`Removing uri "${uri}" from nexus "${this}" ...`);

    try {
      await this.node!.call('removeChildNexus', {
        uuid: this.uuid,
        uri: uri
      });
    } catch (err) {
      throw new GrpcError(
        grpcCode.INTERNAL,
        `Failed to remove uri "${uri}" from nexus "${this}": ${err}`
      );
    }
    // get index again in case the list changed in the meantime
    const idx = this.children.findIndex((ch) => ch.uri === uri);
    if (idx >= 0) {
      this.children.splice(idx, 1);
    }
    log.info(`Replica uri "${uri}" removed from the nexus "${this}"`);
    this._emitMod();
  }

  // Destroy nexus on storage node.
  async destroy() {
    log.debug(`Destroying nexus "${this}" ...`);
    if (!this.node!.isSynced()) {
      // We don't want to block the volume life-cycle in case that the node
      // is down - it may never come back online.
      log.warn(`Faking the destroy of "${this}" because it is unreachable`);
    } else {
      await this.node!.call(
        'destroyNexus',
        { uuid: this.uuid },
        NEXUS_DESTROY_TIMEOUT_MS,
      );
      log.info(`Destroyed nexus "${this}"`);
    }
    this.unbind();
  }
}