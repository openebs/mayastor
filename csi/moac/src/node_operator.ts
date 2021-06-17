// Node operator is responsible for managing mayastor node custom resources
// that represent nodes in the cluster that run mayastor (storage nodes).
//
// Roles:
// * The operator creates/modifies/deletes the resources to keep them up to date.
// * A user can delete a stale resource (can happen that moac doesn't know)

import assert from 'assert';
import * as fs from 'fs';
import * as path from 'path';
import {
  ApiextensionsV1Api,
  KubeConfig,
} from '@kubernetes/client-node';
import {
  CustomResource,
  CustomResourceCache,
  CustomResourceMeta,
} from './watcher';
import { EventStream } from './event_stream';
import { Workq } from './workq';
import { Logger } from './logger';

const log = Logger('node-operator');

const yaml = require('js-yaml');

const RESOURCE_NAME: string = 'mayastornode';
const crdNode = yaml.load(
  fs.readFileSync(path.join(__dirname, '../crds/mayastornode.yaml'), 'utf8')
);

// State of a storage node.
enum NodeState {
  Unknown = "unknown",
  Online = "online",
  Offline = "offline",
}

// Object defines properties of node resource.
export class NodeResource extends CustomResource {
  apiVersion?: string;
  kind?: string;
  metadata: CustomResourceMeta;
  spec: { grpcEndpoint: string };
  status?: NodeState;

  constructor(cr: CustomResource) {
    super();
    this.apiVersion = cr.apiVersion;
    this.kind = cr.kind;
    if (cr.status === NodeState.Online) {
      this.status = NodeState.Online;
    } else if (cr.status === NodeState.Offline) {
      this.status = NodeState.Offline;
    } else {
      this.status = NodeState.Unknown;
    }
    if (cr.metadata === undefined) {
      throw new Error('missing metadata');
    } else {
      this.metadata = cr.metadata;
    }
    if (cr.spec === undefined) {
      throw new Error('missing spec');
    } else {
      let grpcEndpoint = (cr.spec as any).grpcEndpoint;
      if (!grpcEndpoint) {
        grpcEndpoint = '';
      }
      this.spec = { grpcEndpoint };
    }
  }
}

export class NodeOperator {
  watcher: CustomResourceCache<NodeResource>; // k8s resource watcher for nodes
  registry: any;
  namespace: string;
  workq: Workq; // for serializing node operations
  eventStream: any; // events from the registry

  // Create node operator object.
  //
  // @param namespace     Namespace the operator should operate on.
  // @param kubeConfig    KubeConfig.
  // @param registry      Registry with node objects.
  // @param [idleTimeout] Timeout for restarting watcher connection when idle.
  constructor (
    namespace: string,
    kubeConfig: KubeConfig,
    registry: any,
    idleTimeout: number | undefined,
  ) {
    assert(registry);
    this.namespace = namespace;
    this.workq = new Workq('mayastornode');
    this.registry = registry;
    this.watcher = new CustomResourceCache(
      this.namespace,
      RESOURCE_NAME,
      kubeConfig,
      NodeResource,
      { idleTimeout }
    );
  }

  // Create node CRD if it doesn't exist.
  //
  // @param kubeConfig  KubeConfig.
  async init (kubeConfig: KubeConfig) {
    log.info('Initializing node operator');
    let k8sExtApi = kubeConfig.makeApiClient(ApiextensionsV1Api);
    try {
      await k8sExtApi.createCustomResourceDefinition(crdNode);
      log.info(`Created CRD ${RESOURCE_NAME}`);
    } catch (err) {
      // API returns a 409 Conflict if CRD already exists.
      if (err.statusCode !== 409) throw err;
    }
  }

  // Bind watcher's new/del events to node operator's callbacks.
  //
  // Not interested in mod events as the operator is the only who should
  // be doing modifications to these objects.
  //
  // @param {object} watcher   k8s node resource watcher.
  //
  _bindWatcher (watcher: CustomResourceCache<NodeResource>) {
    watcher.on('new', (obj: NodeResource) => {
      if (obj.metadata && obj.spec.grpcEndpoint) {
        this.registry.addNode(obj.metadata.name, obj.spec.grpcEndpoint);
      }
    });
    watcher.on('del', (obj: NodeResource) => {
      this.registry.removeNode(obj.metadata.name);
    });
  }

  // Start node operator's watcher loop.
  async start () {
    // install event handlers to follow changes to resources.
    this._bindWatcher(this.watcher);
    await this.watcher.start();

    // This will start async processing of node events.
    this.eventStream = new EventStream({ registry: this.registry });
    this.eventStream.on('data', async (ev: any) => {
      if (ev.kind !== 'node') return;
      await this.workq.push(ev, this._onNodeEvent.bind(this));
    });
  }

  async _onNodeEvent (ev: any) {
    const name = ev.object.name;
    if (ev.eventType === 'new') {
      const grpcEndpoint = ev.object.endpoint || '';
      let origObj = this.watcher.get(name);
      if (origObj === undefined) {
        await this._createResource(name, grpcEndpoint);
      } else {
        await this._updateSpec(name, grpcEndpoint);
      }
      await this._updateStatus(
        name,
        ev.object.isSynced() ? NodeState.Online : NodeState.Offline,
      );
    } else if (ev.eventType === 'mod') {
      const grpcEndpoint = ev.object.endpoint || '';
      let origObj = this.watcher.get(name);
      // The node might be just going away - do nothing if not in the cache
      if (origObj !== undefined) {
        await this._updateSpec(name, grpcEndpoint);
        await this._updateStatus(
          name,
          ev.object.isSynced() ? NodeState.Online : NodeState.Offline,
        );
      }
    } else if (ev.eventType === 'del') {
      await this._deleteResource(ev.object.name);
    } else {
      assert.strictEqual(ev.eventType, 'sync');
    }
  }

  async _createResource(name: string, grpcEndpoint: string) {
    log.info(`Creating node resource "${name}"`);
    try {
      await this.watcher.create({
        apiVersion: 'openebs.io/v1alpha1',
        kind: 'MayastorNode',
        metadata: {
          name,
          namespace: this.namespace
        },
        spec: { grpcEndpoint }
      });
    } catch (err) {
      log.error(`Failed to create node resource "${name}": ${err}`);
    }
  }

  // Update properties of k8s CRD object or create it if it does not exist.
  //
  // @param name          Name of the updated node.
  // @param grpcEndpoint  Endpoint property of the object.
  //
  async _updateSpec (name: string, grpcEndpoint: string) {
    try {
      await this.watcher.update(name, (orig: NodeResource) => {
        // Update object only if it has really changed
        if (orig.spec.grpcEndpoint === grpcEndpoint) {
          return;
        }
        log.info(`Updating spec of node resource "${name}"`);
        return {
          apiVersion: 'openebs.io/v1alpha1',
          kind: 'MayastorNode',
          metadata: orig.metadata,
          spec: { grpcEndpoint }
        };
      });
    } catch (err) {
      log.error(`Failed to update node resource "${name}": ${err}`);
    }
  }

  // Update state of the resource.
  //
  // NOTE: This method does not throw if the operation fails as there is nothing
  // we can do if it fails. Though we log an error message in such a case.
  //
  // @param name    UUID of the resource.
  // @param status  State of the node.
  //
  async _updateStatus (name: string, status: NodeState) {
    try {
      await this.watcher.updateStatus(name, (orig: NodeResource) => {
        // avoid unnecessary status updates
        if (orig.status === status) {
          return;
        }
        log.debug(`Updating status of node resource "${name}"`);
        return {
          apiVersion: 'openebs.io/v1alpha1',
          kind: 'MayastorNode',
          metadata: orig.metadata,
          spec: orig.spec,
          status: status,
        };
      });
    } catch (err) {
      log.error(`Failed to update status of node resource "${name}": ${err}`);
    }
  }

  // Delete node resource with specified name.
  //
  // @param {string} name   Name of the node resource to delete.
  //
  async _deleteResource (name: string) {
    try {
      log.info(`Deleting node resource "${name}"`);
      await this.watcher.delete(name);
    } catch (err) {
      log.error(`Failed to delete node resource "${name}": ${err}`);
    }
  }

  // Stop listening for watcher and node events and reset the cache
  stop () {
    this.watcher.stop();
    this.watcher.removeAllListeners();
    if (this.eventStream) {
      this.eventStream.destroy();
      this.eventStream = null;
    }
  }
}
