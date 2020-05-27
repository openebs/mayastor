// Node operator is responsible for managing mayastor node custom resources
// that represent nodes in the cluster that run mayastor (storage nodes).
//
// Roles:
// * The operator creates/modifies/deletes the resources to keep them up to date.
// * A user can delete a stale resource (can happen that moac doesn't know)

'use strict';

const assert = require('assert');
const fs = require('fs');
const path = require('path');
const yaml = require('js-yaml');
const EventStream = require('./event_stream');
const log = require('./logger').Logger('node-operator');
const Watcher = require('./watcher');

const crdNode = yaml.safeLoad(
  fs.readFileSync(path.join(__dirname, '/crds/mayastornode.yaml'), 'utf8')
);

// Node operator watches k8s CSINode resources and based on that detects
// running mayastor instances in the cluster.
class NodeOperator {
  // init() is decoupled from constructor because tests do their own
  // initialization of the object.
  //
  // @param {string} namespace   Namespace the operator should operate on.
  constructor (namespace) {
    this.k8sClient = null; // k8s client for sending requests to api srv
    this.watcher = null; // k8s resource watcher for CSI nodes resource
    this.registry = null;
    this.namespace = namespace;
  }

  // Create node CRD if it doesn't exist and augment client object so that CRD
  // can be manipulated as any other standard k8s api object.
  //
  // @param {object} k8sClient   Client for k8s api server.
  // @param {object} registry    Registry with node objects.
  //
  async init (k8sClient, registry) {
    log.info('Initializing node operator');
    assert(registry);

    try {
      await k8sClient.apis[
        'apiextensions.k8s.io'
      ].v1beta1.customresourcedefinitions.post({ body: crdNode });
      log.info('Created CRD ' + crdNode.spec.names.kind);
    } catch (err) {
      // API returns a 409 Conflict if CRD already exists.
      if (err.statusCode !== 409) throw err;
    }
    k8sClient.addCustomResourceDefinition(crdNode);

    this.k8sClient = k8sClient;
    this.registry = registry;

    // Initialize watcher with all callbacks for new/mod/del events
    this.watcher = new Watcher(
      'node',
      this.k8sClient.apis['openebs.io'].v1alpha1.namespaces(
        this.namespace
      ).mayastornodes,
      this.k8sClient.apis['openebs.io'].v1alpha1.watch.namespaces(
        this.namespace
      ).mayastornodes,
      this._filterMayastorNode
    );
  }

  // Normalize k8s mayastor node resource.
  //
  // @param   {object} msn   MayaStor node custom resource.
  // @returns {object} Properties defining the node.
  //
  _filterMayastorNode (msn) {
    if (!msn.spec.grpcEndpoint) {
      log.warn('Ignoring mayastor node resource without grpc endpoint');
      return null;
    }
    return {
      metadata: { name: msn.metadata.name },
      spec: {
        grpcEndpoint: msn.spec.grpcEndpoint
      },
      status: msn.status || 'unknown'
    };
  }

  // Bind watcher's new/del events to node operator's callbacks.
  //
  // Not interested in mod events as the operator is the only who should
  // be doing modifications to these objects.
  //
  // @param {object} watcher   k8s node resource watcher.
  //
  _bindWatcher (watcher) {
    var self = this;
    watcher.on('new', (obj) => {
      self.registry.addNode(obj.metadata.name, obj.spec.grpcEndpoint);
    });
    watcher.on('del', (obj) => {
      self.registry.removeNode(obj.metadata.name);
    });
  }

  // Start node operator's watcher loop.
  async start () {
    var self = this;

    // install event handlers to follow changes to resources.
    self._bindWatcher(self.watcher);
    await self.watcher.start();

    // This will start async processing of node events.
    self.eventStream = new EventStream({ registry: self.registry });
    self.eventStream.on('data', async (ev) => {
      if (ev.kind !== 'node') return;

      if (ev.eventType === 'new' || ev.eventType === 'mod') {
        const name = ev.object.name;
        const endpoint = ev.object.endpoint;
        const k8sNode = self.watcher.getRaw(name);

        if (k8sNode) {
          // Update object only if it has really changed
          if (k8sNode.spec.grpcEndpoint !== endpoint) {
            try {
              await self._updateResource(name, k8sNode, endpoint);
            } catch (err) {
              log.error(`Failed to update node resource "${name}": ${err}`);
              return;
            }
          }
        } else if (ev.eventType === 'new' && !k8sNode) {
          try {
            await self._createResource(name, endpoint);
          } catch (err) {
            log.error(`Failed to create node resource "${name}": ${err}`);
            return;
          }
        }
        await this._updateStatus(name, ev.object.isSynced() ? 'online' : 'offline');
      } else if (ev.eventType === 'del') {
        await self._deleteResource(ev.object.name);
      } else {
        assert.strictEqual(ev.eventType, 'sync');
      }
    });
  }

  // Create k8s CRD object.
  //
  // @param {string} name          Node of the created node.
  // @param {string} grpcEndpoint  Endpoint property of the object.
  //
  async _createResource (name, grpcEndpoint) {
    log.info(`Creating node resource "${name}"`);
    await this.k8sClient.apis['openebs.io'].v1alpha1
      .namespaces(this.namespace)
      .mayastornodes.post({
        body: {
          apiVersion: 'openebs.io/v1alpha1',
          kind: 'MayastorNode',
          metadata: {
            name,
            namespace: this.namespace
          },
          spec: { grpcEndpoint }
        }
      });
  }

  // Update properties of k8s CRD object or create it if it does not exist.
  //
  // @param {string} name          Name of the updated node.
  // @param {object} k8sNode       Existing k8s resource object.
  // @param {string} grpcEndpoint  Endpoint property of the object.
  //
  async _updateResource (name, k8sNode, grpcEndpoint) {
    log.info(`Updating spec of node resource "${name}"`);
    await this.k8sClient.apis['openebs.io'].v1alpha1
      .namespaces(this.namespace)
      .mayastornodes(name)
      .put({
        body: {
          apiVersion: 'openebs.io/v1alpha1',
          kind: 'MayastorNode',
          metadata: k8sNode.metadata,
          spec: { grpcEndpoint }
        }
      });
  }

  // Update state of the resource.
  //
  // NOTE: This method does not throw if the operation fails as there is nothing
  // we can do if it fails. Though we log an error message in such a case.
  //
  // @param {string} name    UUID of the resource.
  // @param {string} status  State of the node.
  //
  async _updateStatus (name, status) {
    var k8sNode = this.watcher.getRaw(name);
    if (!k8sNode) {
      log.warn(
        `Wanted to update state of node resource "${name}" that disappeared`
      );
      return;
    }
    if (k8sNode.status === status) {
      // avoid unnecessary status updates
      return;
    }
    log.debug(`Updating status of node resource "${name}"`);
    k8sNode.status = status;
    try {
      await this.k8sClient.apis['openebs.io'].v1alpha1
        .namespaces(this.namespace)
        .mayastornodes(name)
        .status.put({ body: k8sNode });
    } catch (err) {
      log.error(`Failed to update status of node resource "${name}": ${err}`);
    }
  }

  // Delete node resource with specified name.
  //
  // @param {string} name   Name of the node resource to delete.
  //
  async _deleteResource (name) {
    var k8sNode = this.watcher.getRaw(name);
    if (k8sNode) {
      log.info(`Deleting node resource "${name}"`);
      try {
        await this.k8sClient.apis['openebs.io'].v1alpha1
          .namespaces(this.namespace)
          .mayastornodes(name)
          .delete();
      } catch (err) {
        log.error(`Failed to delete node resource "${name}": ${err}`);
      }
    }
  }

  // Stop listening for watcher and node events and reset the cache
  async stop () {
    this.watcher.removeAllListeners();
    await this.watcher.stop();
    this.eventStream.destroy();
    this.eventStream = null;
  }
}

module.exports = NodeOperator;
