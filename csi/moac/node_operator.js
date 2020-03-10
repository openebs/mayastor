// Node operator is responsible for keeping track of mayastor storage nodes
// present in the cluster.

'use strict';

const assert = require('assert');
const EventEmitter = require('events');
const log = require('./logger').Logger('node-operator');
const Watcher = require('./watcher');
const { PLUGIN_NAME, parseMayastorNodeId } = require('./common');

// Node operator watches k8s CSINode resources and based on that detects
// running mayastor instances in the cluster.
class NodeOperator extends EventEmitter {
  // init() is decoupled from constructor because tests do their own
  // initialization of the object.
  constructor() {
    super();
    this.watcher = null; // k8s resource watcher for CSI nodes resource
    this.registry = null;
  }

  // Initialize k8s watcher (but do not start it yet) and enable watcher events.
  //
  // @param {object} k8sClient   k8s client for connecting to k8s api server.
  // @param {object} registry    Registry object.
  //
  init(k8sClient, registry) {
    assert(registry);

    log.info('Initializing node operator');

    let watcher = new Watcher(
      'node',
      k8sClient.apis['storage.k8s.io'].v1beta1.csinodes,
      k8sClient.apis['storage.k8s.io'].v1beta1.watch.csinodes,
      this.filterMayastorNode
    );

    this._bindWatcher(watcher);
    this.watcher = watcher;
    this.registry = registry;
  }

  // Process CSINode entry and return mayastor node info.
  //
  // If mayastor does not run on the node (there can be other CSI plugins),
  // then return null.
  //
  // csi node info example:
  // ```
  // "kind": "CSINodeList",
  // "apiVersion": "storage.k8s.io/v1beta1",
  // "metadata": {
  //   "selfLink": "/apis/storage.k8s.io/v1beta1/csinodes",
  //   "resourceVersion": "1368155"
  // },
  // "items": [
  //   {
  //     "metadata": {
  //       "name": "node1",
  //       "selfLink": "/apis/storage.k8s.io/v1beta1/csinodes/node1",
  //       "uid": "cb8c76d2-5bba-11e9-8f3c-589cfc0d76a7",
  //       "resourceVersion": "1352402",
  //       "creationTimestamp": "2019-04-10T18:02:24Z",
  //       "ownerReferences": [
  //         {
  //           "apiVersion": "v1",
  //           "kind": "Node",
  //           "name": "node1",
  //           "uid": "e6e982a1-5b8b-11e9-8f3c-589cfc0d76a7"
  //         }
  //       ]
  //     },
  //     "spec": {
  //       "drivers": [
  //         {
  //           "name": "io.openebs.csi-mayastor",
  //           "nodeID": "mayastor://node1/10.244.2.6:10124",
  //           "topologyKeys": null
  //         }
  //       ]
  //     }
  //   }
  // ]
  // ```
  //
  // @param   {object} csiNode   CSINode object as received from k8s api server.
  // @returns {object} Mayastor storage node information.
  //
  filterMayastorNode(csiNode) {
    // find mayastor driver if there is any
    const drivers = csiNode.spec.drivers;
    if (!drivers) {
      // it can happen if there is not any CSI driver on the node (k8s quirk)
      return {
        name: csiNode.metadata.name,
        id: null,
        endpoint: null,
      };
    }
    const driver = drivers.find(drv => drv.name === PLUGIN_NAME);
    if (!driver) {
      // not our CSI driver
      return {
        name: csiNode.metadata.name,
        id: null,
        endpoint: null,
      };
    }

    // Ignore mayastors with IDs that we don't understand (likely newer
    // versions which are not backward compatible)
    var nodeId;
    try {
      nodeId = parseMayastorNodeId(driver.nodeID);
    } catch (err) {
      log.error(err.toString());
      return null;
    }
    if (nodeId.node !== csiNode.metadata.name) {
      log.error('Inconsistent mayastor node ID: ' + driver.nodeID);
      return null;
    }

    return {
      name: nodeId.node,
      id: driver.nodeID,
      endpoint: nodeId.endpoint,
    };
  }

  // Bind the watcher to node operator's callbacks for new/mod/del events.
  //
  // Beware! Events correspond to CSINode object which may contain
  // multiple CSI plugins, so new and mod handlers must be prepared to
  // handle cases when the mayastor plugin is actually removed instead of
  // being added or modified.
  _bindWatcher(watcher) {
    watcher.on('new', this._nodeEventCallback.bind(this));
    watcher.on('mod', this._nodeEventCallback.bind(this));
    // del is triggered when the whole CSINode record is deleted
    var self = this;
    watcher.on('del', ev => {
      delete ev.id;
      delete ev.endpoint;
      self._nodeEventCallback(ev);
    });
  }

  // Called when there is an event (new/mod/del) on CSINode resource.
  //
  // @param {object} newProps   New CSINode properties.
  _nodeEventCallback(newProps) {
    let name = newProps.name;
    let curObj = this.registry.getNode(name);

    if (curObj) {
      if (newProps.id) {
        // The endpoint might have changed.
        // i.e. if pod is restarted and IP changes
        curObj.connect(newProps.endpoint);
      } else {
        this.registry.removeNode(name);
      }
    } else {
      if (newProps.id) {
        this.registry.addNode(name, newProps.endpoint);
      } else {
        // record which we did not know about was removed - ignore
        log.warn(`Unknown mayastor node "${name}" removed`);
      }
    }
  }

  // Get list of CSINode resources and start the watcher. It emits
  // ready event when the initialization is done.
  async start() {
    await this.watcher.start();
    this.emit('ready');
  }

  // Stop the watcher.
  async stop() {
    this.watcher.removeAllListeners();
    await this.watcher.stop();
  }
}

module.exports = NodeOperator;
