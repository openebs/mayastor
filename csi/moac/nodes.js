'use strict';

const assert = require('assert');
const EventEmitter = require('events');
const log = require('./logger').Logger('node-operator');
const Watcher = require('./watcher').Watcher;
const { PLUGIN_NAME, parseMayastorNodeId } = require('./common');

// Node operator can be integrated with other parts of the program using public
// methods (without leading underscore) and by events:
//
//  event ready: Monitoring of the nodes has been started (safe to call get()).
//  event add({node, endpoint}): A new mayastor node has appeared.
//  event remove({node}): Mayastor node was removed.
//
class NodeOperator extends EventEmitter {
  constructor() {
    super();
    this.nodes = {}; // List of mayastor nodes indexed by node name
    this.watcher = null;
  }

  // Create CRDs needed for node operator if they don't exist and augment
  // client object by these CRDs so that they can be manipulated as any other
  // standard k8s api objects.
  //
  // NOTE: We won't probably need this when CSI node info becomes GA in k8s.
  async init(client) {
    log.info('Initializing node operator');

    // Create the node watcher
    let watcher = new Watcher(
      'node',
      client.apis['storage.k8s.io'].v1beta1.csinodes,
      client.apis['storage.k8s.io'].v1beta1.watch.csinodes,
      this.filterMayastorNode
    );

    this._bindWatcher(watcher);
    this.watcher = watcher;
  }

  // Bind the watcher to node operator's callbacks for new/mod/del events.
  //
  // Beware! The events correspond to CSINode object which may contain
  // multiple CSI plugins, so new and mod handlers must be prepared to
  // handle cases when the mayastor plugin is actually removed.
  _bindWatcher(watcher) {
    watcher.on('new', this.nodeEventCallback.bind(this));
    watcher.on('mod', this.nodeEventCallback.bind(this));
    // del is triggered when the whole CSINode record is deleted
    var self = this;
    watcher.on('del', ev => {
      delete ev.id;
      delete ev.endpoint;
      self.nodeEventCallback(ev);
    });
  }

  nodeEventCallback(obj) {
    let old = this.nodes[obj.name];

    if (old) {
      if (obj.id) {
        // See if anything has changed.
        // "add" event is emitted also for any change of properties
        // (currently that's just endpoint property).
        this.nodes[obj.name] = obj;
        if (old.endpoint !== obj.endpoint) {
          // can happen i.e. if pod is restarted and IP changes
          log.info(
            `mayastor endpoint on node "${obj.name}" changed from "${old.endpoint}" to "${obj.endpoint}"`
          );
          this.emit('add', { node: obj.name, endpoint: obj.endpoint });
        }
      } else {
        // Delete mayastor node from the internal list of nodes and emit
        // "remove" event
        delete this.nodes[obj.name];
        log.info(`mayastor on node "${obj.name}" is gone`);
        this.emit('remove', { node: obj.name });
      }
    } else {
      if (obj.id) {
        // Add mayastor node to the internal list of nodes and emit "add" event
        this.nodes[obj.name] = obj;
        log.info(
          `mayastor on node "${obj.name}" and with endpoint "${obj.endpoint}" joined the cluster`
        );
        this.emit('add', { node: obj.name, endpoint: obj.endpoint });
      } else {
        // record which we did not know about was removed - ignore
      }
    }
  }

  // Get specified mayastor node or list of all mayastor nodes if called
  // without argument.
  //
  // NOTE: Do not call until node operator emits ready event.
  get(name) {
    if (name) {
      let node = this.nodes[name];

      if (node) {
        return {
          node: node.name,
          endpoint: node.endpoint,
        };
      }
    } else {
      return Object.values(this.nodes).map(ent => {
        return {
          node: ent.name,
          endpoint: ent.endpoint,
        };
      });
    }
  }

  // Process csinode entry and return mayastor node info.
  // If mayastor does not run on the node, then return null.
  //
  // csi node info example:
  //
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
  filterMayastorNode(csiNode) {
    // find mayastor driver if there is any
    const drivers = csiNode.spec.drivers;
    if (!drivers) {
      // it can happen if there are not any CSI drivers on the node (k8s quirk)
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

    // This defines the node object properties
    return {
      name: nodeId.node,
      id: driver.nodeID,
      endpoint: nodeId.endpoint,
    };
  }

  async start() {
    await this.watcher.start();
    this.emit('ready');
  }

  async stop() {
    this.watcher.removeAllListeners();
    await this.watcher.stop();
  }
}

// Node operator mock
class NodeOperatorMock extends EventEmitter {
  constructor(nodes) {
    super();
    // this is what we return by get method
    this.nodes = nodes || [];
  }

  get(name) {
    if (name) {
      return this.nodes.find(ent => ent.node == name);
    } else {
      return this.nodes;
    }
  }

  addNode(nodeName, endpoint) {
    var node = {
      node: nodeName,
      endpoint: endpoint,
    };
    this.nodes.push(node);
    var self = this;
    setTimeout(() => {
      self.emit('add', node);
    }, 0);
  }

  removeNode(node) {
    let idx = this.nodes.findIndex(ent => ent.node == node);
    if (idx >= 0) {
      this.nodes.splice(idx, 1);
      setTimeout(() => {
        this.emit('remove', { node });
      }, 0);
    }
  }
}

module.exports = {
  NodeOperator,
  NodeOperatorMock,
};
