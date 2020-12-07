// This is the component of the moac that maintains the state of the storage
// nodes. It keeps track of the nodes, pools, replicas and nexus. It serves as
// a database (other components can query it to get a list of objects) and also
// as a message bus (other components can subscribe to events).

'use strict';

const assert = require('assert');
const EventEmitter = require('events');
const log = require('./logger').Logger('registry');
const { Node } = require('./node');

// List of events emitted by the registry.
//
// The payload of the event is as follows:
// ```
// {
//   eventType: "sync", "new", "mod", "del"
//   object: node, pool, replica, nexus object
// }
// ```
const eventObjects = ['node', 'nexus', 'pool', 'replica'];

class Registry extends EventEmitter {
  constructor () {
    super();
    this.nodes = {}; // node objects indexed by name
    // This gives a chance to override Node class used for creating new
    // node objects, which is useful for testing of the registry.
    this.Node = Node;
  }

  // Disconnect all nodes.
  close () {
    const self = this;
    Object.keys(this.nodes).forEach((name) => {
      self.removeNode(name);
    });
  }

  // Add mayastor node to the list of nodes and subscribe to events
  // emitted by the node to relay them further. It can be called also for
  // existing nodes to update their grpc endpoint.
  //
  // @param {string} name      Name of the node.
  // @param {string} endpoint  Endpoint for gRPC communication.
  addNode (name, endpoint) {
    let node = this.nodes[name];
    if (node) {
      // if grpc endpoint has not changed, then this will not do anything
      if (node.endpoint !== endpoint) {
        node.connect(endpoint);
        this.emit('node', {
          eventType: 'mod',
          object: node
        });
      }
    } else {
      node = new this.Node(name);
      node.connect(endpoint);
      this.emit('node', {
        eventType: 'new',
        object: node
      });
      this._registerNode(node);
    }
  }

  // Register node object in registry and listen to events on it.
  //
  // NOTE: This would be normally done in addNode() but for testing it's easier
  // to have a separate methods because in the tests we like to create our own
  // nodes.
  //
  // @param {object} node    Node object to register.
  _registerNode (node) {
    assert(!this.nodes[node.name]);
    this.nodes[node.name] = node;

    log.info(
      `mayastor on node "${node.name}" and endpoint "${node.endpoint}" just joined`
    );

    eventObjects.forEach((objType) => {
      node.on(objType, (ev) => this.emit(objType, ev));
    });
  }

  // Remove mayastor node from the list of nodes and unsubscribe events.
  //
  // @param {string} name   Name of the node to remove.
  removeNode (name) {
    const node = this.nodes[name];
    if (!node) return;
    delete this.nodes[name];
    node.disconnect();
    node.unbind();

    log.info(`mayastor on node "${name}" left`);
    this.emit('node', {
      eventType: 'del',
      object: node
    });

    eventObjects.forEach((objType) => {
      node.removeAllListeners(objType);
    });
  }

  // Get specified mayastor node or list of all mayastor nodes if called
  // without argument.
  //
  // @param   {string} name    Name of the node to return.
  // @returns {(object|Array)} Node object or null if not found or list of all objects.
  getNode (name) {
    if (name) {
      return this.nodes[name] || null;
    } else {
      return Object.values(this.nodes);
    }
  }

  // Get specified storage pool or list of all storage pools if called
  // without argument.
  //
  // @param   {string} [name]     Name of the storage pool.
  // @returns {(object|object[])} Pool object (null if not found) or list of all objects.
  getPool (name) {
    const pools = Object.values(this.nodes).reduce(
      (acc, node) => acc.concat(node.pools),
      []
    );
    if (name) {
      return pools.find((p) => p.name === name) || null;
    } else {
      return pools;
    }
  }

  // Get specified nexus object or list of nexus objects if called without
  // argument.
  //
  // @param   {string} [uuid]     ID of the nexus.
  // @returns {(object|object[])} Nexus object (null if not found) or list of all objects.
  getNexus (uuid) {
    const nexus = Object.values(this.nodes).reduce(
      (acc, node) => acc.concat(node.nexus),
      []
    );
    if (uuid) {
      return nexus.find((n) => n.uuid === uuid) || null;
    } else {
      return nexus;
    }
  }

  // Get replica objects with specified uuid or all replicas if called without
  // argument.
  //
  // @param   {string}    [uuid]  Replica ID.
  // @returns {object[]}  Array of matching replicas.
  getReplicaSet (uuid) {
    const replicas = Object.values(this.nodes).reduce(
      (acc, node) => acc.concat(node.getReplicas()),
      []
    );
    if (uuid) {
      return replicas.filter((r) => r.uuid === uuid);
    } else {
      return replicas;
    }
  }

  // Return total capacity of all pools summed together or capacity of pools on
  // a single node if node name is specified.
  //
  // @param {string}   [nodeName]  Name of the node to get the capacity for.
  // @returns {number} Total capacity in bytes.
  //
  getCapacity (nodeName) {
    let pools;

    if (nodeName) {
      pools = this.getPool().filter((p) => p.node.name === nodeName);
    } else {
      pools = this.getPool();
    }
    return pools
      .filter((p) => p.isAccessible())
      .reduce((acc, p) => acc + (p.capacity - p.used), 0);
  }

  // Return ordered list of storage pools suitable for new volume creation
  // sorted by preference (only a single pool from each node).
  //
  // The rules are simple:
  //  1) must be online (or degraded if there are no online pools)
  //  2) must have sufficient space
  //  3) the least busy pools first
  //
  choosePools (requiredBytes, mustNodes, shouldNodes) {
    let pools = this.getPool().filter((p) => {
      return (
        p.isAccessible() &&
        p.capacity - p.used >= requiredBytes &&
        (mustNodes.length === 0 || mustNodes.indexOf(p.node.name) >= 0)
      );
    });

    pools.sort((a, b) => {
      // Rule #1: User preference
      if (shouldNodes.length > 0) {
        if (
          shouldNodes.indexOf(a.node.name) >= 0 &&
          shouldNodes.indexOf(b.node.name) < 0
        ) {
          return -1;
        } else if (
          shouldNodes.indexOf(a.node.name) < 0 &&
          shouldNodes.indexOf(b.node.name) >= 0
        ) {
          return 1;
        }
      }

      // Rule #2: Avoid degraded pools whenever possible
      if (a.state === 'POOL_ONLINE' && b.state !== 'POOL_ONLINE') {
        return -1;
      } else if (a.state !== 'POOL_ONLINE' && b.state === 'POOL_ONLINE') {
        return 1;
      }

      // Rule #3: Use the least busy pool (with fewer replicas)
      if (a.replicas.length < b.replicas.length) {
        return -1;
      } else if (a.replicas.length > b.replicas.length) {
        return 1;
      }

      // Rule #4: Pools with more free space take precedence
      const aFree = a.capacity - a.used;
      const bFree = b.capacity - b.used;
      return bFree - aFree;
    });

    // only one pool from each node
    const nodes = [];
    pools = pools.filter((p) => {
      if (nodes.indexOf(p.node) < 0) {
        nodes.push(p.node);
        return true;
      } else {
        return false;
      }
    });

    return pools;
  }
}

module.exports = Registry;
