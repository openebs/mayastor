// This is the component of the moac that maintains the state of the storage
// nodes. It keeps track of the nodes, pools, replicas and nexus. It serves as
// a database (other components can query it to get a list of objects) and also
// as a message bus (other components can subscribe to events).

import assert from 'assert';
import events = require('events');
import { Node, NodeOpts } from './node';
import { Pool } from './pool';
import { Nexus } from './nexus';
import { Replica } from './replica';
import { PersistentStore } from './persistent_store';
import { Logger } from './logger';

const log = Logger('registry');

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

interface NodeConstructor {
  new (name: string, opts: any): Node;
}

export class Registry extends events.EventEmitter {
  private nodes: Record<string, Node>;
  private Node: NodeConstructor;
  private nodeOpts: NodeOpts;
  private persistent_store: PersistentStore;

  constructor (nodeOpts: NodeOpts, persistent_store: PersistentStore) {
    super();
    this.nodes = {}; // node objects indexed by name
    this.nodeOpts = nodeOpts;
    // This gives a chance to override Node class used for creating new
    // node objects, which is useful for testing of the registry.
    this.Node = Node;
    this.persistent_store = persistent_store;
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
  // @param name      Name of the node.
  // @param endpoint  Endpoint for gRPC communication.
  addNode (name: string, endpoint: string) {
    let node = this.nodes[name];
    if (node) {
      node.connect(endpoint);
    } else {
      node = new this.Node(name, this.nodeOpts);
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
  _registerNode (node: Node) {
    assert(!this.nodes[node.name]);
    this.nodes[node.name] = node;

    log.info(
      `mayastor on node "${node.name}" and endpoint "${node.endpoint}" just joined`
    );

    eventObjects.forEach((objType) => {
      node.on(objType, (ev) => this.emit(objType, ev));
    });
  }

  // Disconnect the node and offline it (but keep it in the list).
  //
  // @param name   Name of the node to offline.
  disconnectNode (name: string) {
    const node = this.nodes[name];
    if (!node) return;
    log.info(`mayastor on node "${name}" left`);
    node.disconnect();
  }

  // Remove mayastor node from the list of nodes and unsubscribe events.
  //
  // @param name   Name of the node to remove.
  removeNode (name: string) {
    const node = this.nodes[name];
    if (!node) return;
    delete this.nodes[name];
    node.disconnect();

    // There is a hidden problem here. Some actions that should have been
    // done on behalf of node.disconnect() above, might not have sufficient time
    // to run and after we would remove the node from list of the nodes and
    // unsubscribe event subscribers, further event propagation on this node
    // would stop. As a workaround we never remove the node unless we are
    // shutting down the moac. Users can remove a node by kubectl if they wish.
    node.unbind();
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
  // @param   name    Name of the node to return.
  // @returns Node object if found or undefined if not found.
  getNode (name: string): Node | undefined {
    return this.nodes[name];
  }

  // Get list of all mayastor nodes.
  getNodes (): Node[] {
    return Object.values(this.nodes);
  }

  // Get specified storage pool or undefined if not found.
  getPool (name: string): Pool | undefined {
    return this.getPools().find((p) => p.name === name);
  }

  // Get list of all storage pools.
  getPools (): Pool[] {
    return Object.values(this.nodes).reduce(
      (acc: Pool[], node: Node) => acc.concat(node.pools),
      []
    );
  }

  // Get specified nexus object.
  getNexus (uuid: string): Nexus | undefined {
    return this.getNexuses().find((n) => n.uuid === uuid);
  }

  // Get list of nexus objects.
  getNexuses (): Nexus[] {
    return Object.values(this.nodes).reduce(
      (acc: Nexus[], node: Node) => acc.concat(node.nexus),
      []
    );
  }

  // Get replica objects with specified uuid.
  getReplicaSet (uuid: string): Replica[] {
    return this.getReplicas().filter((r: Replica) => r.uuid === uuid);
  }

  // Get all replicas.
  getReplicas (): Replica[] {
    return Object.values(this.nodes).reduce(
      (acc: Replica[], node: Node) => acc.concat(node.getReplicas()),
      []
    );
  }

  // Return total capacity of all pools summed together or capacity of pools on
  // a single node if node name is specified.
  //
  // @param [nodeName]  Name of the node to get the capacity for.
  // @returns Total capacity in bytes.
  //
  getCapacity (nodeName?: string) {
    let pools;

    if (nodeName) {
      pools = this.getPools().filter((p) => p.node?.name === nodeName);
    } else {
      pools = this.getPools();
    }
    return pools
      .filter((p: Pool) => p.isAccessible())
      .reduce((acc: number, p: Pool) => acc + (p.capacity - p.used), 0);
  }

  // Return ordered list of storage pools suitable for new volume creation
  // sorted by preference (only a single pool from each node).
  //
  // The rules are simple:
  //  1) must be online (or degraded if there are no online pools)
  //  2) must have sufficient space
  //  3) the least busy pools first
  //
  choosePools (requiredBytes: number, mustNodes: string[], shouldNodes: string[]): Pool[] {
    let pools = this.getPools().filter((p) => {
      return (
        p.isAccessible() &&
        p.node &&
        p.capacity - p.used >= requiredBytes &&
        (mustNodes.length === 0 || mustNodes.indexOf(p.node.name) >= 0)
      );
    });

    pools.sort((a, b) => {
      // Rule #1: User preference
      if (shouldNodes.length > 0) {
        if (
          shouldNodes.indexOf(a.node!.name) >= 0 &&
          shouldNodes.indexOf(b.node!.name) < 0
        ) {
          return -1;
        } else if (
          shouldNodes.indexOf(a.node!.name) < 0 &&
          shouldNodes.indexOf(b.node!.name) >= 0
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
    const nodes: Node[] = [];
    pools = pools.filter((p) => {
      if (nodes.indexOf(p.node!) < 0) {
        nodes.push(p.node!);
        return true;
      } else {
        return false;
      }
    });

    return pools;
  }

  // Returns the persistent store which is kept within the registry
  getPersistentStore(): PersistentStore {
    return this.persistent_store;
  }
}
