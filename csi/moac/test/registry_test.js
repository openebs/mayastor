// Unit tests for the registry class.

'use strict';

/* eslint-disable no-unused-expressions */

const _ = require('lodash');
const expect = require('chai').expect;
const sinon = require('sinon');
const { Registry } = require('../dist/registry');
const { Replica } = require('../dist/replica');
const { Pool } = require('../dist/pool');
const { Nexus } = require('../dist/nexus');
const Node = require('./node_stub');

module.exports = function () {
  it('should add a node to the registry and look up the node', () => {
    const registry = new Registry({});
    registry.Node = Node;
    let nodeEvent;

    registry.once('node', (ev) => {
      nodeEvent = ev;
    });
    registry.addNode('node', '127.0.0.1:123');
    expect(nodeEvent.eventType).to.equal('new');
    expect(nodeEvent.object.name).to.equal('node');
    expect(nodeEvent.object.endpoint).to.equal('127.0.0.1:123');

    const node = registry.getNode('node');
    expect(node.name).to.equal('node');
    expect(node.endpoint).to.equal('127.0.0.1:123');

    // ensure the events from the node are relayed by the registry
    const events = ['node', 'pool', 'replica', 'nexus'];
    events.forEach((ev) => {
      registry.on(ev, () => {
        const idx = events.findIndex((ent) => ent === ev);
        expect(idx).to.not.equal(-1);
        events.splice(idx, 1);
      });
    });
    _.clone(events).forEach((ev) => node.emit(ev, {}));
    expect(events).to.be.empty;
  });

  it('should not do anything if the same node already exists in the registry', () => {
    const registry = new Registry({});
    registry.Node = Node;

    const nodeEvents = [];
    registry.on('node', (ev) => {
      nodeEvents.push(ev);
    });

    registry.addNode('node', '127.0.0.1:123');
    expect(nodeEvents).to.have.lengthOf(1);
    expect(nodeEvents[0].eventType).to.equal('new');

    registry.addNode('node', '127.0.0.1:123');
    expect(nodeEvents).to.have.lengthOf(1);
  });

  it('should reconnect node if it exists but grpc endpoint has changed', () => {
    const registry = new Registry({});
    registry.Node = Node;

    const nodeEvents = [];
    registry.on('node', (ev) => {
      nodeEvents.push(ev);
    });

    registry.addNode('node', '127.0.0.1:123');
    registry.addNode('node', '127.0.0.1:124');
    expect(nodeEvents).to.have.lengthOf(2);
    expect(nodeEvents[0].eventType).to.equal('new');
    expect(nodeEvents[1].eventType).to.equal('mod');
  });

  it('should get a list of nodes from registry', () => {
    const registry = new Registry({});
    registry.nodes.node1 = new Node('node1');
    registry.nodes.node2 = new Node('node2');
    registry.nodes.node3 = new Node('node3');
    const list = registry.getNodes();
    expect(list).to.have.lengthOf(3);
  });

  it('should remove a node from the registry', () => {
    const registry = new Registry({});
    const node = new Node('node');
    registry.nodes.node = node;
    let nodeEvent;
    registry.once('node', (ev) => {
      nodeEvent = ev;
    });
    registry.removeNode('node');
    expect(registry.nodes).to.not.have.keys('node');
    expect(nodeEvent.eventType).to.equal('del');
    expect(nodeEvent.object.name).to.equal('node');

    // ensure the events from the node are not relayed
    const events = ['node', 'pool', 'replica', 'nexus'];
    events.forEach((ev) => {
      registry.on(ev, () => {
        throw new Error('Received event after the node was removed');
      });
    });
    events.forEach((ev) => node.emit(ev, {}));
  });

  it('should not do anything if removed node does not exist', () => {
    const registry = new Registry({});
    let nodeEvent;
    registry.once('node', (ev) => {
      nodeEvent = ev;
    });
    registry.removeNode('node');
    expect(nodeEvent).to.be.undefined;
  });

  it('should get a list of pools from registry', () => {
    const registry = new Registry({});
    const node1 = new Node('node1', {}, [
      new Pool({ name: 'pool1', disks: [] })
    ]);
    const node2 = new Node('node2', {}, [
      new Pool({ name: 'pool2a', disks: [] }),
      new Pool({ name: 'pool2b', disks: [] })
    ]);
    registry.nodes.node1 = node1;
    registry.nodes.node2 = node2;

    const pools = registry.getPools();
    pools.sort();
    expect(pools).to.have.lengthOf(3);
    expect(pools[0].name).to.equal('pool1');
    expect(pools[1].name).to.equal('pool2a');
    expect(pools[2].name).to.equal('pool2b');
    const pool = registry.getPool('pool2a');
    expect(pool.name).to.equal('pool2a');
  });

  it('should get a list of nexus from registry', () => {
    const UUID1 = 'ba5e39e9-0c0e-4973-8a3a-0dccada09cb1';
    const UUID2 = 'ba5e39e9-0c0e-4973-8a3a-0dccada09cb2';
    const UUID3 = 'ba5e39e9-0c0e-4973-8a3a-0dccada09cb3';
    const registry = new Registry({});
    const node1 = new Node('node1', {}, [], [new Nexus({ uuid: UUID1 })]);
    const node2 = new Node(
      'node2',
      {},
      [],
      [new Nexus({ uuid: UUID2 }), new Nexus({ uuid: UUID3 })]
    );
    registry.nodes.node1 = node1;
    registry.nodes.node2 = node2;

    const nexuses = registry.getNexuses();
    nexuses.sort();
    expect(nexuses).to.have.lengthOf(3);
    expect(nexuses[0].uuid).to.equal(UUID1);
    expect(nexuses[1].uuid).to.equal(UUID2);
    expect(nexuses[2].uuid).to.equal(UUID3);
    const nexus = registry.getNexus(UUID2);
    expect(nexus.uuid).to.equal(UUID2);
  });

  it('should get a list of replicas from registry', () => {
    const UUID1 = 'ba5e39e9-0c0e-4973-8a3a-0dccada09cb1';
    const UUID2 = 'ba5e39e9-0c0e-4973-8a3a-0dccada09cb2';
    const UUID3 = 'ba5e39e9-0c0e-4973-8a3a-0dccada09cb3';
    const pool1 = new Pool({ name: 'pool1', disks: [] });
    const pool2a = new Pool({ name: 'pool2a', disks: [] });
    const pool2b = new Pool({ name: 'pool2b', disks: [] });
    const node1 = new Node('node1');
    node1.pools = [pool1];
    const node2 = new Node('node2');
    node2.pools = [pool2a, pool2b];
    const registry = new Registry({});
    registry.nodes.node1 = node1;
    registry.nodes.node2 = node2;
    pool1.replicas = [
      new Replica({ uuid: UUID1 }),
      new Replica({ uuid: UUID2 })
    ];
    pool2b.replicas = [new Replica({ uuid: UUID3 })];

    let replicas = registry.getReplicas();
    replicas.sort();
    expect(replicas).to.have.lengthOf(3);
    expect(replicas[0].uuid).to.equal(UUID1);
    expect(replicas[1].uuid).to.equal(UUID2);
    expect(replicas[2].uuid).to.equal(UUID3);
    replicas = registry.getReplicaSet(UUID1);
    expect(replicas).to.have.lengthOf(1);
    expect(replicas[0].uuid).to.equal(UUID1);
  });

  it('should close the registry', () => {
    const registry = new Registry({});
    const node = new Node('node');
    const connectStub = sinon.stub(node, 'connect');
    const disconnectStub = sinon.stub(node, 'disconnect');
    registry.nodes.node = node;
    registry.close();

    sinon.assert.notCalled(connectStub);
    sinon.assert.calledOnce(disconnectStub);
    expect(registry.nodes).to.not.have.keys('node');
  });

  it('should get capacity of pools on all or specified nodes', () => {
    // should count
    const pool1 = new Pool({
      name: 'pool1',
      disks: [],
      state: 'POOL_ONLINE',
      capacity: 100,
      used: 10
    });
    // should count
    const pool2a = new Pool({
      name: 'pool2a',
      disks: [],
      state: 'POOL_DEGRADED',
      capacity: 100,
      used: 25
    });
    // should not count
    const pool2b = new Pool({
      name: 'pool2b',
      disks: [],
      state: 'POOL_FAULTED',
      capacity: 100,
      used: 55
    });
    // should not count
    const pool2c = new Pool({
      name: 'pool2c',
      disks: [],
      state: 'POOL_OFFLINE',
      capacity: 100,
      used: 99
    });
    const node1 = new Node('node1');
    node1.pools = [pool1];
    pool1.bind(node1);
    const node2 = new Node('node2');
    node2.pools = [pool2a, pool2b, pool2c];
    pool2a.bind(node2);
    pool2b.bind(node2);
    pool2c.bind(node2);
    const registry = new Registry({});
    registry.nodes.node1 = node1;
    registry.nodes.node2 = node2;

    let cap = registry.getCapacity();
    expect(cap).to.equal(90 + 75);
    cap = registry.getCapacity('node2');
    expect(cap).to.equal(75);
  });

  describe('pool selection', function () {
    it('should prefer ONLINE pool', () => {
      // has more free space but is degraded
      const pool1 = new Pool({
        name: 'pool1',
        disks: [],
        state: 'POOL_DEGRADED',
        capacity: 100,
        used: 10
      });
      const pool2 = new Pool({
        name: 'pool2',
        disks: [],
        state: 'POOL_ONLINE',
        capacity: 100,
        used: 25
      });
      const pool3 = new Pool({
        name: 'pool3',
        disks: [],
        state: 'POOL_OFFLINE',
        capacity: 100,
        used: 0
      });
      const node1 = new Node('node1', {}, [pool1]);
      const node2 = new Node('node2', {}, [pool2]);
      const node3 = new Node('node3', {}, [pool3]);
      const registry = new Registry({});
      registry.nodes.node1 = node1;
      registry.nodes.node2 = node2;
      registry.nodes.node3 = node3;

      let pools = registry.choosePools(75, [], []);
      expect(pools).to.have.lengthOf(2);
      expect(pools[0].name).to.equal('pool2');
      expect(pools[0].state).to.equal('POOL_ONLINE');
      expect(pools[1].name).to.equal('pool1');
      pool1.state = 'POOL_ONLINE';
      pool2.state = 'POOL_DEGRADED';
      pools = registry.choosePools(75, [], []);
      expect(pools).to.have.lengthOf(2);
      expect(pools[0].name).to.equal('pool1');
      expect(pools[1].name).to.equal('pool2');
    });

    it('should prefer pool with fewer volumes', () => {
      const UUID1 = 'ba5e39e9-0c0e-4973-8a3a-0dccada09cb1';
      const UUID2 = 'ba5e39e9-0c0e-4973-8a3a-0dccada09cb2';
      // has more free space but has more replicas
      const pool1 = new Pool({
        name: 'pool1',
        disks: [],
        state: 'POOL_ONLINE',
        capacity: 100,
        used: 10
      });
      pool1.replicas = [
        new Replica({ uuid: UUID1 }),
        new Replica({ uuid: UUID2 })
      ];
      const pool2 = new Pool({
        name: 'pool2',
        disks: [],
        state: 'POOL_ONLINE',
        capacity: 100,
        used: 25
      });
      pool2.replicas = [new Replica({ uuid: UUID1 })];
      const node1 = new Node('node1', {}, [pool1]);
      const node2 = new Node('node2', {}, [pool2]);
      const registry = new Registry({});
      registry.nodes.node1 = node1;
      registry.nodes.node2 = node2;

      let pools = registry.choosePools(75, [], []);
      expect(pools).to.have.lengthOf(2);
      expect(pools[0].name).to.equal('pool2');
      expect(pools[1].name).to.equal('pool1');
      pool1.replicas = [];
      pools = registry.choosePools(75, [], []);
      expect(pools).to.have.lengthOf(2);
      expect(pools[0].name).to.equal('pool1');
      expect(pools[1].name).to.equal('pool2');
    });

    it('should prefer pool with more free space', () => {
      // has more free space
      const pool1 = new Pool({
        name: 'pool1',
        disks: [],
        state: 'POOL_DEGRADED',
        capacity: 100,
        used: 10
      });
      const pool2 = new Pool({
        name: 'pool2',
        disks: [],
        state: 'POOL_DEGRADED',
        capacity: 100,
        used: 20
      });
      const node1 = new Node('node1', {}, [pool1]);
      const node2 = new Node('node2', {}, [pool2]);
      const registry = new Registry({});
      registry.nodes.node1 = node1;
      registry.nodes.node2 = node2;

      let pools = registry.choosePools(75, [], []);
      expect(pools).to.have.lengthOf(2);
      expect(pools[0].name).to.equal('pool1');
      expect(pools[1].name).to.equal('pool2');
      pool1.used = 25;
      pools = registry.choosePools(75, [], []);
      expect(pools).to.have.lengthOf(2);
      expect(pools[0].name).to.equal('pool2');
      expect(pools[1].name).to.equal('pool1');
    });

    it('should not return any pool if no suitable pool was found', () => {
      // this one is corrupted
      const pool1 = new Pool({
        name: 'pool1',
        disks: [],
        state: 'POOL_FAULTED',
        capacity: 100,
        used: 10
      });
      // this one is too small
      const pool2 = new Pool({
        name: 'pool2',
        disks: [],
        state: 'POOL_ONLINE',
        capacity: 100,
        used: 26
      });
      // is not in must list
      const pool3 = new Pool({
        name: 'pool3',
        disks: [],
        state: 'POOL_ONLINE',
        capacity: 100,
        used: 10
      });
      const node1 = new Node('node1', {}, [pool1]);
      const node2 = new Node('node2', {}, [pool2]);
      const node3 = new Node('node3', {}, [pool3]);
      const registry = new Registry({});
      registry.nodes.node1 = node1;
      registry.nodes.node2 = node2;
      registry.nodes.node3 = node3;

      const pools = registry.choosePools(75, ['node1', 'node2'], []);
      expect(pools).to.have.lengthOf(0);
    });

    it('should not return two pools on the same node', () => {
      const pool1 = new Pool({
        name: 'pool1',
        disks: [],
        state: 'POOL_ONLINE',
        capacity: 100,
        used: 11
      });
      const pool2 = new Pool({
        name: 'pool2',
        disks: [],
        state: 'POOL_ONLINE',
        capacity: 100,
        used: 10
      });
      const node1 = new Node('node1', {}, [pool1, pool2]);
      const registry = new Registry({});
      registry.nodes.node1 = node1;

      const pools = registry.choosePools(75, [], []);
      expect(pools).to.have.lengthOf(1);
    });

    it('should choose a pool on node requested by user', () => {
      // this one would be normally preferred
      const pool1 = new Pool({
        name: 'pool1',
        disks: [],
        state: 'POOL_ONLINE',
        capacity: 100,
        used: 0
      });
      const pool2 = new Pool({
        name: 'pool2',
        disks: [],
        state: 'POOL_DEGRADED',
        capacity: 100,
        used: 25
      });
      const node1 = new Node('node1', {}, [pool1]);
      const node2 = new Node('node2', {}, [pool2]);
      const registry = new Registry({});
      registry.nodes.node1 = node1;
      registry.nodes.node2 = node2;

      const pools = registry.choosePools(75, ['node2'], []);
      expect(pools).to.have.lengthOf(1);
      expect(pools[0].name).to.equal('pool2');
    });

    it('should prefer pool on node preferred by user', () => {
      // this one would be normally preferred
      const pool1 = new Pool({
        name: 'pool1',
        disks: [],
        state: 'POOL_ONLINE',
        capacity: 100,
        used: 0
      });
      const pool2 = new Pool({
        name: 'pool2',
        disks: [],
        state: 'POOL_DEGRADED',
        capacity: 100,
        used: 25
      });
      const node1 = new Node('node1', {}, [pool1]);
      const node2 = new Node('node2', {}, [pool2]);
      const registry = new Registry({});
      registry.nodes.node1 = node1;
      registry.nodes.node2 = node2;

      const pools = registry.choosePools(75, [], ['node2']);
      expect(pools).to.have.lengthOf(2);
      expect(pools[0].name).to.equal('pool2');
      expect(pools[1].name).to.equal('pool1');
    });
  });
};
