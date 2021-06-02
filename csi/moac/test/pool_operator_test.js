// Unit tests for the pool operator
//
// Pool operator depends on a couple of modules:
//  * registry (real)
//  * node object (fake)
//  * pool object (fake)
//  * watcher (mocked)
//
// As you can see most of them must be fake in order to do detailed testing
// of pool operator. That makes the code more complicated and less readable.

'use strict';

/* eslint-disable no-unused-expressions */

const expect = require('chai').expect;
const sinon = require('sinon');
const sleep = require('sleep-promise');
const { KubeConfig } = require('@kubernetes/client-node');
const { Registry } = require('../dist/registry');
const { GrpcError, grpcCode } = require('../dist/grpc_client');
const { PoolOperator, PoolResource } = require('../dist/pool_operator');
const { Pool } = require('../dist/pool');
const { Replica } = require('../dist/replica');
const { mockCache } = require('./watcher_stub');
const Node = require('./node_stub');

const NAMESPACE = 'mayastor';
const EVENT_PROPAGATION_DELAY = 10;

const fakeConfig = {
  clusters: [
    {
      name: 'cluster',
      server: 'foo.company.com'
    }
  ],
  contexts: [
    {
      cluster: 'cluster',
      user: 'user'
    }
  ],
  users: [{ name: 'user' }]
};

// Create k8s pool resource object
function createK8sPoolResource (
  name,
  node,
  disks,
  finalizers,
  state,
  reason,
  capacity,
  used
) {
  const obj = {
    apiVersion: 'openebs.io/v1alpha1',
    kind: 'MayastorPool',
    metadata: {
      creationTimestamp: '2019-02-15T18:23:53Z',
      generation: 1,
      name: name,
      namespace: NAMESPACE,
      finalizers: finalizers,
      resourceVersion: '627981',
      selfLink: `/apis/openebs.io/v1alpha1/namespaces/${NAMESPACE}/mayastorpools/${name}`,
      uid: 'd99f06a9-314e-11e9-b086-589cfc0d76a7'
    },
    spec: {
      node: node,
      disks: disks
    }
  };
  if (state) {
    const status = { state };
    status.disks = disks.map((d) => `aio://${d}`);
    if (reason != null) status.reason = reason;
    if (capacity != null) status.capacity = capacity;
    if (used != null) status.used = used;
    if (state != null) {
      status.spec = {
        node: node,
        disks: disks
      };
    }
    obj.status = status;
  }
  return obj;
}

function createPoolResource (
  name,
  node,
  disks,
  finalizers,
  state,
  reason,
  capacity,
  used,
  statusSpec
) {
  return new PoolResource(createK8sPoolResource(
    name,
    node,
    disks,
    finalizers,
    state,
    reason,
    capacity,
    used,
    statusSpec
  ));
}

// Create a pool operator object suitable for testing - with mocked watcher etc.
function createPoolOperator (nodes) {
  const registry = new Registry({});
  registry.Node = Node;
  nodes = nodes || [];
  nodes.forEach((n) => (registry.nodes[n.name] = n));
  const kc = new KubeConfig();
  Object.assign(kc, fakeConfig);
  return new PoolOperator(NAMESPACE, kc, registry);
}

module.exports = function () {
  describe('PoolResource constructor', () => {
    it('should create valid mayastor pool with status', () => {
      const obj = createPoolResource(
        'pool',
        'node',
        ['/dev/sdc', '/dev/sdb'],
        ['some.finalizer.com'],
        'offline',
        'The node is down'
      );
      expect(obj.metadata.name).to.equal('pool');
      expect(obj.spec.node).to.equal('node');
      // the filter should sort the disks
      expect(JSON.stringify(obj.spec.disks)).to.equal(
        JSON.stringify(['/dev/sdb', '/dev/sdc'])
      );
      expect(obj.status.state).to.equal('offline');
      expect(obj.status.reason).to.equal('The node is down');
      expect(obj.status.disks).to.deep.equal(['aio:///dev/sdc', 'aio:///dev/sdb']);
      expect(obj.status.capacity).to.be.undefined;
      expect(obj.status.used).to.be.undefined;
    });

    it('should create valid mayastor pool without status', () => {
      const obj = createPoolResource('pool', 'node', ['/dev/sdc', '/dev/sdb']);
      expect(obj.metadata.name).to.equal('pool');
      expect(obj.spec.node).to.equal('node');
      expect(obj.status.state).to.equal('unknown');
    });

    it('should not create mayastor pool without node specification', () => {
      expect(() => createPoolResource(
        'pool',
        undefined,
        ['/dev/sdc', '/dev/sdb']
      )).to.throw();
    });
  });

  describe('init method', () => {
    let kc, oper, fakeApiStub;

    beforeEach(() => {
      const registry = new Registry({});
      kc = new KubeConfig();
      Object.assign(kc, fakeConfig);
      oper = new PoolOperator(NAMESPACE, kc, registry);
      const makeApiStub = sinon.stub(kc, 'makeApiClient');
      const fakeApi = {
        createCustomResourceDefinition: () => null
      };
      fakeApiStub = sinon.stub(fakeApi, 'createCustomResourceDefinition');
      makeApiStub.returns(fakeApi);
    });

    afterEach(() => {
      if (oper) {
        oper.stop();
        oper = undefined;
      }
    });

    it('should create CRD if it does not exist', async () => {
      fakeApiStub.resolves();
      await oper.init(kc);
    });

    it('should ignore error if CRD already exists', async () => {
      fakeApiStub.rejects({
        statusCode: 409
      });
      await oper.init(kc);
    });

    it('should throw if CRD creation fails', async () => {
      fakeApiStub.rejects({
        statusCode: 404
      });
      try {
        await oper.init(kc);
      } catch (err) {
        return;
      }
      throw new Error('Init did not fail');
    });
  });

  describe('watcher events', () => {
    let oper; // pool operator

    afterEach(() => {
      if (oper) {
        oper.stop();
        oper = null;
      }
    });

    describe('new event', () => {
      it('should process resources that existed before the operator was started', async () => {
        let stubs;
        oper = createPoolOperator([]);
        const poolResource1 = createPoolResource('pool', 'node', ['/dev/sdb']);
        const poolResource2 = createPoolResource('pool', 'node', ['/dev/sdb']);
        poolResource2.status.spec = { node: 'node', disks: ['/dev/sdb'] };
        mockCache(oper.watcher, (arg) => {
          stubs = arg;
          stubs.get.onCall(0).returns(poolResource1);
          stubs.get.onCall(1).returns(poolResource2);
          stubs.list.onCall(0).returns([poolResource1]);
          stubs.list.onCall(1).returns([poolResource2]);
        });
        await oper.start();
        // give time to registry to install its callbacks
        await sleep(EVENT_PROPAGATION_DELAY);

        sinon.assert.notCalled(stubs.create);
        sinon.assert.notCalled(stubs.delete);
        sinon.assert.notCalled(stubs.update);
        // twice because we update the status to match the spec
        sinon.assert.calledTwice(stubs.updateStatus);
        expect(stubs.updateStatus.args[1][5].metadata.name).to.equal('pool');
        expect(stubs.updateStatus.args[1][5].status).to.deep.equal({
          state: 'pending',
          reason: 'mayastor does not run on node "node"',
          disks: undefined,
          spec: { node: 'node', disks: ['/dev/sdb'] }
        });
      });

      it('should set "state" to PENDING when creating a pool', async () => {
        let stubs;
        const node = new Node('node');
        const createPoolStub = sinon.stub(node, 'createPool');
        createPoolStub.resolves(
          new Pool({
            name: 'pool',
            node: node,
            disks: ['aio:///dev/sdb'],
            state: 'POOL_DEGRADED',
            capacity: 100,
            used: 10
          })
        );
        oper = createPoolOperator([node]);
        const poolResource = createPoolResource('pool', 'node', ['/dev/sdb']);
        mockCache(oper.watcher, (arg) => {
          stubs = arg;
          stubs.get.returns(poolResource);
        });
        await oper.start();
        // give time to registry to install its callbacks
        await sleep(EVENT_PROPAGATION_DELAY);
        // trigger "new" event
        oper.watcher.emit('new', poolResource);
        // give event callbacks time to propagate
        await sleep(EVENT_PROPAGATION_DELAY);

        sinon.assert.calledOnce(createPoolStub);
        sinon.assert.calledWith(createPoolStub, 'pool', ['/dev/sdb']);
        sinon.assert.notCalled(stubs.create);
        sinon.assert.notCalled(stubs.delete);
        sinon.assert.notCalled(stubs.update);
        sinon.assert.calledOnce(stubs.updateStatus);
        expect(stubs.updateStatus.args[0][5].metadata.name).to.equal('pool');
        expect(stubs.updateStatus.args[0][5].status).to.deep.equal({
          state: 'pending',
          reason: 'Creating the pool',
          disks: undefined,
          spec: { node: 'node', disks: ['/dev/sdb'] }
        });
      });

      it('should not try to create a pool if the node has not been synced', async () => {
        let stubs;
        const node = new Node('node');
        sinon.stub(node, 'isSynced').returns(false);
        const createPoolStub = sinon.stub(node, 'createPool');
        createPoolStub.resolves(
          new Pool({
            name: 'pool',
            node: node,
            disks: ['aio:///dev/sdb'],
            state: 'POOL_DEGRADED',
            capacity: 100,
            used: 10
          })
        );
        oper = createPoolOperator([node]);
        const poolResource = createPoolResource('pool', 'node', ['/dev/sdb']);
        mockCache(oper.watcher, (arg) => {
          stubs = arg;
          stubs.get.returns(poolResource);
        });
        await oper.start();
        // give time to registry to install its callbacks
        await sleep(EVENT_PROPAGATION_DELAY);
        // trigger "new" event
        oper.watcher.emit('new', poolResource);
        // give event callbacks time to propagate
        await sleep(EVENT_PROPAGATION_DELAY);

        sinon.assert.notCalled(createPoolStub);
        sinon.assert.notCalled(stubs.create);
        sinon.assert.notCalled(stubs.delete);
        sinon.assert.notCalled(stubs.update);
        sinon.assert.calledOnce(stubs.updateStatus);
      });

      it('should not try to create a pool when pool with the same name already exists', async () => {
        let stubs;
        const node = new Node('node', {}, []);
        const pool = new Pool({
          name: 'pool',
          disks: ['aio:///dev/sdb'],
          state: 'POOL_DEGRADED',
          capacity: 100,
          used: 10
        });
        const createPoolStub = sinon.stub(node, 'createPool');
        createPoolStub.resolves(pool);

        oper = createPoolOperator([node]);
        const poolResource = createPoolResource('pool', 'node', ['/dev/sdb', '/dev/sdc']);
        mockCache(oper.watcher, (arg) => {
          stubs = arg;
          stubs.get.returns(poolResource);
        });
        await oper.start();
        // give time to registry to install its callbacks
        await sleep(EVENT_PROPAGATION_DELAY);
        // this creates the inconsistency between real and k8s state which we are testing
        node.pools.push(pool);
        // trigger "new" event
        oper.watcher.emit('new', poolResource);
        // give event callbacks time to propagate
        await sleep(EVENT_PROPAGATION_DELAY);

        sinon.assert.notCalled(createPoolStub);
        sinon.assert.notCalled(stubs.create);
        sinon.assert.notCalled(stubs.delete);
        sinon.assert.notCalled(stubs.update);
        sinon.assert.calledOnce(stubs.updateStatus);
        expect(stubs.updateStatus.args[0][5].status).to.deep.equal({
          state: 'degraded',
          reason: '',
          disks: ['aio:///dev/sdb'],
          capacity: 100,
          used: 10,
          spec: { node: 'node', disks: ['/dev/sdb', '/dev/sdc'] }
        });
      });

      // important test as moving the pool between nodes would destroy data
      it('should leave the pool untouched when pool exists and is on a different node', async () => {
        let stubs;
        const node1 = new Node('node1', {}, []);
        const node2 = new Node('node2');
        const pool = new Pool({
          name: 'pool',
          disks: ['aio:///dev/sdb'],
          state: 'POOL_DEGRADED',
          capacity: 100,
          used: 10
        });
        const createPoolStub1 = sinon.stub(node1, 'createPool');
        const createPoolStub2 = sinon.stub(node2, 'createPool');
        createPoolStub1.resolves(pool);
        createPoolStub2.resolves(pool);

        oper = createPoolOperator([node1, node2]);
        const poolResource = createPoolResource('pool', 'node2', ['/dev/sdb', '/dev/sdc']);
        mockCache(oper.watcher, (arg) => {
          stubs = arg;
          stubs.get.returns(poolResource);
        });
        await oper.start();
        // give time to registry to install its callbacks
        await sleep(EVENT_PROPAGATION_DELAY);
        // we assign the pool to node1 but later in the event it will be on node2
        node1.pools.push(pool);
        // trigger "new" event
        oper.watcher.emit('new', poolResource);
        // give event callbacks time to propagate
        await sleep(EVENT_PROPAGATION_DELAY);

        sinon.assert.notCalled(createPoolStub1);
        sinon.assert.notCalled(createPoolStub2);
        sinon.assert.notCalled(stubs.create);
        sinon.assert.notCalled(stubs.delete);
        sinon.assert.notCalled(stubs.update);
        sinon.assert.calledOnce(stubs.updateStatus);
        expect(stubs.updateStatus.args[0][5].status).to.deep.equal({
          state: 'degraded',
          reason: '',
          disks: ['aio:///dev/sdb'],
          capacity: 100,
          used: 10,
          spec: { node: 'node2', disks: ['/dev/sdb', '/dev/sdc'] }
        });
      });

      it('should set "reason" to error message when create pool fails', async () => {
        let stubs;
        const node = new Node('node');
        const createPoolStub = sinon.stub(node, 'createPool');
        createPoolStub.rejects(
          new GrpcError(grpcCode.INTERNAL, 'create failed')
        );
        oper = createPoolOperator([node]);
        const poolResource = createPoolResource('pool', 'node', ['/dev/sdb']);
        mockCache(oper.watcher, (arg) => {
          stubs = arg;
          stubs.get.returns(poolResource);
        });
        await oper.start();
        // give time to registry to install its callbacks
        await sleep(EVENT_PROPAGATION_DELAY);
        // trigger "new" event
        oper.watcher.emit('new', poolResource);
        // give event callbacks time to propagate
        await sleep(EVENT_PROPAGATION_DELAY);

        sinon.assert.calledOnce(createPoolStub);
        sinon.assert.calledWith(createPoolStub, 'pool', ['/dev/sdb']);
        sinon.assert.notCalled(stubs.create);
        sinon.assert.notCalled(stubs.delete);
        sinon.assert.notCalled(stubs.update);
        sinon.assert.calledTwice(stubs.updateStatus);
        expect(stubs.updateStatus.args[0][5].status).to.deep.equal({
          state: 'pending',
          reason: 'Creating the pool',
          disks: undefined,
          spec: { node: 'node', disks: ['/dev/sdb'] }
        });
        expect(stubs.updateStatus.args[1][5].status).to.deep.equal({
          state: 'error',
          reason: 'Error: create failed',
          disks: undefined,
          spec: { node: 'node', disks: ['/dev/sdb'] }
        });
      });

      it('should ignore failure to update the resource state', async () => {
        let stubs;
        const node = new Node('node');
        const createPoolStub = sinon.stub(node, 'createPool');
        createPoolStub.rejects(
          new GrpcError(grpcCode.INTERNAL, 'create failed')
        );
        oper = createPoolOperator([node]);
        const poolResource = createPoolResource('pool', 'node', ['/dev/sdb']);
        mockCache(oper.watcher, (arg) => {
          stubs = arg;
          stubs.get.returns(poolResource);
          stubs.updateStatus.resolves(new Error('http put error'));
        });
        await oper.start();
        // give time to registry to install its callbacks
        await sleep(EVENT_PROPAGATION_DELAY);
        // trigger "new" event
        oper.watcher.emit('new', poolResource);
        // give event callbacks time to propagate
        await sleep(EVENT_PROPAGATION_DELAY);

        sinon.assert.calledOnce(createPoolStub);
        sinon.assert.calledWith(createPoolStub, 'pool', ['/dev/sdb']);
        sinon.assert.notCalled(stubs.create);
        sinon.assert.notCalled(stubs.delete);
        sinon.assert.notCalled(stubs.update);
        sinon.assert.calledTwice(stubs.updateStatus);
      });

      it('should not create a pool if node does not exist', async () => {
        let stubs;
        oper = createPoolOperator([]);
        const poolResource = createPoolResource('pool', 'node', ['/dev/sdb']);
        mockCache(oper.watcher, (arg) => {
          stubs = arg;
          stubs.get.returns(poolResource);
        });
        await oper.start();
        // give time to registry to install its callbacks
        await sleep(EVENT_PROPAGATION_DELAY);
        // trigger "new" event
        oper.watcher.emit('new', poolResource);
        // give event callbacks time to propagate
        await sleep(EVENT_PROPAGATION_DELAY);

        sinon.assert.notCalled(stubs.create);
        sinon.assert.notCalled(stubs.delete);
        sinon.assert.notCalled(stubs.update);
        sinon.assert.calledOnce(stubs.updateStatus);
        expect(stubs.updateStatus.args[0][5].status).to.deep.equal({
          state: 'pending',
          reason: 'mayastor does not run on node "node"',
          disks: undefined,
          spec: { node: 'node', disks: ['/dev/sdb'] }
        });
      });

      it('should create a pool once the node arrives and is synced', async () => {
        let stubs;
        oper = createPoolOperator([]);
        const poolResource = createPoolResource('pool', 'node', ['/dev/sdb']);
        const poolResource2 = createPoolResource('pool', 'node', ['/dev/sdb']);
        poolResource2.status.spec = { node: 'node', disks: ['/dev/sdb'] };
        mockCache(oper.watcher, (arg) => {
          stubs = arg;
          stubs.get.returns(poolResource);
          stubs.list.returns([poolResource]);
        });
        await oper.start();
        // give time to registry to install its callbacks
        await sleep(EVENT_PROPAGATION_DELAY);

        sinon.assert.notCalled(stubs.create);
        sinon.assert.notCalled(stubs.delete);
        sinon.assert.notCalled(stubs.update);
        sinon.assert.calledTwice(stubs.updateStatus);
        expect(stubs.updateStatus.args[1][5].status).to.deep.equal({
          state: 'pending',
          reason: 'mayastor does not run on node "node"',
          disks: undefined,
          spec: undefined
        });

        const node = new Node('node');
        const syncedStub = sinon.stub(node, 'isSynced');
        syncedStub.returns(false);
        oper.registry._registerNode(node);
        oper.registry.emit('node', {
          eventType: 'mod',
          object: node
        });
        // give event callbacks time to propagate
        await sleep(EVENT_PROPAGATION_DELAY);

        // node is not yet synced
        sinon.assert.callCount(stubs.updateStatus, 4);
        expect(stubs.updateStatus.args[1][5].status).to.deep.equal({
          state: 'pending',
          reason: 'mayastor does not run on node "node"',
          disks: undefined,
          spec: undefined
        });
        expect(stubs.updateStatus.args[3][5].status).to.deep.equal({
          state: 'pending',
          reason: 'mayastor on node "node" is offline',
          disks: undefined,
          spec: undefined
        });

        syncedStub.returns(true);
        oper.registry.emit('node', {
          eventType: 'mod',
          object: node
        });
        // give event callbacks time to propagate
        await sleep(EVENT_PROPAGATION_DELAY);

        // tried to create the pool but the node is a fake
        sinon.assert.callCount(stubs.updateStatus, 7);
        expect(stubs.updateStatus.args[5][5].status).to.deep.equal({
          state: 'pending',
          reason: 'Creating the pool',
          disks: undefined,
          spec: undefined
        });
        expect(stubs.updateStatus.args[6][5].status).to.deep.equal({
          state: 'error',
          reason: 'Error: Broken connection to mayastor on node "node"',
          disks: undefined,
          spec: undefined
        });
      });
    });

    describe('del event', () => {
      it('should destroy a pool', async () => {
        let stubs;
        const pool = new Pool({
          name: 'pool',
          disks: ['aio:///dev/sdb'],
          state: 'POOL_DEGRADED',
          capacity: 100,
          used: 10
        });
        const destroyStub = sinon.stub(pool, 'destroy');
        destroyStub.resolves();
        const node = new Node('node', {}, [pool]);
        oper = createPoolOperator([node]);
        const poolResource = createPoolResource(
          'pool',
          'node',
          ['/dev/sdb'],
          [],
          'degraded',
          '',
          100,
          10,
          { disks: ['/dev/sdb'], node: 'node' }
        );
        mockCache(oper.watcher, (arg) => {
          stubs = arg;
          stubs.get.returns(poolResource);
        });
        await oper.start();
        // give time to registry to install its callbacks
        await sleep(EVENT_PROPAGATION_DELAY);
        // trigger "del" event
        oper.watcher.emit('del', poolResource);
        // give event callbacks time to propagate
        await sleep(EVENT_PROPAGATION_DELAY);

        // called in response to registry new event
        sinon.assert.notCalled(stubs.updateStatus);
        sinon.assert.calledOnce(destroyStub);
        sinon.assert.notCalled(stubs.create);
        sinon.assert.notCalled(stubs.update);
        sinon.assert.notCalled(stubs.delete);
      });

      it('should not fail if pool does not exist', async () => {
        let stubs;
        const pool = new Pool({
          name: 'pool',
          disks: ['aio:///dev/sdb'],
          state: 'POOL_DEGRADED',
          capacity: 100,
          used: 10
        });
        const destroyStub = sinon.stub(pool, 'destroy');
        destroyStub.resolves();
        const node = new Node('node', {}, [pool]);
        oper = createPoolOperator([node]);
        const poolResource = createPoolResource(
          'pool',
          'node',
          ['/dev/sdb'],
          [],
          'offline',
          ''
        );
        mockCache(oper.watcher, (arg) => {
          stubs = arg;
          stubs.get.returns(poolResource);
        });
        await oper.start();
        // give time to registry to install its callbacks
        await sleep(EVENT_PROPAGATION_DELAY);
        // we create the inconsistency between k8s and real state
        node.pools = [];
        // trigger "del" event
        oper.watcher.emit('del', poolResource);
        // give event callbacks time to propagate
        await sleep(EVENT_PROPAGATION_DELAY);

        // called in response to registry new event
        sinon.assert.calledOnce(stubs.updateStatus);
        sinon.assert.notCalled(destroyStub);
        sinon.assert.notCalled(stubs.create);
        sinon.assert.notCalled(stubs.update);
        sinon.assert.notCalled(stubs.delete);
      });

      it('should destroy the pool even if it is on a different node', async () => {
        let stubs;
        const pool = new Pool({
          name: 'pool',
          disks: ['aio:///dev/sdb'],
          state: 'POOL_DEGRADED',
          capacity: 100,
          used: 10
        });
        const destroyStub = sinon.stub(pool, 'destroy');
        destroyStub.resolves();
        const node1 = new Node('node1', {}, []);
        const node2 = new Node('node2', {}, [pool]);
        oper = createPoolOperator([node1, node2]);
        const poolResource = createPoolResource(
          'pool',
          'node1',
          ['/dev/sdb'],
          [],
          'degraded',
          '',
          100,
          10
        );
        mockCache(oper.watcher, (arg) => {
          stubs = arg;
          stubs.get.returns(poolResource);
        });
        await oper.start();
        // give time to registry to install its callbacks
        await sleep(EVENT_PROPAGATION_DELAY);
        // trigger "del" event
        oper.watcher.emit('del', poolResource);
        // give event callbacks time to propagate
        await sleep(EVENT_PROPAGATION_DELAY);

        // called in response to registry new event
        sinon.assert.notCalled(stubs.updateStatus);
        sinon.assert.calledOnce(destroyStub);
        sinon.assert.notCalled(stubs.create);
        sinon.assert.notCalled(stubs.update);
        sinon.assert.notCalled(stubs.delete);
      });

      it('should not crash if the destroy fails', async () => {
        let stubs;
        const pool = new Pool({
          name: 'pool',
          disks: ['aio:///dev/sdb'],
          state: 'POOL_DEGRADED',
          capacity: 100,
          used: 10
        });
        const destroyStub = sinon.stub(pool, 'destroy');
        destroyStub.rejects(new GrpcError(grpcCode.INTERNAL, 'destroy failed'));
        const node = new Node('node', {}, [pool]);
        oper = createPoolOperator([node]);
        const poolResource = createPoolResource(
          'pool',
          'node',
          ['/dev/sdb'],
          [],
          'degraded',
          '',
          100,
          10
        );
        mockCache(oper.watcher, (arg) => {
          stubs = arg;
          stubs.get.returns(poolResource);
        });
        await oper.start();
        // give time to registry to install its callbacks
        await sleep(EVENT_PROPAGATION_DELAY);
        // trigger "del" event
        oper.watcher.emit('del', poolResource);
        // give event callbacks time to propagate
        await sleep(EVENT_PROPAGATION_DELAY);

        // called in response to registry new event
        sinon.assert.notCalled(stubs.updateStatus);
        sinon.assert.calledOnce(destroyStub);
        sinon.assert.notCalled(stubs.create);
        sinon.assert.notCalled(stubs.update);
        sinon.assert.notCalled(stubs.delete);
      });
    });

    describe('mod event', () => {
      it('should not do anything if pool object has not changed', async () => {
        let stubs;
        const pool = new Pool({
          name: 'pool',
          disks: ['aio:///dev/sdb', 'aio:///dev/sdc'],
          state: 'POOL_DEGRADED',
          capacity: 100,
          used: 10
        });
        const node = new Node('node', {}, [pool]);
        oper = createPoolOperator([node]);
        const poolResource = createPoolResource(
          'pool',
          'node',
          ['/dev/sdb', '/dev/sdc'],
          [],
          'degraded',
          ''
        );
        mockCache(oper.watcher, (arg) => {
          stubs = arg;
          stubs.get.returns(poolResource);
        });
        await oper.start();
        // give time to registry to install its callbacks
        await sleep(EVENT_PROPAGATION_DELAY);
        // trigger "mod" event
        oper.watcher.emit('mod', poolResource);
        // give event callbacks time to propagate
        await sleep(EVENT_PROPAGATION_DELAY);

        // called in response to registry new event
        sinon.assert.calledOnce(stubs.updateStatus);
        sinon.assert.notCalled(stubs.create);
        sinon.assert.notCalled(stubs.update);
        sinon.assert.notCalled(stubs.delete);
      });

      it('should not do anything if disks change', async () => {
        let stubs;
        const pool = new Pool({
          name: 'pool',
          disks: ['aio:///dev/sdb'],
          state: 'POOL_DEGRADED',
          capacity: 100,
          used: 10
        });
        const node = new Node('node', {}, [pool]);
        oper = createPoolOperator([node]);
        const poolResource = createPoolResource(
          'pool',
          'node',
          ['/dev/sdc'],
          [],
          'degraded',
          ''
        );
        mockCache(oper.watcher, (arg) => {
          stubs = arg;
          stubs.get.returns(poolResource);
        });
        await oper.start();
        // give time to registry to install its callbacks
        await sleep(EVENT_PROPAGATION_DELAY);
        // trigger "mod" event
        oper.watcher.emit('mod', poolResource);
        // give event callbacks time to propagate
        await sleep(EVENT_PROPAGATION_DELAY);

        // called in response to registry new event
        sinon.assert.calledOnce(stubs.updateStatus);
        sinon.assert.notCalled(stubs.create);
        sinon.assert.notCalled(stubs.update);
        sinon.assert.notCalled(stubs.delete);
        // the real state
        expect(node.pools[0].disks[0]).to.equal('aio:///dev/sdb');
      });

      it('should not do anything if node changes', async () => {
        let stubs;
        const pool = new Pool({
          name: 'pool',
          disks: ['aio:///dev/sdb'],
          state: 'POOL_DEGRADED',
          capacity: 100,
          used: 10
        });
        const node1 = new Node('node1', {}, [pool]);
        const node2 = new Node('node2', {}, []);
        oper = createPoolOperator([node1, node2]);
        const poolResource = createPoolResource(
          'pool',
          'node2',
          ['/dev/sdb'],
          [],
          'degraded',
          ''
        );
        mockCache(oper.watcher, (arg) => {
          stubs = arg;
          stubs.get.returns(poolResource);
        });
        await oper.start();
        // give time to registry to install its callbacks
        await sleep(EVENT_PROPAGATION_DELAY);
        // trigger "mod" event
        oper.watcher.emit('mod', poolResource);
        // give event callbacks time to propagate
        await sleep(EVENT_PROPAGATION_DELAY);

        // called in response to registry new event
        sinon.assert.calledOnce(stubs.updateStatus);
        sinon.assert.notCalled(stubs.create);
        sinon.assert.notCalled(stubs.update);
        sinon.assert.notCalled(stubs.delete);
      });
    });
  });

  describe('node events', () => {
    let oper; // pool operator

    afterEach(async () => {
      if (oper) {
        await oper.stop();
        oper = null;
      }
    });

    it('should create pool upon node sync event if it does not exist', async () => {
      let stubs;
      const pool = new Pool({
        name: 'pool',
        disks: ['aio:///dev/sdb'],
        state: 'POOL_DEGRADED',
        capacity: 100,
        used: 10
      });
      const node = new Node('node', {}, []);
      const createPoolStub = sinon.stub(node, 'createPool');
      const isSyncedStub = sinon.stub(node, 'isSynced');
      createPoolStub.resolves(pool);
      isSyncedStub.onCall(0).returns(false);
      isSyncedStub.onCall(1).returns(true);
      oper = createPoolOperator([node]);
      const poolResource1 = createPoolResource(
        'pool',
        'node',
        ['/dev/sdb'],
        [],
        'degraded',
        ''
      );
      const poolResource2 = createPoolResource(
        'pool',
        'node',
        ['/dev/sdb'],
        [],
        'pending',
        'mayastor on node "node" is offline'
      );
      mockCache(oper.watcher, (arg) => {
        stubs = arg;
        stubs.get.onCall(0).returns(poolResource1);
        stubs.get.onCall(1).returns(poolResource2);
        stubs.list.returns([poolResource1]);
      });
      await oper.start();
      // give time to registry to install its callbacks
      await sleep(EVENT_PROPAGATION_DELAY);
      oper.registry.emit('node', {
        eventType: 'sync',
        object: node
      });
      await sleep(EVENT_PROPAGATION_DELAY);

      sinon.assert.notCalled(stubs.create);
      sinon.assert.notCalled(stubs.delete);
      sinon.assert.notCalled(stubs.update);
      sinon.assert.calledTwice(stubs.updateStatus);
      expect(stubs.updateStatus.args[0][5].status).to.deep.equal({
        state: 'pending',
        reason: 'mayastor on node "node" is offline',
        disks: ['aio:///dev/sdb'],
        spec: { node: 'node', disks: ['/dev/sdb'] }
      });
      expect(stubs.updateStatus.args[1][5].status).to.deep.equal({
        state: 'pending',
        reason: 'Creating the pool',
        disks: ['aio:///dev/sdb'],
        spec: { node: 'node', disks: ['/dev/sdb'] }
      });
      sinon.assert.calledOnce(createPoolStub);
      sinon.assert.calledWith(createPoolStub, 'pool', ['aio:///dev/sdb']);
    });

    it('should add finalizer for new pool resource', async () => {
      let stubs;
      const pool = new Pool({
        name: 'pool',
        disks: ['aio:///dev/sdb'],
        state: 'POOL_ONLINE',
        capacity: 100,
        used: 4
      });
      // replica will trigger finalizer
      const replica1 = new Replica({ uuid: 'UUID1' });
      const replica2 = new Replica({ uuid: 'UUID2' });
      replica1.pool = pool;
      pool.replicas = [replica1];
      const node = new Node('node', {}, [pool]);
      oper = createPoolOperator([node]);

      const poolResource = createK8sPoolResource(
        'pool',
        'node1',
        ['/dev/sdb'],
        [],
        'online',
        '',
        100,
        4
      );
      mockCache(oper.watcher, (arg) => {
        stubs = arg;
        stubs.get.returns(poolResource);
        stubs.update.resolves();
      });
      await oper.start();
      // give time to registry to install its callbacks
      await sleep(EVENT_PROPAGATION_DELAY);

      sinon.assert.calledOnce(stubs.update);
      expect(stubs.update.args[0][5].metadata.finalizers).to.deep.equal([
        'finalizer.mayastor.openebs.io'
      ]);

      // add a second replica - should not change anything
      pool.replicas.push(replica2);
      oper.registry.emit('replica', {
        eventType: 'new',
        object: replica2
      });
      await sleep(EVENT_PROPAGATION_DELAY);

      sinon.assert.calledOnce(stubs.update);
      sinon.assert.notCalled(stubs.create);
      sinon.assert.notCalled(stubs.delete);
      sinon.assert.notCalled(stubs.updateStatus);
    });

    it('should remove finalizer when last replica is removed', async () => {
      let stubs;
      const pool = new Pool({
        name: 'pool',
        disks: ['aio:///dev/sdb'],
        state: 'POOL_ONLINE',
        capacity: 100,
        used: 4
      });
      const replica1 = new Replica({ uuid: 'UUID1' });
      const replica2 = new Replica({ uuid: 'UUID2' });
      pool.replicas = [replica1, replica2];
      replica1.pool = pool;
      replica2.pool = pool;
      const node = new Node('node', {}, [pool]);
      oper = createPoolOperator([node]);

      const poolResource = createK8sPoolResource(
        'pool',
        'node1',
        ['/dev/sdb'],
        ['finalizer.mayastor.openebs.io'],
        'online',
        '',
        100,
        4
      );
      mockCache(oper.watcher, (arg) => {
        stubs = arg;
        stubs.get.returns(poolResource);
        stubs.update.resolves();
      });
      await oper.start();
      // give time to registry to install its callbacks
      await sleep(EVENT_PROPAGATION_DELAY);

      sinon.assert.notCalled(stubs.update);
      pool.replicas.splice(1, 1);
      oper.registry.emit('replica', {
        eventType: 'del',
        object: replica2
      });
      await sleep(EVENT_PROPAGATION_DELAY);
      sinon.assert.notCalled(stubs.update);
      pool.replicas = [];
      oper.registry.emit('replica', {
        eventType: 'del',
        object: replica1
      });
      await sleep(EVENT_PROPAGATION_DELAY);
      sinon.assert.calledOnce(stubs.update);
      expect(stubs.update.args[0][5].metadata.finalizers).to.have.lengthOf(0);
      sinon.assert.notCalled(stubs.create);
      sinon.assert.notCalled(stubs.delete);
      sinon.assert.notCalled(stubs.updateStatus);
    });

    it('should not create pool upon node sync event if it exists', async () => {
      let stubs;
      const pool = new Pool({
        name: 'pool',
        disks: ['aio:///dev/sdb'],
        state: 'POOL_DEGRADED',
        capacity: 100,
        used: 10
      });
      const node = new Node('node', {}, [pool]);
      const createPoolStub = sinon.stub(node, 'createPool');
      createPoolStub.resolves(pool);
      oper = createPoolOperator([node]);
      const poolResource = createPoolResource(
        'pool',
        'node',
        ['/dev/sdb'],
        [],
        'degraded',
        '',
        100,
        10
      );
      mockCache(oper.watcher, (arg) => {
        stubs = arg;
        stubs.get.returns(poolResource);
        stubs.list.returns([poolResource]);
      });
      await oper.start();
      // give time to registry to install its callbacks
      await sleep(EVENT_PROPAGATION_DELAY);

      sinon.assert.notCalled(stubs.create);
      sinon.assert.notCalled(stubs.update);
      sinon.assert.notCalled(stubs.delete);
      sinon.assert.notCalled(stubs.updateStatus);
      sinon.assert.notCalled(createPoolStub);
    });

    it('should not create pool upon node sync event if it exists on another node', async () => {
      let stubs;
      const pool = new Pool({
        name: 'pool',
        disks: ['aio:///dev/sdb'],
        state: 'POOL_DEGRADED',
        capacity: 100,
        used: 10
      });
      const node1 = new Node('node1', {}, []);
      const node2 = new Node('node2', {}, [pool]);
      const createPoolStub1 = sinon.stub(node1, 'createPool');
      const createPoolStub2 = sinon.stub(node2, 'createPool');
      createPoolStub1.resolves(pool);
      createPoolStub2.resolves(pool);
      oper = createPoolOperator([node1, node2]);
      const poolResource = createPoolResource(
        'pool',
        'node1',
        ['/dev/sdb'],
        [],
        'degraded',
        '',
        100,
        10
      );
      mockCache(oper.watcher, (arg) => {
        stubs = arg;
        stubs.get.returns(poolResource);
        stubs.list.returns([poolResource]);
      });
      await oper.start();
      // give time to registry to install its callbacks
      await sleep(EVENT_PROPAGATION_DELAY);

      sinon.assert.notCalled(stubs.create);
      sinon.assert.notCalled(stubs.update);
      sinon.assert.notCalled(stubs.delete);
      sinon.assert.notCalled(stubs.updateStatus);
      sinon.assert.notCalled(createPoolStub1);
      sinon.assert.notCalled(createPoolStub2);
    });

    it('should create pool with original spec if the resource spec changes', async () => {
      let stubs;
      const node = new Node('node');
      const createPoolStub = sinon.stub(node, 'createPool');
      createPoolStub.rejects(
        new GrpcError(grpcCode.INTERNAL, 'create failed')
      );
      const nodeNew = new Node('node_new');
      const createPoolStubNew = sinon.stub(nodeNew, 'createPool');
      createPoolStubNew.rejects(
        new GrpcError(grpcCode.INTERNAL, 'create failed')
      );

      oper = createPoolOperator([node]);
      const poolResource = createPoolResource(
        'pool',
        // modified spec with new node and new disk
        'node_new',
        ['/dev/sdb_new']
      );
      // this is the original spec cached in the status
      poolResource.status.spec = { node: 'node', disks: ['/dev/sdb'] };
      poolResource.status.disks = undefined;
      mockCache(oper.watcher, (arg) => {
        stubs = arg;
        stubs.get.returns(poolResource);
        stubs.list.returns([poolResource]);
      });
      await oper.start();
      // give time to registry to install its callbacks
      await sleep(EVENT_PROPAGATION_DELAY);

      sinon.assert.notCalled(stubs.create);
      sinon.assert.notCalled(stubs.update);
      sinon.assert.notCalled(stubs.delete);
      sinon.assert.calledTwice(stubs.updateStatus);
      // new SPEC points to node_new, but MOAC knows better
      sinon.assert.notCalled(createPoolStubNew);
      // instead, it tries to recreate the pool based on the original SPEC
      sinon.assert.calledOnce(createPoolStub);
      sinon.assert.calledWith(createPoolStub, 'pool', ['/dev/sdb']);
    });

    it('should recreate pool with original disk URI', async () => {
      let stubs;
      const node = new Node('node');
      const createPoolStub = sinon.stub(node, 'createPool');
      createPoolStub.rejects(
        new GrpcError(grpcCode.INTERNAL, 'create failed')
      );

      oper = createPoolOperator([node]);
      // note this sets the disk URI
      const poolResource = createPoolResource(
        'pool',
        'node',
        ['/dev/sdb'],
        '',
        'pending'
      );
      // this is the original spec cached in the status
      poolResource.status.spec = { node: 'node', disks: ['/dev/sdb'] };
      mockCache(oper.watcher, (arg) => {
        stubs = arg;
        stubs.get.returns(poolResource);
        stubs.list.returns([poolResource]);
      });
      await oper.start();
      // give time to registry to install its callbacks
      await sleep(EVENT_PROPAGATION_DELAY);

      sinon.assert.notCalled(stubs.create);
      sinon.assert.notCalled(stubs.update);
      sinon.assert.notCalled(stubs.delete);
      sinon.assert.callCount(stubs.updateStatus, 4);
      sinon.assert.calledTwice(createPoolStub);
      sinon.assert.calledWith(createPoolStub, 'pool', ['aio:///dev/sdb']);
    });

    it('should remove pool upon pool new event if there is no pool resource', async () => {
      let stubs;
      const pool = new Pool({
        name: 'pool',
        disks: ['aio:///dev/sdb'],
        state: 'POOL_ONLINE',
        capacity: 100,
        used: 4
      });
      const destroyStub = sinon.stub(pool, 'destroy');
      destroyStub.resolves();
      const node = new Node('node', {}, [pool]);
      oper = createPoolOperator([node]);

      mockCache(oper.watcher, (arg) => {
        stubs = arg;
      });
      await oper.start();
      // give time to registry to install its callbacks
      await sleep(EVENT_PROPAGATION_DELAY);

      sinon.assert.notCalled(stubs.create);
      sinon.assert.notCalled(stubs.update);
      sinon.assert.notCalled(stubs.delete);
      sinon.assert.notCalled(stubs.updateStatus);
      sinon.assert.calledOnce(destroyStub);
    });

    it('should update resource properties upon pool mod event', async () => {
      let stubs;
      const offlineReason = 'mayastor does not run on the node "node"';
      const pool = new Pool({
        name: 'pool',
        disks: ['aio:///dev/sdb'],
        state: 'POOL_ONLINE',
        capacity: 100,
        used: 4
      });
      const node = new Node('node', {}, [pool]);
      oper = createPoolOperator([node]);

      const poolResource = createPoolResource(
        'pool',
        'node1',
        ['/dev/sdb'],
        [],
        'online',
        '',
        100,
        4
      );
      mockCache(oper.watcher, (arg) => {
        stubs = arg;
        stubs.get.returns(poolResource);
      });
      await oper.start();
      // give time to registry to install its callbacks
      await sleep(EVENT_PROPAGATION_DELAY);

      // simulate pool mod event
      pool.state = 'POOL_OFFLINE';
      oper.registry.emit('pool', {
        eventType: 'mod',
        object: pool
      });
      // Give event time to propagate
      await sleep(EVENT_PROPAGATION_DELAY);

      sinon.assert.notCalled(stubs.create);
      sinon.assert.notCalled(stubs.delete);
      sinon.assert.notCalled(stubs.update);
      sinon.assert.calledOnce(stubs.updateStatus);
      expect(stubs.updateStatus.args[0][5].status).to.deep.equal({
        state: 'offline',
        reason: offlineReason,
        capacity: 100,
        disks: ['aio:///dev/sdb'],
        used: 4,
        spec: { node: 'node1', disks: ['/dev/sdb'] }
      });
    });

    it('should ignore pool mod event if pool resource does not exist', async () => {
      let stubs;
      const pool = new Pool({
        name: 'pool',
        disks: ['aio:///dev/sdb'],
        state: 'POOL_ONLINE',
        capacity: 100,
        used: 4
      });
      const node = new Node('node', {}, [pool]);
      oper = createPoolOperator([node]);

      mockCache(oper.watcher, (arg) => {
        stubs = arg;
      });
      await oper.start();
      // give time to registry to install its callbacks
      await sleep(EVENT_PROPAGATION_DELAY);

      // simulate pool mod event
      pool.state = 'POOL_OFFLINE';
      oper.registry.emit('pool', {
        eventType: 'mod',
        object: pool
      });
      // Give event time to propagate
      await sleep(EVENT_PROPAGATION_DELAY);

      sinon.assert.notCalled(stubs.create);
      sinon.assert.notCalled(stubs.update);
      sinon.assert.notCalled(stubs.delete);
      sinon.assert.notCalled(stubs.updateStatus);
    });

    it('should create pool upon pool del event if pool resource exist', async () => {
      let stubs;
      const pool = new Pool({
        name: 'pool',
        disks: ['aio:///dev/sdb'],
        state: 'POOL_ONLINE',
        capacity: 100,
        used: 4
      });
      const node = new Node('node', {}, [pool]);
      const createPoolStub = sinon.stub(node, 'createPool');
      createPoolStub.resolves(pool);
      oper = createPoolOperator([node]);
      const poolResource = createPoolResource(
        'pool',
        'node',
        ['/dev/sdb'],
        [],
        'online',
        '',
        100,
        4
      );
      mockCache(oper.watcher, (arg) => {
        stubs = arg;
        stubs.get.returns(poolResource);
      });
      await oper.start();
      // give time to registry to install its callbacks
      await sleep(EVENT_PROPAGATION_DELAY);
      sinon.assert.notCalled(createPoolStub);

      node.pools = [];
      oper.registry.emit('pool', {
        eventType: 'del',
        object: pool
      });
      // Give event time to propagate
      await sleep(EVENT_PROPAGATION_DELAY);

      sinon.assert.calledOnce(createPoolStub);
      sinon.assert.calledWith(createPoolStub, 'pool', ['aio:///dev/sdb']);
      sinon.assert.notCalled(stubs.create);
      sinon.assert.notCalled(stubs.update);
      sinon.assert.notCalled(stubs.delete);
      sinon.assert.calledOnce(stubs.updateStatus);
      expect(stubs.updateStatus.args[0][5].status).to.deep.equal({
        state: 'pending',
        reason: 'Creating the pool',
        disks: ['aio:///dev/sdb'],
        spec: { node: 'node', disks: ['/dev/sdb'] }
      });
    });

    it('should ignore pool del event if pool resource does not exist', async () => {
      let stubs;
      const pool = new Pool({
        name: 'pool',
        disks: ['aio:///dev/sdb'],
        state: 'POOL_ONLINE',
        capacity: 100,
        used: 4
      });
      const node = new Node('node', {}, []);
      oper = createPoolOperator([node]);
      mockCache(oper.watcher, (arg) => {
        stubs = arg;
      });
      await oper.start();
      // give time to registry to install its callbacks
      await sleep(EVENT_PROPAGATION_DELAY);

      node.pools = [];
      oper.registry.emit('pool', {
        eventType: 'del',
        object: pool
      });
      // Give event time to propagate
      await sleep(EVENT_PROPAGATION_DELAY);

      sinon.assert.notCalled(stubs.create);
      sinon.assert.notCalled(stubs.update);
      sinon.assert.notCalled(stubs.delete);
      sinon.assert.notCalled(stubs.updateStatus);
    });
  });
};
