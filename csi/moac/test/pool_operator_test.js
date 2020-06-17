// Unit tests for the pool operator
//
// We don't test the init method which depends on k8s api client and watcher.
// That method *must* be tested manually and in real k8s environment. For the
// rest of the dependencies we provide fake objects which mimic the real
// behaviour and allow us to test pool operator in isolation from other
// components.
//
// Pool operator depends on a couple of modules:
//  * registry (real)
//  * node object (fake)
//  * pool object (fake)
//  * watcher (fake)
//  * k8s client (fake)
//
// As you can see most of them must be fake in order to do detailed testing
// of pool operator. That makes the code more complicated and less readable.

'use strict';

const expect = require('chai').expect;
const sinon = require('sinon');
const sleep = require('sleep-promise');
const Registry = require('../registry');
const { GrpcError, GrpcCode } = require('../grpc_client');
const PoolOperator = require('../pool_operator');
const { Pool } = require('../pool');
const Watcher = require('./watcher_stub');
const Node = require('./node_stub');

const NAMESPACE = 'mayastor';

module.exports = function () {
  var msStub, putStub;

  // Create k8s pool resource object
  function createPoolResource (
    name,
    node,
    disks,
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
      if (reason != null) status.reason = reason;
      if (capacity != null) status.capacity = capacity;
      if (used != null) status.used = used;
      obj.status = status;
    }
    return obj;
  }

  // k8s api client stub.
  //
  // Note that this stub serves only for PUT method on mayastor resource
  // endpoint to update the status of resource. Fake watcher that is used
  // in the tests does not use this client stub.
  function createK8sClient (watcher) {
    const mayastorpools = { mayastorpools: function (name) {} };
    const namespaces = function (ns) {
      expect(ns).to.equal(NAMESPACE);
      return mayastorpools;
    };
    const client = {
      apis: {
        'openebs.io': {
          v1alpha1: { namespaces }
        }
      }
    };
    msStub = sinon.stub(mayastorpools, 'mayastorpools');
    const msObject = {
      status: {
        // the tricky thing here is that we have to update watcher's cache
        // if we use this fake k8s client to change the object in order to
        // mimic real behaviour.
        put: async function (payload) {
          watcher.objects[payload.body.metadata.name].status =
            payload.body.status;
          // simulate the asynchronicity of the put
          // await sleep(1);
        }
      }
    };
    putStub = sinon.stub(msObject.status, 'put');
    putStub.callThrough();
    msStub.returns(msObject);
    return client;
  }

  // Create a pool operator object suitable for testing - with fake watcher
  // and fake k8s api client.
  async function MockedPoolOperator (k8sObjects, nodes) {
    const oper = new PoolOperator(NAMESPACE);
    const registry = new Registry();
    registry.Node = Node;
    nodes = nodes || [];
    nodes.forEach((n) => (registry.nodes[n.name] = n));
    oper.registry = registry;
    oper.watcher = new Watcher(oper._filterMayastorPool, k8sObjects);
    oper.k8sClient = createK8sClient(oper.watcher);

    await oper.start();

    // Let the initial "new" events pass by so that they don't interfere with
    // whatever we are going to do with the operator after we return.
    //
    // TODO: Hardcoded delays are ugly. Find a better way. Applies to all
    // sleeps in this file.
    if (nodes.length > 0) {
      await sleep(10);
    }

    return oper;
  }

  describe('resource filter', () => {
    it('valid mayastor pool should pass the filter', () => {
      const obj = createPoolResource(
        'pool',
        'node',
        ['/dev/sdc', '/dev/sdb'],
        'OFFLINE',
        'The node is down'
      );
      const res = PoolOperator.prototype._filterMayastorPool(obj);
      expect(res).to.have.all.keys('name', 'node', 'disks');
      expect(res.name).to.equal('pool');
      expect(res.node).to.equal('node');
      // the filter should sort the disks
      expect(JSON.stringify(res.disks)).to.equal(
        JSON.stringify(['/dev/sdb', '/dev/sdc'])
      );
      expect(res.state).to.be.undefined();
    });

    it('valid mayastor pool without status should pass the filter', () => {
      const obj = createPoolResource('pool', 'node', ['/dev/sdc', '/dev/sdb']);
      const res = PoolOperator.prototype._filterMayastorPool(obj);
      expect(res).to.have.all.keys('name', 'node', 'disks');
      expect(res.name).to.equal('pool');
      expect(res.node).to.equal('node');
      expect(res.state).to.be.undefined();
    });
  });

  describe('watcher events', () => {
    var oper; // pool operator

    afterEach(async () => {
      if (oper) {
        await oper.stop();
        oper = null;
      }
    });

    describe('new event', () => {
      it('should set "state" to PENDING when creating a pool', async () => {
        const node = new Node('node');
        const createPoolStub = sinon.stub(node, 'createPool');
        createPoolStub.resolves(
          new Pool({
            name: 'pool',
            node: node,
            disks: ['/dev/sdb'],
            state: 'POOL_DEGRADED',
            capacity: 100,
            used: 10
          })
        );
        oper = await MockedPoolOperator([], [node]);
        // trigger "new" event
        oper.watcher.newObject(
          createPoolResource('pool', 'node', ['/dev/sdb'])
        );

        // give event callbacks time to propagate
        await sleep(10);

        sinon.assert.calledOnce(createPoolStub);
        sinon.assert.calledWith(createPoolStub, 'pool', ['/dev/sdb']);
        sinon.assert.calledOnce(msStub);
        sinon.assert.calledWith(msStub, 'pool');
        sinon.assert.calledOnce(putStub);
        sinon.assert.calledWithMatch(putStub, {
          body: {
            kind: 'MayastorPool',
            metadata: {
              name: 'pool',
              generation: 1,
              resourceVersion: '627981'
            },
            status: {
              state: 'pending',
              reason: 'Creating the pool'
            }
          }
        });
      });

      it('should not try to create a pool if the node has not been synced', async () => {
        const node = new Node('node');
        sinon.stub(node, 'isSynced').returns(false);
        const createPoolStub = sinon.stub(node, 'createPool');
        createPoolStub.resolves(
          new Pool({
            name: 'pool',
            node: node,
            disks: ['/dev/sdb'],
            state: 'POOL_DEGRADED',
            capacity: 100,
            used: 10
          })
        );
        oper = await MockedPoolOperator([], [node]);
        // trigger "new" event
        oper.watcher.newObject(
          createPoolResource('pool', 'node', ['/dev/sdb'])
        );

        // give event callbacks time to propagate
        await sleep(10);

        sinon.assert.notCalled(createPoolStub);
        sinon.assert.notCalled(msStub);
        sinon.assert.notCalled(putStub);
      });

      it('should not try to create a pool when pool with the same name already exists', async () => {
        const pool = new Pool({
          name: 'pool',
          disks: ['/dev/sdb'],
          state: 'POOL_DEGRADED',
          capacity: 100,
          used: 10
        });
        const node = new Node('node', {}, []);
        const createPoolStub = sinon.stub(node, 'createPool');
        createPoolStub.resolves(pool);
        oper = await MockedPoolOperator([], [node]);
        // this creates the inconsistency between real and k8s state which we are testing
        node.pools.push(pool);
        // trigger "new" event
        oper.watcher.newObject(
          // does not matter that the disks are different - still the same pool
          createPoolResource('pool', 'node', ['/dev/sdb', '/dev/sdc'])
        );

        // give event callbacks time to propagate
        await sleep(10);

        // the stub is called when the new node is synced
        sinon.assert.calledOnce(msStub);
        sinon.assert.calledWith(msStub, 'pool');
        sinon.assert.calledOnce(putStub);
        sinon.assert.calledWithMatch(putStub, {
          body: {
            status: {
              state: 'degraded',
              reason: '',
              capacity: 100,
              used: 10
            }
          }
        });
        sinon.assert.notCalled(createPoolStub);
      });

      // important test as moving the pool between nodes would destroy data
      it('should leave the pool untouched when pool exists and is on a different node', async () => {
        const pool = new Pool({
          name: 'pool',
          disks: ['/dev/sdb'],
          state: 'POOL_ONLINE',
          capacity: 100,
          used: 10
        });
        const node1 = new Node('node1', {}, []);
        const node2 = new Node('node2');
        const createPoolStub1 = sinon.stub(node1, 'createPool');
        const createPoolStub2 = sinon.stub(node2, 'createPool');
        createPoolStub1.resolves(pool);
        createPoolStub2.resolves(pool);
        oper = await MockedPoolOperator([], [node1, node2]);
        // we assign the pool to node1 but later in the event it will be on node2
        node1.pools.push(pool);
        // trigger "new" event
        oper.watcher.newObject(
          // does not matter that the disks are different - still the same pool
          createPoolResource('pool', 'node2', ['/dev/sdb', '/dev/sdc'])
        );

        // give event callbacks time to propagate
        await sleep(10);

        // the stub is called when the new node is synced
        sinon.assert.calledOnce(msStub);
        sinon.assert.calledWith(msStub, 'pool');
        sinon.assert.calledOnce(putStub);
        sinon.assert.calledWithMatch(putStub, {
          body: {
            status: {
              state: 'online',
              reason: ''
            }
          }
        });
        sinon.assert.notCalled(createPoolStub1);
        sinon.assert.notCalled(createPoolStub2);
      });

      it('should set "reason" to error message when create pool fails', async () => {
        const node = new Node('node');
        const createPoolStub = sinon.stub(node, 'createPool');
        createPoolStub.rejects(
          new GrpcError(GrpcCode.INTERNAL, 'create failed')
        );
        oper = await MockedPoolOperator([], [node]);
        // trigger "new" event
        oper.watcher.newObject(
          createPoolResource('pool', 'node', ['/dev/sdb'])
        );

        // give event callbacks time to propagate
        await sleep(10);

        sinon.assert.calledTwice(msStub);
        sinon.assert.alwaysCalledWith(msStub, 'pool');
        sinon.assert.calledTwice(putStub);
        sinon.assert.calledWithMatch(putStub.firstCall, {
          body: {
            status: {
              state: 'pending',
              reason: 'Creating the pool'
            }
          }
        });
        sinon.assert.calledWithMatch(putStub.secondCall, {
          body: {
            status: {
              state: 'pending',
              reason: 'Error: create failed'
            }
          }
        });
        sinon.assert.calledOnce(createPoolStub);
        sinon.assert.calledWith(createPoolStub, 'pool', ['/dev/sdb']);
      });

      it('should ignore failure to update the resource state', async () => {
        const node = new Node('node');
        const createPoolStub = sinon.stub(node, 'createPool');
        createPoolStub.rejects(
          new GrpcError(GrpcCode.INTERNAL, 'create failed')
        );
        oper = await MockedPoolOperator([], [node]);
        putStub.rejects(new Error('http put error'));
        // trigger "new" event
        oper.watcher.newObject(
          createPoolResource('pool', 'node', ['/dev/sdb'])
        );

        // give event callbacks time to propagate
        await sleep(10);

        sinon.assert.calledTwice(msStub);
        sinon.assert.alwaysCalledWith(msStub, 'pool');
        sinon.assert.calledTwice(putStub);
        sinon.assert.calledWithMatch(putStub.firstCall, {
          body: {
            status: {
              state: 'pending',
              reason: 'Creating the pool'
            }
          }
        });
        sinon.assert.calledWithMatch(putStub.secondCall, {
          body: {
            status: {
              state: 'pending',
              reason: 'Error: create failed'
            }
          }
        });
        sinon.assert.calledOnce(createPoolStub);
        sinon.assert.calledWith(createPoolStub, 'pool', ['/dev/sdb']);
      });

      it('should not create a pool if node does not exist', async () => {
        oper = await MockedPoolOperator([], []);
        // trigger "new" event
        oper.watcher.newObject(
          createPoolResource('pool', 'node', ['/dev/sdb'])
        );

        // give event callbacks time to propagate
        await sleep(10);

        sinon.assert.calledOnce(msStub);
        sinon.assert.calledWith(msStub, 'pool');
        sinon.assert.calledOnce(putStub);
        sinon.assert.calledWithMatch(putStub, {
          body: {
            status: {
              state: 'pending',
              reason: 'mayastor does not run on node "node"'
            }
          }
        });
      });

      it('should not create a pool if disk name is invalid', async () => {
        const node = new Node('node');
        const createPoolStub = sinon.stub(node, 'createPool');
        createPoolStub.resolves(
          new Pool({
            name: 'pool',
            node: node,
            disks: ['/dev/../sdb'],
            state: 'POOL_ONLINE',
            capacity: 100,
            used: 4
          })
        );
        oper = await MockedPoolOperator([], [node]);
        // trigger "new" event
        oper.watcher.newObject(
          createPoolResource('pool', 'node', ['/dev/../sdb'])
        );

        // give event callbacks time to propagate
        await sleep(10);

        sinon.assert.calledOnce(msStub);
        sinon.assert.calledWith(msStub, 'pool');
        sinon.assert.calledOnce(putStub);
        sinon.assert.calledWithMatch(putStub, {
          body: {
            status: {
              state: 'pending',
              reason: 'Disk must be absolute path beginning with /dev'
            }
          }
        });
        sinon.assert.notCalled(createPoolStub);
      });
    });

    describe('del event', () => {
      it('should destroy a pool', async () => {
        const pool = new Pool({
          name: 'pool',
          disks: ['/dev/sdb'],
          state: 'POOL_DEGRADED',
          capacity: 100,
          used: 10
        });
        const destroyStub = sinon.stub(pool, 'destroy');
        destroyStub.resolves();
        const node = new Node('node', {}, [pool]);
        oper = await MockedPoolOperator(
          [
            createPoolResource(
              'pool',
              'node',
              ['/dev/sdb'],
              'degraded',
              '',
              100,
              10
            )
          ],
          [node]
        );

        // trigger "del" event
        oper.watcher.delObject('pool');
        // give event callbacks time to propagate
        await sleep(10);

        sinon.assert.notCalled(msStub);
        sinon.assert.calledOnce(destroyStub);
        expect(oper.resource).to.not.have.key('pool');
      });

      it('should not fail if pool does not exist', async () => {
        const pool = new Pool({
          name: 'pool',
          disks: ['/dev/sdb'],
          state: 'POOL_DEGRADED',
          capacity: 100,
          used: 10
        });
        const node = new Node('node', {}, [pool]);
        oper = await MockedPoolOperator(
          [createPoolResource('pool', 'node', ['/dev/sdb'], 'OFFLINE', '')],
          [node]
        );
        // we create the inconsistency between k8s and real state
        node.pools = [];
        // trigger "del" event
        oper.watcher.delObject('pool');

        // called during the initial sync
        sinon.assert.calledOnce(msStub);
        expect(oper.resource).to.not.have.key('pool');
      });

      it('should destroy the pool even if it is on a different node', async () => {
        const pool = new Pool({
          name: 'pool',
          disks: ['/dev/sdb'],
          state: 'POOL_DEGRADED',
          capacity: 100,
          used: 10
        });
        const destroyStub = sinon.stub(pool, 'destroy');
        destroyStub.resolves();
        const node1 = new Node('node1', {}, []);
        const node2 = new Node('node2', {}, [pool]);
        oper = await MockedPoolOperator(
          [createPoolResource('pool', 'node1', ['/dev/sdb'], 'online', '')],
          [node1, node2]
        );
        // trigger "del" event
        oper.watcher.delObject('pool');

        // called during the initial sync
        sinon.assert.calledOnce(msStub);

        sinon.assert.calledOnce(destroyStub);
        expect(oper.resource).to.not.have.key('pool');
      });

      it('should delete the resource even if the destroy fails', async () => {
        const pool = new Pool({
          name: 'pool',
          disks: ['/dev/sdb'],
          state: 'POOL_DEGRADED',
          capacity: 100,
          used: 10,
          destroy: async function () {}
        });
        const destroyStub = sinon.stub(pool, 'destroy');
        destroyStub.rejects(new GrpcError(GrpcCode.INTERNAL, 'destroy failed'));
        const node = new Node('node', {}, [pool]);
        oper = await MockedPoolOperator(
          [createPoolResource('pool', 'node', ['/dev/sdb'], 'DEGRADED', '')],
          [node]
        );
        // trigger "del" event
        oper.watcher.delObject('pool');

        // give event callbacks time to propagate
        await sleep(10);

        // called during the initial sync
        sinon.assert.calledOnce(msStub);

        sinon.assert.calledOnce(destroyStub);
        expect(oper.resource).to.not.have.key('pool');
      });
    });

    describe('mod event', () => {
      it('should not do anything if pool object has not changed', async () => {
        const pool = new Pool({
          name: 'pool',
          disks: ['/dev/sdb', '/dev/sdc'],
          state: 'POOL_DEGRADED',
          capacity: 100,
          used: 10
        });
        const node = new Node('node', {}, [pool]);
        oper = await MockedPoolOperator(
          [
            createPoolResource(
              'pool',
              'node',
              ['/dev/sdb', '/dev/sdc'],
              'DEGRADED',
              ''
            )
          ],
          [node]
        );

        // called during the initial sync
        sinon.assert.calledOnce(msStub);

        // trigger "mod" event
        oper.watcher.modObject(
          createPoolResource('pool', 'node', ['/dev/sdc', '/dev/sdb'])
        );

        // called during the initial sync
        sinon.assert.calledOnce(msStub);
        // operator state
        expect(oper.resource.pool.disks).to.have.lengthOf(2);
        expect(oper.resource.pool.disks[0]).to.equal('/dev/sdb');
        expect(oper.resource.pool.disks[1]).to.equal('/dev/sdc');
      });

      it('should not do anything if disks change', async () => {
        const pool = new Pool({
          name: 'pool',
          disks: ['/dev/sdb'],
          state: 'POOL_DEGRADED',
          capacity: 100,
          used: 10
        });
        const node = new Node('node', {}, [pool]);
        oper = await MockedPoolOperator(
          [createPoolResource('pool', 'node', ['/dev/sdb'], 'DEGRADED', '')],
          [node]
        );

        // trigger "mod" event
        oper.watcher.modObject(
          createPoolResource('pool', 'node', ['/dev/sdc'])
        );

        // called during the initial sync
        sinon.assert.calledOnce(msStub);
        // the real state
        expect(node.pools[0].disks[0]).to.equal('/dev/sdb');
        // watcher state
        expect(oper.watcher.list()[0].disks[0]).to.equal('/dev/sdc');
        // operator state
        expect(oper.resource.pool.disks[0]).to.equal('/dev/sdc');
      });

      it('should not do anything if node changes', async () => {
        const pool = new Pool({
          name: 'pool',
          disks: ['/dev/sdb'],
          state: 'POOL_DEGRADED',
          capacity: 100,
          used: 10
        });
        const node1 = new Node('node1', {}, [pool]);
        const node2 = new Node('node2', {}, []);
        oper = await MockedPoolOperator(
          [createPoolResource('pool', 'node1', ['/dev/sdb'], 'DEGRADED', '')],
          [node1]
        );

        // trigger "mod" event
        oper.watcher.modObject(
          createPoolResource('pool', 'node2', ['/dev/sdb'])
        );

        // called during the initial sync
        sinon.assert.calledOnce(msStub);
        // the real state
        expect(node1.pools).to.have.lengthOf(1);
        expect(node2.pools).to.have.lengthOf(0);
        // watcher state
        expect(oper.watcher.list()[0].node).to.equal('node2');
        // operator state
        expect(oper.resource.pool.node).to.equal('node2');
      });
    });
  });

  describe('node events', () => {
    var oper; // pool operator

    afterEach(async () => {
      if (oper) {
        await oper.stop();
        oper = null;
      }
    });

    it('should create pool upon node sync event if it does not exist', async () => {
      const node = new Node('node', {}, []);
      const createPoolStub = sinon.stub(node, 'createPool');
      createPoolStub.resolves(
        new Pool({
          name: 'pool',
          node: node,
          disks: ['/dev/sdb'],
          state: 'POOL_ONLINE',
          capacity: 100,
          used: 4
        })
      );
      oper = await MockedPoolOperator(
        [createPoolResource('pool', 'node', ['/dev/sdb'])],
        [node]
      );

      sinon.assert.calledOnce(msStub);
      sinon.assert.calledWith(msStub, 'pool');
      sinon.assert.calledOnce(putStub);
      sinon.assert.calledWithMatch(putStub, {
        body: {
          status: {
            state: 'pending',
            reason: 'Creating the pool'
          }
        }
      });
      sinon.assert.calledOnce(createPoolStub);
      sinon.assert.calledWith(createPoolStub, 'pool', ['/dev/sdb']);
    });

    it('should not create pool upon node sync event if it exists', async () => {
      const pool = new Pool({
        name: 'pool',
        disks: ['/dev/sdb'],
        state: 'POOL_ONLINE',
        capacity: 100,
        used: 4
      });
      const node = new Node('node', {}, [pool]);
      const createPoolStub = sinon.stub(node, 'createPool');
      createPoolStub.resolves(pool);
      oper = await MockedPoolOperator(
        [
          createPoolResource(
            'pool',
            'node',
            ['/dev/sdb'],
            'online',
            '',
            100,
            4
          )
        ],
        [node]
      );

      sinon.assert.notCalled(msStub);
      sinon.assert.notCalled(putStub);
      sinon.assert.notCalled(createPoolStub);
    });

    it('should not create pool upon node sync event if it exists on another node', async () => {
      const pool = new Pool({
        name: 'pool',
        disks: ['/dev/sdb'],
        state: 'POOL_ONLINE',
        capacity: 100,
        used: 4
      });
      const node1 = new Node('node1', {}, []);
      const node2 = new Node('node2', {}, [pool]);
      const createPoolStub1 = sinon.stub(node1, 'createPool');
      const createPoolStub2 = sinon.stub(node2, 'createPool');
      createPoolStub1.resolves(pool);
      createPoolStub2.resolves(pool);
      oper = await MockedPoolOperator(
        [
          createPoolResource(
            'pool',
            'node1',
            ['/dev/sdb'],
            'online',
            '',
            100,
            4
          )
        ],
        [node1, node2]
      );

      sinon.assert.notCalled(msStub);
      sinon.assert.notCalled(putStub);
      sinon.assert.notCalled(createPoolStub1);
      sinon.assert.notCalled(createPoolStub2);
    });

    it('should remove pool upon pool new event if there is no pool resource', async () => {
      const pool = new Pool({
        name: 'pool',
        disks: ['/dev/sdb'],
        state: 'POOL_ONLINE',
        capacity: 100,
        used: 4,
        destroy: async function () {}
      });
      const destroyStub = sinon.stub(pool, 'destroy');
      destroyStub.resolves();
      const node = new Node('node', {}, [pool]);
      oper = await MockedPoolOperator([], [node]);

      sinon.assert.notCalled(msStub);
      sinon.assert.notCalled(putStub);
      sinon.assert.calledOnce(destroyStub);
    });

    it('should update resource properties upon pool mod event', async () => {
      const offlineReason = 'mayastor does not run on the node "node"';
      const pool = new Pool({
        name: 'pool',
        disks: ['/dev/sdb'],
        state: 'POOL_ONLINE',
        capacity: 100,
        used: 4
      });
      const node = new Node('node', {}, [pool]);
      oper = await MockedPoolOperator(
        [
          createPoolResource(
            'pool',
            'node',
            ['/dev/sdb'],
            'online',
            '',
            100,
            4
          )
        ],
        [node]
      );

      pool.state = 'POOL_OFFLINE';
      // simulate pool mod event
      oper.registry.emit('pool', {
        eventType: 'mod',
        object: pool
      });

      // Give event time to propagate
      await sleep(10);

      sinon.assert.calledOnce(msStub);
      sinon.assert.calledWith(msStub, 'pool');
      sinon.assert.calledOnce(putStub);
      sinon.assert.calledWithMatch(putStub, {
        body: {
          status: {
            state: 'offline',
            reason: offlineReason
          }
        }
      });
      expect(oper.watcher.objects.pool.status.state).to.equal('offline');
      expect(oper.watcher.objects.pool.status.reason).to.equal(offlineReason);
    });

    it('should ignore pool mod event if pool resource does not exist', async () => {
      const node = new Node('node', {}, []);
      oper = await MockedPoolOperator([], [node]);

      oper.registry.emit('pool', {
        eventType: 'mod',
        object: new Pool({
          name: 'pool',
          disks: ['/dev/sdb'],
          state: 'POOL_OFFLINE',
          capacity: 100,
          used: 4
        })
      });

      // Give event time to propagate
      await sleep(10);

      sinon.assert.notCalled(msStub);
      sinon.assert.notCalled(putStub);
      expect(oper.resource.pool).to.be.undefined();
    });

    it('should create pool upon pool del event if pool resource exist', async () => {
      const pool = new Pool({
        name: 'pool',
        disks: ['/dev/sdb'],
        state: 'POOL_ONLINE',
        capacity: 100,
        used: 4
      });
      const node = new Node('node', {}, [pool]);
      const createPoolStub = sinon.stub(node, 'createPool');
      createPoolStub.resolves(pool);
      oper = await MockedPoolOperator(
        [
          createPoolResource(
            'pool',
            'node',
            ['/dev/sdb'],
            'online',
            '',
            100,
            4
          )
        ],
        [node]
      );

      sinon.assert.notCalled(msStub);
      sinon.assert.notCalled(createPoolStub);

      node.pools = [];
      oper.registry.emit('pool', {
        eventType: 'del',
        object: pool
      });

      // Give event time to propagate
      await sleep(10);

      sinon.assert.calledOnce(msStub);
      sinon.assert.calledWith(msStub, 'pool');
      sinon.assert.calledOnce(putStub);
      sinon.assert.calledWithMatch(putStub, {
        body: {
          status: {
            state: 'pending',
            reason: 'Creating the pool'
          }
        }
      });
      sinon.assert.calledOnce(createPoolStub);
      sinon.assert.calledWith(createPoolStub, 'pool', ['/dev/sdb']);
    });

    it('should ignore pool del event if pool resource does not exist', async () => {
      const node = new Node('node', {}, []);
      oper = await MockedPoolOperator([], [node]);

      oper.registry.emit('pool', {
        eventType: 'del',
        object: new Pool({
          name: 'pool',
          disks: ['/dev/sdb'],
          state: 'POOL_ONLINE',
          capacity: 100,
          used: 4
        })
      });

      // Give event time to propagate
      await sleep(10);
      sinon.assert.notCalled(msStub);
    });
  });
};
