// Unit tests for the pool operator
//
// We don't test the init method which depends on k8s api client and node
// operator. That method *must* be tested manually and in real k8s
// environment. For k8s api client and node operator we provide fake objects
// which mimic the real behaviour and allow us to test pool operator in
// isolation from other components.

const assert = require('chai').assert;
const EventEmitter = require('events');
const sleep = require('sleep-promise');
const { WatcherMock } = require('./watcher');
const { MayastorServer } = require('./mayastor_mock');
const { NodeOperatorMock } = require('./nodes');
const poolsModule = require('./pools');
const PoolOperator = poolsModule.PoolOperator;

const EGRESS_ENDPOINT = '127.0.0.1:12345';

// k8s api client mock.
//
// Currently the only endpoint needed by pool operator is PUT on mayastorpool
// status endpoint (the rest of the endpoints for mayastor pools is used
// internally by the watcher). Each time the endpoint is called, an event is
// recorded and can be later asynchronously retrieved.
class FakeApiClient extends EventEmitter {
  constructor() {
    super();
    this.calls = [];
    this.waits = [];
    var self = this;
    this.apis = {
      'openebs.io': {
        v1alpha1: {
          mayastorpools: function(name) {
            return {
              status: {
                put: function(payload) {
                  let body = payload.body;
                  assert.isDefined(body.metadata.name);
                  assert.isDefined(body.metadata.generation);
                  assert.isDefined(body.metadata.resourceVersion);
                  var wait = self.waits.shift();
                  var call = {
                    name: name,
                    stat: body.status,
                  };
                  if (wait) {
                    // we don't want possible exceptions to fail the whole
                    // status set operator, hence run it asynchronously
                    setTimeout(() => {
                      wait(call);
                    }, 0);
                  } else {
                    self.calls.push(call);
                  }
                },
              },
            };
          },
        },
      },
    };
  }

  // Retrieve the first recorded call from the queue or time out after 10ms.
  async called() {
    var self = this;
    return new Promise((resolve, reject) => {
      var call = self.calls.shift();
      if (call) {
        resolve(call);
      } else {
        self.waits.push(resolve);
        setTimeout(() => {
          let wait = self.waits.find(ent => ent == resolve);
          if (wait) {
            self.waits = self.waits.filter(ent => ent != resolve);
            reject(new Error('Timed out waiting for REST call'));
          }
        }, 1000);
      }
    });
  }
}

// Create a pool operator object bound to the:
//  1) fake watcher
//  2) fake node operator
//  3) fake k8s api client
//  4) real grpc server with fake mayastor service (started elsewhere)
async function MockedPoolOperator(objs, nodes) {
  let oper = new PoolOperator();
  oper.nodes = new NodeOperatorMock(nodes);
  oper.client = new FakeApiClient();
  oper.watcher = new WatcherMock(oper.filterMayastorPool, []);

  await oper.start();

  // we secretly insert the initial objects as if they were there since ever
  objs.forEach(k8sObj => {
    oper.watcher.injectObject(k8sObj);
    let obj = oper.filterMayastorPool(k8sObj);
    oper.pools[obj.name] = obj;
  });

  oper.nodes.nodes = [
    {
      node: 'node',
      endpoint: EGRESS_ENDPOINT,
    },
  ];

  return oper;
}

// Create k8s pool CR object
function createPoolCR(name, node, disks, state, reason) {
  let obj = {
    apiVersion: 'openebs.io/v1alpha1',
    kind: 'MayastorPool',
    metadata: {
      creationTimestamp: '2019-02-15T18:23:53Z',
      generation: 1,
      name: name,
      resourceVersion: '627981',
      selfLink: '/apis/openebs.io/v1alpha1/mayastorpools/pool-name',
      uid: 'd99f06a9-314e-11e9-b086-589cfc0d76a7',
    },
    spec: {
      node: node,
      disks: disks,
    },
  };
  if (state || reason) {
    obj.status = { state, reason };
  }
  return obj;
}

function startMayastorServer(pools) {
  return new MayastorServer(EGRESS_ENDPOINT, pools).start();
}

module.exports = function() {
  var srv; // mayastor server

  // close grpc server after each unit test, otherwise the test suite would
  // hang at the end.
  afterEach(() => {
    if (srv) {
      srv.stop();
      srv = null;
    }
  });

  it('valid mayastor pool should pass the filter', () => {
    let obj = createPoolCR(
      'pool',
      'node',
      ['/dev/sdc', '/dev/sdb'],
      'OFFLINE',
      'The node is down'
    );
    let res = PoolOperator.prototype.filterMayastorPool(obj);
    assert.hasAllKeys(res, ['name', 'node', 'disks', 'state', 'reason']);
    assert.equal(res.name, 'pool');
    assert.equal(res.node, 'node');
    assert.equal(
      JSON.stringify(res.disks),
      JSON.stringify(['/dev/sdb', '/dev/sdc'])
    ); // it should switch the order
    assert.equal(res.state, 'OFFLINE');
    assert.equal(res.reason, 'The node is down');
  });

  it('valid mayastor pool without status should pass the filter', () => {
    let obj = createPoolCR('pool', 'node', ['/dev/sdc', '/dev/sdb']);
    let res = PoolOperator.prototype.filterMayastorPool(obj);
    assert.hasAllKeys(res, ['name', 'node', 'disks', 'state', 'reason']);
    assert.equal(res.name, 'pool');
    assert.isUndefined(res.state);
    assert.isUndefined(res.reason);
  });

  describe('pool events', () => {
    var oper; // pool operator

    afterEach(async () => {
      if (oper) {
        await oper.stop();
        oper = null;
      }
    });

    describe('new event', () => {
      it('should create a pool', async () => {
        srv = startMayastorServer();
        oper = await MockedPoolOperator([]);
        oper.watcher.newObject(createPoolCR('pool', 'node', ['/dev/sdb']));

        // triggered when pool state is updated in k8s api server
        let { name, stat } = await oper.client.called();
        assert.equal(name, 'pool');
        assert.equal(stat.state, 'ONLINE');
        assert.equal(stat.reason, '');

        // verify state in the mocked mayastor server
        let plist = srv.getPools();
        assert.lengthOf(plist, 1);
        assert.equal(plist[0].name, 'pool');
        assert.lengthOf(plist[0].disks, 1);
        assert.equal(plist[0].disks[0], '/dev/sdb');
        assert.equal(plist[0].state, 0);
        assert.equal(plist[0].capacity, 100);
        assert.equal(plist[0].used, 4);

        // verify state in the pool operator
        plist = oper.get();
        assert.lengthOf(plist, 1);
        assert.equal(plist[0].name, 'pool');
        assert.equal(plist[0].node, 'node');
        assert.deepEqual(plist[0].disks, ['/dev/sdb']);
        assert.equal(plist[0].state, 'ONLINE');
        assert.equal(plist[0].reason, '');
        assert.equal(plist[0].capacity, 100);
        assert.equal(plist[0].used, 4);
      });

      it('should not fail if the same pool already exists', async () => {
        srv = startMayastorServer([
          {
            name: 'pool',
            disks: ['/dev/sdc', '/dev/sdb'],
            state: 1,
            capacity: 100,
            used: 50,
          },
        ]);
        oper = await MockedPoolOperator([]);
        oper.watcher.newObject(
          createPoolCR('pool', 'node', ['/dev/sdb', '/dev/sdc'])
        );

        // triggered when pool state is updated in k8s api server
        let { name, stat } = await oper.client.called();
        assert.equal(name, 'pool');
        assert.equal(stat.state, 'DEGRADED');
        assert.equal(stat.reason, '');

        // verify state in the pool operator
        plist = oper.get();
        assert.lengthOf(plist, 1);
        assert.equal(plist[0].name, 'pool');
        assert.equal(plist[0].node, 'node');
        assert.deepEqual(plist[0].disks, ['/dev/sdb', '/dev/sdc']);
        assert.equal(plist[0].state, 'DEGRADED');
        assert.equal(plist[0].reason, '');
        assert.equal(plist[0].capacity, 100);
        assert.equal(plist[0].used, 50);
      });

      it('should fail if a different pool with the same name exists', async () => {
        srv = startMayastorServer([
          {
            name: 'pool',
            disks: ['/dev/sdc'],
            state: 1,
            capacity: 100,
            used: 50,
          },
        ]);
        oper = await MockedPoolOperator([]);
        oper.watcher.newObject(createPoolCR('pool', 'node', ['/dev/sdb']));

        // triggered when pool state is updated in k8s api server
        let { name, stat } = await oper.client.called();
        assert.equal(name, 'pool');
        assert.equal(stat.state, 'PENDING');
        assert.match(
          stat.reason,
          /A different pool with the same name already exists/
        );

        // verify state in the pool operator
        plist = oper.get();
        assert.lengthOf(plist, 1);
        assert.equal(plist[0].name, 'pool');
        assert.equal(plist[0].node, 'node');
        assert.deepEqual(plist[0].disks, ['/dev/sdb']);
        assert.equal(plist[0].state, 'PENDING');
        assert.match(
          plist[0].reason,
          /A different pool with the same name already exists/
        );
        assert.isUndefined(plist[0].capacity);
        assert.isUndefined(plist[0].used);
      });

      it('should not create a pool if grpc call fails', async () => {
        oper = await MockedPoolOperator([]);
        oper.watcher.newObject(createPoolCR('pool', 'node', ['/dev/sdb']));

        let { name, stat } = await oper.client.called();
        assert.equal(name, 'pool');
        assert.equal(stat.state, 'PENDING');
        // The error msg varies according to nodejs version
        assert.match(stat.reason, /(failed to connect)|(Connect Failed)/);

        let plist = oper.get();
        assert.lengthOf(plist, 1);
        assert.equal(plist[0].name, 'pool');
        assert.equal(plist[0].state, 'PENDING');
        assert.match(plist[0].reason, /(failed to connect)|(Connect Failed)/);
        assert.isUndefined(plist[0].capacity);
        assert.isUndefined(plist[0].used);
      });

      it('should not create a pool if node does not exist', async () => {
        oper = await MockedPoolOperator([]);
        oper.watcher.newObject(
          createPoolCR('pool', 'unknown-node', ['/dev/sdb'])
        );

        let { name, stat } = await oper.client.called();
        assert.equal(name, 'pool');
        assert.equal(stat.state, 'PENDING');
        assert.equal(
          stat.reason,
          'mayastor on node "unknown-node" is not running'
        );

        let plist = oper.get();
        assert.lengthOf(plist, 1);
        assert.equal(plist[0].name, 'pool');
      });

      it('should not create a pool if disk name is invalid', async () => {
        oper = await MockedPoolOperator([]);
        oper.watcher.newObject(
          createPoolCR('pool', 'node', ['sdb'], 'PENDING', 'something')
        );

        let { name, stat } = await oper.client.called();
        assert.equal(name, 'pool');
        assert.equal(stat.state, 'PENDING');
        assert.equal(
          stat.reason,
          'All disks must be absolute paths beginning with /dev'
        );

        let plist = oper.get();
        assert.lengthOf(plist, 1);
        assert.equal(plist[0].name, 'pool');
      });
    });

    describe('del event', () => {
      it('should destroy a pool', async () => {
        srv = startMayastorServer([
          {
            name: 'pool',
            disks: ['/dev/sdb'],
            state: 0,
            capacity: 100,
            used: 50,
          },
        ]);

        oper = await MockedPoolOperator([
          createPoolCR('pool', 'node', ['/dev/sdb'], 'ONLINE', ''),
        ]);
        oper.watcher.delObject('pool');

        await new Promise((resolve, reject) => {
          oper.once('destroy', poolName => {
            assert.equal(poolName, 'pool');
            resolve();
          });
        });

        let plist = srv.getPools();
        assert.lengthOf(plist, 0);
        plist = oper.get();
        assert.lengthOf(plist, 0);
      });

      it('should not fail if pool does not exist', async () => {
        srv = startMayastorServer([]);

        oper = await MockedPoolOperator([
          createPoolCR('pool', 'node', ['/dev/sdb'], 'ONLINE', ''),
        ]);
        oper.watcher.delObject('pool');

        await new Promise((resolve, reject) => {
          oper.once('destroy', reject);
          setTimeout(resolve, 10);
        });

        let plist = oper.get();
        assert.lengthOf(plist, 0);
      });

      it('should not destroy a pool if node does not exist', async () => {
        srv = startMayastorServer([
          {
            name: 'pool',
            disks: ['/dev/sdb'],
            state: 0,
            capacity: 100,
            used: 50,
          },
        ]);

        oper = await MockedPoolOperator([
          createPoolCR('pool', 'unknown-node', ['/dev/sdb'], 'OFFLINE', 'Down'),
        ]);
        oper.watcher.delObject('pool');

        await new Promise((resolve, reject) => {
          oper.once('destroy', reject);
          setTimeout(resolve, 10);
        });

        let plist = srv.getPools();
        assert.lengthOf(plist, 1);
        // Although not destroyed it is still removed from internal state
        plist = oper.get();
        assert.lengthOf(plist, 0);
      });

      it('should not destroy a pool if grpc fails', async () => {
        oper = await MockedPoolOperator([
          createPoolCR('pool', 'node', ['/dev/sdb']),
        ]);
        oper.watcher.delObject('pool');

        await new Promise((resolve, reject) => {
          oper.once('destroy', reject);
          setTimeout(resolve, 10);
        });

        // Although not destroyed it is still removed from internal state
        let plist = oper.get();
        assert.lengthOf(plist, 0);
      });
    });

    describe('mod event', () => {
      it('should not do anything if disks change', async () => {
        oper = await MockedPoolOperator([
          createPoolCR('pool', 'node', ['/dev/sdb']),
        ]);
        oper.watcher.modObject(
          createPoolCR('pool', 'node', ['/dev/sdb', '/dev/sdc'])
        );

        try {
          await oper.client.called();
        } catch (err) {
          let plist = oper.get();
          assert.lengthOf(plist, 1);
          assert.equal(plist[0].name, 'pool');
          assert.deepEqual(plist[0].disks, ['/dev/sdb']);
          assert.isUndefined(plist[0].state);
          assert.isUndefined(plist[0].reason);
          return;
        }
        assert(false, 'unexpected status update');
      });

      it('should not do anything if pool object has not changed', async () => {
        oper = await MockedPoolOperator([
          createPoolCR('pool', 'node', ['/dev/sdb']),
        ]);
        oper.watcher.modObject(
          createPoolCR('pool', 'node', ['/dev/sdb'], 'PENDING')
        );

        try {
          await oper.client.called();
        } catch (err) {
          let plist = oper.get();
          assert.lengthOf(plist, 1);
          assert.equal(plist[0].name, 'pool');
          assert.deepEqual(plist[0].disks, ['/dev/sdb']);
          assert.isUndefined(plist[0].state);
          assert.isUndefined(plist[0].reason);
          return;
        }
        assert(false, 'unexpected status update');
      });

      it('should destroy and create a pool if node changes', async () => {
        srv = startMayastorServer([
          {
            name: 'pool',
            disks: ['/dev/sdb'],
            state: 1,
            capacity: 100,
            used: 50,
          },
        ]);

        oper = await MockedPoolOperator([
          createPoolCR('pool', 'node', ['/dev/sdb'], 'DEGRADED', ''),
        ]);
        oper.nodes.nodes.push({
          node: 'new-node',
          endpoint: EGRESS_ENDPOINT,
        });
        oper.watcher.modObject(createPoolCR('pool', 'new-node', ['/dev/sdb']));

        await new Promise((resolve, reject) => {
          oper.once('destroy', poolName => {
            assert.equal(poolName, 'pool');
            resolve();
          });
        });

        let { name, stat } = await oper.client.called();
        assert.equal(name, 'pool');
        assert.equal(stat.state, 'ONLINE');
        assert.equal(stat.reason, '');

        let plist = srv.getPools();
        assert.lengthOf(plist, 1);
        assert.equal(plist[0].name, 'pool');
        assert.equal(plist[0].state, 0);
        assert.equal(plist[0].capacity, 100);
        assert.equal(plist[0].used, 4);

        plist = oper.get();
        assert.lengthOf(plist, 1);
        assert.equal(plist[0].name, 'pool');
        assert.equal(plist[0].node, 'new-node');
        assert.equal(plist[0].state, 'ONLINE');
        assert.equal(plist[0].reason, '');
        assert.equal(plist[0].capacity, 100);
        assert.equal(plist[0].used, 4);
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

    it('should update status of a pool upon node "remove" event', async () => {
      oper = await MockedPoolOperator(
        [createPoolCR('pool', 'node', ['/dev/sdb'], 'ONLINE', '')],
        [
          {
            node: 'node',
            endpoint: EGRESS_ENDPOINT,
          },
        ]
      );

      oper.nodes.removeNode('node');

      let { name, stat } = await oper.client.called();
      assert.equal(name, 'pool');
      assert.equal(stat.state, 'OFFLINE');
      assert.equal(stat.reason, 'mayastor on node "node" is not running');

      let plist = oper.get();
      assert.lengthOf(plist, 1);
      assert.equal(plist[0].name, 'pool');
      assert.equal(plist[0].state, 'OFFLINE');
      assert.equal(plist[0].reason, 'mayastor on node "node" is not running');
    });

    it('should create missing pool upon node "add" event', async () => {
      srv = startMayastorServer();
      oper = await MockedPoolOperator([
        createPoolCR('pool', 'node', ['/dev/sdc'], 'OFFLINE', 'down'),
      ]);
      oper.nodes.addNode('node', EGRESS_ENDPOINT);

      let { name, stat } = await oper.client.called();
      assert.equal(name, 'pool');
      assert.equal(stat.state, 'ONLINE');
      assert.equal(stat.reason, '');

      let plist = srv.getPools();
      assert.lengthOf(plist, 1);
      assert.equal(plist[0].name, 'pool');
      assert.equal(plist[0].state, 0);

      plist = oper.get();
      assert.lengthOf(plist, 1);
      assert.equal(plist[0].name, 'pool');
      assert.equal(plist[0].state, 'ONLINE');
      assert.equal(plist[0].reason, '');
    });

    it('should remove orphaned pool upon node "add" event', async () => {
      srv = startMayastorServer([
        {
          name: 'pool',
          disks: ['/dev/sdc'],
          state: 0,
          capacity: 100,
          used: 50,
        },
      ]);

      oper = await MockedPoolOperator([]);
      oper.nodes.addNode('node', EGRESS_ENDPOINT);

      await new Promise((resolve, reject) => {
        oper.once('destroy', resolve);
      });

      let plist = srv.getPools();
      assert.lengthOf(plist, 0);
      plist = oper.get();
      assert.lengthOf(plist, 0);
    });

    it('should update state of a pool upon node "add" event', async () => {
      srv = startMayastorServer([
        {
          name: 'pool',
          disks: ['/dev/sdb'],
          state: 1,
          capacity: 100,
          used: 50,
        },
      ]);

      oper = await MockedPoolOperator([
        createPoolCR('pool', 'node', ['/dev/sdb'], 'OFFLINE', 'down'),
      ]);
      oper.nodes.addNode('node', EGRESS_ENDPOINT);

      let { name, stat } = await oper.client.called();
      assert.equal(name, 'pool');
      assert.equal(stat.state, 'DEGRADED');
      assert.equal(stat.reason, '');

      let plist = srv.getPools();
      assert.lengthOf(plist, 1);
      assert.equal(plist[0].name, 'pool');

      plist = oper.get();
      assert.lengthOf(plist, 1);
      assert.equal(plist[0].name, 'pool');
      assert.deepEqual(plist[0].disks, ['/dev/sdb']);
      assert.equal(plist[0].state, 'DEGRADED');
      assert.equal(plist[0].reason, '');
      assert.equal(plist[0].capacity, 100);
      assert.equal(plist[0].used, 50);
    });
  });

  describe('sync', function() {
    var oper;

    // Create initial state before starting the operator
    before(() => {
      // it will check stale pools every second
      poolsModule.checkInterval = 1;
      oper = new PoolOperator();
      oper.nodes = new NodeOperatorMock();
      oper.client = new FakeApiClient();
      oper.watcher = new WatcherMock(oper.filterMayastorPool, [
        createPoolCR('pool', 'node', ['/dev/sdb']),
        createPoolCR('pool-to-create', 'node', ['/dev/sda']),
        createPoolCR('inaccessible-pool', 'node-unknown', ['/dev/sdc']),
      ]);
      oper.nodes.nodes = [
        {
          node: 'node',
          endpoint: EGRESS_ENDPOINT,
        },
        {
          node: 'node-unknown',
          endpoint: EGRESS_ENDPOINT + '1',
        },
      ];
    });

    after(async () => {
      await oper.stop();
    });

    it('should start the pool operator syncing the state of pools and nodes', async () => {
      srv = startMayastorServer([
        {
          name: 'pool',
          disks: ['/dev/sdb'],
          state: 1,
          capacity: 100,
          used: 99,
        },
        {
          name: 'pool-to-destroy',
          disks: ['/dev/sdc'],
          state: 1,
          capacity: 100,
          used: 50,
        },
      ]);

      await oper.start();

      let pools = srv.getPools();
      assert.lengthOf(pools, 2);
      assert.equal(pools[0].name, 'pool');
      assert.equal(pools[0].state, 1);
      assert.equal(pools[0].capacity, 100);
      assert.equal(pools[0].used, 99);
      assert.equal(pools[1].name, 'pool-to-create');
      assert.equal(pools[1].state, 0);
      assert.equal(pools[1].capacity, 100);
      assert.equal(pools[1].used, 4);

      pools = oper.get();
      assert.lengthOf(pools, 3);
      assert.equal(pools[0].name, 'pool');
      assert.equal(pools[0].state, 'DEGRADED');
      assert.equal(pools[0].capacity, 100);
      assert.equal(pools[0].used, 99);
      assert.equal(pools[1].name, 'pool-to-create');
      assert.equal(pools[1].state, 'ONLINE');
      assert.equal(pools[1].capacity, 100);
      assert.equal(pools[1].used, 4);
      assert.equal(pools[2].name, 'inaccessible-pool');
      assert.equal(pools[2].state, 'OFFLINE');
    });

    it('should periodically sync the pools', async () => {
      // this should update the existing pool and create the missing
      srv = startMayastorServer([
        {
          name: 'pool',
          disks: ['/dev/sdb'],
          state: 0,
          capacity: 100,
          used: 10,
        },
      ]);

      // it will sync a node after one second
      poolsModule.syncInterval = 1;
      await sleep(1500);
      poolsModule.syncInterval = 60;

      let pools = srv.getPools();
      assert.lengthOf(pools, 2);
      assert.equal(pools[0].name, 'pool');
      assert.equal(pools[0].state, 0);
      assert.equal(pools[1].name, 'pool-to-create');
      assert.equal(pools[1].state, 0);

      pools = oper.get();
      assert.lengthOf(pools, 3);
      assert.equal(pools[0].name, 'pool');
      assert.equal(pools[0].state, 'ONLINE');
      assert.equal(pools[0].capacity, 100);
      assert.equal(pools[0].used, 10);
      assert.equal(pools[1].name, 'pool-to-create');
      assert.equal(pools[1].state, 'ONLINE');
      assert.equal(pools[1].capacity, 100);
      assert.equal(pools[1].used, 4);
      assert.equal(pools[2].name, 'inaccessible-pool');
      assert.equal(pools[2].state, 'OFFLINE');
    });

    it('should explicitly sync pools on a particular node', async () => {
      // disable periodic sync of pools
      clearInterval(oper.syncTimer);

      srv = startMayastorServer([
        {
          name: 'pool',
          disks: ['/dev/sdb'],
          state: 1,
          capacity: 100,
          used: 20,
        },
        {
          name: 'pool-to-destroy',
          disks: ['/dev/sdc'],
          state: 0,
          capacity: 100,
          used: 4,
        },
      ]);

      await oper.syncNode('node');

      let pools = srv.getPools();
      assert.lengthOf(pools, 2);
      assert.equal(pools[0].name, 'pool');
      assert.equal(pools[0].state, 1);
      assert.equal(pools[1].name, 'pool-to-create');
      assert.equal(pools[1].state, 0);

      pools = oper.get();
      assert.lengthOf(pools, 3);
      assert.equal(pools[0].name, 'pool');
      assert.equal(pools[0].state, 'DEGRADED');
      assert.equal(pools[0].capacity, 100);
      assert.equal(pools[0].used, 20);
      assert.equal(pools[1].name, 'pool-to-create');
      assert.equal(pools[1].state, 'ONLINE');
      assert.equal(pools[1].capacity, 100);
      assert.equal(pools[1].used, 4);
      assert.equal(pools[2].name, 'inaccessible-pool');
      assert.equal(pools[2].state, 'OFFLINE');
    });

    it('should explicitly sync all pools', async () => {
      // disable periodic sync of pools
      clearInterval(oper.syncTimer);

      srv = startMayastorServer([
        {
          name: 'pool',
          disks: ['/dev/sdb'],
          state: 0,
          capacity: 100,
          used: 99,
        },
        {
          name: 'pool-to-create',
          disks: ['/dev/sda'],
          state: 1,
          capacity: 100,
          used: 1,
        },
      ]);

      await oper.syncNode();

      let pools = srv.getPools();
      assert.lengthOf(pools, 2);
      assert.equal(pools[0].name, 'pool');
      assert.equal(pools[0].state, 0);
      assert.equal(pools[1].name, 'pool-to-create');
      assert.equal(pools[1].state, 1);

      pools = oper.get();
      assert.lengthOf(pools, 3);
      assert.equal(pools[0].name, 'pool');
      assert.equal(pools[0].state, 'ONLINE');
      assert.equal(pools[0].capacity, 100);
      assert.equal(pools[0].used, 99);
      assert.equal(pools[1].name, 'pool-to-create');
      assert.equal(pools[1].state, 'DEGRADED');
      assert.equal(pools[1].capacity, 100);
      assert.equal(pools[1].used, 1);
      assert.equal(pools[2].name, 'inaccessible-pool');
      assert.equal(pools[2].state, 'OFFLINE');
    });

    it('should put a pool to offline state if sync of the node failed', async () => {
      await oper.syncNode('node');

      let pools = oper.get();
      assert.lengthOf(pools, 3);
      assert.equal(pools[0].name, 'pool');
      assert.equal(pools[0].state, 'OFFLINE');
      assert.equal(pools[1].name, 'pool-to-create');
      assert.equal(pools[1].state, 'OFFLINE');
      assert.equal(pools[2].name, 'inaccessible-pool');
      assert.equal(pools[2].state, 'OFFLINE');
    });
  });
};
