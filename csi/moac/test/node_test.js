// Unit tests for the node object

'use strict';

const _ = require('lodash');
const expect = require('chai').expect;
const Node = require('../node');
const Nexus = require('../nexus');
const Pool = require('../pool');
const Replica = require('../replica');
const { MayastorServer } = require('./mayastor_mock');

const UUID = 'ba5e39e9-0c0e-4973-8a3a-0dccada09cbb';
const EGRESS_ENDPOINT = '127.0.0.1:12345';

module.exports = function() {
  var srv;
  var node;
  var pools = [
    {
      name: 'pool',
      disks: ['/dev/sdb', '/dev/sdc'],
      state: 0,
      capacity: 100,
      used: 14,
    },
  ];
  var replicas = [
    {
      uuid: UUID,
      pool: 'pool',
      size: 10,
      thin: false,
      share: 'REPLICA_NONE',
      uri: 'bdev:///' + UUID,
    },
  ];
  var nexus = [
    {
      uuid: UUID,
      size: 10,
      share: 0, // value of NEXUS_NBD for now.
      state: 'ONLINE',
      children: [
        {
          uri: 'bdev:///' + UUID,
          state: 'ONLINE',
        },
      ],
    },
  ];

  it('should stringify a node object', () => {
    let node = new Node('node-name');
    expect(node.toString()).to.equal('node-name');
  });

  describe('node events', function() {
    this.timeout(500);

    // start a fake mayastor server
    before(() => {
      srv = new MayastorServer(EGRESS_ENDPOINT, pools, replicas, nexus).start();
    });

    after(() => {
      if (srv) srv.stop();
      srv = null;
    });

    describe('initial sync', () => {
      after(() => {
        // properly shut down the node object
        if (node) {
          node.removeAllListeners();
          node.disconnect();
          node = null;
        }
      });

      it('should sync the state with storage node and emit event', done => {
        // the first sync takes sometimes >20ms so don't set the interval too low
        let syncInterval = 100;
        let syncCount = 0;
        let poolObjects = [];
        let replicaObjects = [];
        let nexusObjects = [];

        node = new Node('node', {
          syncPeriod: syncInterval,
          syncRetry: syncInterval,
          syncBadLimit: 0,
        });

        node.on('pool', ev => {
          expect(ev.eventType).to.equal('new');
          poolObjects.push(ev.object);
        });
        node.on('replica', ev => {
          expect(ev.eventType).to.equal('new');
          replicaObjects.push(ev.object);
        });
        node.on('nexus', ev => {
          expect(ev.eventType).to.equal('new');
          nexusObjects.push(ev.object);
        });
        node.on('node', ev => {
          expect(ev.eventType).to.equal('sync');
          expect(ev.object).to.equal(node);
          syncCount++;
        });
        node.connect(EGRESS_ENDPOINT);

        setTimeout(() => {
          // jshint ignore:start
          expect(node.isSynced()).to.be.true;
          // jshint ignore:end
          expect(syncCount).to.equal(1);

          expect(poolObjects).to.have.lengthOf(1);
          expect(poolObjects[0].name).to.equal('pool');
          expect(poolObjects[0].disks).to.have.lengthOf(2);
          expect(poolObjects[0].disks[0]).to.equal('/dev/sdb');
          expect(poolObjects[0].disks[1]).to.equal('/dev/sdc');
          expect(poolObjects[0].state).to.equal('ONLINE');
          expect(poolObjects[0].capacity).to.equal(100);
          expect(poolObjects[0].used).to.equal(14);

          expect(replicaObjects).to.have.lengthOf(1);
          expect(replicaObjects[0].uuid).to.equal(UUID);
          expect(replicaObjects[0].pool.name).to.equal('pool');
          expect(replicaObjects[0].size).to.equal(10);
          expect(replicaObjects[0].share).to.equal('REPLICA_NONE');
          expect(replicaObjects[0].uri).to.equal('bdev:///' + UUID);

          expect(nexusObjects).to.have.lengthOf(1);
          expect(nexusObjects[0].uuid).to.equal(UUID);
          expect(nexusObjects[0].size).to.equal(10);
          expect(nexusObjects[0].state).to.equal('ONLINE');
          expect(nexusObjects[0].children).to.have.lengthOf(1);
          expect(nexusObjects[0].children[0].uri).to.equal('bdev:///' + UUID);
          expect(nexusObjects[0].children[0].state).to.equal('ONLINE');

          done();
        }, syncInterval * 3);
      });
    });

    describe('new/mod/del events', () => {
      let syncInterval = 10;

      before(() => {
        // we make a deep copy of srv objects because the tests modify them
        srv.pools = _.cloneDeep(pools);
        srv.replicas = _.cloneDeep(replicas);
        srv.nexus = _.cloneDeep(nexus);
      });

      // wait for the initial sync
      beforeEach(done => {
        node = new Node('node', {
          syncPeriod: syncInterval,
          syncRetry: syncInterval,
          syncBadLimit: 0,
        });

        node.once('node', ev => {
          expect(ev.eventType).to.equal('sync');
          done();
        });
        node.connect(EGRESS_ENDPOINT);
      });

      afterEach(() => {
        if (node) {
          node.removeAllListeners();
          node.disconnect();
          node = null;
        }
        srv.pools = _.cloneDeep(pools);
        srv.replicas = _.cloneDeep(replicas);
        srv.nexus = _.cloneDeep(nexus);
      });

      it('should emit event when a replica is changed', done => {
        node.once('replica', ev => {
          expect(ev.eventType).to.equal('mod');
          expect(ev.object).to.be.an.instanceof(Replica);
          expect(ev.object.share).to.equal('REPLICA_NVMF');
          expect(ev.object.uri).to.equal('nvmf://blabla');
          done();
        });
        // modify replica property
        let newReplicas = _.cloneDeep(replicas);
        newReplicas[0].share = 'REPLICA_NVMF';
        newReplicas[0].uri = 'nvmf://blabla';
        srv.replicas = newReplicas;
      });

      it('should emit event when a replica is deleted', done => {
        node.once('replica', ev => {
          expect(ev.eventType).to.equal('del');
          expect(ev.object).to.be.an.instanceof(Replica);
          expect(ev.object.uuid).to.equal(UUID);
          done();
        });
        // empty the replica list
        srv.replicas = [];
      });

      it('should emit event when a replica is created', done => {
        let newUuid = 'f04015e1-3689-4e34-9bed-e2dbba1e4a27';
        node.once('replica', ev => {
          expect(ev.eventType).to.equal('new');
          expect(ev.object).to.be.an.instanceof(Replica);
          expect(ev.object.uuid).to.equal(newUuid);
          done();
        });
        // add a new replica
        srv.replicas.push({
          uuid: newUuid,
          pool: 'pool',
          size: 20,
          thin: false,
          share: 'REPLICA_NONE',
          uri: 'bdev:///' + newUuid,
        });
      });

      it('should not emit event when a replica that does not belong to any pool is created', done => {
        let newUuid = 'f04015e1-3689-4e34-9bed-e2dbba1e4a28';
        let emitted = false;

        node.once('replica', ev => {
          emitted = true;
          done(new Error('Event emitted'));
        });
        setTimeout(() => {
          if (!emitted) done();
        }, 2 * syncInterval);
        // add a new replica
        srv.replicas.push({
          uuid: newUuid,
          pool: 'unknown-pool',
          size: 20,
          thin: false,
          share: 'REPLICA_NONE',
          uri: 'bdev:///' + newUuid,
        });
      });

      it('should emit event when a pool is changed', done => {
        node.once('pool', ev => {
          expect(ev.eventType).to.equal('mod');
          expect(ev.object).to.be.an.instanceof(Pool);
          expect(ev.object.state).to.equal('DEGRADED');
          done();
        });
        // modify pool property
        let newPools = _.cloneDeep(pools);
        newPools[0].state = 1;
        srv.pools = newPools;
      });

      it('should emit event when a pool is deleted', done => {
        var replicaRemoved = false;

        node.once('replica', ev => {
          expect(ev.eventType).to.equal('del');
          expect(ev.object).to.be.an.instanceof(Replica);
          expect(ev.object.uuid).to.equal(UUID);
          replicaRemoved = true;
        });
        node.once('pool', ev => {
          expect(ev.eventType).to.equal('del');
          expect(ev.object).to.be.an.instanceof(Pool);
          expect(ev.object.name).to.equal('pool');
          // jshint ignore:start
          expect(replicaRemoved).to.be.true;
          // jshint ignore:end
          done();
        });
        // empty the pool list
        srv.pools = [];
      });

      it('should emit event when a pool with replica is created', done => {
        let newUuid = 'f04015e1-3689-4e34-9bed-e2dbba1e4a29';
        var poolAdded = false;

        node.once('pool', ev => {
          expect(ev.eventType).to.equal('new');
          expect(ev.object).to.be.an.instanceof(Pool);
          expect(ev.object.name).to.equal('new-pool');
          poolAdded = true;
        });
        node.once('replica', ev => {
          expect(ev.eventType).to.equal('new');
          expect(ev.object).to.be.an.instanceof(Replica);
          expect(ev.object.uuid).to.equal(newUuid);
          // jshint ignore:start
          expect(poolAdded).to.be.true;
          // jshint ignore:end
          done();
        });
        // add a new pool with a replica
        srv.pools.push({
          name: 'new-pool',
          disks: ['/dev/sda'],
          state: 0,
          capacity: 100,
          used: 14,
        });
        srv.replicas.push({
          uuid: newUuid,
          pool: 'new-pool',
          size: 10,
          thin: false,
          share: 'REPLICA_NONE',
          uri: 'bdev:///' + newUuid,
        });
      });

      it('should emit event when a nexus is changed', done => {
        node.once('nexus', ev => {
          expect(ev.eventType).to.equal('mod');
          expect(ev.object).to.be.an.instanceof(Nexus);
          expect(ev.object.uuid).to.equal(UUID);
          expect(ev.object.children).to.have.lengthOf(2);
          done();
        });
        // modify nexus property
        let newNexus = _.cloneDeep(nexus);
        newNexus[0].children = [
          {
            uri: 'bdev:///' + UUID,
            state: 'ONLINE',
          },
          {
            uri: 'nvmf:///something',
            state: 'ONLINE',
          },
        ];
        srv.nexus = newNexus;
      });

      it('should emit event when a nexus is deleted', done => {
        node.once('nexus', ev => {
          expect(ev.eventType).to.equal('del');
          expect(ev.object).to.be.an.instanceof(Nexus);
          expect(ev.object.uuid).to.equal(UUID);
          done();
        });
        // empty the nexus list
        srv.nexus = [];
      });

      it('should emit event when a nexus is created', done => {
        let newUuid = 'f04015e1-3689-4e34-9bed-e2dbba1e4a27';
        node.once('nexus', ev => {
          expect(ev.eventType).to.equal('new');
          expect(ev.object).to.be.an.instanceof(Nexus);
          expect(ev.object.uuid).to.equal(newUuid);
          done();
        });
        // add a new nexus
        srv.nexus.push({
          uuid: newUuid,
          size: 10,
          state: 'ONLINE',
          children: [],
        });
      });
    });
  });

  describe('sync failures', () => {
    // start a fake mayastor server
    beforeEach(() => {
      srv = new MayastorServer(EGRESS_ENDPOINT, pools, replicas, nexus).start();
    });

    afterEach(() => {
      if (node) {
        node.removeAllListeners();
        node.disconnect();
        node = null;
      }
      if (srv) srv.stop();
      srv = null;
    });

    it('should emit event for all objects when the node goes out of sync', done => {
      let syncInterval = 100;
      let offlineCount = 0;

      node = new Node('node', {
        syncPeriod: syncInterval,
        syncRetry: syncInterval,
        syncBadLimit: 0,
      });

      node.once('node', ev => {
        expect(ev.eventType).to.equal('sync');
        expect(ev.object).to.equal(node);
        let firstSync = Date.now();
        srv.stop();
        srv = null;

        node.once('pool', ev => {
          expect(ev.eventType).to.equal('mod');
          expect(ev.object.name).to.equal('pool');
          expect(ev.object.state).to.equal('OFFLINE');
          offline();
        });
        node.once('replica', ev => {
          expect(ev.eventType).to.equal('mod');
          expect(ev.object.uuid).to.equal(UUID);
          expect(ev.object.state).to.equal('OFFLINE');
          offline();
        });
        node.once('nexus', ev => {
          expect(ev.eventType).to.equal('mod');
          expect(ev.object.uuid).to.equal(UUID);
          expect(ev.object.state).to.equal('OFFLINE');
          offline();
        });

        function offline() {
          if (++offlineCount == 3) {
            // jshint ignore:start
            expect(node.isSynced()).to.be.false;
            // jshint ignore:end
            expect(Date.now() - firstSync).to.be.below(syncInterval * 1.5);
            done();
          }
        }
      });
      node.connect(EGRESS_ENDPOINT);
    });

    it('should tollerate n sync failures when configured so', done => {
      let syncPeriod = 200;
      let syncRetry = 40;

      node = new Node('node', {
        syncPeriod: syncPeriod,
        syncRetry: syncRetry,
        syncBadLimit: 2,
      });

      node.once('node', ev => {
        expect(ev.eventType).to.equal('sync');
        expect(ev.object).to.equal(node);
        let firstSync = Date.now();
        srv.stop();
        srv = null;

        node.once('pool', ev => {
          expect(ev.eventType).to.equal('mod');
          expect(ev.object.name).to.equal('pool');
          expect(ev.object.state).to.equal('OFFLINE');
          // jshint ignore:start
          expect(node.isSynced()).to.be.false;
          // jshint ignore:end
          expect(Date.now() - firstSync).to.be.above(
            syncPeriod + syncRetry * 2
          );
          expect(Date.now() - firstSync).to.be.below(
            syncPeriod + syncRetry * 4
          );
          done();
        });
      });
      node.connect(EGRESS_ENDPOINT);
    });

    it('should emit event when the node is synced after being disconnected', done => {
      let syncPeriod = 20;

      node = new Node('node', {
        syncPeriod: syncPeriod,
        syncRetry: syncPeriod,
        syncBadLimit: 0,
      });

      node.once('node', ev => {
        expect(ev.eventType).to.equal('sync');
        expect(ev.object).to.equal(node);
        // jshint ignore:start
        expect(node.isSynced()).to.be.true;
        // jshint ignore:end

        srv.stop();
        srv = null;

        node.once('pool', ev => {
          expect(ev.eventType).to.equal('mod');
          expect(ev.object.name).to.equal('pool');
          expect(ev.object.state).to.equal('OFFLINE');
          // jshint ignore:start
          expect(node.isSynced()).to.be.false;
          // jshint ignore:end

          srv = new MayastorServer(
            EGRESS_ENDPOINT,
            pools,
            replicas,
            nexus
          ).start();

          node.once('node', ev => {
            expect(ev.eventType).to.equal('sync');
            expect(ev.object).to.equal(node);
            // jshint ignore:start
            expect(node.isSynced()).to.be.true;
            // jshint ignore:end
            done();
          });
        });
      });
      node.connect(EGRESS_ENDPOINT);
    });
  });

  describe('object create', function() {
    var replica;
    var pool;
    var nexus;

    this.timeout(100);

    // start a fake mayastor server
    before(done => {
      srv = new MayastorServer(EGRESS_ENDPOINT, [], [], []).start();

      // wait for the initial sync
      node = new Node('node');
      node.once('node', ev => {
        expect(ev.eventType).to.equal('sync');
        done();
      });
      node.connect(EGRESS_ENDPOINT);
    });

    after(() => {
      if (node) {
        node.removeAllListeners();
        node.disconnect();
        node = null;
      }
      if (srv) srv.stop();
      srv = null;
    });

    it('should create a pool on the node', async () => {
      let emitted = false;

      node.once('pool', ev => {
        expect(ev.eventType).to.equal('new');
        expect(ev.object.name).to.equal('pool');
        expect(ev.object.disks).to.have.lengthOf(1);
        expect(ev.object.disks[0]).to.equal('/dev/sda');
        expect(node.pools).to.have.lengthOf(1);
        emitted = true;
      });

      pool = await node.createPool('pool', ['/dev/sda']);
      expect(pool).to.be.an.instanceof(Pool);
      // jshint ignore:start
      expect(emitted).to.be.true;
      // jshint ignore:end
    });

    it('should create a replica on the pool', async () => {
      let emitted = false;

      node.once('replica', ev => {
        expect(ev.eventType).to.equal('new');
        expect(ev.object.uuid).to.equal(UUID);
        expect(ev.object.size).to.equal(100);
        expect(pool.replicas).to.have.lengthOf(1);
        emitted = true;
      });
      replica = await pool.createReplica(UUID, 100);
      expect(replica).to.be.an.instanceof(Replica);
      // jshint ignore:start
      expect(emitted).to.be.true;
      // jshint ignore:end
    });

    it('should create a nexus on the node', async () => {
      let emitted = false;

      node.once('nexus', ev => {
        expect(ev.eventType).to.equal('new');
        expect(ev.object.uuid).to.equal(UUID);
        expect(ev.object.size).to.equal(100);
        expect(ev.object.children).to.have.lengthOf(1);
        expect(ev.object.children[0].uri).to.match(/^bdev:\/\/\//);
        expect(ev.object.children[0].state).to.equal('online');
        expect(node.nexus).to.have.lengthOf(1);
        emitted = true;
      });

      nexus = await node.createNexus(UUID, 100, [replica]);
      expect(nexus).to.be.an.instanceof(Nexus);
      // jshint ignore:start
      expect(emitted).to.be.true;
      // jshint ignore:end
    });
  });

  describe('object list', function() {
    const UUID1 = 'ba5e39e9-0c0e-4973-8a3a-0dccada09cb1';
    const UUID2 = 'ba5e39e9-0c0e-4973-8a3a-0dccada09cb2';
    const UUID3 = 'ba5e39e9-0c0e-4973-8a3a-0dccada09cb3';
    const UUID4 = 'ba5e39e9-0c0e-4973-8a3a-0dccada09cb4';

    // start a fake mayastor server
    before(done => {
      var pools = [
        {
          name: 'pool1',
          disks: ['/dev/sdb', '/dev/sdc'],
          state: 0,
          capacity: 100,
          used: 14,
        },
        {
          name: 'pool2',
          disks: ['/dev/sda'],
          state: 0,
          capacity: 100,
          used: 14,
        },
      ];
      var replicas = [
        {
          uuid: UUID1,
          pool: 'pool1',
          size: 10,
          thin: false,
          share: 'REPLICA_NONE',
          uri: 'bdev:///' + UUID,
        },
        {
          uuid: UUID2,
          pool: 'pool1',
          size: 10,
          thin: false,
          share: 'REPLICA_NONE',
          uri: 'bdev:///' + UUID,
        },
        {
          uuid: UUID3,
          pool: 'pool2',
          size: 10,
          thin: false,
          share: 'REPLICA_NONE',
          uri: 'bdev:///' + UUID,
        },
        // this replica does not belong to any pool so should be ignored
        {
          uuid: UUID4,
          pool: 'unknown-pool',
          size: 10,
          thin: false,
          share: 'REPLICA_NONE',
          uri: 'bdev:///' + UUID,
        },
      ];
      srv = new MayastorServer(EGRESS_ENDPOINT, pools, replicas, []).start();

      // wait for the initial sync
      node = new Node('node');
      node.once('node', ev => {
        expect(ev.eventType).to.equal('sync');
        done();
      });
      node.connect(EGRESS_ENDPOINT);
    });

    after(() => {
      if (node) {
        node.removeAllListeners();
        node.disconnect();
        node = null;
      }
      if (srv) srv.stop();
      srv = null;
    });

    it('should get a list of replicas on the node', () => {
      let replicas = node.getReplicas();
      expect(replicas).to.have.lengthOf(3);
      expect(replicas[0].uuid).to.equal(UUID1);
      expect(replicas[1].uuid).to.equal(UUID2);
      expect(replicas[2].uuid).to.equal(UUID3);
    });
  });
};
