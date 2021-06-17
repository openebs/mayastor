// Unit tests for the node object

'use strict';

/* eslint-disable no-unused-expressions */

const _ = require('lodash');
const expect = require('chai').expect;

const { Node } = require('../dist/node');
const { Nexus } = require('../dist/nexus');
const { Pool } = require('../dist/pool');
const { Replica } = require('../dist/replica');
const { grpcCode } = require('../dist/grpc_client');

const { MayastorServer } = require('./mayastor_mock');
const { shouldFailWith } = require('./utils');
const enums = require('./grpc_enums');

const UUID = 'ba5e39e9-0c0e-4973-8a3a-0dccada09cbb';
const MS_ENDPOINT = '127.0.0.1:12345';

module.exports = function () {
  let srv;
  let node;
  const pools = [
    {
      name: 'pool',
      disks: ['aio:///dev/sdb', 'aio:///dev/sdc'],
      state: enums.POOL_ONLINE,
      capacity: 100,
      used: 14
    }
  ];
  const replicas = [
    {
      uuid: UUID,
      pool: 'pool',
      size: 10,
      thin: false,
      share: 'REPLICA_NONE',
      uri: `bdev:///${UUID}?uuid=1`
    }
  ];
  const nexus = [
    {
      uuid: UUID,
      size: 10,
      share: 0, // value of NEXUS_NBD for now.
      state: 'NEXUS_ONLINE',
      children: [
        {
          uri: `bdev:///${UUID}?uuid=1`,
          state: 'CHILD_ONLINE'
        }
      ]
    }
  ];

  it('should stringify a node object', () => {
    const node = new Node('node-name');
    expect(node.toString()).to.equal('node-name');
  });

  describe('node events', function () {
    this.timeout(500);

    // start a fake mayastor server
    before((done) => {
      srv = new MayastorServer(MS_ENDPOINT, pools, replicas, nexus);
      srv.start(done);
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

      it('should sync the state with storage node and emit event', (done) => {
        // the first sync takes sometimes >20ms so don't set the interval too low
        const syncInterval = 100;
        const nodeEvents = [];
        const poolObjects = [];
        const replicaObjects = [];
        const nexusObjects = [];

        node = new Node('node', {
          syncPeriod: syncInterval,
          syncRetry: syncInterval,
          syncBadLimit: 0
        });

        node.on('pool', (ev) => {
          expect(ev.eventType).to.equal('new');
          poolObjects.push(ev.object);
        });
        node.on('replica', (ev) => {
          expect(ev.eventType).to.equal('new');
          replicaObjects.push(ev.object);
        });
        node.on('nexus', (ev) => {
          expect(ev.eventType).to.equal('new');
          nexusObjects.push(ev.object);
        });
        node.on('node', (ev) => {
          nodeEvents.push(ev);
        });
        node.connect(MS_ENDPOINT);

        setTimeout(() => {
          expect(node.isSynced()).to.be.true;
          expect(nodeEvents).to.have.lengthOf(1);
          expect(nodeEvents[0].eventType).to.equal('mod');
          expect(nodeEvents[0].object).to.equal(node);

          expect(poolObjects).to.have.lengthOf(1);
          expect(poolObjects[0].name).to.equal('pool');
          expect(poolObjects[0].disks).to.have.lengthOf(2);
          expect(poolObjects[0].disks[0]).to.equal('aio:///dev/sdb');
          expect(poolObjects[0].disks[1]).to.equal('aio:///dev/sdc');
          expect(poolObjects[0].state).to.equal('POOL_ONLINE');
          expect(poolObjects[0].capacity).to.equal(100);
          expect(poolObjects[0].used).to.equal(14);

          expect(replicaObjects).to.have.lengthOf(1);
          expect(replicaObjects[0].uuid).to.equal(UUID);
          expect(replicaObjects[0].pool.name).to.equal('pool');
          expect(replicaObjects[0].size).to.equal(10);
          expect(replicaObjects[0].share).to.equal('REPLICA_NONE');
          expect(replicaObjects[0].uri).to.equal(`bdev:///${UUID}?uuid=1`);

          expect(nexusObjects).to.have.lengthOf(1);
          expect(nexusObjects[0].uuid).to.equal(UUID);
          expect(nexusObjects[0].size).to.equal(10);
          expect(nexusObjects[0].state).to.equal('NEXUS_ONLINE');
          expect(nexusObjects[0].children).to.have.lengthOf(1);
          expect(nexusObjects[0].children[0].uri).to.equal(`bdev:///${UUID}?uuid=1`);
          expect(nexusObjects[0].children[0].state).to.equal('CHILD_ONLINE');

          done();
        }, syncInterval * 3);
      });
    });

    describe('new/mod/del events', () => {
      const syncInterval = 10;

      before(() => {
        // we make a deep copy of srv objects because the tests modify them
        srv.pools = _.cloneDeep(pools);
        srv.replicas = _.cloneDeep(replicas);
        srv.nexus = _.cloneDeep(nexus);
      });

      // wait for the initial sync
      beforeEach((done) => {
        node = new Node('node', {
          syncPeriod: syncInterval,
          syncRetry: syncInterval,
          syncBadLimit: 0
        });

        node.once('node', (ev) => {
          expect(ev.eventType).to.equal('mod');
          done();
        });
        node.connect(MS_ENDPOINT);
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

      it('should emit event when a replica is changed', (done) => {
        node.once('replica', (ev) => {
          expect(ev.eventType).to.equal('mod');
          expect(ev.object).to.be.an.instanceof(Replica);
          expect(ev.object.share).to.equal('REPLICA_NVMF');
          expect(ev.object.uri).to.equal('nvmf://blabla');
          done();
        });
        // modify replica property
        const newReplicas = _.cloneDeep(replicas);
        newReplicas[0].share = 'REPLICA_NVMF';
        newReplicas[0].uri = 'nvmf://blabla';
        srv.replicas = newReplicas;
      });

      it('should emit event when a replica is deleted', (done) => {
        node.once('replica', (ev) => {
          expect(ev.eventType).to.equal('del');
          expect(ev.object).to.be.an.instanceof(Replica);
          expect(ev.object.uuid).to.equal(UUID);
          done();
        });
        // empty the replica list
        srv.replicas = [];
      });

      it('should emit event when a replica is created', (done) => {
        const newUuid = 'f04015e1-3689-4e34-9bed-e2dbba1e4a27';
        node.once('replica', (ev) => {
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
          uri: `bdev:///${newUuid}?uuid=1234`
        });
      });

      it('should not emit event when a replica that does not belong to any pool is created', (done) => {
        const newUuid = 'f04015e1-3689-4e34-9bed-e2dbba1e4a28';
        let emitted = false;

        node.once('replica', (ev) => {
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
          uri: `bdev:///${newUuid}?uuid=1234`
        });
      });

      it('should emit event when a pool is changed', (done) => {
        node.once('pool', (ev) => {
          expect(ev.eventType).to.equal('mod');
          expect(ev.object).to.be.an.instanceof(Pool);
          expect(ev.object.state).to.equal('POOL_DEGRADED');
          done();
        });
        // modify pool property
        const newPools = _.cloneDeep(pools);
        newPools[0].state = enums.POOL_DEGRADED;
        srv.pools = newPools;
      });

      it('should emit event when a pool is deleted', (done) => {
        let replicaRemoved = false;

        node.once('replica', (ev) => {
          expect(ev.eventType).to.equal('del');
          expect(ev.object).to.be.an.instanceof(Replica);
          expect(ev.object.uuid).to.equal(UUID);
          replicaRemoved = true;
        });
        node.once('pool', (ev) => {
          expect(ev.eventType).to.equal('del');
          expect(ev.object).to.be.an.instanceof(Pool);
          expect(ev.object.name).to.equal('pool');
          expect(replicaRemoved).to.be.true;
          done();
        });
        // empty the pool list
        srv.pools = [];
      });

      it('should emit event when a pool with replica is created', (done) => {
        const newUuid = 'f04015e1-3689-4e34-9bed-e2dbba1e4a29';
        let poolAdded = false;

        node.once('pool', (ev) => {
          expect(ev.eventType).to.equal('new');
          expect(ev.object).to.be.an.instanceof(Pool);
          expect(ev.object.name).to.equal('new-pool');
          poolAdded = true;
        });
        node.once('replica', (ev) => {
          expect(ev.eventType).to.equal('new');
          expect(ev.object).to.be.an.instanceof(Replica);
          expect(ev.object.uuid).to.equal(newUuid);
          expect(poolAdded).to.be.true;
          done();
        });
        // add a new pool with a replica
        srv.pools.push({
          name: 'new-pool',
          disks: ['/dev/sda'],
          state: enums.POOL_ONLINE,
          capacity: 100,
          used: 14
        });
        srv.replicas.push({
          uuid: newUuid,
          pool: 'new-pool',
          size: 10,
          thin: false,
          share: 'REPLICA_NONE',
          uri: `bdev:///${newUuid}?uuid=1234`
        });
      });

      it('should emit event when a nexus is changed', (done) => {
        node.once('nexus', (ev) => {
          expect(ev.eventType).to.equal('mod');
          expect(ev.object).to.be.an.instanceof(Nexus);
          expect(ev.object.uuid).to.equal(UUID);
          expect(ev.object.children).to.have.lengthOf(2);
          done();
        });
        // modify nexus property
        const newNexus = _.cloneDeep(nexus);
        newNexus[0].children = [
          {
            uri: `bdev:///${UUID}?uuid=1`,
            state: 'CHILD_ONLINE'
          },
          {
            uri: 'nvmf:///something',
            state: 'CHILD_ONLINE'
          }
        ];
        srv.nexus = newNexus;
      });

      it('should emit event when a nexus is deleted', (done) => {
        node.once('nexus', (ev) => {
          expect(ev.eventType).to.equal('del');
          expect(ev.object).to.be.an.instanceof(Nexus);
          expect(ev.object.uuid).to.equal(UUID);
          done();
        });
        // empty the nexus list
        srv.nexus = [];
      });

      it('should emit event when a nexus is created', (done) => {
        const newUuid = 'f04015e1-3689-4e34-9bed-e2dbba1e4a27';
        node.once('nexus', (ev) => {
          expect(ev.eventType).to.equal('new');
          expect(ev.object).to.be.an.instanceof(Nexus);
          expect(ev.object.uuid).to.equal(newUuid);
          done();
        });
        // add a new nexus
        srv.nexus.push({
          uuid: newUuid,
          size: 10,
          state: 'NEXUS_ONLINE',
          children: []
        });
      });
    });
  });

  describe('sync failures', () => {
    // start a fake mayastor server
    beforeEach((done) => {
      srv = new MayastorServer(MS_ENDPOINT, pools, replicas, nexus);
      srv.start(done);
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

    it('should emit event for all objects when the node goes out of sync', (done) => {
      const syncInterval = 100;
      let offlineCount = 0;

      node = new Node('node', {
        syncPeriod: syncInterval,
        syncRetry: syncInterval,
        syncBadLimit: 0
      });

      node.once('node', (ev) => {
        expect(ev.eventType).to.equal('mod');
        expect(ev.object).to.equal(node);
        const firstSync = Date.now();
        srv.stop();
        srv = null;

        node.once('pool', (ev) => {
          expect(ev.eventType).to.equal('mod');
          expect(ev.object.name).to.equal('pool');
          expect(ev.object.state).to.equal('POOL_OFFLINE');
          offline();
        });
        node.once('replica', (ev) => {
          expect(ev.eventType).to.equal('mod');
          expect(ev.object.uuid).to.equal(UUID);
          expect(ev.object.isOffline()).to.be.true;
          offline();
        });
        node.once('nexus', (ev) => {
          expect(ev.eventType).to.equal('mod');
          expect(ev.object.uuid).to.equal(UUID);
          expect(ev.object.state).to.equal('NEXUS_OFFLINE');
          offline();
        });

        function offline () {
          if (++offlineCount === 3) {
            expect(node.isSynced()).to.be.false;
            expect(Date.now() - firstSync).to.be.below(syncInterval * 1.5);
            done();
          }
        }
      });
      node.connect(MS_ENDPOINT);
    });

    it('should tollerate n sync failures when configured so', (done) => {
      const syncPeriod = 200;
      const syncRetry = 40;

      node = new Node('node', {
        syncPeriod: syncPeriod,
        syncRetry: syncRetry,
        syncBadLimit: 2
      });

      node.once('node', (ev) => {
        expect(ev.eventType).to.equal('mod');
        expect(ev.object).to.equal(node);
        const firstSync = Date.now();
        srv.stop();
        srv = null;

        node.once('pool', (ev) => {
          expect(ev.eventType).to.equal('mod');
          expect(ev.object.name).to.equal('pool');
          expect(ev.object.state).to.equal('POOL_OFFLINE');
          expect(node.isSynced()).to.be.false;
          expect(Date.now() - firstSync).to.be.above(
            syncPeriod + syncRetry * 2 - 1
          );
          expect(Date.now() - firstSync).to.be.below(
            syncPeriod + syncRetry * 4 + 1
          );
          done();
        });
      });
      node.connect(MS_ENDPOINT);
    });

    it('should emit event when the node is synced after being disconnected', (done) => {
      const syncPeriod = 20;

      node = new Node('node', {
        syncPeriod: syncPeriod,
        syncRetry: syncPeriod,
        syncBadLimit: 0
      });

      node.once('node', (ev) => {
        expect(ev.eventType).to.equal('mod');
        expect(ev.object).to.equal(node);
        expect(node.isSynced()).to.be.true;

        srv.stop();
        srv = null;

        node.once('pool', (ev) => {
          expect(ev.eventType).to.equal('mod');
          expect(ev.object.name).to.equal('pool');
          expect(ev.object.state).to.equal('POOL_OFFLINE');
          expect(node.isSynced()).to.be.false;

          srv = new MayastorServer(
            MS_ENDPOINT,
            pools,
            replicas,
            nexus
          );
          srv.start((err) => {
            if (err) return done(err);

            // pool/replica/nexus event should be emitted before node event and
            // node should be online when emitting those events.
            let poolEvent;
            node.once('pool', (ev) => {
              expect(node.isSynced()).to.be.true;
              poolEvent = ev;
            });
            node.once('node', (ev) => {
              expect(poolEvent).not.to.be.undefined;
              expect(ev.eventType).to.equal('mod');
              expect(ev.object).to.equal(node);
              expect(node.isSynced()).to.be.true;
              done();
            });
          });
        });
      });
      node.connect(MS_ENDPOINT);
    });
  });

  describe('object create', function () {
    const DELAY_MS = 100;
    let replica;
    let pool;
    let nexus;

    this.timeout(500);

    // start a fake mayastor server
    before((done) => {
      srv = new MayastorServer(MS_ENDPOINT, [], [], [], DELAY_MS);
      srv.start((err) => {
        if (err) return done(err);
        // wait for the initial sync
        node = new Node('node');
        node.once('node', (ev) => {
          expect(ev.eventType).to.equal('mod');
          done();
        });
        node.connect(MS_ENDPOINT);
      });
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

      node.once('pool', (ev) => {
        expect(ev.eventType).to.equal('new');
        expect(ev.object.name).to.equal('pool');
        expect(ev.object.disks).to.have.lengthOf(1);
        expect(ev.object.disks[0]).to.equal('aio:///dev/sda');
        expect(node.pools).to.have.lengthOf(1);
        emitted = true;
      });

      pool = await node.createPool('pool', ['/dev/sda']);
      expect(pool).to.be.an.instanceof(Pool);
      expect(emitted).to.be.true;
    });

    it('should create a replica on the pool', async () => {
      let emitted = false;

      node.once('replica', (ev) => {
        expect(ev.eventType).to.equal('new');
        expect(ev.object.uuid).to.equal(UUID);
        expect(ev.object.size).to.equal(100);
        expect(pool.replicas).to.have.lengthOf(1);
        emitted = true;
      });
      replica = await pool.createReplica(UUID, 100);
      expect(replica).to.be.an.instanceof(Replica);
      expect(emitted).to.be.true;
    });

    it('should create a nexus on the node', async () => {
      let emitted = false;

      node.once('nexus', (ev) => {
        expect(ev.eventType).to.equal('new');
        expect(ev.object.uuid).to.equal(UUID);
        expect(ev.object.size).to.equal(100);
        expect(ev.object.children).to.have.lengthOf(1);
        expect(ev.object.children[0].uri).to.match(/^bdev:\/\/\//);
        expect(ev.object.children[0].state).to.equal('CHILD_ONLINE');
        expect(node.nexus).to.have.lengthOf(1);
        emitted = true;
      });

      nexus = await node.createNexus(UUID, 100, [replica]);
      expect(nexus).to.be.an.instanceof(Nexus);
      expect(emitted).to.be.true;
    });

    it('should timeout on a call that takes too long', async () => {
      const UUID2 = 'ba5e39e9-0c0e-4973-8a3a-0dccada09cb2';
      await shouldFailWith(
        grpcCode.DEADLINE_EXCEEDED,
        () => node.call(
          'createNexus',
          {
            uuid: UUID2,
            size: 100,
            children: [replica.uri]
          },
          DELAY_MS / 2
        )
      );
    });
  });

  describe('object list', function () {
    const UUID1 = 'ba5e39e9-0c0e-4973-8a3a-0dccada09cb1';
    const UUID2 = 'ba5e39e9-0c0e-4973-8a3a-0dccada09cb2';
    const UUID3 = 'ba5e39e9-0c0e-4973-8a3a-0dccada09cb3';
    const UUID4 = 'ba5e39e9-0c0e-4973-8a3a-0dccada09cb4';

    // start a fake mayastor server
    before((done) => {
      const pools = [
        {
          name: 'pool1',
          disks: ['/dev/sdb', '/dev/sdc'],
          state: enums.POOL_ONLINE,
          capacity: 100,
          used: 14
        },
        {
          name: 'pool2',
          disks: ['/dev/sda'],
          state: enums.POOL_ONLINE,
          capacity: 100,
          used: 14
        }
      ];
      const replicas = [
        {
          uuid: UUID1,
          pool: 'pool1',
          size: 10,
          thin: false,
          share: 'REPLICA_NONE',
          uri: `bdev:///${UUID1}?uuid=1`
        },
        {
          uuid: UUID2,
          pool: 'pool1',
          size: 10,
          thin: false,
          share: 'REPLICA_NONE',
          uri: `bdev:///${UUID2}?uuid=2`
        },
        {
          uuid: UUID3,
          pool: 'pool2',
          size: 10,
          thin: false,
          share: 'REPLICA_NONE',
          uri: `bdev:///${UUID3}?uuid=3`
        },
        // this replica does not belong to any pool so should be ignored
        {
          uuid: UUID4,
          pool: 'unknown-pool',
          size: 10,
          thin: false,
          share: 'REPLICA_NONE',
          uri: `bdev:///${UUID4}?uuid=4`
        }
      ];
      srv = new MayastorServer(MS_ENDPOINT, pools, replicas, []);
      srv.start((err) => {
        if (err) return done(err);
        // wait for the initial sync
        node = new Node('node');
        node.once('node', (ev) => {
          expect(ev.eventType).to.equal('mod');
          done();
        });
        node.connect(MS_ENDPOINT);
      });
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
      const replicas = node.getReplicas();
      expect(replicas).to.have.lengthOf(3);
      expect(replicas[0].uuid).to.equal(UUID1);
      expect(replicas[1].uuid).to.equal(UUID2);
      expect(replicas[2].uuid).to.equal(UUID3);
    });
  });
};
