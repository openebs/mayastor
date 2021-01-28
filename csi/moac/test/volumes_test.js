// Unit tests for the volume manager and volume object.
//
// Volume ensure method is tested here rather than in volume tests because
// it's easier to test with volume manager, which routes events from registry
// to volumes.

'use strict';

const _ = require('lodash');
const expect = require('chai').expect;
const sinon = require('sinon');
const { Nexus } = require('../nexus');
const { Node } = require('../node');
const { Pool } = require('../pool');
const Registry = require('../registry');
const { Replica } = require('../replica');
const { Volume } = require('../volume');
const { Volumes } = require('../volumes');
const { GrpcCode } = require('../grpc_client');
const { shouldFailWith, waitUntil } = require('./utils');
const enums = require('./grpc_enums');
const sleep = require('sleep-promise');

const UUID = 'ba5e39e9-0c0e-4973-8a3a-0dccada09cbb';
const UUID2 = 'aa5e39e9-0c0e-4973-8a3a-0dccada09cbc';
const EYE_BLINK_MS = 30;

module.exports = function () {
  let registry, volumes;
  let pool1, pool2, pool3;
  let node1, node2, node3;
  let stub1, stub2, stub3;
  let nexus, replica1, replica2;
  let volume;
  let volEvents;
  let isSynced1, isSynced2, isSynced3;

  // Create pristine test env with 3 pools on 3 nodes
  function createTestEnv () {
    registry = new Registry();
    volumes = new Volumes(registry);
    node1 = new Node('node1');
    node2 = new Node('node2');
    node3 = new Node('node3');
    isSynced1 = sinon.stub(node1, 'isSynced');
    isSynced1.returns(true);
    isSynced2 = sinon.stub(node2, 'isSynced');
    isSynced2.returns(true);
    isSynced3 = sinon.stub(node3, 'isSynced');
    isSynced3.returns(true);

    // pools sorted from the most to the least preferred
    pool1 = new Pool({
      name: 'pool1',
      disks: [],
      capacity: 100,
      used: 0,
      state: 'POOL_ONLINE'
    });
    pool2 = new Pool({
      name: 'pool2',
      disks: [],
      capacity: 100,
      used: 4,
      state: 'POOL_ONLINE'
    });
    pool3 = new Pool({
      name: 'pool3',
      disks: [],
      capacity: 100,
      used: 4,
      state: 'POOL_DEGRADED'
    });
    // we don't want connect and disconnect to do anything
    sinon.spy(node1, 'connect');
    sinon.spy(node2, 'connect');
    sinon.spy(node3, 'connect');
    sinon.spy(node1, 'disconnect');
    sinon.spy(node2, 'disconnect');
    sinon.spy(node3, 'disconnect');
    stub1 = sinon.stub(node1, 'call');
    stub2 = sinon.stub(node2, 'call');
    stub3 = sinon.stub(node3, 'call');

    registry._registerNode(node1);
    registry._registerNode(node2);
    registry._registerNode(node3);
    node1._registerPool(pool1);
    node2._registerPool(pool2);
    node3._registerPool(pool3);

    volEvents = [];
    volumes.on('volume', (ev) => {
      volEvents.push(_.cloneDeep(ev));
    });
  }

  // Create a setup with standard env (from createTestEnv()) and on top of that
  // a volume with two replicas on node1 and node2 and nexus on node1 if the
  // volume should be created in published state.
  async function setUpReferenceEnv (published) {
    createTestEnv();

    replica1 = new Replica({
      uuid: UUID,
      size: 95,
      share: 'REPLICA_NONE',
      uri: `bdev:///${UUID}`
    });
    pool1.registerReplica(replica1);

    replica2 = new Replica({
      uuid: UUID,
      size: 95,
      share: 'REPLICA_NVMF',
      uri: `nvmf://remote/${UUID}`
    });
    pool2.registerReplica(replica2);

    if (published) {
      nexus = new Nexus({
        uuid: UUID,
        size: 95,
        deviceUri: 'nvmf://host/nqn',
        state: 'NEXUS_ONLINE',
        children: [
          {
            uri: `bdev:///${UUID}`,
            state: 'CHILD_ONLINE',
            rebuildProgress: 0
          },
          {
            uri: `nvmf://remote/${UUID}`,
            state: 'CHILD_ONLINE',
            rebuildProgress: 0
          }
        ]
      });
      node1._registerNexus(nexus);
    }

    // Fake the volume
    volume = new Volume(UUID, registry, (type) => {
      volumes.emit('volume', {
        eventType: type,
        object: volume
      });
    }, {
      replicaCount: 2,
      preferredNodes: [],
      requiredNodes: [],
      requiredBytes: 90,
      limitBytes: 110,
      protocol: 'nvmf'
    }, 'pending', 95, published ? 'node1' : undefined);
    volumes.volumes[UUID] = volume;

    volumes.start();
    await waitUntil(() => {
      return volEvents.length >= (published ? 3 : 2);
    }, 'volume events');
    volume.state = 'healthy';
  }

  function tearDownReferenceEnv () {}

  // Each test creates a volume so the setup needs to run for each case.
  describe('create volume', function () {
    // this creates an env with 3 pools on 3 nodes without any replica and nexus
    beforeEach(createTestEnv);

    afterEach(() => {
      volumes.stop();
    });

    it('should return error when there is no suitable pool', async () => {
      volumes.start();
      await shouldFailWith(GrpcCode.RESOURCE_EXHAUSTED, () =>
        // node2 and node3 are too small
        volumes.createVolume(UUID, {
          replicaCount: 3,
          preferredNodes: [],
          requiredNodes: ['node2', 'node3'],
          requiredBytes: 100,
          limitBytes: 110,
          protocol: 'nvmf'
        })
      );
      expect(volEvents).to.have.lengthOf(3);
      expect(volEvents[0].eventType).to.equal('new');
      expect(volEvents[1].eventType).to.equal('mod');
      expect(volEvents[2].eventType).to.equal('del');
      expect(volEvents[2].object.uuid).to.equal(UUID);
      expect(volEvents[2].object.state).to.equal('destroyed');
    });

    it('should set the size of the volume to required minimum if limit is not set', async () => {
      // on node 1 is created replica and nexus
      stub1.onCall(0).resolves({
        uuid: UUID,
        pool: 'pool1',
        size: 90,
        thin: false,
        share: 'REPLICA_NONE',
        uri: 'bdev:///' + UUID
      });
      stub1.onCall(1).resolves({
        uuid: UUID,
        size: 90,
        state: 'NEXUS_ONLINE',
        children: [
          {
            uri: 'bdev:///' + UUID,
            state: 'CHILD_ONLINE',
            rebuildProgress: 0
          }
        ]
      });

      volumes.start();
      volume = await volumes.createVolume(UUID, {
        replicaCount: 1,
        preferredNodes: [],
        requiredNodes: [],
        requiredBytes: 90,
        limitBytes: 0,
        protocol: 'nvmf'
      });
      await waitUntil(() => volume.state === 'healthy', 'healthy volume');
      expect(volume.size).to.equal(90);
      sinon.assert.calledWithMatch(stub1.firstCall, 'createReplica', {
        uuid: UUID,
        pool: 'pool1',
        size: 90,
        thin: false,
        share: 'REPLICA_NONE'
      });
      expect(volEvents).to.have.lengthOf(5);
    });

    it('should limit the size of created volume', async () => {
      // on node 1 is created replica and nexus
      stub1.onCall(0).resolves({
        uuid: UUID,
        pool: 'pool1',
        size: 50,
        thin: false,
        share: 'REPLICA_NONE',
        uri: 'bdev:///' + UUID
      });
      stub1.onCall(1).resolves({
        uuid: UUID,
        size: 50,
        state: 'NEXUS_ONLINE',
        children: [
          {
            uri: 'bdev:///' + UUID,
            state: 'CHILD_ONLINE',
            rebuildProgress: 0
          }
        ]
      });

      volumes.start();
      volume = await volumes.createVolume(UUID, {
        replicaCount: 1,
        preferredNodes: [],
        requiredNodes: [],
        requiredBytes: 10,
        limitBytes: 50,
        protocol: 'nvmf'
      });
      await waitUntil(() => volume.state === 'healthy', 'healthy volume');
      expect(volume.size).to.equal(50);
      sinon.assert.calledWithMatch(stub1.firstCall, 'createReplica', {
        uuid: UUID,
        pool: 'pool1',
        size: 50,
        thin: false,
        share: 'REPLICA_NONE'
      });
      expect(volEvents).to.have.lengthOf(5);
    });

    it('should fail if the size is zero', async () => {
      volumes.start();
      await shouldFailWith(GrpcCode.INVALID_ARGUMENT, () =>
        volumes.createVolume(UUID, {
          replicaCount: 1,
          preferredNodes: [],
          requiredNodes: [],
          requiredBytes: 0,
          limitBytes: 0,
          protocol: 'nvmf'
        })
      );
      sinon.assert.notCalled(stub1);
      sinon.assert.notCalled(stub2);
      sinon.assert.notCalled(stub3);
      expect(volEvents).to.have.lengthOf(0);
    });

    it('should create the volume and include pre-existing replicas', async () => {
      stub1.onCall(0).resolves({
        uuid: UUID,
        size: 10,
        state: 'NEXUS_ONLINE',
        children: [
          {
            uri: `bdev:///${UUID}`,
            state: 'CHILD_FAULTED',
            rebuildProgress: 0
          }
        ]
      });
      const replica = new Replica({
        uuid: UUID,
        size: 10,
        share: 'REPLICA_NONE',
        uri: `bdev:///${UUID}`
      });
      replica.pool = { node: node1 };
      const getReplicaSetStub = sinon.stub(registry, 'getReplicaSet');
      getReplicaSetStub.returns([replica]);

      volumes.start();
      volume = await volumes.createVolume(UUID, {
        replicaCount: 1,
        preferredNodes: [],
        requiredNodes: [],
        requiredBytes: 10,
        limitBytes: 50,
        protocol: 'nvmf'
      });
      await waitUntil(() => volume.state === 'faulted', 'faulted volume');
      sinon.assert.notCalled(stub2);
      sinon.assert.notCalled(stub3);
      sinon.assert.calledOnce(stub1);
      sinon.assert.calledWithMatch(stub1.firstCall, 'createNexus', {
        uuid: UUID,
        size: 10,
        children: [`bdev:///${UUID}`]
      });
      expect(Object.keys(volume.replicas)).to.have.lengthOf(1);
      expect(Object.values(volume.replicas)[0]).to.equal(replica);
      expect(volEvents).to.have.lengthOf(4);
      expect(volEvents[0].eventType).to.equal('new');
      expect(volEvents[1].eventType).to.equal('mod');
      expect(volEvents[2].eventType).to.equal('mod');
      expect(volEvents[3].eventType).to.equal('mod');
    });

    it('should create the volume object and include pre-existing nexus', async () => {
      // on node 1 is created replica and nexus
      stub1.onCall(0).resolves({
        uuid: UUID,
        pool: 'pool1',
        size: 10,
        thin: false,
        share: 'REPLICA_NONE',
        uri: `bdev:///${UUID}`
      });
      stub1.onCall(1).resolves({
        uri: `bdev:///${UUID}`,
        state: 'CHILD_DEGRADED',
        rebuildProgress: 10
      });
      const nexus = new Nexus({
        uuid: UUID,
        size: 10,
        deviceUri: '',
        state: 'NEXUS_ONLINE',
        children: [
          {
            uri: `nvmf://blabla/${UUID}`,
            state: 'CHILD_ONLINE',
            rebuildProgress: 0
          }
        ]
      });
      nexus.node = node1;
      const getNexusStub = sinon.stub(registry, 'getNexus');
      getNexusStub.returns(nexus);

      volumes.start();
      volume = await volumes.createVolume(UUID, {
        replicaCount: 1,
        preferredNodes: [],
        requiredNodes: [],
        requiredBytes: 10,
        limitBytes: 50,
        protocol: 'nvmf'
      });
      await waitUntil(
        () =>
          Object.keys(nexus.children).length === 2 &&
          volume.state === 'degraded',
        'replica and degraded volume'
      );
      sinon.assert.notCalled(stub2);
      sinon.assert.notCalled(stub3);
      sinon.assert.calledTwice(stub1);
      sinon.assert.calledWithMatch(stub1.firstCall, 'createReplica', {
        uuid: UUID,
        size: 10,
        pool: 'pool1',
        thin: false,
        share: 'REPLICA_NONE'
      });
      sinon.assert.calledWithMatch(stub1.secondCall, 'addChildNexus', {
        uuid: UUID,
        uri: `bdev:///${UUID}`,
        norebuild: false
      });
      expect(Object.keys(volume.replicas)).to.have.lengthOf(1);
      expect(volume.nexus).to.equal(nexus);
      expect(volEvents).to.have.lengthOf(5);
    });

    it('should distribute nexuses evenly over available nodes', async () => {
      const replica1 = new Replica({
        uuid: UUID,
        size: 95,
        share: 'REPLICA_NONE',
        uri: `bdev:///${UUID}`
      });
      const replica2 = new Replica({
        uuid: UUID,
        size: 95,
        share: 'REPLICA_NONE',
        uri: `bdev:///${UUID}`
      });
      const replica3 = new Replica({
        uuid: UUID,
        size: 95,
        share: 'REPLICA_NONE',
        uri: `bdev:///${UUID}`
      });
      const replica4 = new Replica({
        uuid: UUID2,
        size: 95,
        share: 'REPLICA_NONE',
        uri: `bdev:///${UUID2}`
      });
      const replica5 = new Replica({
        uuid: UUID2,
        size: 95,
        share: 'REPLICA_NONE',
        uri: `bdev:///${UUID2}`
      });
      const replica6 = new Replica({
        uuid: UUID2,
        size: 95,
        share: 'REPLICA_NONE',
        uri: `bdev:///${UUID2}`
      });
      replica1.pool = pool1;
      replica2.pool = pool2;
      replica3.pool = pool3;
      replica4.pool = pool1;
      replica5.pool = pool2;
      replica6.pool = pool3;

      // Fake the volume
      volume = new Volume(UUID, registry, () => {}, {
        replicaCount: 3,
        preferredNodes: [],
        requiredNodes: [],
        requiredBytes: 90,
        limitBytes: 110,
        protocol: 'nvmf'
      });
      volume.newReplica(replica1);
      volume.newReplica(replica2);
      volume.newReplica(replica3);

      const volume2 = new Volume(UUID2, registry, () => {}, {
        replicaCount: 3,
        preferredNodes: [],
        requiredNodes: [],
        requiredBytes: 90,
        limitBytes: 110,
        protocol: 'nvmf'
      });
      volume2.newReplica(replica4);
      volume2.newReplica(replica5);
      volume2.newReplica(replica6);
      volumes.volumes[UUID] = volume;
      volumes.volumes[UUID2] = volume2;
      volume.state = 'healthy';
      volume2.state = 'healthy';

      volumes.start();

      // set share pcols for replicas of the first volume
      stub2.onCall(0).resolves({ uri: `nvmf://${UUID}` });
      stub3.onCall(0).resolves({ uri: `nvmf://${UUID}` });
      // create first nexus reply
      stub1.onCall(0).resolves({
        uuid: UUID,
        deviceUri: '',
        size: 95,
        state: 'NEXUS_ONLINE',
        children: [{
          uri: `bdev:///${UUID}`,
          state: 'CHILD_ONLINE'
        }, {
          uri: `nvmf://${UUID}`,
          state: 'CHILD_ONLINE'
        }, {
          uri: `nvmf://${UUID}`,
          state: 'CHILD_ONLINE'
        }]
      });
      // nexus publish reply
      stub1.onCall(1).resolves({
        deviceUri: `nvmf://${UUID}`
      });

      // publish the first volume
      let uri = await volume.publish('nvmf');
      expect(uri).to.equal(`nvmf://${UUID}`);
      expect(volume.publishedOn).to.equal('node1');

      // set share pcols for replicas of the first volume
      stub1.onCall(2).resolves({ uri: `nvmf://${UUID2}` });
      stub3.onCall(1).resolves({ uri: `nvmf://${UUID2}` });
      // create second nexus reply
      stub2.onCall(1).resolves({
        uuid: UUID2,
        deviceUri: '',
        size: 95,
        state: 'NEXUS_ONLINE',
        children: [{
          uri: `bdev:///${UUID2}`,
          state: 'CHILD_ONLINE'
        }, {
          uri: `nvmf://${UUID2}`,
          state: 'CHILD_ONLINE'
        }, {
          uri: `nvmf://${UUID2}`,
          state: 'CHILD_ONLINE'
        }]
      });
      // nexus publish reply
      stub2.onCall(2).resolves({
        deviceUri: `nvmf://${UUID2}`
      });

      // publish the second volume - should be on a different node
      uri = await volume2.publish('nvmf');
      expect(uri).to.equal(`nvmf://${UUID2}`);
      expect(volume2.publishedOn).to.equal('node2');
    });
  });

  describe('import volume', function () {
    // this creates an env with 3 pools on 3 nodes without any replica and nexus
    beforeEach(createTestEnv);

    afterEach(() => {
      volumes.stop();
    });

    const volumeSpec = {
      replicaCount: 1,
      preferredNodes: [],
      requiredNodes: [],
      requiredBytes: 10,
      limitBytes: 50,
      protocol: 'nvmf'
    };

    it('should import a volume and fault it if there are no replicas', async () => {
      volumes.start();
      volume = await volumes.importVolume(UUID, volumeSpec, { size: 40 });
      // give FSA a chance to run
      await sleep(EYE_BLINK_MS);
      expect(volume.state).to.equal('faulted');
      expect(Object.keys(volume.replicas)).to.have.lengthOf(0);
    });

    it('should import a volume without nexus', async () => {
      // we use two replicas in this test because it uncovers some corner cases
      const customVolumeSpec = {
        replicaCount: 2,
        preferredNodes: [],
        requiredNodes: [],
        requiredBytes: 10,
        limitBytes: 50,
        protocol: 'nvmf'
      };
      const replica1 = new Replica({
        uuid: UUID,
        size: 40,
        share: 'REPLICA_NONE',
        uri: `bdev:///${UUID}`
      });
      replica1.pool = { node: node1 };
      const replica2 = new Replica({
        uuid: UUID,
        size: 40,
        share: 'REPLICA_NVMF',
        uri: `nvmf://${UUID}`
      });
      replica2.pool = { node: node2 };
      const getReplicaSetStub = sinon.stub(registry, 'getReplicaSet');
      getReplicaSetStub.returns([replica1, replica2]);

      // import creates a nexus
      stub1.onCall(0).resolves({
        uuid: UUID,
        deviceUri: '',
        size: 95,
        state: 'NEXUS_ONLINE',
        children: [{
          uri: `bdev:///${UUID}`,
          state: 'CHILD_ONLINE'
        }, {
          uri: `nvmf://${UUID}`,
          state: 'CHILD_ONLINE'
        }]
      });
      // and then it is destroyed again
      stub1.onCall(1).resolves({});

      volumes.start();
      volume = await volumes.importVolume(UUID, customVolumeSpec, { size: 40 });
      expect(volume.state).to.equal('unknown');
      expect(Object.keys(volume.replicas)).to.have.lengthOf(2);
      // give FSA a chance to run
      await sleep(EYE_BLINK_MS);
      expect(volume.nexus).to.be.null();
      expect(volume.state).to.equal('healthy');
      expect(volume.size).to.equal(40);
      expect(volEvents).to.have.lengthOf(3);
    });

    it('should import unpublished volume with nexus', async () => {
      const replica = new Replica({
        uuid: UUID,
        size: 40,
        share: 'REPLICA_NONE',
        uri: `bdev:///${UUID}`
      });
      replica.pool = { node: node1 };
      const nexus = new Nexus({
        uuid: UUID,
        size: 20,
        deviceUri: '',
        state: 'NEXUS_ONLINE',
        children: [
          {
            uri: `bdev:///${UUID}`,
            state: 'CHILD_ONLINE'
          }
        ]
      });
      nexus.node = node1;
      const getReplicaSetStub = sinon.stub(registry, 'getReplicaSet');
      getReplicaSetStub.returns([replica]);
      const getNexusStub = sinon.stub(registry, 'getNexus');
      getNexusStub.returns(nexus);

      volumes.start();
      volume = await volumes.importVolume(UUID, volumeSpec, { size: 40 });
      // give FSA a chance to run
      await sleep(EYE_BLINK_MS);
      expect(volume.nexus).to.be.null();
      expect(Object.keys(volume.replicas)).to.have.lengthOf(1);
      expect(Object.values(volume.replicas)[0]).to.equal(replica);
      expect(volume.state).to.equal('healthy');
      expect(volEvents).to.have.lengthOf(4);
    });

    it('should import published volume with nexus', async () => {
      const deviceUri = 'nvmf://host/nqn';
      const replica = new Replica({
        uuid: UUID,
        size: 40,
        share: 'REPLICA_NONE',
        uri: `bdev:///${UUID}`
      });
      replica.pool = { node: node1 };
      const nexus = new Nexus({
        uuid: UUID,
        size: 20,
        deviceUri: '',
        state: 'NEXUS_ONLINE',
        children: [
          {
            uri: `bdev:///${UUID}`,
            state: 'CHILD_ONLINE'
          }
        ]
      });
      nexus.node = node1;
      const getReplicaSetStub = sinon.stub(registry, 'getReplicaSet');
      getReplicaSetStub.returns([replica]);
      const getNexusStub = sinon.stub(registry, 'getNexus');
      getNexusStub.returns(nexus);

      stub1.onCall(0).resolves({ deviceUri });
      volumes.start();
      volume = await volumes.importVolume(UUID, volumeSpec, {
        size: 40,
        targetNodes: ['node1']
      });
      await waitUntil(() => volume.nexus.deviceUri === deviceUri, 'published nexus');
      expect(Object.keys(volume.replicas)).to.have.lengthOf(1);
      expect(Object.values(volume.replicas)[0]).to.equal(replica);
      expect(volume.state).to.equal('healthy');
      expect(volEvents).to.have.lengthOf(4);
    });
  });

  describe('update volume', function () {
    let modCount;

    // We create an artificial volume at the beginning of each test.
    this.beforeEach(() => {
      createTestEnv();

      const nexus = new Nexus({
        uuid: UUID,
        size: 95,
        deviceUri: '',
        state: 'NEXUS_ONLINE',
        children: [
          {
            uri: `bdev:///${UUID}`,
            state: 'CHILD_ONLINE',
            rebuildProgress: 0
          }
        ]
      });
      nexus.node = node1;
      const replica = new Replica({
        uuid: UUID,
        size: 95,
        share: 'REPLICA_NONE',
        uri: `bdev:///${UUID}`
      });
      replica.pool = { node: node1 };
      const getReplicaSetStub = sinon.stub(registry, 'getReplicaSet');
      getReplicaSetStub.returns([replica]);
      const getNexusStub = sinon.stub(registry, 'getNexus');
      getNexusStub.returns(nexus);

      // Fake the volume
      volume = new Volume(UUID, registry, () => {
        modCount += 1;
      }, {
        replicaCount: 1,
        preferredNodes: [],
        requiredNodes: [],
        requiredBytes: 90,
        limitBytes: 110,
        protocol: 'nvmf'
      });
      volume.newReplica(replica);
      volumes.volumes[UUID] = volume;
      volume.newNexus(nexus);
      volume.state = 'healthy';
      modCount = 0;

      volumes.start();
    });

    this.afterEach(() => {
      volumes.stop();
      modCount = 0;
    });

    it('should update volume parameters if a volume to be created already exists', async () => {
      // We intentionally update parameters in a way that won't require
      // scaling up and down, that is tested by different tests.
      const returnedVolume = await volumes.createVolume(UUID, {
        replicaCount: 1,
        preferredNodes: [node2.name],
        requiredNodes: [node1.name],
        requiredBytes: 89,
        limitBytes: 111,
        protocol: 'nvmf'
      });
      sinon.assert.notCalled(stub1);
      sinon.assert.notCalled(stub2);
      sinon.assert.notCalled(stub3);
      expect(returnedVolume).to.equal(volume);
      expect(volume.replicaCount).to.equal(1);
      expect(volume.size).to.equal(95);
      expect(volume.preferredNodes[0]).to.equal(node2.name);
      expect(volume.requiredNodes[0]).to.equal(node1.name);
      expect(volume.requiredBytes).to.equal(89);
      expect(volume.limitBytes).to.equal(111);
      expect(volume.state).to.equal('healthy');
      expect(modCount).to.equal(1);
    });

    it('should not do anything if creating a volume that exists and has the same parameters', async () => {
      const returnedVolume = await volumes.createVolume(UUID, {
        replicaCount: 1,
        preferredNodes: [],
        requiredNodes: [],
        requiredBytes: 90,
        limitBytes: 110,
        protocol: 'nvmf'
      });
      sinon.assert.notCalled(stub1);
      sinon.assert.notCalled(stub2);
      sinon.assert.notCalled(stub3);
      expect(returnedVolume).to.equal(volume);
      expect(modCount).to.equal(0);
    });

    it('should fail to shrink the volume', async () => {
      await shouldFailWith(GrpcCode.INVALID_ARGUMENT, () =>
        volumes.createVolume(UUID, {
          replicaCount: 1,
          preferredNodes: [],
          requiredNodes: [],
          requiredBytes: 90,
          limitBytes: 94,
          protocol: 'nvmf'
        })
      );
    });

    it('should fail to extend the volume', async () => {
      await shouldFailWith(GrpcCode.INVALID_ARGUMENT, () =>
        volumes.createVolume(UUID, {
          replicaCount: 1,
          preferredNodes: [],
          requiredNodes: [],
          requiredBytes: 96,
          limitBytes: 110,
          protocol: 'nvmf'
        })
      );
    });

    it('should fail to change the protocol', async () => {
      await shouldFailWith(GrpcCode.INVALID_ARGUMENT, () => volumes.createVolume(UUID, {
        replicaCount: 1,
        preferredNodes: [node2.name],
        requiredNodes: [node1.name],
        requiredBytes: 89,
        limitBytes: 111,
        protocol: 'iscsi'
      }));
    });
  });

  describe('scale up/down', function () {
    describe('with used nexus', function () {
      beforeEach(() => setUpReferenceEnv(true));
      afterEach(tearDownReferenceEnv);

      it('should scale up if a child is faulted', async () => {
        // on node 3 is created the new replica
        stub3.onCall(0).resolves({
          uuid: UUID,
          pool: 'pool3',
          size: 95,
          thin: false,
          share: 'REPLICA_NONE',
          uri: 'bdev:///' + UUID
        });
        stub3.onCall(1).resolves({ uri: 'nvmf://replica3' });
        // the faulted replica should be eventually removed
        stub2.onCall(0).resolves({});
        // nexus should be updated twice (add and remove a replica)
        stub1.onCall(0).resolves({
          uri: 'nvmf://replica3',
          state: 'CHILD_DEGRADED',
          rebuildProgress: 10
        });
        stub1.onCall(1).resolves({});
        // and finally the nexus should be destroyed again
        stub1.onCall(2).resolves({});

        nexus.children[1].state = 'CHILD_FAULTED';
        registry.emit('nexus', {
          eventType: 'mod',
          object: nexus
        });

        await waitUntil(
          () =>
            nexus.children.length === 3 &&
            nexus.children.find((ch) => ch.uri === 'nvmf://replica3'),
          'new replica'
        );

        expect(volume.state).to.equal('degraded');
        const child = nexus.children.find((ch) => ch.uri === 'nvmf://replica3');
        child.state = 'CHILD_ONLINE';
        registry.emit('nexus', {
          eventType: 'mod',
          object: nexus
        });

        await waitUntil(
          () =>
            nexus.children.length === 2 &&
            !nexus.children.find((ch) => ch.uri === `nvmf://remote/${UUID}`) &&
            nexus.children.find((ch) => ch.uri === 'nvmf://replica3'),
          'faulted replica removal'
        );
        expect(volume.state).to.equal('healthy');
      });

      it('should not scale up if the replica is there but just being rebuilt', async () => {
        // this would have been normally done but should not be the case now
        stub3.onCall(0).resolves({
          uuid: UUID,
          pool: 'pool3',
          size: 95,
          thin: false,
          share: 'REPLICA_NONE',
          uri: 'bdev:///' + UUID
        });
        stub3.onCall(1).resolves({ uri: 'nvmf://replica3' });
        stub1.onCall(0).resolves({
          uri: 'nvmf://replica3',
          state: 'CHILD_DEGRADED',
          rebuildProgress: 10
        });

        nexus.children[0].state = 'CHILD_DEGRADED';
        registry.emit('nexus', {
          eventType: 'mod',
          object: nexus
        });
        await waitUntil(() => volume.state === 'degraded', 'degraded volume');

        try {
          await waitUntil(
            () => nexus.children.length === 3,
            100, // 100 ms
            'new replica not to appear'
          );
        } catch (err) {
          // we are fine
          expect(volume.nexus.children).to.have.lengthOf(2);
          expect(volume.state).to.equal('degraded');
          return;
        }
        throw new Error('well, the new replica did appear');
      });

      it('should not scale up if replica is offline but the child is online', async () => {
        // this would have been normally done but should not be the case now
        stub3.onCall(0).resolves({
          uuid: UUID,
          pool: 'pool3',
          size: 95,
          thin: false,
          share: 'REPLICA_NONE',
          uri: 'bdev:///' + UUID
        });
        stub3.onCall(1).resolves({ uri: 'nvmf://replica3' });
        stub1.onCall(0).resolves({
          uri: 'nvmf://replica3',
          state: 'CHILD_DEGRADED',
          rebuildProgress: 10
        });

        replica1.offline();

        try {
          await waitUntil(
            () => nexus.children.length === 3,
            100, // 100 ms
            'new replica not to appear'
          );
        } catch (err) {
          // we are fine
          expect(volume.nexus.children).to.have.lengthOf(2);
          expect(volume.state).to.equal('healthy');
          return;
        }
        throw new Error('well, the new replica did appear');
      });

      it('should not scale down if a rebuild is in progress', async () => {
        // node 1: updated nexus (remove-child)
        stub1.onCall(0).resolves({});
        // node 2: destroyed replica
        stub2.onCall(1).resolves({});

        nexus.children[0].state = 'CHILD_DEGRADED';
        registry.emit('nexus', {
          eventType: 'mod',
          object: nexus
        });
        await waitUntil(() => volume.state === 'degraded', 'degraded volume');

        // update the spec
        await volumes.createVolume(UUID, {
          replicaCount: 1,
          preferredNodes: [],
          requiredNodes: [],
          requiredBytes: 90,
          limitBytes: 110,
          protocol: 'nvmf'
        });

        try {
          await waitUntil(
            () => nexus.children.length === 1,
            100,
            'replica to be destroyed'
          );
        } catch (err) {
          expect(volume.state).to.equal('degraded');
          return;
        }
        throw new Error('The replica was removed even if in rebuild state');
      });

      it('should scale up and then scale down when a volume is moved', async () => {
        // on node 3 is created the new replica
        stub3.onCall(0).resolves({
          uuid: UUID,
          pool: 'pool3',
          size: 95,
          thin: false,
          share: 'REPLICA_NONE',
          uri: 'bdev:///' + UUID
        });
        stub3.onCall(1).resolves({ uri: 'nvmf://replica3' });
        // nexus should be updated to add the new child
        stub1.onCall(0).resolves({
          uri: 'nvmf://replica3',
          state: 'CHILD_DEGRADED',
          rebuildProgress: 10
        });

        // update the spec: node2 remains but the first replica should move
        // from node1 to node3
        volume = await volumes.createVolume(UUID, {
          replicaCount: 2,
          preferredNodes: [],
          requiredNodes: ['node2', 'node3'],
          requiredBytes: 90,
          limitBytes: 110,
          protocol: 'nvmf'
        });

        await waitUntil(
          () => nexus.children.length === 3 && volume.state === 'degraded',
          'new replica'
        );

        const newChild = volume.nexus.children.find(
          (ch) => ch.state === 'CHILD_DEGRADED'
        );
        expect(newChild.uri).to.equal('nvmf://replica3');
        newChild.state = 'CHILD_ONLINE';
        registry.emit('nexus', {
          eventType: 'mod',
          object: nexus
        });

        await waitUntil(() => nexus.children.length === 2, 'replica removal');
        expect(volume.state).to.equal('healthy');
        expect(Object.keys(volume.replicas)).to.deep.equal(['node2', 'node3']);
      });

      it('should scale up if a new pool is created', async () => {
        // on node 3 we destroy (and create) the pool and create the new replica
        stub3.onCall(0).resolves({});
        stub3.onCall(1).resolves({
          uuid: UUID,
          pool: 'pool3',
          size: 95,
          thin: false,
          share: 'REPLICA_NONE',
          uri: 'bdev:///' + UUID
        });
        stub3.onCall(2).resolves({ uri: 'nvmf://replica3' });
        // nexus should be updated to add the new child
        stub1.onCall(0).resolves({
          uri: 'nvmf://replica3',
          state: 'CHILD_DEGRADED',
          rebuildProgress: 10
        });

        // delete the third pool to pretend we ran out of pools
        await pool3.destroy();

        // now we cannot create the new replica (this is the update op in fact)
        await volumes.createVolume(UUID, {
          replicaCount: 3,
          preferredNodes: [],
          requiredNodes: [],
          requiredBytes: 90,
          limitBytes: 110,
          protocol: 'nvmf'
        });
        await waitUntil(() => volume.state === 'degraded', 'degraded volume');

        // now create the pool and see if it gets used for the new replica
        pool3 = new Pool({
          name: 'pool3',
          disks: [],
          capacity: 100,
          used: 4,
          state: 'POOL_DEGRADED'
        });
        node3._registerPool(pool3);

        await waitUntil(
          () => nexus.children.length === 3 && volume.state === 'degraded',
          'degraded volume with new replica'
        );

        const newChild = volume.nexus.children.find(
          (ch) => ch.state === 'CHILD_DEGRADED'
        );
        expect(newChild.uri).to.equal('nvmf://replica3');
        newChild.state = 'CHILD_ONLINE';
        registry.emit('nexus', {
          eventType: 'mod',
          object: nexus
        });

        await waitUntil(
          () => nexus.children.length === 3 && volume.state === 'healthy',
          'healthy volume'
        );
      });
    });

    describe('without nexus', function () {
      beforeEach(() => setUpReferenceEnv(false));
      afterEach(tearDownReferenceEnv);

      it('should scale up if replicaCount is increased', async () => {
        // scale up involves nexus creation
        stub1.onCall(0).resolves({
          uuid: UUID,
          size: 95,
          state: 'NEXUS_ONLINE',
          children: [
            {
              uri: `bdev:///${UUID}`,
              state: 'CHILD_ONLINE',
              rebuildProgress: 0
            },
            {
              uri: `nvmf://remote/${UUID}`,
              state: 'CHILD_ONLINE',
              rebuildProgress: 0
            }
          ]
        });
        // on node 3 is created the new replica
        stub3.onCall(0).resolves({
          uuid: UUID,
          pool: 'pool3',
          size: 95,
          thin: false,
          share: 'REPLICA_NONE',
          uri: 'bdev:///' + UUID
        });
        stub3.onCall(1).resolves({ uri: 'nvmf://replica3' });
        // nexus should be updated to add the new child
        stub1.onCall(1).resolves({
          uri: 'nvmf://replica3',
          state: 'CHILD_DEGRADED',
          rebuildProgress: 10
        });
        // nexus will be destroyed at the end
        stub1.onCall(2).resolves({});

        // update the spec
        volume = await volumes.createVolume(UUID, {
          replicaCount: 3,
          preferredNodes: [],
          requiredNodes: [],
          requiredBytes: 90,
          limitBytes: 110,
          protocol: 'nvmf'
        });

        await waitUntil(
          () =>
            volume.state === 'degraded' &&
            Object.keys(volume.replicas).length === 3 &&
            volume.nexus.children.length === 3,
          'new replica'
        );
        const newReplica = Object.values(volume.replicas).find((r) => r.uri === 'nvmf://replica3');
        const nexus = volume.nexus;
        const child = nexus.children[2];
        expect(nexus).not.to.be.null();
        expect(newReplica.pool.name).to.equal('pool3');
        expect(child.state).to.equal('CHILD_DEGRADED');

        // simulate rebuild finish - the nexus should go away
        child.state = 'CHILD_ONLINE';
        registry.emit('nexus', {
          eventType: 'mod',
          object: nexus
        });
        await waitUntil(
          () => volume.state === 'healthy' && volume.nexus === null,
          'healthy volume'
        );
        expect(Object.keys(volume.replicas)).has.lengthOf(3);
      });

      it('should scale down if replicaCount is decreased', async () => {
        // scale down involves nexus creation
        stub1.onCall(0).resolves({
          uuid: UUID,
          size: 95,
          state: 'NEXUS_ONLINE',
          children: [
            {
              uri: `bdev:///${UUID}`,
              state: 'CHILD_ONLINE',
              rebuildProgress: 0
            },
            {
              uri: `nvmf://remote/${UUID}`,
              state: 'CHILD_ONLINE',
              rebuildProgress: 0
            }
          ]
        });
        // node 1: updated nexus (remove-child)
        stub1.onCall(1).resolves({});
        // node 2: destroyed replica
        stub2.onCall(0).resolves({});
        // nexus will be destroyed at the end
        stub1.onCall(2).resolves({});

        // update the spec
        await volumes.createVolume(UUID, {
          replicaCount: 1,
          preferredNodes: [],
          requiredNodes: [],
          requiredBytes: 90,
          limitBytes: 110,
          protocol: 'nvmf'
        });

        // Nexus gets created and destroyed inbetween but it's difficult to
        // capture that moment in the tests because we don't know the precise
        // timing.
        await waitUntil(
          () =>
            Object.keys(volume.replicas).length === 1 &&
            volume.nexus === null,
          'replica to be destroyed'
        );
        expect(volume.state).to.equal('healthy');
        const oldReplica = Object.values(volume.replicas).find((r) => r.uri === `nvmf://remote/${UUID}`);
        expect(oldReplica).to.be.undefined();
      });
    });
  });

  describe('state transitions on a volume without nexus', function () {
    beforeEach(() => setUpReferenceEnv(false));
    afterEach(tearDownReferenceEnv);

    it('should move to "faulted" when none of replicas is online', async () => {
      node3._offline(); // prevent FSA from scheduling a new replica
      replica1.offline();
      replica2.offline();
      await waitUntil(() => volume.state === 'faulted', 'faulted volume');
    });
  });

  describe('state transitions on a volume with nexus', function () {
    beforeEach(() => setUpReferenceEnv(true));
    afterEach(tearDownReferenceEnv);

    it('should move to "faulted" when none of replicas is online', async () => {
      nexus.children.forEach((ch) => (ch.state = 'CHILD_FAULTED'));
      registry.emit('nexus', {
        eventType: 'mod',
        object: nexus
      });

      await waitUntil(() => volume.state === 'faulted', 'faulted volume');
      expect(nexus.children).to.have.length(2);
    });

    it('should move to "degraded" when rebuild starts and back to healthy when it ends', async () => {
      nexus.children[0].state = 'CHILD_DEGRADED';
      registry.emit('nexus', {
        eventType: 'mod',
        object: nexus
      });
      await waitUntil(() => volume.state === 'degraded', 'degraded volume');

      nexus.children[0].state = 'CHILD_ONLINE';
      registry.emit('nexus', {
        eventType: 'mod',
        object: nexus
      });
      await waitUntil(() => volume.state === 'healthy', 'healthy volume');
    });

    it('should move to "faulted" state when nexus goes offline', async () => {
      nexus.state = 'NEXUS_OFFLINE';
      registry.emit('nexus', {
        eventType: 'mod',
        object: nexus
      });
      await waitUntil(() => volume.state === 'faulted', 'offline volume');
    });

    it('should remain what it was when volume is unpublished', async () => {
      nexus.children[0].state = 'CHILD_DEGRADED';
      registry.emit('nexus', {
        eventType: 'mod',
        object: nexus
      });
      await waitUntil(() => volume.state === 'degraded', 'degraded volume');
      await volume.unpublish();
      await sleep(EYE_BLINK_MS);
      await waitUntil(() => volume.state === 'degraded', 'degraded volume');
    });

    it('should not move to any state when in "destroyed" state', async () => {
      volume.state = 'destroyed';
      // try to move all replicas to faulted and the state should not change
      nexus.children.forEach((ch) => (ch.state = 'CHILD_FAULTED'));
      registry.emit('nexus', {
        eventType: 'mod',
        object: nexus
      });
      try {
        await waitUntil(() => volume.state === 'faulted', 100, 'faulted volume');
      } catch (err) {
        // ok - the state did not change
      } finally {
        // this will throw
        expect(volume.state).to.equal('destroyed');
      }
    });
  });

  describe('nexus failover', function () {
    beforeEach(() => setUpReferenceEnv(true));
    afterEach(tearDownReferenceEnv);

    it('should create nexus on the same node where it was published', async () => {
      // FSA should try to create and share the nexus again
      stub1.onCall(0).resolves({
        uuid: UUID,
        size: 96,
        state: 'NEXUS_ONLINE',
        children: [
          {
            uri: `bdev:///${UUID}`,
            state: 'CHILD_ONLINE',
            rebuildProgress: 0
          },
          {
            uri: `nvmf://remote/${UUID}`,
            state: 'CHILD_ONLINE',
            rebuildProgress: 0
          }
        ]
      });
      stub1.onCall(1).resolves({
        deviceUri: 'nvmf://host/nqn'
      });

      // pretend that node1 is down
      isSynced1.returns(false);
      // we unbind the nexus - that happens when node goes down
      nexus.unbind();
      await waitUntil(() => volume.state === 'faulted', 'volume faulted');
      expect(volume.nexus).to.be.null();
      expect(volume.publishedOn).to.equal('node1');

      // this simulates node that has been just successfully sync'd
      isSynced1.returns(true);
      node1.emit('node', {
        eventType: 'mod',
        object: node1
      });
      await waitUntil(() => volume.state === 'healthy', 'healthy volume');
      expect(volume.nexus.deviceUri).to.equal('nvmf://host/nqn');
      expect(volume.publishedOn).to.equal('node1');
    });

    it('should set state to healthy again when nexus comes online', async () => {
      nexus.offline();
      await waitUntil(() => volume.state === 'faulted', 'volume faulted');

      nexus.state = 'NEXUS_ONLINE';
      registry.emit('nexus', {
        eventType: 'mod',
        object: nexus
      });
      await waitUntil(() => volume.state === 'healthy', 'healthy volume');
    });

    it('should destroy a new nexus on wrong node', async () => {
      stub2.onCall(0).resolves({});
      const wrongNexus = new Nexus({
        uuid: UUID,
        size: 95,
        deviceUri: '',
        state: 'NEXUS_ONLINE',
        children: [
          {
            uri: `bdev:///${UUID}`,
            state: 'CHILD_ONLINE',
            rebuildProgress: 0
          },
          {
            uri: `nvmf://remote/${UUID}`,
            state: 'CHILD_ONLINE',
            rebuildProgress: 0
          }
        ]
      });
      node2._registerNexus(wrongNexus);

      await waitUntil(() => stub2.callCount > 0, 'destroy grpc call');
      sinon.assert.calledOnce(stub2);
      sinon.assert.calledWithMatch(stub2, 'destroyNexus', { uuid: UUID });
      expect(volume.nexus).to.equal(nexus);
      expect(volume.state).to.equal('healthy');
    });

    it('should replace a nexus in volume on wrong node', async () => {
      volume.publishedOn = 'node2';
      stub1.onCall(0).resolves({});
      const newNexus = new Nexus({
        uuid: UUID,
        size: 95,
        deviceUri: '',
        state: 'NEXUS_ONLINE',
        children: [
          {
            uri: `bdev:///${UUID}`,
            state: 'CHILD_ONLINE',
            rebuildProgress: 0
          },
          {
            uri: `nvmf://remote/${UUID}`,
            state: 'CHILD_ONLINE',
            rebuildProgress: 0
          }
        ]
      });
      node2._registerNexus(newNexus);

      await waitUntil(() => stub1.callCount > 0, 'destroy grpc call');
      sinon.assert.calledOnce(stub1);
      sinon.assert.calledWithMatch(stub1, 'destroyNexus', { uuid: UUID });
      expect(volume.nexus).to.equal(newNexus);
      expect(volume.state).to.equal('healthy');
    });
  });

  // Volume is created once in the first test and then all tests use it.
  // This tests the typical life-cycle of a volume from create to destroy.
  describe('misc', function () {
    before(createTestEnv);

    afterEach(() => {
      stub1.resetHistory();
      stub2.resetHistory();
      stub3.resetHistory();
      volEvents = [];
    });

    after(() => {
      volumes.stop();
    });

    // this creates a volume used in subsequent cases
    it('should create a new volume', async () => {
      // on node 1 is created replica and nexus
      stub1.onCall(0).resolves({
        uuid: UUID,
        pool: 'pool1',
        size: 96,
        thin: false,
        share: 'REPLICA_NONE',
        uri: 'bdev:///' + UUID
      });
      stub1.onCall(1).resolves({
        uuid: UUID,
        size: 96,
        state: 'NEXUS_ONLINE',
        children: [
          {
            uri: 'bdev:///' + UUID,
            state: 'CHILD_ONLINE',
            rebuildProgress: 0
          },
          {
            uri: 'nvmf://replica2',
            state: 'CHILD_ONLINE',
            rebuildProgress: 0
          },
          {
            uri: 'nvmf://replica3',
            state: 'CHILD_ONLINE',
            rebuildProgress: 0
          }
        ]
      });
      // on node 2 is created replica and it is shared
      stub2.onCall(0).resolves({
        uuid: UUID,
        pool: 'pool2',
        size: 96,
        thin: false,
        share: 'REPLICA_NONE',
        uri: 'bdev:///' + UUID
      });
      stub2.onCall(1).resolves({ uri: 'nvmf://replica2' });
      // on node 3 is created replica and it is shared
      stub3.onCall(0).resolves({
        uuid: UUID,
        pool: 'pool3',
        size: 96,
        thin: false,
        share: 'REPLICA_NONE',
        uri: 'bdev:///' + UUID
      });
      stub3.onCall(1).resolves({ uri: 'nvmf://replica3' });

      volumes.start();
      volume = await volumes.createVolume(UUID, {
        replicaCount: 3,
        preferredNodes: [],
        requiredNodes: [],
        requiredBytes: 90,
        limitBytes: 110,
        protocol: 'nvmf'
      });

      sinon.assert.calledTwice(stub1);
      sinon.assert.calledWithMatch(stub1.firstCall, 'createReplica', {
        uuid: UUID,
        pool: 'pool1',
        size: 96,
        thin: false,
        share: 'REPLICA_NONE'
      });
      sinon.assert.calledWithMatch(stub1.secondCall, 'createNexus', {
        uuid: UUID,
        size: 96,
        children: ['bdev:///' + UUID, 'nvmf://replica2', 'nvmf://replica3']
      });

      sinon.assert.calledTwice(stub2);
      sinon.assert.calledWithMatch(stub2.firstCall, 'createReplica', {
        uuid: UUID,
        pool: 'pool2',
        size: 96,
        thin: false,
        share: 'REPLICA_NONE'
      });
      sinon.assert.calledWithMatch(stub2.secondCall, 'shareReplica', {
        uuid: UUID,
        share: 'REPLICA_NVMF'
      });

      sinon.assert.calledTwice(stub3);
      sinon.assert.calledWithMatch(stub3.firstCall, 'createReplica', {
        uuid: UUID,
        pool: 'pool3',
        size: 96,
        thin: false,
        share: 'REPLICA_NONE'
      });
      sinon.assert.calledWithMatch(stub2.secondCall, 'shareReplica', {
        uuid: UUID,
        share: 'REPLICA_NVMF'
      });

      // wait for the nexus to be destroyed after creation
      await sleep(EYE_BLINK_MS);
      sinon.assert.calledThrice(stub1);
      sinon.assert.calledWithMatch(stub1.thirdCall, 'destroyNexus', {
        uuid: UUID
      });

      expect(volumes.get(UUID)).to.equal(volume);
      expect(volume.uuid).to.equal(UUID);
      expect(volume.getSize()).to.equal(96);
      expect(volume.getNodeName()).to.be.undefined();
      expect(volume.replicaCount).to.equal(3);
      expect(volume.preferredNodes).to.have.lengthOf(0);
      expect(volume.requiredNodes).to.have.lengthOf(0);
      expect(volume.requiredBytes).to.equal(90);
      expect(volume.limitBytes).to.equal(110);
      expect(volume.nexus).to.be.null();
      expect(Object.keys(volume.replicas)).to.have.lengthOf(3);
      expect(volume.replicas.node1.uuid).to.equal(UUID);
      expect(volume.replicas.node2.uuid).to.equal(UUID);
      expect(volume.replicas.node3.uuid).to.equal(UUID);
      expect(volume.state).to.equal('healthy');

      expect(volEvents).to.have.lengthOf(9);
    });

    it('should publish the volume', async () => {
      const deviceUri = 'nvmf://host/nqn';
      // on node 1 is created nexus
      stub1.onCall(0).resolves({
        uuid: UUID,
        size: 96,
        state: 'NEXUS_ONLINE',
        children: [
          {
            uri: 'bdev:///' + UUID,
            state: 'CHILD_ONLINE',
            rebuildProgress: 0
          },
          {
            uri: 'nvmf://replica2',
            state: 'CHILD_ONLINE',
            rebuildProgress: 0
          },
          {
            uri: 'nvmf://replica3',
            state: 'CHILD_ONLINE',
            rebuildProgress: 0
          }
        ]
      });
      stub1.onCall(1).resolves({ deviceUri });

      const uri = await volume.publish('nvmf');
      expect(uri).to.equal(deviceUri);

      sinon.assert.calledTwice(stub1);
      sinon.assert.calledWithMatch(stub1.firstCall, 'createNexus', {
        uuid: UUID,
        size: 96,
        children: ['bdev:///' + UUID, 'nvmf://replica2', 'nvmf://replica3']
      });
      sinon.assert.calledWithMatch(stub1.secondCall, 'publishNexus', {
        uuid: UUID,
        key: '',
        share: enums.NEXUS_NVMF
      });

      sinon.assert.notCalled(stub2);
      sinon.assert.notCalled(stub3);

      expect(volume.getNodeName()).to.equal('node1');
      expect(volume.getSize()).to.equal(96);
      expect(volume.replicaCount).to.equal(3);
      expect(volume.nexus.uuid).to.equal(UUID);
      expect(Object.keys(volume.replicas)).to.have.lengthOf(3);
      expect(volume.state).to.equal('healthy');

      // 3 mods (1 new nexus, 1 publish nexus, state change)
      expect(volEvents).to.have.lengthOf(3);
    });

    it('should unpublish the volume', async () => {
      stub1.onCall(0).resolves({});
      stub1.onCall(1).resolves({});
      await volume.unpublish();
      // wait for the nexus to be destroyed after unpublish
      await sleep(EYE_BLINK_MS);
      sinon.assert.calledTwice(stub1);
      sinon.assert.calledWithMatch(stub1.firstCall, 'unpublishNexus', {
        uuid: UUID
      });
      sinon.assert.calledWithMatch(stub1.secondCall, 'destroyNexus', {
        uuid: UUID
      });
      expect(volume.getNodeName()).to.be.undefined();
      expect(volume.uuid).to.equal(UUID);
      expect(volume.nexus).is.null();
      expect(volume.state).to.equal('healthy');
      expect(Object.keys(volume.replicas)).to.have.length(3);
      // 3 nexus events
      expect(volEvents).to.have.lengthOf(3);
    });

    it('should destroy the volume', async () => {
      stub1.onCall(0).resolves({});
      stub2.onCall(0).resolves({});
      stub3.onCall(0).resolves({});

      await volumes.destroyVolume(UUID);

      sinon.assert.calledOnce(stub1);
      sinon.assert.calledWithMatch(stub1, 'destroyReplica', { uuid: UUID });
      sinon.assert.calledOnce(stub2);
      sinon.assert.calledWithMatch(stub2, 'destroyReplica', { uuid: UUID });
      sinon.assert.calledOnce(stub3);
      sinon.assert.calledWithMatch(stub3, 'destroyReplica', { uuid: UUID });

      expect(volumes.get(UUID)).is.undefined();
      expect(volume.getNodeName()).to.be.undefined();
      expect(volume.nexus).is.null();
      expect(volume.state).to.equal('destroyed');
      expect(Object.keys(volume.replicas)).to.have.length(0);
      // 3 replicas and 1 del volume event
      expect(volEvents).to.have.lengthOf(5);
    });

    it('should not fail if destroying a volume that does not exist', async () => {
      stub1.onCall(0).resolves({});
      stub2.onCall(0).resolves({});
      stub3.onCall(0).resolves({});
      expect(volumes.get(UUID)).is.undefined();

      await volumes.destroyVolume(UUID);

      sinon.assert.notCalled(stub1);
      sinon.assert.notCalled(stub2);
      sinon.assert.notCalled(stub3);
      expect(volEvents).to.have.lengthOf(0);
    });
  });
};
