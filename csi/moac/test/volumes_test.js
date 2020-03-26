// Unit tests for the volume manager and volume object.
//
// Volume ensure method is tested here rather than in volume tests because
// it's easier to test with volume manager, which routes events from registry
// to volumes.

'use strict';

const expect = require('chai').expect;
const sinon = require('sinon');
const Nexus = require('../nexus');
const Node = require('../node');
const Pool = require('../pool');
const Registry = require('../registry');
const Replica = require('../replica');
const Volume = require('../volume');
const Volumes = require('../volumes');
const { GrpcCode } = require('../grpc_client');
const { shouldFailWith } = require('./utils');

const UUID = 'ba5e39e9-0c0e-4973-8a3a-0dccada09cbb';

module.exports = function() {
  describe('ensure', () => {
    var registry, volumes;
    var node1, node2, node3;
    var stub1, stub2, stub3;
    var volume;
    var volEvents;

    // create test environment
    function createTestEnv() {
      registry = new Registry();
      volumes = new Volumes(registry);
      node1 = new Node('node1');
      node2 = new Node('node2');
      node3 = new Node('node3');
      // pools sorted from the most to the least preferred
      let pool1 = new Pool({
        name: 'pool1',
        disks: [],
        capacity: 100,
        used: 0,
        state: 'POOL_ONLINE',
      });
      let pool2 = new Pool({
        name: 'pool2',
        disks: [],
        capacity: 100,
        used: 4,
        state: 'POOL_ONLINE',
      });
      let pool3 = new Pool({
        name: 'pool3',
        disks: [],
        capacity: 100,
        used: 4,
        state: 'POOL_DEGRADED',
      });
      // we don't want connect and disconnect to do anything
      sinon.spy(node1, 'connect');
      sinon.spy(node2, 'connect');
      sinon.spy(node3, 'connect');
      sinon.spy(node1, 'disconnect');
      sinon.spy(node2, 'disconnect');
      sinon.spy(node3, 'disconnect');

      registry._registerNode(node1);
      registry._registerNode(node2);
      registry._registerNode(node3);
      node1._registerPool(pool1);
      node2._registerPool(pool2);
      node3._registerPool(pool3);

      volEvents = [];
      volumes.on('volume', ev => {
        volEvents.push(ev);
      });
    }

    // Each test creates a volume so the setup needs to run for each case.
    describe('create volume', function() {
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
          })
        );
        expect(volEvents).to.have.lengthOf(2);
        expect(volEvents[0].eventType).to.equal('new');
        expect(volEvents[0].object.uuid).to.equal(UUID);
        expect(volEvents[0].object.state).to.equal('PENDING');
        expect(volEvents[1].eventType).to.equal('del');
        expect(volEvents[1].object.state).to.equal('PENDING');
        // 1 new + 1 del
        expect(volEvents).to.have.lengthOf(2);
      });

      it('should set size to required minimum if limit is not set', async () => {
        stub1 = sinon.stub(node1, 'call');
        stub2 = sinon.stub(node2, 'call');
        stub3 = sinon.stub(node3, 'call');

        // on node 1 is created replica and nexus
        stub1.onCall(0).resolves({});
        stub1.onCall(1).resolves({
          replicas: [
            {
              uuid: UUID,
              pool: 'pool1',
              size: 90,
              thin: false,
              share: 'REPLICA_NONE',
              state: 'ONLINE',
              uri: 'bdev:///' + UUID,
            },
          ],
        });
        stub1.onCall(2).resolves({});
        stub1.onCall(3).resolves({
          nexusList: [
            {
              uuid: UUID,
              size: 90,
              state: 'ONLINE',
              children: [
                {
                  uri: 'bdev:///' + UUID,
                  state: 'ONLINE',
                },
              ],
            },
          ],
        });

        volumes.start();
        volume = await volumes.createVolume(UUID, {
          replicaCount: 1,
          preferredNodes: [],
          requiredNodes: [],
          requiredBytes: 90,
          limitBytes: 0,
        });
        expect(volume.size).to.equal(90);
        sinon.assert.calledWithMatch(stub1.firstCall, 'createReplica', {
          uuid: UUID,
          pool: 'pool1',
          size: 90,
          thin: false,
          share: 'REPLICA_NONE',
        });
        // 1 new + 2 mods
        expect(volEvents).to.have.lengthOf(3);
      });

      it('should limit the size of created volume', async () => {
        stub1 = sinon.stub(node1, 'call');
        stub2 = sinon.stub(node2, 'call');
        stub3 = sinon.stub(node3, 'call');

        // on node 1 is created replica and nexus
        stub1.onCall(0).resolves({});
        stub1.onCall(1).resolves({
          replicas: [
            {
              uuid: UUID,
              pool: 'pool1',
              size: 50,
              thin: false,
              share: 'REPLICA_NONE',
              state: 'ONLINE',
              uri: 'bdev:///' + UUID,
            },
          ],
        });
        stub1.onCall(2).resolves({});
        stub1.onCall(3).resolves({
          nexusList: [
            {
              uuid: UUID,
              size: 50,
              state: 'ONLINE',
              children: [
                {
                  uri: 'bdev:///' + UUID,
                  state: 'ONLINE',
                },
              ],
            },
          ],
        });

        volumes.start();
        volume = await volumes.createVolume(UUID, {
          replicaCount: 1,
          preferredNodes: [],
          requiredNodes: [],
          requiredBytes: 10,
          limitBytes: 50,
        });
        expect(volume.size).to.equal(50);
        sinon.assert.calledWithMatch(stub1.firstCall, 'createReplica', {
          uuid: UUID,
          pool: 'pool1',
          size: 50,
          thin: false,
          share: 'REPLICA_NONE',
        });
        // 1 new + 2 mods
        expect(volEvents).to.have.lengthOf(3);
      });

      it('should fail if the size is zero', async () => {
        stub1 = sinon.stub(node1, 'call');
        stub2 = sinon.stub(node2, 'call');
        stub3 = sinon.stub(node3, 'call');

        volumes.start();
        await shouldFailWith(GrpcCode.INVALID_ARGUMENT, () =>
          volumes.createVolume(UUID, {
            replicaCount: 1,
            preferredNodes: [],
            requiredNodes: [],
            requiredBytes: 0,
            limitBytes: 0,
          })
        );
        sinon.assert.notCalled(stub1);
        sinon.assert.notCalled(stub2);
        sinon.assert.notCalled(stub3);
        expect(volEvents).to.have.lengthOf(0);
      });

      it('should create the volume and include pre-existing nexus and replica', async () => {
        stub1 = sinon.stub(node1, 'call');
        stub2 = sinon.stub(node2, 'call');
        stub3 = sinon.stub(node3, 'call');
        let replica = new Replica({
          uuid: UUID,
          size: 10,
          share: 'REPLICA_NONE',
          uri: 'bdev:///' + UUID,
          state: 'ONLINE',
        });
        replica.pool = { node: node1 };
        let getReplicaSetStub = sinon.stub(registry, 'getReplicaSet');
        getReplicaSetStub.returns([replica]);
        let nexus = new Nexus({
          uuid: UUID,
          size: 10,
          devicePath: '/dev/nbd0',
          state: 'ONLINE',
          children: [
            {
              uri: 'bdev:///' + UUID,
              state: 'ONLINE',
            },
          ],
        });
        nexus.node = node1;
        let getNexusStub = sinon.stub(registry, 'getNexus');
        getNexusStub.returns(nexus);

        volumes.start();
        volume = await volumes.createVolume(UUID, {
          replicaCount: 1,
          preferredNodes: [],
          requiredNodes: [],
          requiredBytes: 10,
          limitBytes: 50,
        });
        sinon.assert.notCalled(stub1);
        sinon.assert.notCalled(stub2);
        sinon.assert.notCalled(stub3);
        expect(Object.keys(volume.replicas)).to.have.lengthOf(1);
        expect(Object.values(volume.replicas)[0]).to.equal(replica);
        expect(volume.nexus).to.equal(nexus);
        expect(volEvents).to.have.lengthOf(1);
        expect(volEvents[0].eventType).to.equal('new');
      });
    });

    // Volume is created once in the first test and then all tests use it
    describe('misc', function() {
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
        stub1 = sinon.stub(node1, 'call');
        stub2 = sinon.stub(node2, 'call');
        stub3 = sinon.stub(node3, 'call');

        // on node 1 is created replica and nexus
        stub1.onCall(0).resolves({});
        stub1.onCall(1).resolves({
          replicas: [
            {
              uuid: UUID,
              pool: 'pool1',
              size: 96,
              thin: false,
              share: 'REPLICA_NONE',
              state: 'ONLINE',
              uri: 'bdev:///' + UUID,
            },
          ],
        });
        stub1.onCall(2).resolves({});
        stub1.onCall(3).resolves({
          nexusList: [
            {
              uuid: UUID,
              size: 96,
              state: 'ONLINE',
              children: [
                {
                  uri: 'bdev:///' + UUID,
                  state: 'ONLINE',
                },
                {
                  uri: 'nvmf://replica2',
                  state: 'ONLINE',
                },
                {
                  uri: 'nvmf://replica3',
                  state: 'ONLINE',
                },
              ],
            },
          ],
        });

        // on node 2 is created replica and it is shared
        stub2.onCall(0).resolves({});
        stub2.onCall(1).resolves({
          replicas: [
            {
              uuid: UUID,
              pool: 'pool2',
              size: 96,
              thin: false,
              share: 'REPLICA_NONE',
              state: 'ONLINE',
              uri: 'bdev:///' + UUID,
            },
          ],
        });
        stub2.onCall(2).resolves({ uri: 'nvmf://replica2' });

        // on node 3 is created replica and it is shared
        stub3.onCall(0).resolves({});
        stub3.onCall(1).resolves({
          replicas: [
            {
              uuid: UUID,
              pool: 'pool3',
              size: 96,
              thin: false,
              share: 'REPLICA_NONE',
              state: 'ONLINE',
              uri: 'bdev:///' + UUID,
            },
          ],
        });
        stub3.onCall(2).resolves({ uri: 'nvmf://replica3' });

        volumes.start();
        volume = await volumes.createVolume(UUID, {
          replicaCount: 3,
          preferredNodes: [],
          requiredNodes: [],
          requiredBytes: 90,
          limitBytes: 110,
        });

        expect(stub1.callCount).to.equal(4);
        sinon.assert.calledWithMatch(stub1.firstCall, 'createReplica', {
          uuid: UUID,
          pool: 'pool1',
          size: 96,
          thin: false,
          share: 'REPLICA_NONE',
        });
        sinon.assert.calledWithMatch(stub1.secondCall, 'listReplicas', {});
        sinon.assert.calledWithMatch(stub1.thirdCall, 'createNexus', {
          uuid: UUID,
          size: 96,
          children: ['bdev:///' + UUID, 'nvmf://replica2', 'nvmf://replica3'],
        });
        sinon.assert.calledWithMatch(stub1.lastCall, 'listNexus', {});

        expect(stub2.callCount).to.equal(3);
        sinon.assert.calledWithMatch(stub2.firstCall, 'createReplica', {
          uuid: UUID,
          pool: 'pool2',
          size: 96,
          thin: false,
          share: 'REPLICA_NONE',
        });
        sinon.assert.calledWithMatch(stub2.secondCall, 'listReplicas', {});
        sinon.assert.calledWithMatch(stub2.thirdCall, 'shareReplica', {
          uuid: UUID,
          share: 'REPLICA_NVMF',
        });

        expect(stub3.callCount).to.equal(3);
        sinon.assert.calledWithMatch(stub3.firstCall, 'createReplica', {
          uuid: UUID,
          pool: 'pool3',
          size: 96,
          thin: false,
          share: 'REPLICA_NONE',
        });
        sinon.assert.calledWithMatch(stub3.secondCall, 'listReplicas', {});
        sinon.assert.calledWithMatch(stub3.thirdCall, 'shareReplica', {
          uuid: UUID,
          share: 'REPLICA_NVMF',
        });

        expect(volumes.get(UUID)).to.equal(volume);
        expect(volume.uuid).to.equal(UUID);
        expect(volume.size).to.equal(96);
        expect(volume.replicaCount).to.equal(3);
        expect(volume.preferredNodes).to.have.lengthOf(0);
        expect(volume.requiredNodes).to.have.lengthOf(0);
        expect(volume.requiredBytes).to.equal(90);
        expect(volume.limitBytes).to.equal(110);
        expect(volume.nexus.uuid).to.equal(UUID);
        expect(Object.keys(volume.replicas)).to.have.lengthOf(3);
        expect(volume.replicas.node1.uuid).to.equal(UUID);
        expect(volume.replicas.node2.uuid).to.equal(UUID);
        expect(volume.replicas.node3.uuid).to.equal(UUID);
        expect(volume.state).to.equal('ONLINE');
        expect(volume.reason).to.equal('');

        // 1 new + 6 mods (3 new replicas, 2 set share, 1 new nexus)
        expect(volEvents).to.have.lengthOf(7);
      });

      it('should remove superfluous replica', async () => {
        stub1.onCall(0).resolves({});
        stub2.onCall(0).resolves({});
        stub3.onCall(0).resolves({});

        let count = --volume.replicaCount;
        expect(Object.keys(volume.replicas)).to.have.lengthOf(count + 1);
        await volume.ensure();
        expect(Object.keys(volume.replicas)).to.have.lengthOf(count);
        // node3 has degraded pool so it should be removed as first
        expect(Object.keys(volume.replicas)).not.to.include('node3');

        expect(stub1.callCount).to.equal(1);
        sinon.assert.calledWithMatch(stub1.firstCall, 'removeChildNexus', {
          uuid: UUID,
          uri: 'nvmf://replica3',
        });
        expect(stub2.callCount).to.equal(0);
        expect(stub3.callCount).to.equal(1);
        sinon.assert.calledWithMatch(stub3.firstCall, 'destroyReplica', {
          uuid: UUID,
        });
        // one for replica remove and one for nexus update
        expect(volEvents).to.have.lengthOf(2);
      });

      it('should add missing replica', async () => {
        stub1.onCall(0).resolves({});
        stub2.onCall(0).resolves({});
        stub3.onCall(0).resolves({});
        stub3.onCall(1).resolves({
          replicas: [
            {
              uuid: UUID,
              pool: 'pool3',
              size: 96,
              thin: false,
              share: 'REPLICA_NONE',
              state: 'ONLINE',
              uri: 'bdev:///' + UUID,
            },
          ],
        });
        stub3.onCall(2).resolves({ uri: 'nvmf://replica3' });

        let count = ++volume.replicaCount;
        expect(Object.keys(volume.replicas)).to.have.lengthOf(count - 1);
        await volume.ensure();
        expect(Object.keys(volume.replicas)).to.have.lengthOf(count);

        expect(stub1.callCount).to.equal(1);
        sinon.assert.calledWithMatch(stub1.firstCall, 'addChildNexus', {
          uuid: UUID,
          uri: 'nvmf://replica3',
        });
        expect(stub2.callCount).to.equal(0);
        expect(stub3.callCount).to.equal(3);
        sinon.assert.calledWithMatch(stub3.firstCall, 'createReplica', {
          uuid: UUID,
          pool: 'pool3',
          size: 96,
          thin: false,
          share: 'REPLICA_NONE',
        });
        sinon.assert.calledWithMatch(stub3.secondCall, 'listReplicas', {});
        sinon.assert.calledWithMatch(stub3.thirdCall, 'shareReplica', {
          uuid: UUID,
          share: 'REPLICA_NVMF',
        });
        // new replica, set share and update nexus
        expect(volEvents).to.have.lengthOf(3);
      });

      it('should set share protocols of replicas to accomodate nexus', async () => {
        // We switch one of the remote replicas to local and vice versa
        let local = node1.pools[0].replicas[0];
        let remote = node2.pools[0].replicas[0];
        local.share = 'REPLICA_NVMF';
        local.uri = 'nvmf://remote-replica';
        remote.share = 'REPLICA_NONE';
        remote.uri = 'bdev:///' + UUID;

        stub1.onCall(0).resolves({ uri: 'bdev:///' + UUID });
        stub2.onCall(0).resolves({ uri: 'nvmf://replica2' });
        stub3.onCall(0).resolves({});

        await volume.ensure();

        expect(stub3.callCount).to.equal(0);
        expect(stub1.callCount).to.equal(1);
        sinon.assert.calledWithMatch(stub1.firstCall, 'shareReplica', {
          uuid: UUID,
          share: 'REPLICA_NONE',
        });
        expect(stub2.callCount).to.equal(1);
        sinon.assert.calledWithMatch(stub2.firstCall, 'shareReplica', {
          uuid: UUID,
          share: 'REPLICA_NVMF',
        });
        expect(volEvents).to.have.lengthOf(2);
      });

      it('should create missing nexus', async () => {
        node1.nexus = [];
        volume.nexus = null;

        stub3.onCall(0).resolves({});
        stub2.onCall(0).resolves({});
        stub1.onCall(0).resolves({});
        stub1.onCall(1).resolves({
          nexusList: [
            {
              uuid: UUID,
              size: 96,
              state: 'ONLINE',
              children: [
                {
                  uri: 'bdev:///' + UUID,
                  state: 'ONLINE',
                },
                {
                  uri: 'nvmf://replica2',
                  state: 'ONLINE',
                },
                {
                  uri: 'nvmf://replica3',
                  state: 'ONLINE',
                },
              ],
            },
          ],
        });

        await volume.ensure();

        expect(stub3.callCount).to.equal(0);
        expect(stub2.callCount).to.equal(0);
        expect(stub1.callCount).to.equal(2);
        sinon.assert.calledWithMatch(stub1.firstCall, 'createNexus', {
          uuid: UUID,
          size: 96,
          children: ['bdev:///' + UUID, 'nvmf://replica2', 'nvmf://replica3'],
        });
        sinon.assert.calledWithMatch(stub1.secondCall, 'listNexus', {});
        expect(volEvents).to.have.lengthOf(1);
      });

      it('should fail to shrink the volume', async () => {
        await shouldFailWith(GrpcCode.INVALID_ARGUMENT, () =>
          volumes.createVolume(UUID, {
            replicaCount: 3,
            preferredNodes: [],
            requiredNodes: [],
            requiredBytes: 90,
            limitBytes: 95,
          })
        );
      });

      it('should fail to extend the volume', async () => {
        await shouldFailWith(GrpcCode.INVALID_ARGUMENT, () =>
          volumes.createVolume(UUID, {
            replicaCount: 3,
            preferredNodes: [],
            requiredNodes: [],
            requiredBytes: 97,
            limitBytes: 110,
          })
        );
      });

      it('should publish the volume', async () => {
        stub1.onCall(0).resolves({ devicePath: '/dev/nbd0' });

        await volume.publish();

        sinon.assert.calledOnce(stub1);
        sinon.assert.calledWithMatch(stub1, 'publishNexus', {
          uuid: UUID,
          key: '',
        });
        expect(volume.nexus.devicePath).to.equal('/dev/nbd0');
        expect(volEvents).to.have.lengthOf(1);
      });

      it('should unpublish the volume', async () => {
        stub1.onCall(0).resolves({});

        await volume.unpublish();

        sinon.assert.calledOnce(stub1);
        sinon.assert.calledWithMatch(stub1, 'unpublishNexus', { uuid: UUID });
        // jshint ignore:start
        expect(volume.nexus.devicePath).to.be.empty;
        // jshint ignore:end
        expect(volEvents).to.have.lengthOf(1);
      });

      it('should update the volume spec', async () => {
        let volume = await volumes.createVolume(UUID, {
          replicaCount: 1,
          preferredNodes: ['node3'],
          requiredNodes: ['node2'],
          requiredBytes: 10,
          limitBytes: 200,
        });

        expect(volume.replicaCount).to.equal(1);
        expect(volume.preferredNodes[0]).to.equal('node3');
        expect(volume.requiredNodes[0]).to.equal('node2');
        expect(volume.requiredBytes).to.equal(10);
        expect(volume.limitBytes).to.equal(200);
        expect(volume.size).to.equal(96);
        expect(Object.keys(volume.replicas)).to.have.lengthOf(1);
        expect(Object.keys(volume.replicas)[0]).to.equal('node2');
        // 2 removed replicas, 2 nexus updates and final 1 mod volume
        expect(volEvents).to.have.lengthOf(5);
      });

      it('should not do anything if updating the volume with the same parameters', async () => {
        await volumes.createVolume(UUID, {
          replicaCount: 1,
          preferredNodes: ['node3'],
          requiredNodes: ['node2'],
          requiredBytes: 10,
          limitBytes: 200,
        });
        expect(volEvents).to.have.lengthOf(0);
      });

      it('should destroy the volume', async () => {
        stub1.onCall(0).resolves({});
        stub2.onCall(0).resolves({});
        stub3.onCall(0).resolves({});

        await volumes.destroyVolume(UUID);

        sinon.assert.calledOnce(stub1);
        sinon.assert.calledWithMatch(stub1.firstCall, 'destroyNexus', {
          uuid: UUID,
        });
        sinon.assert.calledOnce(stub2);
        sinon.assert.calledWithMatch(stub2, 'destroyReplica', { uuid: UUID });
        sinon.assert.notCalled(stub3);

        // jshint ignore:start
        expect(volumes.get(UUID)).is.null;
        expect(volume.nexus).is.null;
        // jshint ignore:end
        expect(Object.keys(volume.replicas)).to.have.length(0);
        // 1 replica, 1 nexus and 1 del volume event
        expect(volEvents).to.have.lengthOf(3);
      });

      it('should not fail if destroying a volume that does not exist', async () => {
        stub1.onCall(0).resolves({});
        stub2.onCall(0).resolves({});
        stub3.onCall(0).resolves({});
        // jshint ignore:start
        expect(volumes.get(UUID)).is.null;
        // jshint ignore:end

        await volumes.destroyVolume(UUID);

        sinon.assert.notCalled(stub1);
        sinon.assert.notCalled(stub2);
        sinon.assert.notCalled(stub3);
        expect(volEvents).to.have.lengthOf(0);
      });
    });
  });
};
