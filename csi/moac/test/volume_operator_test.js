// Unit tests for the volume operator

'use strict';

/* eslint-disable no-unused-expressions */

const _ = require('lodash');
const EventEmitter = require('events');
const expect = require('chai').expect;
const sinon = require('sinon');
const sleep = require('sleep-promise');
const { KubeConfig } = require('@kubernetes/client-node');
const { Registry } = require('../dist/registry');
const { Volume } = require('../dist/volume');
const { Volumes } = require('../dist/volumes');
const { VolumeOperator, VolumeResource } = require('../dist/volume_operator');
const { GrpcError, grpcCode } = require('../dist/grpc_client');
const { mockCache } = require('./watcher_stub');
const Node = require('./node_stub');
const { Nexus } = require('../dist/nexus');
const { Replica } = require('../dist/replica');
const { Pool } = require('../dist/pool');

const UUID = 'd01b8bfb-0116-47b0-a03a-447fcbdc0e99';
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

function defaultMeta (uuid) {
  return {
    creationTimestamp: '2019-02-15T18:23:53Z',
    generation: 1,
    name: uuid,
    namespace: NAMESPACE,
    resourceVersion: '627981',
    selfLink: `/apis/openebs.io/v1alpha1/namespaces/${NAMESPACE}/mayastorvolumes/${uuid}`,
    uid: 'd99f06a9-314e-11e9-b086-589cfc0d76a7'
  };
}

const defaultSpec = {
  replicaCount: 2,
  local: true,
  preferredNodes: ['node1', 'node2'],
  requiredNodes: ['node3', 'node2', 'node1'],
  requiredBytes: 100,
  limitBytes: 120,
  protocol: 'nvmf'
};

const defaultStatus = {
  size: 110,
  targetNodes: ['node2'],
  state: 'healthy',
  nexus: {
    deviceUri: 'nvmf://host/nqn',
    state: 'NEXUS_ONLINE',
    node: 'node2',
    children: [
      {
        uri: 'bdev:///' + UUID,
        state: 'CHILD_ONLINE'
      },
      {
        uri: 'nvmf://node1/' + UUID,
        state: 'CHILD_ONLINE'
      }
    ]
  },
  replicas: [
    {
      uri: 'bdev:///' + UUID,
      node: 'node2',
      pool: 'pool2',
      offline: false
    },
    {
      uri: 'nvmf://node1/' + UUID,
      node: 'node1',
      pool: 'pool1',
      offline: false
    }
  ]
};

// Function that creates a volume object corresponding to default spec and
// status defined above.
function createDefaultVolume (registry) {
  const node1 = new Node('node1');
  const node2 = new Node('node2');
  const volume = new Volume(UUID, registry, new EventEmitter(), defaultSpec);
  volume.state = defaultStatus.state;
  volume.size = defaultStatus.size;
  volume.publishedOn = defaultStatus.targetNodes[0];
  volume.nexus = new Nexus({
    uuid: UUID,
    size: defaultStatus.size,
    deviceUri: defaultStatus.nexus.deviceUri,
    state: defaultStatus.nexus.state,
    children: defaultStatus.nexus.children
  });
  volume.nexus.node = node2;

  volume.replicas.node1 = new Replica({
    uuid: UUID,
    size: defaultStatus.size,
    share: 'NVMF',
    uri: defaultStatus.replicas[1].uri
  });
  volume.replicas.node1.pool = new Pool({
    name: 'pool1',
    disks: ['/dev/sda'],
    state: 'POOL_ONLINE',
    capacity: 1000,
    used: 100
  });
  volume.replicas.node1.pool.node = node1;

  volume.replicas.node2 = new Replica({
    uuid: UUID,
    size: defaultStatus.size,
    share: 'NONE',
    uri: defaultStatus.replicas[0].uri
  });
  volume.replicas.node2.pool = new Pool({
    name: 'pool2',
    disks: ['/dev/sda'],
    state: 'POOL_ONLINE',
    capacity: 1000,
    used: 100
  });
  volume.replicas.node2.pool.node = node2;

  return volume;
}

// Create k8s volume resource object
function createK8sVolumeResource (uuid, spec, status) {
  const obj = {
    apiVersion: 'openebs.io/v1alpha1',
    kind: 'MayastorVolume',
    metadata: defaultMeta(uuid),
    spec: spec
  };
  if (status) {
    obj.status = status;
  }
  return obj;
}

// Create volume resource object
function createVolumeResource (uuid, spec, status) {
  return new VolumeResource(createK8sVolumeResource(uuid, spec, status));
}

// Create a pool operator object suitable for testing - with fake watcher
// and fake k8s api client.
async function createVolumeOperator (volumes, stubsCb) {
  const kc = new KubeConfig();
  Object.assign(kc, fakeConfig);
  const oper = new VolumeOperator(NAMESPACE, kc, volumes);
  mockCache(oper.watcher, stubsCb);
  await oper.start();
  // give time to registry to install its callbacks
  await sleep(EVENT_PROPAGATION_DELAY);
  return oper;
}

module.exports = function () {
  describe('VolumeResource constructor', () => {
    it('should create mayastor volume with status', () => {
      const res = createVolumeResource(UUID, defaultSpec, defaultStatus);
      expect(res.metadata.name).to.equal(UUID);
      expect(res.spec.replicaCount).to.equal(2);
      expect(res.spec.local).to.be.true;
      expect(res.spec.preferredNodes).to.have.lengthOf(2);
      expect(res.spec.preferredNodes[0]).to.equal('node1');
      expect(res.spec.preferredNodes[1]).to.equal('node2');
      expect(res.spec.requiredNodes).to.have.lengthOf(3);
      expect(res.spec.requiredNodes[0]).to.equal('node3');
      expect(res.spec.requiredNodes[1]).to.equal('node2');
      expect(res.spec.requiredNodes[2]).to.equal('node1');
      expect(res.spec.requiredBytes).to.equal(100);
      expect(res.spec.limitBytes).to.equal(120);
      expect(res.status.size).to.equal(110);
      expect(res.status.state).to.equal('healthy');
      expect(res.status.nexus.deviceUri).to.equal('nvmf://host/nqn');
      expect(res.status.nexus.state).to.equal('NEXUS_ONLINE');
      expect(res.status.nexus.node).to.equal('node2');
      expect(res.status.nexus.children).to.have.length(2);
      expect(res.status.nexus.children[0].uri).to.equal('bdev:///' + UUID);
      expect(res.status.nexus.children[0].state).to.equal('CHILD_ONLINE');
      expect(res.status.nexus.children[1].uri).to.equal('nvmf://node1/' + UUID);
      expect(res.status.nexus.children[1].state).to.equal('CHILD_ONLINE');
      expect(res.status.replicas).to.have.lengthOf(2);
      // replicas should be sorted by node name
      expect(res.status.replicas[0].uri).to.equal('nvmf://node1/' + UUID);
      expect(res.status.replicas[0].node).to.equal('node1');
      expect(res.status.replicas[0].pool).to.equal('pool1');
      expect(res.status.replicas[0].offline).to.equal(false);
      expect(res.status.replicas[1].uri).to.equal('bdev:///' + UUID);
      expect(res.status.replicas[1].node).to.equal('node2');
      expect(res.status.replicas[1].pool).to.equal('pool2');
      expect(res.status.replicas[1].offline).to.equal(false);
    });

    it('should create mayastor volume with unknown state', () => {
      const res = createVolumeResource(
        UUID,
        {
          replicaCount: 1,
          requiredBytes: 100
        },
        {
          size: 100,
          targetNodes: ['node2'],
          state: 'online' // "online" is not a valid volume state
        }
      );
      expect(res.metadata.name).to.equal(UUID);
      expect(res.spec.replicaCount).to.equal(1);
      expect(res.status.size).to.equal(100);
      expect(res.status.targetNodes).to.deep.equal(['node2']);
      expect(res.status.state).to.equal('unknown');
    });

    it('should create mayastor volume with status without nexus', () => {
      const res = createVolumeResource(
        UUID,
        {
          replicaCount: 3,
          local: false,
          preferredNodes: ['node1', 'node2'],
          requiredNodes: ['node2'],
          requiredBytes: 100,
          limitBytes: 120
        },
        {
          size: 110,
          targetNodes: ['node2'],
          state: 'healthy',
          replicas: []
        }
      );

      expect(res.metadata.name).to.equal(UUID);
      expect(res.spec.replicaCount).to.equal(3);
      expect(res.spec.local).to.be.false;
      expect(res.spec.preferredNodes).to.have.lengthOf(2);
      expect(res.spec.preferredNodes[0]).to.equal('node1');
      expect(res.spec.preferredNodes[1]).to.equal('node2');
      expect(res.spec.requiredNodes).to.have.lengthOf(1);
      expect(res.spec.requiredNodes[0]).to.equal('node2');
      expect(res.spec.requiredBytes).to.equal(100);
      expect(res.spec.limitBytes).to.equal(120);
      expect(res.status.size).to.equal(110);
      expect(res.status.targetNodes).to.deep.equal(['node2']);
      expect(res.status.state).to.equal('healthy');
      expect(res.status.nexus).is.undefined;
      expect(res.status.replicas).to.have.lengthOf(0);
    });

    it('should create mayastor volume without status', () => {
      const res = createVolumeResource(UUID, {
        replicaCount: 3,
        local: true,
        preferredNodes: ['node1', 'node2'],
        requiredNodes: ['node2'],
        requiredBytes: 100,
        limitBytes: 120
      });
      expect(res.metadata.name).to.equal(UUID);
      expect(res.spec.replicaCount).to.equal(3);
      expect(res.status).to.be.undefined;
    });

    it('should create mayastor volume without optional parameters', () => {
      const res = createVolumeResource(UUID, {
        requiredBytes: 100
      });
      expect(res.metadata.name).to.equal(UUID);
      expect(res.spec.replicaCount).to.equal(1);
      expect(res.spec.local).to.be.false;
      expect(res.spec.preferredNodes).to.have.lengthOf(0);
      expect(res.spec.requiredNodes).to.have.lengthOf(0);
      expect(res.spec.requiredBytes).to.equal(100);
      expect(res.spec.limitBytes).to.equal(0);
      expect(res.status).to.be.undefined;
    });

    it('should throw if requiredSize is missing', () => {
      expect(() => createVolumeResource(UUID, {
        replicaCount: 3,
        local: true,
        preferredNodes: ['node1', 'node2'],
        requiredNodes: ['node2'],
        limitBytes: 120
      })).to.throw();
    });

    it('should throw if UUID is invalid', () => {
      expect(() => createVolumeResource('blabla', {
        replicaCount: 3,
        local: true,
        preferredNodes: ['node1', 'node2'],
        requiredNodes: ['node2'],
        requiredBytes: 100,
        limitBytes: 120
      })).to.throw();
    });
  });

  describe('init method', () => {
    let kc, oper, fakeApiStub;

    beforeEach(() => {
      const registry = new Registry({});
      kc = new KubeConfig();
      Object.assign(kc, fakeConfig);
      oper = new VolumeOperator(NAMESPACE, kc, registry);
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
    let oper; // volume operator

    afterEach(async () => {
      if (oper) {
        await oper.stop();
        oper = null;
      }
    });

    it('should call import volume for existing resources when starting the operator', async () => {
      let stubs;
      const registry = new Registry({});
      const volumes = new Volumes(registry);
      const importVolumeStub = sinon.stub(volumes, 'importVolume');
      // return value is not used so just return something
      importVolumeStub.returns({ uuid: UUID });

      const volumeResource = createVolumeResource(UUID, defaultSpec, defaultStatus);
      oper = await createVolumeOperator(volumes, (arg) => {
        stubs = arg;
        stubs.get.returns(volumeResource);
      });
      // trigger "new" event
      oper.watcher.emit('new', volumeResource);
      // give event callbacks time to propagate
      await sleep(EVENT_PROPAGATION_DELAY);

      sinon.assert.calledOnce(importVolumeStub);
      sinon.assert.calledWith(importVolumeStub, UUID, defaultSpec);
    });

    it('should set reason in resource if volume import fails upon "new" event', async () => {
      let stubs;
      const registry = new Registry({});
      const volumes = new Volumes(registry);
      const importVolumeStub = sinon.stub(volumes, 'importVolume');
      importVolumeStub.throws(
        new GrpcError(grpcCode.INTERNAL, 'create failed')
      );

      const volumeResource = createVolumeResource(UUID, defaultSpec, defaultStatus);
      oper = await createVolumeOperator(volumes, (arg) => {
        stubs = arg;
        stubs.get.returns(volumeResource);
      });
      // trigger "new" event
      oper.watcher.emit('new', volumeResource);
      // give event callbacks time to propagate
      await sleep(EVENT_PROPAGATION_DELAY);

      sinon.assert.calledOnce(importVolumeStub);
      sinon.assert.calledOnce(stubs.updateStatus);
      expect(stubs.updateStatus.args[0][5].status.state).to.equal('error');
      expect(stubs.updateStatus.args[0][5].status.reason).to.equal('Error: create failed');
    });

    it('should destroy the volume upon "del" event', async () => {
      let stubs;
      const registry = new Registry({});
      const volumes = new Volumes(registry);
      const destroyVolumeStub = sinon.stub(volumes, 'destroyVolume');
      destroyVolumeStub.resolves();
      const volumeResource = createVolumeResource(UUID, defaultSpec, defaultStatus);

      oper = await createVolumeOperator(volumes, (arg) => {
        stubs = arg;
        stubs.get.returns(volumeResource);
      });
      const getVolumeStub = sinon.stub(volumes, 'get');
      getVolumeStub.returns({ uuid: UUID });
      // trigger "del" event
      oper.watcher.emit('del', volumeResource);
      // give event callbacks time to propagate
      await sleep(EVENT_PROPAGATION_DELAY);

      sinon.assert.calledOnce(destroyVolumeStub);
      sinon.assert.calledWith(destroyVolumeStub, UUID);
    });

    it('should handle gracefully if destroy of a volume fails upon "del" event', async () => {
      let stubs;
      const registry = new Registry({});
      const volumes = new Volumes(registry);
      const destroyVolumeStub = sinon.stub(volumes, 'destroyVolume');
      destroyVolumeStub.rejects(
        new GrpcError(grpcCode.INTERNAL, 'destroy failed')
      );
      const volumeResource = createVolumeResource(UUID, defaultSpec, defaultStatus);

      oper = await createVolumeOperator(volumes, (arg) => {
        stubs = arg;
        stubs.get.returns(volumeResource);
      });
      const getVolumeStub = sinon.stub(volumes, 'get');
      getVolumeStub.returns({ uuid: UUID });
      // trigger "del" event
      oper.watcher.emit('del', volumeResource);
      // give event callbacks time to propagate
      await sleep(EVENT_PROPAGATION_DELAY);

      sinon.assert.calledOnce(destroyVolumeStub);
      sinon.assert.calledWith(destroyVolumeStub, UUID);
    });

    it('should modify the volume upon "mod" event', async () => {
      let stubs;
      const registry = new Registry({});
      const volumes = new Volumes(registry);
      const volume = new Volume(UUID, registry, new EventEmitter(), defaultSpec);
      volume.size = 110;
      const fsaStub = sinon.stub(volume, 'fsa');
      fsaStub.returns();
      sinon
        .stub(volumes, 'get')
        .withArgs(UUID)
        .returns(volume);
      sinon
        .stub(volumes, 'list')
        .withArgs()
        .returns([volume]);
      const oldObj = createVolumeResource(UUID, defaultSpec, defaultStatus);
      // new changed specification of the object
      const newObj = createVolumeResource(
        UUID,
        {
          replicaCount: 3,
          local: true,
          preferredNodes: ['node1'],
          requiredNodes: [],
          requiredBytes: 90,
          limitBytes: 130,
          protocol: 'nvmf'
        },
        defaultStatus
      );

      oper = await createVolumeOperator(volumes, (arg) => {
        stubs = arg;
        stubs.get.returns(oldObj);
      });
      // trigger "mod" event
      oper.watcher.emit('mod', newObj);
      // give event callbacks time to propagate
      await sleep(EVENT_PROPAGATION_DELAY);

      sinon.assert.calledOnce(fsaStub);
      expect(volume.spec.replicaCount).to.equal(3);
      expect(volume.spec.local).to.be.true;
      expect(volume.spec.preferredNodes).to.have.lengthOf(1);
      expect(volume.spec.requiredNodes).to.have.lengthOf(0);
      expect(volume.spec.requiredBytes).to.equal(90);
      expect(volume.spec.limitBytes).to.equal(130);
    });

    it('should not crash if update volume fails upon "mod" event', async () => {
      let stubs;
      const registry = new Registry({});
      const volumes = new Volumes(registry);
      const volume = new Volume(UUID, registry, new EventEmitter(), defaultSpec);
      volume.size = 110;
      const fsaStub = sinon.stub(volume, 'fsa');
      fsaStub.resolves();
      sinon
        .stub(volumes, 'get')
        .withArgs(UUID)
        .returns(volume);
      sinon
        .stub(volumes, 'list')
        .withArgs()
        .returns([volume]);
      const oldObj = createVolumeResource(UUID, defaultSpec, defaultStatus);
      // new changed specification of the object
      const newObj = createVolumeResource(
        UUID,
        {
          replicaCount: 3,
          local: true,
          preferredNodes: ['node1'],
          requiredNodes: [],
          requiredBytes: 111,
          limitBytes: 130,
          protocol: 'nvmf'
        },
        defaultStatus
      );

      oper = await createVolumeOperator(volumes, (arg) => {
        stubs = arg;
        stubs.get.returns(oldObj);
      });
      // trigger "mod" event
      oper.watcher.emit('mod', newObj);
      // give event callbacks time to propagate
      await sleep(EVENT_PROPAGATION_DELAY);

      sinon.assert.notCalled(fsaStub);
      expect(volume.spec.replicaCount).to.equal(2);
      expect(volume.spec.requiredBytes).to.equal(100);
      expect(volume.spec.limitBytes).to.equal(120);
    });

    it('should not do anything if volume params stay the same upon "mod" event', async () => {
      let stubs;
      const registry = new Registry({});
      const volumes = new Volumes(registry);
      const volume = new Volume(UUID, registry, new EventEmitter(), defaultSpec);
      volume.size = 110;
      const fsaStub = sinon.stub(volume, 'fsa');
      fsaStub.returns();
      sinon
        .stub(volumes, 'get')
        .withArgs(UUID)
        .returns(volume);
      sinon
        .stub(volumes, 'list')
        .withArgs()
        .returns([]);
      const oldObj = createVolumeResource(UUID, defaultSpec, defaultStatus);
      // new specification of the object that is the same
      const newObj = createVolumeResource(UUID, defaultSpec, defaultStatus);

      oper = await createVolumeOperator(volumes, (arg) => {
        stubs = arg;
        stubs.get.returns(oldObj);
      });
      // trigger "mod" event
      oper.watcher.emit('mod', newObj);
      // give event callbacks time to propagate
      await sleep(EVENT_PROPAGATION_DELAY);

      sinon.assert.notCalled(fsaStub);
    });
  });

  describe('volume events', () => {
    let oper; // volume operator

    afterEach(async () => {
      if (oper) {
        await oper.stop();
        oper = null;
      }
    });

    it('should create a resource upon "new" volume event', async () => {
      let stubs;
      const registry = new Registry({});
      const volume = new Volume(UUID, registry, new EventEmitter(), defaultSpec);
      const volumes = new Volumes(registry);
      sinon
        .stub(volumes, 'get')
        .withArgs(UUID)
        .returns(volume);
      sinon
        .stub(volumes, 'list')
        .withArgs()
        .returns([volume]);

      const volumeResource = createVolumeResource(UUID, defaultSpec);
      oper = await createVolumeOperator(volumes, (arg) => {
        stubs = arg;
        stubs.get.onFirstCall().returns();
        stubs.get.onSecondCall().returns(volumeResource);
        stubs.create.resolves();
        stubs.updateStatus.resolves();
      });

      sinon.assert.calledOnce(stubs.create);
      expect(stubs.create.args[0][4].metadata.name).to.equal(UUID);
      expect(stubs.create.args[0][4].metadata.namespace).to.equal(NAMESPACE);
      expect(stubs.create.args[0][4].spec).to.deep.equal(defaultSpec);
      sinon.assert.calledOnce(stubs.updateStatus);
      expect(stubs.updateStatus.args[0][5].status).to.deep.equal({
        replicas: [],
        size: 0,
        state: 'pending'
      });
      expect(stubs.updateStatus.args[0][5].status.targetNodes).to.be.undefined;
    });

    it('should not crash if POST fails upon "new" volume event', async () => {
      let stubs;
      const registry = new Registry({});
      const volumes = new Volumes(registry);
      const volume = new Volume(UUID, registry, new EventEmitter(), defaultSpec);
      sinon.stub(volumes, 'get').returns([]);

      const volumeResource = createVolumeResource(UUID, defaultSpec);
      oper = await createVolumeOperator(volumes, (arg) => {
        stubs = arg;
        stubs.get.onFirstCall().returns();
        stubs.get.onSecondCall().returns(volumeResource);
        stubs.create.rejects(new Error('POST failed'));
        stubs.updateStatus.resolves();
      });

      volumes.emit('volume', {
        eventType: 'new',
        object: volume
      });
      await sleep(EVENT_PROPAGATION_DELAY);
      sinon.assert.calledOnce(stubs.create);
      sinon.assert.notCalled(stubs.updateStatus);
    });

    it('should update the resource upon "new" volume event if it exists', async () => {
      let stubs;
      const registry = new Registry({});
      const volumes = new Volumes(registry);
      const newSpec = _.cloneDeep(defaultSpec);
      newSpec.replicaCount += 1;
      const volume = new Volume(UUID, registry, new EventEmitter(), newSpec);
      sinon
        .stub(volumes, 'get')
        .withArgs(UUID)
        .returns(volume);
      sinon
        .stub(volumes, 'list')
        .withArgs()
        .returns([volume]);

      const volumeResource = createVolumeResource(UUID, defaultSpec);
      oper = await createVolumeOperator(volumes, (arg) => {
        stubs = arg;
        stubs.get.returns(volumeResource);
        stubs.update.resolves();
        stubs.updateStatus.resolves();
      });

      sinon.assert.notCalled(stubs.create);
      sinon.assert.calledOnce(stubs.update);
      expect(stubs.update.args[0][5].spec).to.deep.equal(newSpec);
      sinon.assert.calledOnce(stubs.updateStatus);
    });

    it('should not update the resource upon "new" volume event if it is the same', async () => {
      let stubs;
      const registry = new Registry({});
      const volumes = new Volumes(registry);
      const volume = new Volume(UUID, registry, new EventEmitter(), defaultSpec, 'pending', 100, 'node2');
      sinon
        .stub(volumes, 'get')
        .withArgs(UUID)
        .returns(volume);
      sinon
        .stub(volumes, 'list')
        .withArgs()
        .returns([volume]);

      const volumeResource = createVolumeResource(UUID, defaultSpec, {
        size: 100,
        targetNodes: ['node2'],
        state: 'pending',
        replicas: []
      });
      oper = await createVolumeOperator(volumes, (arg) => {
        stubs = arg;
        stubs.get.returns(volumeResource);
        stubs.update.resolves();
        stubs.updateStatus.resolves();
      });

      sinon.assert.notCalled(stubs.create);
      sinon.assert.notCalled(stubs.update);
      sinon.assert.notCalled(stubs.updateStatus);
    });

    it('should update the resource upon "mod" volume event', async () => {
      let stubs;
      const registry = new Registry({});
      const volumes = new Volumes(registry);
      sinon.stub(volumes, 'get').returns([]);

      const volumeResource = createVolumeResource(UUID, defaultSpec);
      oper = await createVolumeOperator(volumes, (arg) => {
        stubs = arg;
        stubs.get.returns(volumeResource);
        stubs.update.resolves();
        stubs.updateStatus.resolves();
      });

      const newSpec = {
        replicaCount: 3,
        local: true,
        preferredNodes: [],
        requiredNodes: [],
        requiredBytes: 90,
        limitBytes: 130,
        protocol: 'nvmf'
      };
      const volume = new Volume(UUID, registry, new EventEmitter(), newSpec);
      volumes.emit('volume', {
        eventType: 'mod',
        object: volume
      });
      await sleep(EVENT_PROPAGATION_DELAY);

      sinon.assert.calledOnce(stubs.update);
      expect(stubs.update.args[0][5].spec).to.deep.equal(newSpec);
      sinon.assert.calledOnce(stubs.updateStatus);
    });

    it('should update just the status if spec has not changed upon "mod" volume event', async () => {
      let stubs;
      const registry = new Registry({});
      const volumes = new Volumes(registry);
      sinon.stub(volumes, 'get').returns([]);

      const volumeResource = createVolumeResource(UUID, defaultSpec);
      oper = await createVolumeOperator(volumes, (arg) => {
        stubs = arg;
        stubs.get.returns(volumeResource);
        stubs.update.resolves();
        stubs.updateStatus.resolves();
      });

      const volume = new Volume(UUID, registry, new EventEmitter(), defaultSpec);
      volumes.emit('volume', {
        eventType: 'mod',
        object: volume
      });
      await sleep(EVENT_PROPAGATION_DELAY);

      sinon.assert.notCalled(stubs.update);
      sinon.assert.calledOnce(stubs.updateStatus);
    });

    it('should not update the status if only the order of entries in arrays differ', async () => {
      let stubs;
      const registry = new Registry({});
      const volumes = new Volumes(registry);
      sinon.stub(volumes, 'get').returns([]);

      const volumeResource = createVolumeResource(UUID, defaultSpec, defaultStatus);
      oper = await createVolumeOperator(volumes, (arg) => {
        stubs = arg;
        stubs.get.returns(volumeResource);
        stubs.update.resolves();
        stubs.updateStatus.resolves();
      });

      const volume = createDefaultVolume(registry);
      volumeResource.status.replicas.reverse();
      sinon.stub(volume, 'getReplicas').returns(
        [].concat(Object.values(volume.replicas))
          // reverse the order of replicas
          .sort((a, b) => {
            return (-1) * a.pool.node.name.localeCompare(b.pool.node.name);
          })
      );
      volumes.emit('volume', {
        eventType: 'mod',
        object: volume
      });
      await sleep(EVENT_PROPAGATION_DELAY);

      sinon.assert.notCalled(stubs.update);
      sinon.assert.notCalled(stubs.updateStatus);
    });

    it('should not crash if PUT fails upon "mod" volume event', async () => {
      let stubs;
      const registry = new Registry({});
      const volumes = new Volumes(registry);
      sinon.stub(volumes, 'get').returns([]);

      const volumeResource = createVolumeResource(UUID, defaultSpec);
      oper = await createVolumeOperator(volumes, (arg) => {
        stubs = arg;
        stubs.get.returns(volumeResource);
        stubs.update.rejects(new Error('PUT failed'));
        stubs.updateStatus.resolves();
      });

      const newSpec = {
        replicaCount: 3,
        local: true,
        preferredNodes: [],
        requiredNodes: [],
        requiredBytes: 90,
        limitBytes: 130,
        protocol: 'nvmf'
      };
      const volume = new Volume(UUID, registry, new EventEmitter(), newSpec);
      volumes.emit('volume', {
        eventType: 'mod',
        object: volume
      });
      await sleep(EVENT_PROPAGATION_DELAY);

      sinon.assert.calledTwice(stubs.update);
      sinon.assert.calledOnce(stubs.updateStatus);
    });

    it('should not crash if the resource does not exist upon "mod" volume event', async () => {
      let stubs;
      const registry = new Registry({});
      const volumes = new Volumes(registry);
      sinon.stub(volumes, 'get').returns([]);

      oper = await createVolumeOperator(volumes, (arg) => {
        stubs = arg;
        stubs.get.returns();
      });

      const newSpec = {
        replicaCount: 3,
        local: true,
        preferredNodes: [],
        requiredNodes: [],
        requiredBytes: 90,
        limitBytes: 130,
        protocol: 'nvmf'
      };
      const volume = new Volume(UUID, registry, new EventEmitter(), newSpec);
      volumes.emit('volume', {
        eventType: 'mod',
        object: volume
      });
      await sleep(EVENT_PROPAGATION_DELAY);

      sinon.assert.notCalled(stubs.create);
      sinon.assert.notCalled(stubs.update);
      sinon.assert.notCalled(stubs.updateStatus);
    });

    it('should delete the resource upon "del" volume event', async () => {
      let stubs;
      const volumeResource = createVolumeResource(UUID, defaultSpec);
      const registry = new Registry({});
      const volumes = new Volumes(registry);
      sinon.stub(volumes, 'get').returns([]);

      oper = await createVolumeOperator(volumes, (arg) => {
        stubs = arg;
        stubs.get.returns(volumeResource);
        stubs.delete.resolves();
      });

      const volume = new Volume(UUID, registry, new EventEmitter(), defaultSpec);
      volumes.emit('volume', {
        eventType: 'del',
        object: volume
      });
      await sleep(EVENT_PROPAGATION_DELAY);

      sinon.assert.calledOnce(stubs.delete);
    });

    it('should not crash if DELETE fails upon "del" volume event', async () => {
      let stubs;
      const volumeResource = createVolumeResource(UUID, defaultSpec);
      const registry = new Registry({});
      const volumes = new Volumes(registry);
      sinon.stub(volumes, 'get').returns([]);

      oper = await createVolumeOperator(volumes, (arg) => {
        stubs = arg;
        stubs.get.returns(volumeResource);
        stubs.delete.rejects(new Error('delete failed'));
      });

      const volume = new Volume(UUID, registry, new EventEmitter(), defaultSpec);
      volumes.emit('volume', {
        eventType: 'del',
        object: volume
      });
      await sleep(EVENT_PROPAGATION_DELAY);

      sinon.assert.calledOnce(stubs.delete);
    });

    it('should not crash if the resource does not exist upon "del" volume event', async () => {
      let stubs;
      const registry = new Registry({});
      const volumes = new Volumes(registry);
      sinon.stub(volumes, 'get').returns([]);

      oper = await createVolumeOperator(volumes, (arg) => {
        stubs = arg;
        stubs.get.returns();
        stubs.delete.resolves();
      });

      const volume = new Volume(UUID, registry, new EventEmitter(), defaultSpec);
      volumes.emit('volume', {
        eventType: 'del',
        object: volume
      });
      await sleep(EVENT_PROPAGATION_DELAY);

      sinon.assert.notCalled(stubs.delete);
      sinon.assert.notCalled(stubs.create);
      sinon.assert.notCalled(stubs.update);
    });
  });
};
