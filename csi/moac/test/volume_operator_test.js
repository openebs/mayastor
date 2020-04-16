// Unit tests for the volume operator
//
// We don't test the init method which depends on k8s api client and watcher.
// That method *must* be tested manually and in real k8s environment. For the
// rest of the dependencies we provide fake objects which mimic the real
// behaviour and allow us to test volume operator in isolation from other
// components.

'use strict';

const _ = require('lodash');
const expect = require('chai').expect;
const sinon = require('sinon');
const sleep = require('sleep-promise');
const Registry = require('../registry');
const Volume = require('../volume');
const Volumes = require('../volumes');
const VolumeOperator = require('../volume_operator');
const { GrpcError, GrpcCode } = require('../grpc_client');
const Watcher = require('./watcher_stub');

const UUID = 'd01b8bfb-0116-47b0-a03a-447fcbdc0e99';
const NAMESPACE = 'mayastor';

function defaultMeta(uuid) {
  return {
    creationTimestamp: '2019-02-15T18:23:53Z',
    generation: 1,
    name: uuid,
    namespace: NAMESPACE,
    resourceVersion: '627981',
    selfLink: `/apis/openebs.io/v1alpha1/namespaces/${NAMESPACE}/mayastorvolumes/${uuid}`,
    uid: 'd99f06a9-314e-11e9-b086-589cfc0d76a7',
  };
}

module.exports = function() {
  var msStub, putStub, putStatusStub, deleteStub, postStub;
  var defaultSpec = {
    replicaCount: 1,
    preferredNodes: ['node1', 'node2'],
    requiredNodes: ['node2'],
    requiredBytes: 100,
    limitBytes: 120,
  };
  var defaultStatus = {
    size: 110,
    node: 'node2',
    state: 'ONLINE',
    replicas: [
      {
        uri: 'bdev:///' + UUID,
        node: 'node2',
        state: 'ONLINE',
      },
    ],
  };

  // Create k8s volume resource object
  function createVolumeResource(uuid, spec, status) {
    let obj = {
      apiVersion: 'openebs.io/v1alpha1',
      kind: 'MayastorVolume',
      metadata: defaultMeta(uuid),
      spec: spec,
    };
    if (status) {
      obj.status = status;
    }
    return obj;
  }

  // k8s api client stub.
  //
  // Note that this stub serves only for PUT method on mayastor resource
  // endpoint to update the status of resource. Fake watcher that is used
  // in the tests does not use this client stub.
  function createK8sClient(watcher) {
    let mayastorvolumes = { mayastorvolumes: function(name) {} };
    let namespaces = function(ns) {
      expect(ns).to.equal(NAMESPACE);
      return mayastorvolumes;
    };
    let client = {
      apis: {
        'openebs.io': {
          v1alpha1: { namespaces },
        },
      },
    };

    msStub = sinon.stub(mayastorvolumes, 'mayastorvolumes');
    msStub.post = async function(payload) {
      watcher.objects[payload.body.metadata.name] = payload.body;
      // simulate the asynchronicity of the put
      await sleep(1);
    };
    postStub = sinon.stub(msStub, 'post');
    postStub.callThrough();

    let msObject = {
      // the tricky thing here is that we have to update watcher's cache
      // if we use this fake k8s client to change the object in order to
      // mimic real behaviour.
      put: async function(payload) {
        watcher.objects[payload.body.metadata.name].spec = payload.body.spec;
      },
      delete: async function() {},
      status: {
        put: async function(payload) {
          watcher.objects[payload.body.metadata.name].status =
            payload.body.status;
        },
      },
    };
    putStub = sinon.stub(msObject, 'put');
    putStub.callThrough();
    putStatusStub = sinon.stub(msObject.status, 'put');
    putStatusStub.callThrough();
    deleteStub = sinon.stub(msObject, 'delete');
    deleteStub.callThrough();
    msStub.returns(msObject);
    return client;
  }

  // Create a pool operator object suitable for testing - with fake watcher
  // and fake k8s api client.
  async function mockedVolumeOperator(k8sObjects, volumes) {
    let oper = new VolumeOperator(NAMESPACE);
    oper.volumes = volumes;
    oper.watcher = new Watcher(oper._filterMayastorVolume, k8sObjects);
    oper.k8sClient = createK8sClient(oper.watcher);

    await oper.start();
    return oper;
  }

  describe('resource filter', () => {
    it('valid mayastor volume with status should pass the filter', () => {
      let obj = createVolumeResource(
        UUID,
        {
          replicaCount: 3,
          preferredNodes: ['node1', 'node2'],
          requiredNodes: ['node2'],
          requiredBytes: 100,
          limitBytes: 120,
        },
        {
          size: 110,
          node: 'node2',
          state: 'ONLINE',
          replicas: [
            {
              uri: 'bdev:///' + UUID,
              node: 'node2',
              state: 'ONLINE',
            },
          ],
        }
      );

      let res = VolumeOperator.prototype._filterMayastorVolume(obj);
      expect(res.metadata.name).to.equal(UUID);
      expect(res.spec.replicaCount).to.equal(3);
      expect(res.spec.preferredNodes).to.have.lengthOf(2);
      expect(res.spec.preferredNodes[0]).to.equal('node1');
      expect(res.spec.preferredNodes[1]).to.equal('node2');
      expect(res.spec.requiredNodes).to.have.lengthOf(1);
      expect(res.spec.requiredNodes[0]).to.equal('node2');
      expect(res.spec.requiredBytes).to.equal(100);
      expect(res.spec.limitBytes).to.equal(120);
      expect(res.status.size).to.equal(110);
      expect(res.status.node).to.equal('node2');
      expect(res.status.state).to.equal('ONLINE');
      expect(res.status.replicas).to.have.lengthOf(1);
      expect(res.status.replicas[0].uri).to.equal('bdev:///' + UUID);
      expect(res.status.replicas[0].node).to.equal('node2');
      expect(res.status.replicas[0].state).to.equal('ONLINE');
    });

    it('valid mayastor volume without status should pass the filter', () => {
      let obj = createVolumeResource(UUID, {
        replicaCount: 3,
        preferredNodes: ['node1', 'node2'],
        requiredNodes: ['node2'],
        requiredBytes: 100,
        limitBytes: 120,
      });
      let res = VolumeOperator.prototype._filterMayastorVolume(obj);
      expect(res.metadata.name).to.equal(UUID);
      expect(res.spec.replicaCount).to.equal(3);
      // jshint ignore:start
      expect(res.status).to.be.undefined;
      // jshint ignore:end
    });

    it('mayastor volume without optional parameters should pass the filter', () => {
      let obj = createVolumeResource(UUID, {
        requiredBytes: 100,
      });
      let res = VolumeOperator.prototype._filterMayastorVolume(obj);
      expect(res.metadata.name).to.equal(UUID);
      expect(res.spec.replicaCount).to.equal(1);
      expect(res.spec.preferredNodes).to.have.lengthOf(0);
      expect(res.spec.requiredNodes).to.have.lengthOf(0);
      expect(res.spec.requiredBytes).to.equal(100);
      expect(res.spec.limitBytes).to.equal(0);
      // jshint ignore:start
      expect(res.status).to.be.undefined;
      // jshint ignore:end
    });

    it('mayastor volume without requiredSize should be ignored', () => {
      let obj = createVolumeResource(UUID, {
        replicaCount: 3,
        preferredNodes: ['node1', 'node2'],
        requiredNodes: ['node2'],
        limitBytes: 120,
      });
      let res = VolumeOperator.prototype._filterMayastorVolume(obj);
      // jshint ignore:start
      expect(res).to.be.null;
      // jshint ignore:end
    });

    it('mayastor volume with invalid UUID should be ignored', () => {
      let obj = createVolumeResource('blabla', {
        replicaCount: 3,
        preferredNodes: ['node1', 'node2'],
        requiredNodes: ['node2'],
        requiredBytes: 100,
        limitBytes: 120,
      });
      let res = VolumeOperator.prototype._filterMayastorVolume(obj);
      // jshint ignore:start
      expect(res).to.be.null;
      // jshint ignore:end
    });
  });

  describe('watcher events', () => {
    var oper; // volume operator

    afterEach(async () => {
      if (oper) {
        await oper.stop();
        oper = null;
      }
    });

    it('should call create volume for existing resources when starting the operator', async () => {
      let registry = new Registry();
      let volumes = new Volumes(registry);
      let createVolumeStub = sinon.stub(volumes, 'createVolume');
      // return value is not used so just return something
      createVolumeStub.resolves({ uuid: UUID });

      oper = await mockedVolumeOperator(
        [createVolumeResource(UUID, defaultSpec, defaultStatus)],
        volumes
      );
      sinon.assert.calledOnce(createVolumeStub);
      sinon.assert.calledWith(createVolumeStub, UUID, defaultSpec);
    });

    it('should create volume upon "new" event', async () => {
      let registry = new Registry();
      let volumes = new Volumes(registry);
      let createVolumeStub = sinon.stub(volumes, 'createVolume');
      createVolumeStub.resolves({ uuid: UUID });

      oper = await mockedVolumeOperator([], volumes);
      // trigger "new" event
      oper.watcher.newObject(createVolumeResource(UUID, defaultSpec));
      sinon.assert.calledOnce(createVolumeStub);
      sinon.assert.calledWith(createVolumeStub, UUID, defaultSpec);
    });

    it('should not try to create volume upon "new" event if the resource was self-created', async () => {
      let registry = new Registry();
      let volumes = new Volumes(registry);
      sinon.stub(volumes, 'get').returns([]);
      let createVolumeStub = sinon.stub(volumes, 'createVolume');
      createVolumeStub.resolves({ uuid: UUID });

      oper = await mockedVolumeOperator([], volumes);
      // Pretend the volume creation through i.e. CSI.
      await sleep(10);
      let volume = new Volume(UUID, registry, defaultSpec);
      volumes.emit('volume', {
        eventType: 'new',
        object: volume,
      });
      await sleep(10);
      // now trigger "new" watcher event (natural consequence of the above)
      oper.watcher.newObject(createVolumeResource(UUID, defaultSpec));
      sinon.assert.notCalled(createVolumeStub);
    });

    it('should set reason in resource if volume creation fails upon "new" event', async () => {
      let registry = new Registry();
      let volumes = new Volumes(registry);
      let createVolumeStub = sinon.stub(volumes, 'createVolume');
      createVolumeStub.rejects(
        new GrpcError(GrpcCode.INTERNAL, 'create failed')
      );

      oper = await mockedVolumeOperator([], volumes);
      // trigger "new" event
      oper.watcher.newObject(createVolumeResource(UUID, defaultSpec));
      await sleep(10);
      sinon.assert.calledOnce(createVolumeStub);
      sinon.assert.calledOnce(msStub);
      sinon.assert.calledWith(msStub, UUID);
      sinon.assert.notCalled(postStub);
      sinon.assert.notCalled(putStub);
      sinon.assert.calledOnce(putStatusStub);
      sinon.assert.calledWithMatch(putStatusStub, {
        body: {
          metadata: defaultMeta(UUID),
          status: {
            state: 'PENDING',
            reason: 'Error: create failed',
          },
        },
      });
    });

    it('should destroy the volume upon "del" event', async () => {
      let registry = new Registry();
      let volumes = new Volumes(registry);
      let destroyVolumeStub = sinon.stub(volumes, 'destroyVolume');
      destroyVolumeStub.resolves();
      let obj = createVolumeResource(UUID, defaultSpec, defaultStatus);

      oper = await mockedVolumeOperator([], volumes);
      oper.watcher.injectObject(obj);
      // trigger "del" event
      oper.watcher.delObject(UUID);
      sinon.assert.calledOnce(destroyVolumeStub);
      sinon.assert.calledWith(destroyVolumeStub, UUID);
    });

    it('should handle gracefully if destroy of a volume fails upon "del" event', async () => {
      let registry = new Registry();
      let volumes = new Volumes(registry);
      let destroyVolumeStub = sinon.stub(volumes, 'destroyVolume');
      destroyVolumeStub.rejects(
        new GrpcError(GrpcCode.INTERNAL, 'destroy failed')
      );
      let obj = createVolumeResource(UUID, defaultSpec, defaultStatus);

      oper = await mockedVolumeOperator([], volumes);
      oper.watcher.injectObject(obj);
      // trigger "del" event
      oper.watcher.delObject(UUID);
      sinon.assert.calledOnce(destroyVolumeStub);
      sinon.assert.calledWith(destroyVolumeStub, UUID);
    });

    it('should modify the volume upon "mod" event', async () => {
      let registry = new Registry();
      let volumes = new Volumes(registry);
      let volume = new Volume(UUID, registry, defaultSpec);
      volume.size = 110;
      let ensureStub = sinon.stub(volume, 'ensure');
      ensureStub.resolves();
      sinon
        .stub(volumes, 'get')
        .withArgs(UUID)
        .returns(volume)
        .withArgs()
        .returns([]);
      let oldObj = createVolumeResource(UUID, defaultSpec, defaultStatus);
      // new changed specification of the object
      let newObj = createVolumeResource(
        UUID,
        {
          replicaCount: 3,
          preferredNodes: ['node1'],
          requiredNodes: [],
          requiredBytes: 90,
          limitBytes: 130,
        },
        defaultStatus
      );

      oper = await mockedVolumeOperator([], volumes);
      oper.watcher.injectObject(oldObj);
      // trigger "mod" event
      oper.watcher.modObject(newObj);

      sinon.assert.calledOnce(ensureStub);
      expect(volume.replicaCount).to.equal(3);
      expect(volume.preferredNodes).to.have.lengthOf(1);
      expect(volume.requiredNodes).to.have.lengthOf(0);
      expect(volume.requiredBytes).to.equal(90);
      expect(volume.limitBytes).to.equal(130);
    });

    it('should not crash if update volume fails upon "mod" event', async () => {
      let registry = new Registry();
      let volumes = new Volumes(registry);
      let volume = new Volume(UUID, registry, defaultSpec);
      volume.size = 110;
      let ensureStub = sinon.stub(volume, 'ensure');
      ensureStub.resolves();
      sinon
        .stub(volumes, 'get')
        .withArgs(UUID)
        .returns(volume)
        .withArgs()
        .returns([]);
      let oldObj = createVolumeResource(UUID, defaultSpec, defaultStatus);
      // new changed specification of the object
      let newObj = createVolumeResource(
        UUID,
        {
          replicaCount: 3,
          preferredNodes: ['node1'],
          requiredNodes: [],
          requiredBytes: 111,
          limitBytes: 130,
        },
        defaultStatus
      );

      oper = await mockedVolumeOperator([], volumes);
      oper.watcher.injectObject(oldObj);
      // trigger "mod" event
      oper.watcher.modObject(newObj);

      sinon.assert.notCalled(ensureStub);
      expect(volume.replicaCount).to.equal(1);
      expect(volume.requiredBytes).to.equal(100);
      expect(volume.limitBytes).to.equal(120);
    });

    it('should not crash if ensure volume fails upon "mod" event', async () => {
      let registry = new Registry();
      let volumes = new Volumes(registry);
      let volume = new Volume(UUID, registry, defaultSpec);
      volume.size = 110;
      let ensureStub = sinon.stub(volume, 'ensure');
      ensureStub.rejects(new GrpcError(GrpcCode.INTERNAL, 'ensure failed'));
      sinon
        .stub(volumes, 'get')
        .withArgs(UUID)
        .returns(volume)
        .withArgs()
        .returns([]);
      let oldObj = createVolumeResource(UUID, defaultSpec, defaultStatus);
      // new changed specification of the object
      let newObj = createVolumeResource(
        UUID,
        {
          replicaCount: 3,
          preferredNodes: ['node1'],
          requiredNodes: [],
          requiredBytes: 100,
          limitBytes: 120,
        },
        defaultStatus
      );

      oper = await mockedVolumeOperator([], volumes);
      oper.watcher.injectObject(oldObj);
      // trigger "mod" event
      oper.watcher.modObject(newObj);

      // ensure failed nevertheless the params should have been updated
      sinon.assert.calledOnce(ensureStub);
      expect(volume.replicaCount).to.equal(3);
      expect(volume.requiredBytes).to.equal(100);
      expect(volume.limitBytes).to.equal(120);
    });

    it('should not do anything if volume params stay the same upon "mod" event', async () => {
      let registry = new Registry();
      let volumes = new Volumes(registry);
      let volume = new Volume(UUID, registry, defaultSpec);
      volume.size = 110;
      let ensureStub = sinon.stub(volume, 'ensure');
      ensureStub.rejects(new GrpcError(GrpcCode.INTERNAL, 'ensure failed'));
      sinon
        .stub(volumes, 'get')
        .withArgs(UUID)
        .returns(volume)
        .withArgs()
        .returns([]);
      let oldObj = createVolumeResource(UUID, defaultSpec, defaultStatus);
      // new specification of the object that is the same
      let newObj = createVolumeResource(UUID, defaultSpec, defaultStatus);

      oper = await mockedVolumeOperator([], volumes);
      oper.watcher.injectObject(oldObj);
      // trigger "mod" event
      oper.watcher.modObject(newObj);

      // ensure failed nevertheless the params should have been updated
      sinon.assert.notCalled(ensureStub);
    });
  });

  describe('volume events', () => {
    var oper; // volume operator

    afterEach(async () => {
      if (oper) {
        await oper.stop();
        oper = null;
      }
    });

    it('should create a resource upon "new" volume event', async () => {
      let registry = new Registry();
      let volume = new Volume(UUID, registry, defaultSpec);
      let volumes = new Volumes(registry);
      sinon
        .stub(volumes, 'get')
        .withArgs(UUID)
        .returns(volume)
        .withArgs()
        .returns([volume]);

      oper = await mockedVolumeOperator([], volumes);

      await sleep(20);
      sinon.assert.calledOnce(postStub);
      sinon.assert.calledWithMatch(postStub, {
        body: {
          metadata: {
            name: UUID,
            namespace: NAMESPACE,
          },
          spec: defaultSpec,
        },
      });
      sinon.assert.calledOnce(putStatusStub);
      sinon.assert.calledWithMatch(putStatusStub, {
        body: {
          status: {
            node: '',
            reason: 'The volume is being created',
            replicas: [],
            size: 0,
            state: 'PENDING',
          },
        },
      });
    });

    it('should not crash if POST fails upon "new" volume event', async () => {
      let registry = new Registry();
      let volumes = new Volumes(registry);
      let volume = new Volume(UUID, registry, defaultSpec);
      sinon.stub(volumes, 'get').returns([]);

      oper = await mockedVolumeOperator([], volumes);
      postStub.rejects(new Error('post failed'));
      // we have to sleep to give event stream chance to register its handlers
      await sleep(10);
      volumes.emit('volume', {
        eventType: 'new',
        object: volume,
      });
      await sleep(10);
      sinon.assert.calledOnce(postStub);
      sinon.assert.notCalled(putStatusStub);
    });

    it('should update the resource upon "new" volume event if it exists', async () => {
      let registry = new Registry();
      let volumes = new Volumes(registry);
      let newSpec = _.cloneDeep(defaultSpec);
      newSpec.replicaCount += 1;
      let volume = new Volume(UUID, registry, newSpec);
      sinon
        .stub(volumes, 'get')
        .withArgs(UUID)
        .returns(volume)
        .withArgs()
        .returns([volume]);

      oper = await mockedVolumeOperator([], volumes);
      let obj = createVolumeResource(UUID, defaultSpec);
      oper.watcher.injectObject(obj);

      await sleep(10);
      sinon.assert.notCalled(postStub);
      sinon.assert.calledOnce(putStub);
      sinon.assert.calledWithMatch(putStub, {
        body: { spec: newSpec },
      });
      sinon.assert.calledOnce(putStatusStub);
    });

    it('should not update the resource upon "new" volume event if it is the same', async () => {
      let registry = new Registry();
      let volumes = new Volumes(registry);
      let volume = new Volume(UUID, registry, defaultSpec);
      sinon
        .stub(volumes, 'get')
        .withArgs(UUID)
        .returns(volume)
        .withArgs()
        .returns([volume]);

      oper = await mockedVolumeOperator([], volumes);
      let obj = createVolumeResource(UUID, defaultSpec, {
        size: 0,
        node: '',
        state: 'PENDING',
        reason: 'The volume is being created',
        replicas: [],
      });
      oper.watcher.injectObject(obj);

      await sleep(10);
      sinon.assert.notCalled(putStub);
      sinon.assert.notCalled(postStub);
      sinon.assert.notCalled(putStatusStub);
    });

    it('should update the resource upon "mod" volume event', async () => {
      let obj = createVolumeResource(UUID, defaultSpec);
      let registry = new Registry();
      let volumes = new Volumes(registry);
      sinon.stub(volumes, 'get').returns([]);

      oper = await mockedVolumeOperator([], volumes);
      oper.watcher.injectObject(obj);
      // we have to sleep to give event stream chance to register its handlers
      await sleep(10);

      let newSpec = {
        replicaCount: 3,
        preferredNodes: [],
        requiredNodes: [],
        requiredBytes: 90,
        limitBytes: 130,
      };
      let volume = new Volume(UUID, registry, newSpec);
      volumes.emit('volume', {
        eventType: 'mod',
        object: volume,
      });

      await sleep(10);
      sinon.assert.calledOnce(putStub);
      sinon.assert.calledWithMatch(putStub, {
        body: {
          metadata: defaultMeta(UUID),
          spec: newSpec,
        },
      });
      sinon.assert.calledOnce(putStatusStub);
    });

    it('should update just the status if spec has not changed upon "mod" volume event', async () => {
      let obj = createVolumeResource(UUID, defaultSpec);
      let registry = new Registry();
      let volumes = new Volumes(registry);
      sinon.stub(volumes, 'get').returns([]);

      oper = await mockedVolumeOperator([], volumes);
      oper.watcher.injectObject(obj);
      // we have to sleep to give event stream chance to register its handlers
      await sleep(10);

      let volume = new Volume(UUID, registry, defaultSpec);
      volumes.emit('volume', {
        eventType: 'mod',
        object: volume,
      });

      await sleep(10);
      sinon.assert.notCalled(putStub);
      sinon.assert.calledOnce(putStatusStub);
    });

    it('should not crash if PUT fails upon "mod" volume event', async () => {
      let obj = createVolumeResource(UUID, defaultSpec);
      let registry = new Registry();
      let volumes = new Volumes(registry);
      sinon.stub(volumes, 'get').returns([]);

      oper = await mockedVolumeOperator([], volumes);
      oper.watcher.injectObject(obj);
      putStub.rejects(new Error('put failed'));
      // we have to sleep to give event stream chance to register its handlers
      await sleep(10);

      let newSpec = {
        replicaCount: 3,
        preferredNodes: [],
        requiredNodes: [],
        requiredBytes: 90,
        limitBytes: 130,
      };
      let volume = new Volume(UUID, registry, newSpec);
      volumes.emit('volume', {
        eventType: 'mod',
        object: volume,
      });

      await sleep(10);
      sinon.assert.calledOnce(putStub);
      sinon.assert.notCalled(putStatusStub);
    });

    it('should not crash if the resource does not exist upon "mod" volume event', async () => {
      let obj = createVolumeResource(UUID, defaultSpec);
      let registry = new Registry();
      let volumes = new Volumes(registry);
      sinon.stub(volumes, 'get').returns([]);

      oper = await mockedVolumeOperator([], volumes);
      // we have to sleep to give event stream chance to register its handlers
      await sleep(10);

      let newSpec = {
        replicaCount: 3,
        preferredNodes: [],
        requiredNodes: [],
        requiredBytes: 90,
        limitBytes: 130,
      };
      let volume = new Volume(UUID, registry, newSpec);
      volumes.emit('volume', {
        eventType: 'mod',
        object: volume,
      });

      await sleep(10);
      sinon.assert.notCalled(postStub);
      sinon.assert.notCalled(putStub);
      sinon.assert.notCalled(putStatusStub);
    });

    it('should delete the resource upon "del" volume event', async () => {
      let obj = createVolumeResource(UUID, defaultSpec);
      let registry = new Registry();
      let volumes = new Volumes(registry);
      sinon.stub(volumes, 'get').returns([]);

      oper = await mockedVolumeOperator([], volumes);
      oper.watcher.injectObject(obj);
      // we have to sleep to give event stream chance to register its handlers
      await sleep(10);

      let volume = new Volume(UUID, registry, defaultSpec);
      volumes.emit('volume', {
        eventType: 'del',
        object: volume,
      });

      await sleep(10);
      sinon.assert.calledOnce(deleteStub);
    });

    it('should not crash if DELETE fails upon "del" volume event', async () => {
      let obj = createVolumeResource(UUID, defaultSpec);
      let registry = new Registry();
      let volumes = new Volumes(registry);
      sinon.stub(volumes, 'get').returns([]);

      oper = await mockedVolumeOperator([], volumes);
      oper.watcher.injectObject(obj);
      // we have to sleep to give event stream chance to register its handlers
      await sleep(10);

      deleteStub.rejects(new Error('delete failed'));
      let volume = new Volume(UUID, registry, defaultSpec);
      volumes.emit('volume', {
        eventType: 'del',
        object: volume,
      });

      await sleep(10);
      sinon.assert.calledOnce(deleteStub);
    });

    it('should not crash if the resource does not exist upon "del" volume event', async () => {
      let obj = createVolumeResource(UUID, defaultSpec);
      let registry = new Registry();
      let volumes = new Volumes(registry);
      sinon.stub(volumes, 'get').returns([]);

      oper = await mockedVolumeOperator([], volumes);
      // we have to sleep to give event stream chance to register its handlers
      await sleep(10);

      let volume = new Volume(UUID, registry, defaultSpec);
      volumes.emit('volume', {
        eventType: 'del',
        object: volume,
      });

      await sleep(10);
      sinon.assert.notCalled(deleteStub);
      sinon.assert.notCalled(putStub);
      sinon.assert.notCalled(postStub);
    });
  });
};
