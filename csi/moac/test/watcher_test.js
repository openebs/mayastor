// Tests for the object cache (watcher).

'use strict';

/* eslint-disable no-unused-expressions */

const _ = require('lodash');
const expect = require('chai').expect;
const sinon = require('sinon');
const sleep = require('sleep-promise');
const { KubeConfig } = require('@kubernetes/client-node');
const { CustomResourceCache } = require('../dist/watcher');

// slightly modified cache tunings not to wait too long when testing things
const IDLE_TIMEOUT_MS = 500;
const RESTART_DELAY_MS = 300;
const EVENT_TIMEOUT_MS = 200;
const EVENT_DELAY_MS = 100;
const EYE_BLINK_MS = 30;
// Believe it or not but it is possible that timeout callback triggers a bit
// earlier than it should (although that nodejs documentation says that it is
// not possible). Accomodate this weird behaviour.
const TOLERATE_MS = 2;

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

// Create fake k8s object. Example of true k8s object follows:
//
//  "object": {
//    "apiVersion": "csi.storage.k8s.io/v1alpha1",
//    "kind": "CSINodeInfo",
//    "metadata": {
//      "creationTimestamp": "2019-02-15T18:23:53Z",
//      "generation": 13,
//      "name": "node1",
//      "ownerReferences": [
//        {
//          "apiVersion": "v1",
//          "kind": "Node",
//          "name": "node1",
//          "uid": "c696b8e5-fd8c-11e8-a41c-589cfc0d76a7"
//        }
//      ],
//      "resourceVersion": "627981",
//      "selfLink": "/apis/csi.storage.k8s.io/v1alpha1/csinodeinfos/node1",
//      "uid": "d99f06a9-314e-11e9-b086-589cfc0d76a7"
//    },
//    "spec": {
//        ...
//    },
//    "status": {
//        ...
//    }
//  }
function createApple (name, finalizers, spec) {
  return {
    apiVersion: 'my.group.io/v1alpha1',
    kind: 'apple',
    metadata: { name, finalizers },
    spec
  };
}

// Test class
class Apple {
  constructor (obj) {
    this.metadata = {
      name: obj.metadata.name
    };
    if (obj.spec === 'invalid') {
      throw new Error('Invalid object');
    }
    this.spec = obj.spec;
  }
}

// Create a cache with a listWatch object with fake start method that does
// nothing instead of connecting to k8s cluster.
function createMockedCache () {
  const kc = new KubeConfig();
  Object.assign(kc, fakeConfig);
  const watcher = new CustomResourceCache('namespace', 'apple', kc, Apple, {
    restartDelay: RESTART_DELAY_MS,
    eventTimeout: EVENT_TIMEOUT_MS,
    idleTimeout: IDLE_TIMEOUT_MS
  });
  // convenience function for generating k8s watcher events
  watcher.emitKubeEvent = (ev, data) => {
    watcher.listWatch.callbackCache[ev].forEach((cb) => cb(data));
  };
  const startStub = sinon.stub(watcher.listWatch, 'start');
  startStub.onCall(0).resolves();
  return [watcher, startStub];
}

module.exports = function () {
  this.timeout(10000);

  it('should create a cache and block in start until connected', async () => {
    const kc = new KubeConfig();
    Object.assign(kc, fakeConfig);
    const watcher = new CustomResourceCache('namespace', 'apple', kc, Apple, {
      restartDelay: RESTART_DELAY_MS,
      eventTimeout: EVENT_TIMEOUT_MS
    });
    const startStub = sinon.stub(watcher.listWatch, 'start');
    startStub.onCall(0).rejects();
    startStub.onCall(1).rejects();
    startStub.onCall(2).resolves();
    const startTime = new Date();
    await watcher.start();
    const delta = new Date() - startTime;
    sinon.assert.calledThrice(startStub);
    expect(watcher.isConnected()).to.be.true;
    expect(delta).to.be.within(2 * RESTART_DELAY_MS, 3 * RESTART_DELAY_MS);
    watcher.stop();
  });

  it('should reconnect watcher if it gets disconnected', async () => {
    const [watcher, startStub] = createMockedCache();
    await watcher.start();
    sinon.assert.calledOnce(startStub);
    expect(watcher.isConnected()).to.be.true;
    startStub.onCall(1).rejects(new Error('start failed'));
    startStub.onCall(2).resolves();
    watcher.emitKubeEvent('error', new Error('got disconnected'));
    await sleep(RESTART_DELAY_MS * 1.5);
    sinon.assert.calledTwice(startStub);
    expect(watcher.isConnected()).to.be.false;
    await sleep(RESTART_DELAY_MS);
    sinon.assert.calledThrice(startStub);
    expect(watcher.isConnected()).to.be.true;
    watcher.stop();
  });

  it('should reset watcher if idle for too long', async () => {
    const [watcher, startStub] = createMockedCache();
    await watcher.start();
    sinon.assert.calledOnce(startStub);
    expect(watcher.isConnected()).to.be.true;
    startStub.onCall(1).resolves();
    await sleep(IDLE_TIMEOUT_MS * 1.5);
    sinon.assert.calledTwice(startStub);
    expect(watcher.isConnected()).to.be.true;
    watcher.stop();
  });

  describe('methods', function () {
    let watcher;
    let timeout;

    beforeEach(async () => {
      let startStub;
      timeout = undefined;
      [watcher, startStub] = createMockedCache();
      startStub.resolves();
      await watcher.start();
    });

    afterEach(() => {
      if (watcher) {
        watcher.stop();
        watcher = undefined;
      }
      if (timeout) {
        clearTimeout(timeout);
      }
    });

    function assertReplaceCalledWith (stub, name, obj, attrs) {
      const newObj = _.cloneDeep(obj);
      _.merge(newObj, attrs);
      sinon.assert.calledOnce(stub);
      sinon.assert.calledWith(stub, 'openebs.io', 'v1alpha1', 'namespace',
        'apples', name, newObj);
    }

    it('should list all objects', () => {
      const listStub = sinon.stub(watcher.listWatch, 'list');
      listStub.returns([
        createApple('name1', [], 'valid'),
        createApple('name2', [], 'invalid'),
        createApple('name3', [], 'valid')
      ]);
      const objs = watcher.list();
      expect(objs).to.have.length(2);
      expect(objs[0].metadata.name).to.equal('name1');
      expect(objs[1].metadata.name).to.equal('name3');
    });

    it('should get object by name', () => {
      const getStub = sinon.stub(watcher.listWatch, 'get');
      getStub.returns(createApple('name1', [], 'valid'));
      const obj = watcher.get('name1');
      expect(obj).to.be.an.instanceof(Apple);
      expect(obj.metadata.name).to.equal('name1');
      sinon.assert.calledWith(getStub, 'name1');
    });

    it('should get undefined if object does not exist', () => {
      const getStub = sinon.stub(watcher.listWatch, 'get');
      getStub.returns(undefined);
      const obj = watcher.get('name1');
      expect(obj).to.be.undefined;
      sinon.assert.calledWith(getStub, 'name1');
    });

    it('should create an object and wait for new event', async () => {
      const createStub = sinon.stub(watcher.k8sApi, 'createNamespacedCustomObject');
      createStub.resolves();
      const apple = createApple('name1', [], 'valid');
      const startTime = new Date();
      timeout = setTimeout(() => watcher.emitKubeEvent('add', apple), EVENT_DELAY_MS);
      await watcher.create(apple);
      const delta = new Date() - startTime;
      expect(delta).to.be.within(EVENT_DELAY_MS - TOLERATE_MS, EVENT_DELAY_MS + EYE_BLINK_MS);
      sinon.assert.calledOnce(createStub);
    });

    it('should timeout when "add" event does not come after a create', async () => {
      const createStub = sinon.stub(watcher.k8sApi, 'createNamespacedCustomObject');
      createStub.resolves();
      const apple = createApple('name1', [], 'valid');
      const startTime = new Date();
      await watcher.create(apple);
      const delta = new Date() - startTime;
      expect(delta).to.be.within(EVENT_TIMEOUT_MS - TOLERATE_MS, EVENT_TIMEOUT_MS + EYE_BLINK_MS);
      sinon.assert.calledOnce(createStub);
    });

    it('should update object and wait for mod event', async () => {
      const replaceStub = sinon.stub(watcher.k8sApi, 'replaceNamespacedCustomObject');
      replaceStub.resolves();
      const getStub = sinon.stub(watcher.listWatch, 'get');
      const apple = createApple('name1', [], 'valid');
      const newApple = createApple('name1', [], 'also valid');
      getStub.returns(apple);
      const startTime = new Date();
      timeout = setTimeout(() => watcher.emitKubeEvent('update', newApple), EVENT_DELAY_MS);
      await watcher.update('name1', (orig) => {
        return createApple(orig.metadata.name, [], 'also valid');
      });
      const delta = new Date() - startTime;
      expect(delta).to.be.within(EVENT_DELAY_MS - TOLERATE_MS, EVENT_DELAY_MS + EYE_BLINK_MS);
      assertReplaceCalledWith(replaceStub, 'name1', apple, {
        spec: 'also valid'
      });
    });

    it('should not try to update object if it does not exist', async () => {
      const replaceStub = sinon.stub(watcher.k8sApi, 'replaceNamespacedCustomObject');
      replaceStub.resolves();
      const getStub = sinon.stub(watcher.listWatch, 'get');
      getStub.returns();
      await watcher.update('name1', (orig) => {
        return createApple(orig.metadata.name, [], 'also valid');
      });
      sinon.assert.notCalled(replaceStub);
    });

    it('should timeout when "update" event does not come after an update', async () => {
      const replaceStub = sinon.stub(watcher.k8sApi, 'replaceNamespacedCustomObject');
      replaceStub.resolves();
      const getStub = sinon.stub(watcher.listWatch, 'get');
      const apple = createApple('name1', [], 'valid');
      getStub.returns(apple);
      const startTime = new Date();
      await watcher.update('name1', (orig) => {
        return createApple(orig.metadata.name, [], 'also valid');
      });
      const delta = new Date() - startTime;
      expect(delta).to.be.within(EVENT_TIMEOUT_MS - TOLERATE_MS, EVENT_TIMEOUT_MS + EYE_BLINK_MS);
      sinon.assert.calledOnce(replaceStub);
    });

    it('should retry update of an object if it fails', async () => {
      const replaceStub = sinon.stub(watcher.k8sApi, 'replaceNamespacedCustomObject');
      replaceStub.onCall(0).rejects(new Error('update failed'));
      replaceStub.onCall(1).resolves();
      const getStub = sinon.stub(watcher.listWatch, 'get');
      const apple = createApple('name1', [], 'valid');
      getStub.returns(apple);
      await watcher.update('name1', (orig) => {
        return createApple(orig.metadata.name, [], 'also valid');
      });
      sinon.assert.calledTwice(replaceStub);
    });

    it('should update status of object', async () => {
      const replaceStub = sinon.stub(watcher.k8sApi, 'replaceNamespacedCustomObjectStatus');
      replaceStub.resolves();
      const getStub = sinon.stub(watcher.listWatch, 'get');
      const apple = createApple('name1', [], 'valid');
      getStub.returns(apple);
      await watcher.updateStatus('name1', (orig) => {
        return _.assign({}, apple, {
          status: 'some-state'
        });
      });
      assertReplaceCalledWith(replaceStub, 'name1', apple, {
        status: 'some-state'
      });
    });

    it('should not try to update status of object if it does not exist', async () => {
      const replaceStub = sinon.stub(watcher.k8sApi, 'replaceNamespacedCustomObjectStatus');
      replaceStub.resolves();
      const getStub = sinon.stub(watcher.listWatch, 'get');
      const apple = createApple('name1', [], 'valid');
      getStub.returns();
      await watcher.updateStatus('name1', (orig) => {
        return _.assign({}, apple, {
          status: 'some-state'
        });
      });
      sinon.assert.notCalled(replaceStub);
    });

    it('should timeout when "update" event does not come after status update', async () => {
      const replaceStub = sinon.stub(watcher.k8sApi, 'replaceNamespacedCustomObjectStatus');
      replaceStub.resolves();
      const getStub = sinon.stub(watcher.listWatch, 'get');
      const apple = createApple('name1', [], 'valid');
      getStub.returns(apple);
      const startTime = new Date();
      await watcher.updateStatus('name1', (orig) => {
        return _.assign({}, apple, {
          status: 'some-state'
        });
      });
      const delta = new Date() - startTime;
      expect(delta).to.be.within(EVENT_TIMEOUT_MS - TOLERATE_MS, EVENT_TIMEOUT_MS + EYE_BLINK_MS);
      sinon.assert.calledOnce(replaceStub);
    });

    it('should retry status update of an object if it fails', async () => {
      const replaceStub = sinon.stub(watcher.k8sApi, 'replaceNamespacedCustomObjectStatus');
      replaceStub.onCall(0).rejects(new Error('update failed'));
      replaceStub.onCall(1).resolves();
      const getStub = sinon.stub(watcher.listWatch, 'get');
      const apple = createApple('name1', [], 'valid');
      getStub.returns(apple);
      await watcher.updateStatus('name1', (orig) => {
        return _.assign({}, apple, {
          status: 'some-state'
        });
      });
      sinon.assert.calledTwice(replaceStub);
    });

    it('should fail if status update fails twice', async () => {
      const replaceStub = sinon.stub(watcher.k8sApi, 'replaceNamespacedCustomObjectStatus');
      replaceStub.onCall(0).rejects(new Error('update failed first time'));
      replaceStub.onCall(1).rejects(new Error('update failed second time'));
      const getStub = sinon.stub(watcher.listWatch, 'get');
      const apple = createApple('name1', [], 'valid');
      getStub.returns(apple);
      let error;
      try {
        await watcher.updateStatus('name1', (orig) => {
          return _.assign({}, apple, {
            status: 'some-state'
          });
        });
      } catch (err) {
        error = err;
      }
      expect(error.message).to.equal('Status update of apple "name1" failed: update failed second time');
      sinon.assert.calledTwice(replaceStub);
    });

    it('should delete the object and wait for "delete" event', async () => {
      const deleteStub = sinon.stub(watcher.k8sApi, 'deleteNamespacedCustomObject');
      deleteStub.resolves();
      const getStub = sinon.stub(watcher.listWatch, 'get');
      const apple = createApple('name1', [], 'valid');
      getStub.returns(apple);
      const startTime = new Date();
      timeout = setTimeout(() => watcher.emitKubeEvent('delete', apple), EVENT_DELAY_MS);
      await watcher.delete('name1');
      const delta = new Date() - startTime;
      sinon.assert.calledOnce(deleteStub);
      sinon.assert.calledWith(deleteStub, 'openebs.io', 'v1alpha1', 'namespace',
        'apples', 'name1');
      expect(delta).to.be.within(EVENT_DELAY_MS - TOLERATE_MS, EVENT_DELAY_MS + EYE_BLINK_MS);
    });

    it('should timeout when "delete" event does not come after a delete', async () => {
      const deleteStub = sinon.stub(watcher.k8sApi, 'deleteNamespacedCustomObject');
      deleteStub.resolves();
      const getStub = sinon.stub(watcher.listWatch, 'get');
      const apple = createApple('name1', [], 'valid');
      getStub.returns(apple);
      const startTime = new Date();
      await watcher.delete('name1');
      const delta = new Date() - startTime;
      sinon.assert.calledOnce(deleteStub);
      expect(delta).to.be.within(EVENT_TIMEOUT_MS - TOLERATE_MS, EVENT_TIMEOUT_MS + EYE_BLINK_MS);
    });

    it('should not try to delete object that does not exist', async () => {
      const deleteStub = sinon.stub(watcher.k8sApi, 'deleteNamespacedCustomObject');
      deleteStub.resolves();
      const getStub = sinon.stub(watcher.listWatch, 'get');
      const apple = createApple('name1', [], 'valid');
      getStub.returns();
      timeout = setTimeout(() => watcher.emitKubeEvent('delete', apple), EVENT_DELAY_MS);
      await watcher.delete('name1');
      sinon.assert.notCalled(deleteStub);
    });

    it('should add finalizer to object without any', async () => {
      const replaceStub = sinon.stub(watcher.k8sApi, 'replaceNamespacedCustomObject');
      replaceStub.resolves();
      const getStub = sinon.stub(watcher.listWatch, 'get');
      const apple = createApple('name1', [], 'valid');
      getStub.returns(apple);
      const startTime = new Date();
      timeout = setTimeout(() => watcher.emitKubeEvent('update', apple), EVENT_DELAY_MS);
      await watcher.addFinalizer('name1', 'test.finalizer.com');
      const delta = new Date() - startTime;
      expect(delta).to.be.within(EVENT_DELAY_MS - TOLERATE_MS, EVENT_DELAY_MS + EYE_BLINK_MS);
      assertReplaceCalledWith(replaceStub, 'name1', apple, {
        metadata: {
          finalizers: ['test.finalizer.com']
        }
      });
    });

    it('should add another finalizer to object', async () => {
      const replaceStub = sinon.stub(watcher.k8sApi, 'replaceNamespacedCustomObject');
      replaceStub.resolves();
      const getStub = sinon.stub(watcher.listWatch, 'get');
      const apple = createApple('name1', ['test.finalizer.com', 'test2.finalizer.com'], 'valid');
      getStub.returns(apple);
      const startTime = new Date();
      timeout = setTimeout(() => watcher.emitKubeEvent('update', apple), EVENT_DELAY_MS);
      await watcher.addFinalizer('name1', 'new.finalizer.com');
      const delta = new Date() - startTime;
      expect(delta).to.be.within(EVENT_DELAY_MS - TOLERATE_MS, EVENT_DELAY_MS + EYE_BLINK_MS);
      assertReplaceCalledWith(replaceStub, 'name1', apple, {
        metadata: {
          finalizers: ['new.finalizer.com', 'test.finalizer.com', 'test2.finalizer.com']
        }
      });
    });

    it('should not add twice the same finalizer', async () => {
      const replaceStub = sinon.stub(watcher.k8sApi, 'replaceNamespacedCustomObject');
      replaceStub.resolves();
      const getStub = sinon.stub(watcher.listWatch, 'get');
      const apple = createApple('name1', ['test.finalizer.com', 'test2.finalizer.com'], 'valid');
      getStub.returns(apple);
      timeout = setTimeout(() => watcher.emitKubeEvent('update', apple), EVENT_DELAY_MS);
      await watcher.addFinalizer('name1', 'test.finalizer.com');
      sinon.assert.notCalled(replaceStub);
    });

    it('should not add the finalizer if object does not exist', async () => {
      const replaceStub = sinon.stub(watcher.k8sApi, 'replaceNamespacedCustomObject');
      replaceStub.resolves();
      const getStub = sinon.stub(watcher.listWatch, 'get');
      const apple = createApple('name1', [], 'valid');
      getStub.returns();
      timeout = setTimeout(() => watcher.emitKubeEvent('update', apple), EVENT_DELAY_MS);
      await watcher.addFinalizer('name1', 'test.finalizer.com');
      sinon.assert.notCalled(replaceStub);
    });

    it('should remove finalizer from object', async () => {
      const replaceStub = sinon.stub(watcher.k8sApi, 'replaceNamespacedCustomObject');
      replaceStub.resolves();
      const getStub = sinon.stub(watcher.listWatch, 'get');
      const apple = createApple('name1', ['test.finalizer.com', 'test2.finalizer.com'], 'valid');
      getStub.returns(apple);
      const startTime = new Date();
      timeout = setTimeout(() => watcher.emitKubeEvent('update', apple), EVENT_DELAY_MS);
      await watcher.removeFinalizer('name1', 'test.finalizer.com');
      const delta = new Date() - startTime;
      expect(delta).to.be.within(EVENT_DELAY_MS - TOLERATE_MS, EVENT_DELAY_MS + EYE_BLINK_MS);
      sinon.assert.calledOnce(replaceStub);
      assertReplaceCalledWith(replaceStub, 'name1', apple, {
        metadata: {
          finalizers: ['test2.finalizer.com']
        }
      });
    });

    it('should not try to remove finalizer that does not exist', async () => {
      const replaceStub = sinon.stub(watcher.k8sApi, 'replaceNamespacedCustomObject');
      replaceStub.resolves();
      const getStub = sinon.stub(watcher.listWatch, 'get');
      const apple = createApple('name1', ['test2.finalizer.com'], 'valid');
      getStub.returns(apple);
      timeout = setTimeout(() => watcher.emitKubeEvent('update', apple), EVENT_DELAY_MS);
      await watcher.removeFinalizer('name1', 'test.finalizer.com');
      sinon.assert.notCalled(replaceStub);
    });

    it('should not try to remove finalizer if object does not exist', async () => {
      const replaceStub = sinon.stub(watcher.k8sApi, 'replaceNamespacedCustomObject');
      replaceStub.resolves();
      const getStub = sinon.stub(watcher.listWatch, 'get');
      const apple = createApple('name1', ['test.finalizer.com'], 'valid');
      getStub.returns();
      timeout = setTimeout(() => watcher.emitKubeEvent('update', apple), EVENT_DELAY_MS);
      await watcher.removeFinalizer('name1', 'test.finalizer.com');
      sinon.assert.notCalled(replaceStub);
    });
  });
};
