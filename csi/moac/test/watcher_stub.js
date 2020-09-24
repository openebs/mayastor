// Fake watcher that isolates the watcher from k8s api server using sinon stubs.

'use strict';

const sinon = require('sinon');

// stubsCb callback can override default return values of k8s api calls
function mockCache (cache, stubsCb) {
  // do not wait for confirming events from k8s
  cache.eventTimeout = 0;

  // mock k8s api calls
  cache.createStub = sinon.stub(cache.k8sApi, 'createNamespacedCustomObject');
  cache.updateStub = sinon.stub(cache.k8sApi, 'replaceNamespacedCustomObject');
  cache.updateStatusStub = sinon.stub(cache.k8sApi, 'replaceNamespacedCustomObjectStatus');
  cache.deleteStub = sinon.stub(cache.k8sApi, 'deleteNamespacedCustomObject');
  cache.getStub = sinon.stub(cache.listWatch, 'get');
  cache.listStub = sinon.stub(cache.listWatch, 'list');
  const stubs = {
    create: cache.createStub,
    update: cache.updateStub,
    updateStatus: cache.updateStatusStub,
    delete: cache.deleteStub,
    get: cache.getStub,
    list: cache.listStub
  };
  stubs.create.resolves();
  stubs.update.resolves();
  stubs.updateStatus.resolves();
  stubs.delete.resolves();
  stubs.get.returns();
  stubs.list.returns([]);
  if (stubsCb) stubsCb(stubs);

  // convenience function for emitting watcher events
  stubs.emitKubeEvent = (ev, data) => {
    cache.listWatch.callbackCache[ev].forEach((cb) => cb(data));
  };

  // mock the watcher to start even without k8s
  const startStub = sinon.stub(cache.listWatch, 'start');
  startStub.callsFake(async () => {
    stubs.list().forEach((ent) => {
      stubs.emitKubeEvent('add', ent);
    });
  });
}

module.exports = { mockCache };
