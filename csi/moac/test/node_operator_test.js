// Unit tests for the node operator
//
// We don't test init method which very much depends on k8s api client which
// would be more difficult to fake. Instead we provide a pseudo watcher
// object fully under our control to test response to various watcher events.
// Thus node operator init phase *must* be tested manually and in real k8s
// environment.
//
// NOTE: CSINode info objects don't have generation number, so when creating
// fake CSINode objects we mimic the real behaviour.

const expect = require('chai').expect;
const EventEmitter = require('events');
const sinon = require('sinon');
const NodeOperator = require('../node_operator');
const Watcher = require('./watcher_stub');

module.exports = function() {
  var filterFunc = NodeOperator.prototype.filterMayastorNode;

  describe('node filtering', () => {
    it('valid mayastor node should pass the filter', () => {
      let res = filterFunc({
        apiVersion: 'storage.k8s.io/v1beta1',
        kind: 'CSINode',
        metadata: {
          name: 'node-name',
          creationTimestamp: '2019-02-15T18:23:53Z',
          resourceVersion: '627981',
          selfLink: '/apis/csi.storage.k8s.io/v1beta1/csinodes/node-name',
          uid: 'd99f06a9-314e-11e9-b086-589cfc0d76a7',
          ownerReferences: [
            {
              apiVersion: 'v1',
              kind: 'Node',
              name: 'node-name',
              uid: 'c696b8e5-fd8c-11e8-a41c-589cfc0d76a7',
            },
          ],
        },
        spec: {
          drivers: [
            {
              name: 'csi-hostpath',
              nodeID: 'mynodeid',
              topologyKeys: [],
            },
            {
              name: 'io.openebs.csi-mayastor',
              nodeID: 'mayastor://node-name/127.0.0.1:123',
              topologyKeys: [],
            },
          ],
        },
      });
      expect(res).to.have.all.keys('name', 'id', 'endpoint');
      expect(res.name).to.equal('node-name');
      expect(res.id).to.equal('mayastor://node-name/127.0.0.1:123');
      expect(res.endpoint).to.equal('127.0.0.1:123');
    });

    it('node without mayastor csi driver should not pass the filter', () => {
      let res = filterFunc({
        apiVersion: 'csi.storage.k8s.io/v1beta1',
        kind: 'CSINode',
        metadata: {
          creationTimestamp: '2019-02-15T18:23:53Z',
          name: 'node-name',
          ownerReferences: [
            {
              apiVersion: 'v1',
              kind: 'Node',
              name: 'node-name',
              uid: 'c696b8e5-fd8c-11e8-a41c-589cfc0d76a7',
            },
          ],
          resourceVersion: '627981',
          selfLink: '/apis/csi.storage.k8s.io/v1beta1/csinodes/node-name',
          uid: 'd99f06a9-314e-11e9-b086-589cfc0d76a7',
        },
        spec: {
          drivers: [
            {
              name: 'csi-hostpath',
              nodeID: 'mynodeid',
              topologyKeys: [],
            },
          ],
        },
      });
      // jshint ignore:start
      expect(res.id).to.be.null;
      // jshint ignore:end
    });

    it('node without csi drivers section should not pass the filter', () => {
      let res = filterFunc({
        apiVersion: 'csi.storage.k8s.io/v1beta1',
        kind: 'CSINode',
        metadata: {
          creationTimestamp: '2019-02-15T18:23:53Z',
          name: 'node-name',
          ownerReferences: [
            {
              apiVersion: 'v1',
              kind: 'Node',
              name: 'node-name',
              uid: 'c696b8e5-fd8c-11e8-a41c-589cfc0d76a7',
            },
          ],
          resourceVersion: '627981',
          selfLink: '/apis/csi.storage.k8s.io/v1beta1/csinodes/node-name',
          uid: 'd99f06a9-314e-11e9-b086-589cfc0d76a7',
        },
        spec: {
          drivers: null,
        },
      });
      // jshint ignore:start
      expect(res.id).to.be.null;
      // jshint ignore:end
    });

    it('mayastor node with unknown ID scheme should not pass the filter', () => {
      let res = filterFunc({
        apiVersion: 'csi.storage.k8s.io/v1beta1',
        kind: 'CSINode',
        metadata: {
          creationTimestamp: '2019-02-15T18:23:53Z',
          name: 'node-name',
          ownerReferences: [
            {
              apiVersion: 'v1',
              kind: 'Node',
              name: 'node-name',
              uid: 'c696b8e5-fd8c-11e8-a41c-589cfc0d76a7',
            },
          ],
          resourceVersion: '627981',
          selfLink: '/apis/csi.storage.k8s.io/v1beta1/csinodes/node-name',
          uid: 'd99f06a9-314e-11e9-b086-589cfc0d76a7',
        },
        spec: {
          drivers: [
            {
              name: 'io.openebs.csi-mayastor',
              nodeID: 'mayastorv2://node-name/127.0.0.1:123',
              topologyKeys: [],
            },
          ],
        },
      });
      // jshint ignore:start
      expect(res).to.be.null;
      // jshint ignore:end
    });

    it('mayastor node with inconsistent ID should not pass the filter', () => {
      let res = filterFunc({
        apiVersion: 'csi.storage.k8s.io/v1beta1',
        kind: 'CSINode',
        metadata: {
          creationTimestamp: '2019-02-15T18:23:53Z',
          name: 'node-name',
          ownerReferences: [
            {
              apiVersion: 'v1',
              kind: 'Node',
              name: 'node-name',
              uid: 'c696b8e5-fd8c-11e8-a41c-589cfc0d76a7',
            },
          ],
          resourceVersion: '627981',
          selfLink: '/apis/csi.storage.k8s.io/v1beta1/csinodes/node-name',
          uid: 'd99f06a9-314e-11e9-b086-589cfc0d76a7',
        },
        spec: {
          drivers: [
            {
              name: 'io.openebs.csi-mayastor',
              nodeID: 'mayastor://other-name/127.0.0.1:123',
              topologyKeys: [],
            },
          ],
        },
      });
      // jshint ignore:start
      expect(res).to.be.null;
      // jshint ignore:end
    });
  });

  describe('adding, removing and modifying nodes', () => {
    var addNodeSpy, removeNodeSpy, getNodeStub;

    // Create fake registry tracing calls to addNode and removeNode methods
    // and customizable return value from getNode method.
    function createFakeRegistry(getNodeReturn) {
      let registry = {
        addNode: function() {},
        removeNode: function() {},
        getNode: function() {},
      };
      addNodeSpy = sinon.spy(registry, 'addNode');
      removeNodeSpy = sinon.spy(registry, 'removeNode');
      getNodeStub = sinon.stub(registry, 'getNode');
      getNodeStub.returns(getNodeReturn);
      return registry;
    }

    // Create csi node object with mayastor plugin in drivers.
    // If endpoint is null, then the drivers array is left empty.
    function csiNodeObject(name, endpoint) {
      let node = {
        apiVersion: 'storage.k8s.io/v1beta1',
        kind: 'CSINode',
        metadata: {
          name: name,
          creationTimestamp: '2019-02-15T18:23:53Z',
          resourceVersion: '627981',
          selfLink: '/apis/csi.storage.k8s.io/v1beta1/csinodes/node-name',
          uid: 'd99f06a9-314e-11e9-b086-589cfc0d76a7',
          ownerReferences: [
            {
              apiVersion: 'v1',
              kind: 'Node',
              name: name,
              uid: 'c696b8e5-fd8c-11e8-a41c-589cfc0d76a7',
            },
          ],
        },
        spec: {
          drivers: [],
        },
      };
      if (endpoint) {
        node.spec.drivers.push({
          name: 'io.openebs.csi-mayastor',
          nodeID: 'mayastor://' + name + '/' + endpoint,
          topologyKeys: [],
        });
      }
      return node;
    }

    // Create a node operator object bound to the fake watcher
    function NodeOperatorWithFakeWatcher(watcher, registry) {
      let oper = new NodeOperator();
      oper._bindWatcher(watcher);
      oper.watcher = watcher;
      oper.registry = registry;
      return oper;
    }

    it('should add new node to registry upon new event', () => {
      let registry = createFakeRegistry(null);
      let watcher = new Watcher(filterFunc, []);
      let oper = new NodeOperatorWithFakeWatcher(watcher, registry);

      watcher.newObject(csiNodeObject('node-name', '127.0.0.1:123'));

      sinon.assert.notCalled(removeNodeSpy);
      sinon.assert.calledOnce(addNodeSpy);
      sinon.assert.calledWith(addNodeSpy, 'node-name', '127.0.0.1:123');
    });

    it('should add unknown node to registry upon mod event', () => {
      let registry = createFakeRegistry(null);
      let watcher = new Watcher(filterFunc, []);
      let oper = new NodeOperatorWithFakeWatcher(watcher, registry);

      watcher.modObject(csiNodeObject('node-name', '127.0.0.1:123'));

      sinon.assert.notCalled(removeNodeSpy);
      sinon.assert.calledOnce(addNodeSpy);
      sinon.assert.calledWith(addNodeSpy, 'node-name', '127.0.0.1:123');
    });

    it('should reconnect node upon mod event', () => {
      let node = {
        name: 'node-name',
        endpoint: '127.0.0.1:123',
        connect: function() {},
      };
      let connectSpy = sinon.spy(node, 'connect');
      let registry = createFakeRegistry(node);
      let watcher = new Watcher(filterFunc, [
        csiNodeObject('node-name', '127.0.0.1:123'),
      ]);
      let oper = new NodeOperatorWithFakeWatcher(watcher, registry);

      watcher.modObject(csiNodeObject('node-name', '127.0.0.1:124'));

      sinon.assert.notCalled(removeNodeSpy);
      sinon.assert.notCalled(addNodeSpy);
      sinon.assert.calledOnce(connectSpy);
      sinon.assert.calledWith(connectSpy, '127.0.0.1:124');
    });

    it('should remove node from registry upon mod event without mayastor entry', () => {
      let node = {
        name: 'node-name',
        endpoint: '127.0.0.1:123',
      };
      let registry = createFakeRegistry(node);
      let watcher = new Watcher(filterFunc, [
        csiNodeObject('node-name', '127.0.0.1:123'),
      ]);
      let oper = new NodeOperatorWithFakeWatcher(watcher, registry);

      watcher.modObject(csiNodeObject('node-name', null));

      sinon.assert.notCalled(addNodeSpy);
      sinon.assert.calledOnce(removeNodeSpy);
      sinon.assert.calledWith(removeNodeSpy, 'node-name');
    });

    it('should remove node from registry upon del event', () => {
      let node = {
        name: 'node-name',
        endpoint: '127.0.0.1:123',
      };
      let registry = createFakeRegistry(node);
      let watcher = new Watcher(filterFunc, [
        csiNodeObject('node-name', '127.0.0.1:123'),
      ]);
      let oper = new NodeOperatorWithFakeWatcher(watcher, registry);

      watcher.delObject('node-name');

      sinon.assert.notCalled(addNodeSpy);
      sinon.assert.calledOnce(removeNodeSpy);
      sinon.assert.calledWith(removeNodeSpy, 'node-name');
    });

    it('should ignore del event if node does not exist', () => {
      let registry = createFakeRegistry(null);
      let watcher = new Watcher(filterFunc, [
        csiNodeObject('node-name', '127.0.0.1:123'),
      ]);
      let oper = new NodeOperatorWithFakeWatcher(watcher, registry);

      watcher.delObject('node-name');

      sinon.assert.notCalled(addNodeSpy);
      sinon.assert.notCalled(removeNodeSpy);
    });
  });
};
