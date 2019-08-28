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

const assert = require('chai').assert;
const EventEmitter = require('events');
const { NodeOperator } = require('./nodes');

// Just a stub which allows us to emit arbitrary events
class FakeWatcher extends EventEmitter {
  constructor() {
    super();
  }
}

// Create a node operator object bound to the fake watcher
function NodeOperatorWithFakeWatcher() {
  let nodes = new NodeOperator();
  let watcher = new FakeWatcher();
  nodes._bindWatcher(watcher);
  nodes.watcher = watcher;
  return nodes;
}

// Create customisable payload of a node watcher event (new/mod/del event)
function createEvent(name, endpoint) {
  let obj = { name };
  if (endpoint) {
    obj.id = 'mayastor://' + name + '/' + endpoint;
    obj.endpoint = endpoint;
  }
  return obj;
}

module.exports = function() {
  it('valid mayastor node should pass the filter', () => {
    let res = NodeOperator.prototype.filterMayastorNode({
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
    assert.hasAllKeys(res, ['name', 'id', 'endpoint']);
    assert.equal(res.name, 'node-name');
    assert.equal(res.id, 'mayastor://node-name/127.0.0.1:123');
    assert.equal(res.endpoint, '127.0.0.1:123');
  });

  it('node without mayastor csi driver should not pass the filter', () => {
    let res = NodeOperator.prototype.filterMayastorNode({
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
    assert.isNull(res.id);
  });

  it('node without csi drivers section should not pass the filter', () => {
    let res = NodeOperator.prototype.filterMayastorNode({
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
    assert.isNull(res.id);
  });

  it('mayastor node with unknown ID scheme should not pass the filter', () => {
    let res = NodeOperator.prototype.filterMayastorNode({
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
    assert.isNull(res);
  });

  it('mayastor node with inconsistent ID should not pass the filter', () => {
    let res = NodeOperator.prototype.filterMayastorNode({
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
    assert.isNull(res);
  });

  it('should emit "add" event for new CSINode with mayastor', done => {
    let nodes = new NodeOperatorWithFakeWatcher();
    let addList = [];

    nodes.on('add', node => addList.push(node));
    nodes.watcher.emit('new', createEvent('node-name', '127.0.0.1:123'));

    setTimeout(() => {
      assert.lengthOf(addList, 1);
      assert.equal(addList[0].node, 'node-name');
      assert.equal(addList[0].endpoint, '127.0.0.1:123');

      let nds = nodes.get();
      assert.lengthOf(nds, 1);
      assert.equal(nds[0].node, 'node-name');
      assert.equal(nds[0].endpoint, '127.0.0.1:123');
      let nd = nodes.get('node-name');
      assert.equal(nd.node, 'node-name');
      assert.equal(nd.endpoint, '127.0.0.1:123');
      done();
    }, 0);
  });

  it('should emit "add" event for new mayastor in CSINode', done => {
    let nodes = new NodeOperatorWithFakeWatcher();
    let addList = [];

    nodes.on('add', node => addList.push(node));
    nodes.watcher.emit('new', createEvent('node-name', null));
    nodes.watcher.emit('mod', createEvent('node-name', '127.0.0.1:123'));

    setTimeout(() => {
      assert.lengthOf(addList, 1);
      assert.equal(addList[0].node, 'node-name');
      assert.equal(addList[0].endpoint, '127.0.0.1:123');

      let nds = nodes.get();
      assert.lengthOf(nds, 1);
      assert.equal(nds[0].node, 'node-name');
      assert.equal(nds[0].endpoint, '127.0.0.1:123');
      let nd = nodes.get('node-name');
      assert.equal(nd.node, 'node-name');
      assert.equal(nd.endpoint, '127.0.0.1:123');
      done();
    }, 0);
  });

  it('should emit "add" event when mayastor endpoint has changed', done => {
    let nodes = new NodeOperatorWithFakeWatcher();
    let addList = [];

    nodes.on('add', node => addList.push(node));
    nodes.watcher.emit('new', createEvent('node-name', '127.0.0.1:123'));
    nodes.watcher.emit('mod', createEvent('node-name', '127.0.0.1:124'));

    setTimeout(() => {
      assert.lengthOf(addList, 2);
      assert.equal(addList[0].node, 'node-name');
      assert.equal(addList[0].endpoint, '127.0.0.1:123');
      assert.equal(addList[1].node, 'node-name');
      assert.equal(addList[1].endpoint, '127.0.0.1:124');

      let nds = nodes.get();
      assert.lengthOf(nds, 1);
      assert.equal(nds[0].node, 'node-name');
      assert.equal(nds[0].endpoint, '127.0.0.1:124');
      done();
    }, 0);
  });

  it('should not emit "add" when the node is unchanged', done => {
    let nodes = new NodeOperatorWithFakeWatcher();
    let addList = [];

    nodes.on('add', node => addList.push(node));
    nodes.watcher.emit('new', createEvent('node-name', '127.0.0.1:123'));
    nodes.watcher.emit('mod', createEvent('node-name', '127.0.0.1:123'));

    setTimeout(() => {
      assert.lengthOf(addList, 1);
      assert.equal(addList[0].node, 'node-name');
      assert.equal(addList[0].endpoint, '127.0.0.1:123');

      let nds = nodes.get();
      assert.lengthOf(nds, 1);
      assert.equal(nds[0].node, 'node-name');
      assert.equal(nds[0].endpoint, '127.0.0.1:123');
      done();
    }, 0);
  });

  it('should emit "remove" event for deleted CSINode', done => {
    let nodes = new NodeOperatorWithFakeWatcher();
    let removeList = [];
    let addList = [];

    nodes.on('add', node => addList.push(node));
    nodes.on('remove', node => removeList.push(node));
    nodes.watcher.emit('new', createEvent('node-name', '127.0.0.1:123'));
    nodes.watcher.emit('del', createEvent('node-name', '127.0.0.1:123'));

    setTimeout(() => {
      assert.lengthOf(addList, 1);
      assert.lengthOf(removeList, 1);
      assert.equal(addList[0].node, 'node-name');
      assert.equal(addList[0].endpoint, '127.0.0.1:123');
      assert.equal(removeList[0].node, 'node-name');

      let nds = nodes.get();
      assert.lengthOf(nds, 0);
      done();
    }, 0);
  });

  it('should emit "remove" event for deleted mayastor in CSINode', done => {
    let nodes = new NodeOperatorWithFakeWatcher();
    let removeList = [];
    let addList = [];

    nodes.on('add', node => addList.push(node));
    nodes.on('remove', node => removeList.push(node));
    nodes.watcher.emit('new', createEvent('node-name', '127.0.0.1:123'));
    nodes.watcher.emit('mod', createEvent('node-name', null));

    setTimeout(() => {
      assert.lengthOf(addList, 1);
      assert.lengthOf(removeList, 1);
      assert.equal(addList[0].node, 'node-name');
      assert.equal(addList[0].endpoint, '127.0.0.1:123');
      assert.equal(removeList[0].node, 'node-name');

      let nds = nodes.get();
      assert.lengthOf(nds, 0);
      done();
    }, 0);
  });

  it('should ignore deleted mayastor in CSINode which is not known', done => {
    let nodes = new NodeOperatorWithFakeWatcher();
    let removeList = [];
    let addList = [];

    nodes.on('add', node => addList.push(node));
    nodes.on('remove', node => removeList.push(node));
    nodes.watcher.emit('new', createEvent('node-name', null));
    nodes.watcher.emit('mod', createEvent('node-name', null));

    setTimeout(() => {
      assert.lengthOf(addList, 0);
      assert.lengthOf(removeList, 0);
      let nds = nodes.get();
      assert.lengthOf(nds, 0);
      done();
    }, 0);
  });
};
