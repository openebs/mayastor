// Unit tests for the node operator

'use strict';

const expect = require('chai').expect;
const sinon = require('sinon');
const sleep = require('sleep-promise');
const { KubeConfig } = require('@kubernetes/client-node');
const { Registry } = require('../dist/registry');
const { NodeOperator, NodeResource } = require('../dist/node_operator');
const { mockCache } = require('./watcher_stub');
const Node = require('./node_stub');

const EVENT_PROPAGATION_DELAY = 10;
const NAME = 'node-name';
const NAMESPACE = 'mayastor';
const ENDPOINT = 'localhost:1234';
const ENDPOINT2 = 'localhost:1235';

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

function defaultMeta (name) {
  return {
    creationTimestamp: '2019-02-15T18:23:53Z',
    generation: 1,
    name: name,
    namespace: NAMESPACE,
    resourceVersion: '627981',
    selfLink: `/apis/openebs.io/v1alpha1/namespaces/${NAMESPACE}/mayastornodes/${name}`,
    uid: 'd99f06a9-314e-11e9-b086-589cfc0d76a7'
  };
}

// Create k8s node resource object
function createK8sNodeResource (name, grpcEndpoint, status) {
  const obj = {
    apiVersion: 'openebs.io/v1alpha1',
    kind: 'MayastorNode',
    metadata: defaultMeta(name),
    spec: { grpcEndpoint }
  };
  if (status) {
    obj.status = status;
  }
  return obj;
}

// Create k8s node resource object
function createNodeResource (name, grpcEndpoint, status) {
  return new NodeResource(createK8sNodeResource(name, grpcEndpoint, status));
}

// Create a pool operator object suitable for testing - with fake watcher
// and fake k8s api client.
function createNodeOperator (registry) {
  const kc = new KubeConfig();
  Object.assign(kc, fakeConfig);
  return new NodeOperator(NAMESPACE, kc, registry);
}

module.exports = function () {
  describe('NodeResource constructor', () => {
    it('should create valid node resource with status', () => {
      const obj = createNodeResource(NAME, ENDPOINT, 'online');
      expect(obj.metadata.name).to.equal(NAME);
      expect(obj.spec.grpcEndpoint).to.equal(ENDPOINT);
      expect(obj.status).to.equal('online');
    });

    it('should create valid node resource without status', () => {
      const obj = createNodeResource(NAME, ENDPOINT);
      expect(obj.metadata.name).to.equal(NAME);
      expect(obj.spec.grpcEndpoint).to.equal(ENDPOINT);
      expect(obj.status).to.equal('unknown');
    });

    // empty endpoint means that the node has unregistered itself
    it('should create node resource with empty grpc endpoint', () => {
      const obj = createNodeResource(NAME, '', 'offline');
      expect(obj.metadata.name).to.equal(NAME);
      expect(obj.spec.grpcEndpoint).to.equal('');
      expect(obj.status).to.equal('offline');
    });
  });

  describe('init method', () => {
    let kc, oper, fakeApiStub;

    beforeEach(() => {
      const registry = new Registry({});
      kc = new KubeConfig();
      Object.assign(kc, fakeConfig);
      oper = new NodeOperator(NAMESPACE, kc, registry);
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
    let oper; // node operator
    let stubs, registry, nodeResource;

    beforeEach(async () => {
      registry = new Registry({});
      registry.Node = Node;

      oper = createNodeOperator(registry);
      nodeResource = createNodeResource(NAME, ENDPOINT, 'online');
      mockCache(oper.watcher, (arg) => {
        stubs = arg;
        stubs.get.returns(nodeResource);
      });
      await oper.start();
      // give time to registry to install its callbacks
      await sleep(EVENT_PROPAGATION_DELAY);
    });

    afterEach(() => {
      if (oper) {
        oper.stop();
        oper = null;
      }
    });

    it('should add node to registry upon "new" event', async () => {
      const addNodeSpy = sinon.spy(registry, 'addNode');
      oper.watcher.emit('new', nodeResource);
      await sleep(EVENT_PROPAGATION_DELAY);
      sinon.assert.calledOnce(addNodeSpy);
      sinon.assert.calledWith(addNodeSpy, NAME, ENDPOINT);
    });

    it('should not add node to registry if endpoint is empty', async () => {
      const addNodeSpy = sinon.spy(registry, 'addNode');
      nodeResource.spec.grpcEndpoint = '';
      oper.watcher.emit('new', nodeResource);
      await sleep(EVENT_PROPAGATION_DELAY);
      sinon.assert.notCalled(addNodeSpy);
    });

    it('should remove node from registry upon "del" event', async () => {
      // create registry with a node
      const node = new Node(NAME);
      node.connect(ENDPOINT);
      registry.nodes[NAME] = node;
      const removeNodeSpy = sinon.spy(registry, 'removeNode');

      // trigger "del" event
      oper.watcher.emit('del', nodeResource);
      await sleep(EVENT_PROPAGATION_DELAY);
      sinon.assert.calledWith(removeNodeSpy, NAME);
    });

    it('should not do anything upon "mod" event', async () => {
      // create registry with a node
      const node = new Node(NAME);
      node.connect(ENDPOINT);
      registry.nodes[NAME] = node;
      const addNodeStub = sinon.stub(registry, 'addNode');
      addNodeStub.returns();
      const removeNodeStub = sinon.stub(registry, 'removeNode');
      removeNodeStub.returns();

      // trigger "mod" event
      oper.watcher.emit('mod', nodeResource);
      await sleep(EVENT_PROPAGATION_DELAY);
      sinon.assert.notCalled(removeNodeStub);
      sinon.assert.notCalled(addNodeStub);
    });
  });

  describe('registry events', () => {
    let registry, oper;

    beforeEach(async () => {
      registry = new Registry({});
      registry.Node = Node;
      oper = createNodeOperator(registry);
    });

    afterEach(() => {
      if (oper) {
        oper.stop();
        oper = null;
      }
    });

    it('should create a resource upon "new" node event', async () => {
      let stubs;
      const nodeResource = createNodeResource(NAME, ENDPOINT);
      mockCache(oper.watcher, (arg) => {
        stubs = arg;
        stubs.get.onFirstCall().returns();
        stubs.get.onSecondCall().returns(nodeResource);
      });
      await oper.start();
      // give time to registry to install its callbacks
      await sleep(EVENT_PROPAGATION_DELAY);
      registry.addNode(NAME, ENDPOINT);
      await sleep(EVENT_PROPAGATION_DELAY);

      sinon.assert.calledOnce(stubs.create);
      expect(stubs.create.args[0][4].metadata.name).to.equal(NAME);
      expect(stubs.create.args[0][4].spec.grpcEndpoint).to.equal(ENDPOINT);
      sinon.assert.notCalled(stubs.update);
      sinon.assert.calledOnce(stubs.updateStatus);
      sinon.assert.notCalled(stubs.delete);
    });

    it('should not crash if POST fails upon "new" node event', async () => {
      let stubs;
      const nodeResource = createNodeResource(NAME, ENDPOINT);
      mockCache(oper.watcher, (arg) => {
        stubs = arg;
        stubs.get.onFirstCall().returns();
        stubs.get.onSecondCall().returns(nodeResource);
        stubs.create.rejects(new Error('post failed'));
      });
      await oper.start();
      // give time to registry to install its callbacks
      await sleep(EVENT_PROPAGATION_DELAY);
      registry.addNode(NAME, ENDPOINT);
      await sleep(EVENT_PROPAGATION_DELAY);

      sinon.assert.calledOnce(stubs.create);
      sinon.assert.notCalled(stubs.update);
      sinon.assert.calledOnce(stubs.updateStatus);
      sinon.assert.notCalled(stubs.delete);
    });

    it('should update the resource upon "new" node event if it exists', async () => {
      let stubs;
      const nodeResource = createNodeResource(NAME, ENDPOINT, 'offline');
      mockCache(oper.watcher, (arg) => {
        stubs = arg;
        stubs.get.returns(nodeResource);
      });
      await oper.start();
      // give time to registry to install its callbacks
      await sleep(EVENT_PROPAGATION_DELAY);
      registry.addNode(NAME, ENDPOINT2);
      await sleep(EVENT_PROPAGATION_DELAY);

      sinon.assert.notCalled(stubs.create);
      sinon.assert.calledOnce(stubs.update);
      expect(stubs.update.args[0][5].metadata.name).to.equal(NAME);
      expect(stubs.update.args[0][5].spec.grpcEndpoint).to.equal(ENDPOINT2);
      sinon.assert.calledOnce(stubs.updateStatus);
      expect(stubs.updateStatus.args[0][5].metadata.name).to.equal(NAME);
      expect(stubs.updateStatus.args[0][5].status).to.equal('online');
      sinon.assert.notCalled(stubs.delete);
    });

    it('should not update the resource upon "new" node event if it is the same', async () => {
      let stubs;
      const nodeResource = createNodeResource(NAME, ENDPOINT, 'online');
      mockCache(oper.watcher, (arg) => {
        stubs = arg;
        stubs.get.returns(nodeResource);
      });
      await oper.start();
      // give time to registry to install its callbacks
      await sleep(EVENT_PROPAGATION_DELAY);
      registry.addNode(NAME, ENDPOINT);
      await sleep(EVENT_PROPAGATION_DELAY);

      sinon.assert.notCalled(stubs.create);
      sinon.assert.notCalled(stubs.update);
      sinon.assert.notCalled(stubs.updateStatus);
      sinon.assert.notCalled(stubs.delete);
    });

    it('should update the resource upon "mod" node event', async () => {
      let stubs;
      const nodeResource = createNodeResource(NAME, ENDPOINT, 'online');
      mockCache(oper.watcher, (arg) => {
        stubs = arg;
        stubs.get.returns(nodeResource);
      });
      await oper.start();
      // give time to registry to install its callbacks
      await sleep(EVENT_PROPAGATION_DELAY);
      registry.addNode(NAME, ENDPOINT2);
      await sleep(EVENT_PROPAGATION_DELAY);

      sinon.assert.notCalled(stubs.create);
      sinon.assert.calledOnce(stubs.update);
      expect(stubs.update.args[0][5].metadata.name).to.equal(NAME);
      expect(stubs.update.args[0][5].spec.grpcEndpoint).to.equal(ENDPOINT2);
      sinon.assert.notCalled(stubs.updateStatus);
      sinon.assert.notCalled(stubs.delete);
    });

    it('should update status of the resource upon "mod" node event', async () => {
      let stubs;
      const nodeResource = createNodeResource(NAME, ENDPOINT, 'online');
      mockCache(oper.watcher, (arg) => {
        stubs = arg;
        stubs.get.returns(nodeResource);
      });
      await oper.start();
      // give time to registry to install its callbacks
      await sleep(EVENT_PROPAGATION_DELAY);
      registry.addNode(NAME, ENDPOINT);
      await sleep(EVENT_PROPAGATION_DELAY);
      const node = registry.getNode(NAME);
      const isSyncedStub = sinon.stub(node, 'isSynced');
      isSyncedStub.returns(false);
      node._offline();
      await sleep(EVENT_PROPAGATION_DELAY);

      sinon.assert.notCalled(stubs.create);
      sinon.assert.notCalled(stubs.update);
      sinon.assert.calledOnce(stubs.updateStatus);
      expect(stubs.updateStatus.args[0][5].metadata.name).to.equal(NAME);
      expect(stubs.updateStatus.args[0][5].status).to.equal('offline');
      sinon.assert.notCalled(stubs.delete);
    });

    it('should update spec and status of the resource upon "mod" node event', async () => {
      let stubs;
      const nodeResource = createNodeResource(NAME, ENDPOINT, 'online');
      mockCache(oper.watcher, (arg) => {
        stubs = arg;
        stubs.get.returns(nodeResource);
      });
      await oper.start();
      // give time to registry to install its callbacks
      await sleep(EVENT_PROPAGATION_DELAY);
      registry.addNode(NAME, ENDPOINT);
      await sleep(EVENT_PROPAGATION_DELAY);
      const node = registry.getNode(NAME);
      const isSyncedStub = sinon.stub(node, 'isSynced');
      isSyncedStub.returns(false);
      node.disconnect();
      await sleep(EVENT_PROPAGATION_DELAY);

      sinon.assert.notCalled(stubs.create);
      sinon.assert.calledOnce(stubs.update);
      expect(stubs.update.args[0][5].metadata.name).to.equal(NAME);
      expect(stubs.update.args[0][5].spec.grpcEndpoint).to.equal('');
      sinon.assert.calledOnce(stubs.updateStatus);
      expect(stubs.updateStatus.args[0][5].metadata.name).to.equal(NAME);
      expect(stubs.updateStatus.args[0][5].status).to.equal('offline');
      sinon.assert.notCalled(stubs.delete);
    });

    it('should not crash if PUT fails upon "mod" node event', async () => {
      let stubs;
      const nodeResource = createNodeResource(NAME, ENDPOINT, 'online');
      mockCache(oper.watcher, (arg) => {
        stubs = arg;
        stubs.get.returns(nodeResource);
        stubs.update.rejects(new Error('put failed'));
      });
      await oper.start();
      // give time to registry to install its callbacks
      await sleep(EVENT_PROPAGATION_DELAY);
      registry.addNode(NAME, ENDPOINT2);
      await sleep(EVENT_PROPAGATION_DELAY);

      sinon.assert.notCalled(stubs.create);
      sinon.assert.calledTwice(stubs.update);
      sinon.assert.notCalled(stubs.updateStatus);
      sinon.assert.notCalled(stubs.delete);
    });

    it('should not create the resource upon "mod" node event', async () => {
      let stubs;
      mockCache(oper.watcher, (arg) => {
        stubs = arg;
        stubs.get.returns();
      });
      await oper.start();
      // give time to registry to install its callbacks
      await sleep(EVENT_PROPAGATION_DELAY);
      // secretly inject node to registry (watcher does not know)
      const node = new Node(NAME);
      node.connect(ENDPOINT);
      registry.nodes[NAME] = node;
      registry.addNode(NAME, ENDPOINT2);
      await sleep(EVENT_PROPAGATION_DELAY);

      sinon.assert.notCalled(stubs.create);
      sinon.assert.notCalled(stubs.update);
      sinon.assert.notCalled(stubs.updateStatus);
      sinon.assert.notCalled(stubs.delete);
    });

    it('should delete the resource upon "del" node event', async () => {
      let stubs;
      const nodeResource = createNodeResource(NAME, ENDPOINT, 'online');
      mockCache(oper.watcher, (arg) => {
        stubs = arg;
        stubs.get.returns(nodeResource);
      });
      await oper.start();
      // give time to registry to install its callbacks
      await sleep(EVENT_PROPAGATION_DELAY);
      // secretly inject node to registry (watcher does not know)
      const node = new Node(NAME);
      node.connect(ENDPOINT);
      registry.nodes[NAME] = node;
      registry.removeNode(NAME);
      await sleep(EVENT_PROPAGATION_DELAY);

      sinon.assert.notCalled(stubs.create);
      sinon.assert.notCalled(stubs.update);
      sinon.assert.notCalled(stubs.updateStatus);
      sinon.assert.calledOnce(stubs.delete);
    });

    it('should not crash if DELETE fails upon "del" node event', async () => {
      let stubs;
      const nodeResource = createNodeResource(NAME, ENDPOINT, 'online');
      mockCache(oper.watcher, (arg) => {
        stubs = arg;
        stubs.get.returns(nodeResource);
        stubs.delete.rejects(new Error('delete failed'));
      });
      await oper.start();
      // give time to registry to install its callbacks
      await sleep(EVENT_PROPAGATION_DELAY);
      // secretly inject node to registry (watcher does not know)
      const node = new Node(NAME);
      node.connect(ENDPOINT);
      registry.nodes[NAME] = node;
      registry.removeNode(NAME);
      await sleep(EVENT_PROPAGATION_DELAY);

      sinon.assert.notCalled(stubs.create);
      sinon.assert.notCalled(stubs.update);
      sinon.assert.notCalled(stubs.updateStatus);
      sinon.assert.calledOnce(stubs.delete);
    });

    it('should not crash if the resource does not exist upon "del" node event', async () => {
      let stubs;
      mockCache(oper.watcher, (arg) => {
        stubs = arg;
      });
      await oper.start();
      // give time to registry to install its callbacks
      await sleep(EVENT_PROPAGATION_DELAY);
      // secretly inject node to registry (watcher does not know)
      const node = new Node(NAME);
      node.connect(ENDPOINT);
      registry.nodes[NAME] = node;
      registry.removeNode(NAME);
      await sleep(EVENT_PROPAGATION_DELAY);

      sinon.assert.notCalled(stubs.create);
      sinon.assert.notCalled(stubs.update);
      sinon.assert.notCalled(stubs.updateStatus);
      sinon.assert.notCalled(stubs.delete);
    });
  });
};
