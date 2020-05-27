// Unit tests for the node operator
//
// We don't test the init method which depends on k8s api client and watcher.
// That method *must* be tested manually and in real k8s environment. For the
// rest of the dependencies we provide fake objects which mimic the real
// behaviour and allow us to test node operator in isolation from other
// components.

'use strict';

const expect = require('chai').expect;
const sinon = require('sinon');
const sleep = require('sleep-promise');
const Registry = require('../registry');
const NodeOperator = require('../node_operator');
const Node = require('./node_stub');
const Watcher = require('./watcher_stub');

const NAME = 'node-name';
const NAMESPACE = 'mayastor';
const ENDPOINT = 'localhost:1234';
const ENDPOINT2 = 'localhost:1235';

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

module.exports = function () {
  var msStub, putStub, putStatusStub, deleteStub, postStub;

  // Create k8s node resource object
  function createNodeResource (name, grpcEndpoint, status) {
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

  // k8s api client stub.
  //
  // Note that this stub serves only for PUT method on mayastor resource
  // endpoint to update the status of resource. Fake watcher that is used
  // in the tests does not use this client stub.
  function createK8sClient (watcher) {
    const mayastornodes = { mayastornodes: function (name) {} };
    const namespaces = function (ns) {
      expect(ns).to.equal(NAMESPACE);
      return mayastornodes;
    };
    const client = {
      apis: {
        'openebs.io': {
          v1alpha1: { namespaces }
        }
      }
    };

    msStub = sinon.stub(mayastornodes, 'mayastornodes');
    msStub.post = async function (payload) {
      watcher.objects[payload.body.metadata.name] = payload.body;
      // simulate the asynchronicity of the put
      await sleep(1);
    };
    postStub = sinon.stub(msStub, 'post');
    postStub.callThrough();

    const msObject = {
      // the tricky thing here is that we have to update watcher's cache
      // if we use this fake k8s client to change the object in order to
      // mimic real behaviour.
      put: async function (payload) {
        watcher.objects[payload.body.metadata.name].spec = payload.body.spec;
      },
      delete: async function () {},
      status: {
        put: async function (payload) {
          watcher.objects[payload.body.metadata.name].status =
            payload.body.status;
        }
      }
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
  async function mockedNodeOperator (k8sObjects, registry) {
    const oper = new NodeOperator(NAMESPACE);
    oper.registry = registry;
    oper.watcher = new Watcher(oper._filterMayastorNode, k8sObjects);
    oper.k8sClient = createK8sClient(oper.watcher);

    await oper.start();
    // give event-stream time to run its _start method to prevent race
    // conditions in test code when the underlaying source is modified
    // before _start is run.
    await sleep(1);
    return oper;
  }

  describe('resource filter', () => {
    it('valid mayastor node with status should pass the filter', () => {
      const obj = createNodeResource(NAME, ENDPOINT, 'online');
      const res = NodeOperator.prototype._filterMayastorNode(obj);
      expect(res.metadata.name).to.equal(NAME);
      expect(res.spec.grpcEndpoint).to.equal(ENDPOINT);
      expect(res.status).to.equal('online');
    });

    it('valid mayastor node without status should pass the filter', () => {
      const obj = createNodeResource(NAME, ENDPOINT);
      const res = NodeOperator.prototype._filterMayastorNode(obj);
      expect(res.metadata.name).to.equal(NAME);
      expect(res.spec.grpcEndpoint).to.equal(ENDPOINT);
      expect(res.status).to.equal('unknown');
    });

    it('mayastor node without grpc-endpoint should be ignored', () => {
      const obj = createNodeResource(NAME);
      const res = NodeOperator.prototype._filterMayastorNode(obj);
      expect(res).to.be.null();
    });
  });

  describe('watcher events', () => {
    var oper; // node operator

    afterEach(async () => {
      if (oper) {
        await oper.stop();
        oper = null;
      }
    });

    it('should add node to registry for existing resource when starting the operator', async () => {
      const registry = new Registry();
      registry.Node = Node;
      const addNodeSpy = sinon.spy(registry, 'addNode');

      oper = await mockedNodeOperator(
        [createNodeResource(NAME, ENDPOINT, 'online')],
        registry
      );
      sinon.assert.calledOnce(addNodeSpy);
      sinon.assert.calledWith(addNodeSpy, NAME, ENDPOINT);
    });

    it('should add node to registry upon "new" event', async () => {
      const registry = new Registry();
      registry.Node = Node;
      const addNodeSpy = sinon.spy(registry, 'addNode');
      oper = await mockedNodeOperator([], registry);
      // trigger "new" event
      oper.watcher.newObject(createNodeResource(NAME, ENDPOINT));
      sinon.assert.calledOnce(addNodeSpy);
      sinon.assert.calledWith(addNodeSpy, NAME, ENDPOINT);
    });

    it('should remove node from registry upon "del" event', async () => {
      // create registry with a node
      const registry = new Registry();
      const node = new Node(NAME);
      node.connect(ENDPOINT);
      registry.nodes[NAME] = node;
      const addNodeSpy = sinon.spy(registry, 'addNode');
      const removeNodeSpy = sinon.spy(registry, 'removeNode');

      oper = await mockedNodeOperator(
        [createNodeResource(NAME, ENDPOINT, 'online')],
        registry
      );
      sinon.assert.calledOnce(addNodeSpy);
      // trigger "del" event
      oper.watcher.delObject(NAME);
      sinon.assert.calledOnce(addNodeSpy);
      sinon.assert.calledOnce(removeNodeSpy);
      sinon.assert.calledWith(removeNodeSpy, NAME);
    });

    it('should not do anything upon "mod" event', async () => {
      // create registry with a node
      const registry = new Registry();
      const node = new Node(NAME);
      node.connect(ENDPOINT);
      registry.nodes[NAME] = node;
      const addNodeStub = sinon.stub(registry, 'addNode');
      addNodeStub.returns();
      const removeNodeStub = sinon.stub(registry, 'removeNode');
      removeNodeStub.returns();

      oper = await mockedNodeOperator(
        [createNodeResource(NAME, ENDPOINT, 'online')],
        registry
      );
      sinon.assert.calledOnce(addNodeStub);
      // trigger "mod" event
      oper.watcher.modObject(createNodeResource(NAME, ENDPOINT));
      sinon.assert.notCalled(removeNodeStub);
      sinon.assert.calledOnce(addNodeStub);
    });
  });

  describe('registry node events', () => {
    var oper; // node operator

    afterEach(async () => {
      if (oper) {
        await oper.stop();
        oper = null;
      }
    });

    it('should create a resource upon "new" node event', async () => {
      const registry = new Registry();
      registry.Node = Node;
      oper = await mockedNodeOperator([], registry);
      registry.addNode(NAME, ENDPOINT);
      await sleep(20);
      sinon.assert.calledOnce(postStub);
      sinon.assert.calledWithMatch(postStub, {
        body: {
          metadata: {
            name: NAME,
            namespace: NAMESPACE
          },
          spec: {
            grpcEndpoint: ENDPOINT
          }
        }
      });
      sinon.assert.notCalled(putStub);
      sinon.assert.calledOnce(putStatusStub);
      sinon.assert.calledWithMatch(putStatusStub, {
        body: {
          status: 'online'
        }
      });
      sinon.assert.notCalled(deleteStub);
    });

    it('should not crash if POST fails upon "new" node event', async () => {
      const registry = new Registry();
      registry.Node = Node;
      oper = await mockedNodeOperator([], registry);
      postStub.rejects(new Error('post failed'));
      registry.addNode(NAME, ENDPOINT);
      await sleep(10);
      sinon.assert.calledOnce(postStub);
      sinon.assert.calledWithMatch(postStub, {
        body: {
          metadata: {
            name: NAME,
            namespace: NAMESPACE
          },
          spec: {
            grpcEndpoint: ENDPOINT
          }
        }
      });
      sinon.assert.notCalled(putStatusStub);
      sinon.assert.notCalled(deleteStub);
    });

    it('should update the resource upon "new" node event if it exists', async () => {
      const registry = new Registry();
      registry.Node = Node;
      oper = await mockedNodeOperator([], registry);
      oper.watcher.injectObject(createNodeResource(NAME, ENDPOINT, 'offline'));
      registry.addNode(NAME, ENDPOINT2);
      await sleep(10);
      sinon.assert.notCalled(postStub);
      sinon.assert.calledOnce(putStub);
      sinon.assert.calledWithMatch(putStub, {
        body: {
          spec: {
            grpcEndpoint: ENDPOINT2
          }
        }
      });
      sinon.assert.calledOnce(putStatusStub);
      sinon.assert.calledWithMatch(putStatusStub, {
        body: {
          status: 'online'
        }
      });
      sinon.assert.notCalled(deleteStub);
    });

    it('should not update the resource upon "new" node event if it is the same', async () => {
      const registry = new Registry();
      registry.Node = Node;
      oper = await mockedNodeOperator([], registry);
      oper.watcher.injectObject(createNodeResource(NAME, ENDPOINT, 'online'));
      registry.addNode(NAME, ENDPOINT);
      await sleep(10);
      sinon.assert.notCalled(postStub);
      sinon.assert.notCalled(putStub);
      sinon.assert.notCalled(putStatusStub);
      sinon.assert.notCalled(deleteStub);
    });

    it('should update the resource upon "mod" node event', async () => {
      const registry = new Registry();
      registry.Node = Node;
      oper = await mockedNodeOperator(
        [createNodeResource(NAME, ENDPOINT, 'online')],
        registry
      );
      registry.addNode(NAME, ENDPOINT2);
      await sleep(10);
      sinon.assert.notCalled(postStub);
      sinon.assert.calledOnce(putStub);
      sinon.assert.calledWithMatch(putStub, {
        body: {
          spec: {
            grpcEndpoint: ENDPOINT2
          }
        }
      });
      sinon.assert.notCalled(putStatusStub);
      sinon.assert.notCalled(deleteStub);
    });

    it('should update status of the resource upon "mod" node event', async () => {
      const registry = new Registry();
      registry.Node = Node;
      oper = await mockedNodeOperator(
        [createNodeResource(NAME, ENDPOINT, 'online')],
        registry
      );
      registry.addNode(NAME, ENDPOINT);
      const node = registry.getNode(NAME);
      const isSyncedStub = sinon.stub(node, 'isSynced');
      isSyncedStub.returns(false);
      node._offline();
      await sleep(10);
      sinon.assert.notCalled(postStub);
      sinon.assert.notCalled(putStub);
      sinon.assert.calledOnce(putStatusStub);
      sinon.assert.calledWithMatch(putStatusStub, {
        body: {
          status: 'offline'
        }
      });
      sinon.assert.notCalled(deleteStub);
    });

    it('should not crash if PUT fails upon "mod" node event', async () => {
      const registry = new Registry();
      registry.Node = Node;
      oper = await mockedNodeOperator(
        [createNodeResource(NAME, ENDPOINT, 'online')],
        registry
      );
      putStub.rejects(new Error('put failed'));
      registry.addNode(NAME, ENDPOINT2);
      await sleep(10);
      sinon.assert.notCalled(postStub);
      sinon.assert.calledOnce(putStub);
      sinon.assert.notCalled(putStatusStub);
      sinon.assert.notCalled(deleteStub);
    });

    it('should not crash if the resource does not exist upon "mod" node event', async () => {
      const registry = new Registry();
      registry.Node = Node;
      oper = await mockedNodeOperator([], registry);
      // secretly inject node to registry (watcher does not know)
      const node = new Node(NAME);
      node.connect(ENDPOINT);
      registry.nodes[NAME] = node;
      // modify the node
      registry.addNode(NAME, ENDPOINT2);
      await sleep(10);
      sinon.assert.notCalled(postStub);
      sinon.assert.notCalled(putStub);
      sinon.assert.notCalled(putStatusStub);
      sinon.assert.notCalled(deleteStub);
    });

    it('should delete the resource upon "del" node event', async () => {
      const registry = new Registry();
      registry.Node = Node;
      oper = await mockedNodeOperator(
        [createNodeResource(NAME, ENDPOINT, 'online')],
        registry
      );
      registry.removeNode(NAME);
      await sleep(10);
      sinon.assert.notCalled(postStub);
      sinon.assert.notCalled(putStub);
      sinon.assert.notCalled(putStatusStub);
      sinon.assert.calledOnce(deleteStub);
    });

    it('should not crash if DELETE fails upon "del" node event', async () => {
      const registry = new Registry();
      registry.Node = Node;
      oper = await mockedNodeOperator(
        [createNodeResource(NAME, ENDPOINT, 'online')],
        registry
      );
      deleteStub.rejects(new Error('delete failed'));
      registry.removeNode(NAME);
      await sleep(10);
      sinon.assert.notCalled(postStub);
      sinon.assert.notCalled(putStub);
      sinon.assert.notCalled(putStatusStub);
      sinon.assert.calledOnce(deleteStub);
    });

    it('should not crash if the resource does not exist upon "del" node event', async () => {
      const registry = new Registry();
      registry.Node = Node;
      oper = await mockedNodeOperator([], registry);
      // secretly inject node to registry (watcher does not know)
      const node = new Node(NAME);
      node.connect(ENDPOINT);
      registry.nodes[NAME] = node;
      // modify the node
      registry.removeNode(NAME);
      await sleep(10);
      sinon.assert.notCalled(postStub);
      sinon.assert.notCalled(putStub);
      sinon.assert.notCalled(putStatusStub);
      sinon.assert.notCalled(deleteStub);
    });
  });
};
