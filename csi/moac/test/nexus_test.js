// Unit tests for the nexus object

'use strict';

/* eslint-disable no-unused-expressions */

const _ = require('lodash');
const expect = require('chai').expect;
const sinon = require('sinon');
const { Node } = require('../dist/node');
const { Replica } = require('../dist/replica');
const { Nexus } = require('../dist/nexus');
const { shouldFailWith } = require('./utils');
const { grpcCode, GrpcError } = require('../dist/grpc_client');

const UUID = 'ba5e39e9-0c0e-4973-8a3a-0dccada09cbb';

module.exports = function () {
  const props = {
    uuid: UUID,
    size: 100,
    deviceUri: '',
    state: 'NEXUS_ONLINE',
    children: [
      {
        uri: 'nvmf://' + UUID,
        state: 'CHILD_ONLINE'
      },
      {
        uri: 'bdev:///' + UUID,
        state: 'CHILD_ONLINE'
      }
    ]
  };

  it('should bind the nexus to node and then unbind it', (done) => {
    const node = new Node('node');
    const nexus = new Nexus(props);
    node.once('nexus', (ev) => {
      expect(ev.eventType).to.equal('new');
      expect(ev.object).to.equal(nexus);
      expect(nexus.node).to.equal(node);

      node.once('nexus', (ev) => {
        expect(ev.eventType).to.equal('del');
        expect(ev.object).to.equal(nexus);
        setTimeout(() => {
          expect(nexus.node).to.be.undefined;
          done();
        }, 0);
      });
      nexus.unbind();
    });
    nexus.bind(node);
  });

  it('should offline the nexus', () => {
    const node = new Node('node');
    const nexus = new Nexus(props);
    node._registerNexus(nexus);

    node.once('nexus', (ev) => {
      expect(ev.eventType).to.equal('mod');
      expect(ev.object).to.equal(nexus);
      expect(nexus.state).to.equal('NEXUS_OFFLINE');
    });
    nexus.offline();
  });

  describe('mod event', () => {
    let node, eventSpy, nexus, newProps;

    beforeEach(() => {
      node = new Node('node');
      eventSpy = sinon.spy(node, 'emit');
      nexus = new Nexus(props);
      node._registerNexus(nexus);
      newProps = _.clone(props);
    });

    it('should emit event upon change of size property', () => {
      newProps.size = 1000;
      nexus.merge(newProps);

      // First event is new nexus event
      sinon.assert.calledTwice(eventSpy);
      sinon.assert.calledWith(eventSpy.firstCall, 'nexus', {
        eventType: 'new',
        object: nexus
      });
      sinon.assert.calledWith(eventSpy.secondCall, 'nexus', {
        eventType: 'mod',
        object: nexus
      });
      expect(nexus.size).to.equal(1000);
    });

    it('should emit event upon change of deviceUri property', () => {
      newProps.deviceUri = 'nvmf://host/nqn';
      nexus.merge(newProps);

      // First event is new nexus event
      sinon.assert.calledTwice(eventSpy);
      sinon.assert.calledWith(eventSpy.secondCall, 'nexus', {
        eventType: 'mod',
        object: nexus
      });
      expect(nexus.deviceUri).to.equal('nvmf://host/nqn');
    });

    it('should emit event upon change of state property', () => {
      newProps.state = 'NEXUS_DEGRADED';
      nexus.merge(newProps);

      // First event is new nexus event
      sinon.assert.calledTwice(eventSpy);
      sinon.assert.calledWith(eventSpy.secondCall, 'nexus', {
        eventType: 'mod',
        object: nexus
      });
      expect(nexus.state).to.equal('NEXUS_DEGRADED');
    });

    it('should emit event upon change of children property', () => {
      newProps.children = [
        {
          uri: 'bdev:///' + UUID,
          state: 'CHILD_ONLINE'
        }
      ];
      nexus.merge(newProps);

      // First event is new nexus event
      sinon.assert.calledTwice(eventSpy);
      sinon.assert.calledWith(eventSpy.secondCall, 'nexus', {
        eventType: 'mod',
        object: nexus
      });
      expect(nexus.children).to.have.lengthOf(1);
      expect(nexus.children[0].uri).to.equal(`bdev:///${UUID}`);
      expect(nexus.children[0].state).to.equal('CHILD_ONLINE');
    });

    it('should not emit event when children are the same', () => {
      newProps.children = [
        {
          uri: 'bdev:///' + UUID,
          state: 'CHILD_ONLINE'
        },
        {
          uri: 'nvmf://' + UUID,
          state: 'CHILD_ONLINE'
        }
      ];
      nexus.merge(newProps);

      // First event is new nexus event
      sinon.assert.calledOnce(eventSpy);
      sinon.assert.calledWith(eventSpy, 'nexus', {
        eventType: 'new',
        object: nexus
      });
    });
  });

  describe('grpc', () => {
    let node, nexus, eventSpy, callStub, isSyncedStub;

    // Create a sample nexus bound to a node
    beforeEach((done) => {
      node = new Node('node');
      nexus = new Nexus(props);
      node.once('nexus', (ev) => {
        expect(ev.eventType).to.equal('new');
        eventSpy = sinon.spy(node, 'emit');
        callStub = sinon.stub(node, 'call');
        isSyncedStub = sinon.stub(node, 'isSynced');
        isSyncedStub.returns(true);
        done();
      });
      node._registerNexus(nexus);
    });

    afterEach(() => {
      eventSpy.resetHistory();
      callStub.reset();
      isSyncedStub.reset();
    });

    it('should not publish the nexus with whatever protocol', async () => {
      callStub.resolves({ deviceUri: 'file:///dev/whatever0' });
      callStub.rejects(new GrpcError(grpcCode.NOT_FOUND, 'Test failure'));

      await shouldFailWith(grpcCode.NOT_FOUND, async () => {
        await nexus.publish('whatever');
      });

      sinon.assert.notCalled(callStub);
    });

    it('should publish the nexus with iscsi protocol', async () => {
      callStub.resolves({ deviceUri: 'iscsi://host/dev/iscsi' });

      await nexus.publish('iscsi');

      sinon.assert.calledOnce(callStub);
      sinon.assert.calledWith(callStub, 'publishNexus', {
        uuid: UUID,
        key: '',
        share: 2
      });
      expect(nexus.deviceUri).to.equal('iscsi://host/dev/iscsi');
      sinon.assert.calledOnce(eventSpy);
      sinon.assert.calledWith(eventSpy, 'nexus', {
        eventType: 'mod',
        object: nexus
      });
    });

    it('should publish the nexus with nvmf protocol', async () => {
      callStub.resolves({ deviceUri: 'nvmf://host/nvme0' });

      await nexus.publish('nvmf');

      sinon.assert.calledOnce(callStub);
      sinon.assert.calledWith(callStub, 'publishNexus', {
        uuid: UUID,
        key: '',
        share: 1
      });
      expect(nexus.deviceUri).to.equal('nvmf://host/nvme0');
      sinon.assert.calledOnce(eventSpy);
      sinon.assert.calledWith(eventSpy, 'nexus', {
        eventType: 'mod',
        object: nexus
      });
    });

    it('should publish the nexus with nvmf protocol', async () => {
      callStub.resolves({ deviceUri: 'nvmf://host/nqn' });

      await nexus.publish('nvmf');

      sinon.assert.calledOnce(callStub);
      sinon.assert.calledWith(callStub, 'publishNexus', {
        uuid: UUID,
        key: '',
        share: 1
      });
      expect(nexus.deviceUri).to.equal('nvmf://host/nqn');
      sinon.assert.calledOnce(eventSpy);
      sinon.assert.calledWith(eventSpy, 'nexus', {
        eventType: 'mod',
        object: nexus
      });
    });

    it('should unpublish the nexus', async () => {
      callStub.resolves({});

      await nexus.unpublish();

      sinon.assert.calledOnce(callStub);
      sinon.assert.calledWith(callStub, 'unpublishNexus', { uuid: UUID });
      expect(nexus.deviceUri).to.equal('');
      sinon.assert.calledOnce(eventSpy);
      sinon.assert.calledWith(eventSpy, 'nexus', {
        eventType: 'mod',
        object: nexus
      });
    });

    it('should not fail to unpublish the nexus if it does not exist', async () => {
      callStub.rejects(new GrpcError(grpcCode.NOT_FOUND, 'test not found'));

      await nexus.unpublish();

      sinon.assert.calledOnce(callStub);
      sinon.assert.calledWith(callStub, 'unpublishNexus', { uuid: UUID });
      expect(nexus.deviceUri).to.equal('');
      sinon.assert.calledOnce(eventSpy);
      sinon.assert.calledWith(eventSpy, 'nexus', {
        eventType: 'mod',
        object: nexus
      });
    });

    it('should fake the unpublish if the node is offline', async () => {
      callStub.resolves({});
      isSyncedStub.returns(false);

      await nexus.unpublish();

      sinon.assert.notCalled(callStub);
      expect(nexus.deviceUri).to.equal('');
      sinon.assert.calledOnce(eventSpy);
      sinon.assert.calledWith(eventSpy, 'nexus', {
        eventType: 'mod',
        object: nexus
      });
    });

    it('should add replica to nexus', async () => {
      const uri = 'iscsi://' + UUID;
      const replica = new Replica({
        uuid: UUID,
        uri
      });
      callStub.resolves({
        uri,
        state: 'CHILD_DEGRADED',
        rebuildProgress: 0
      });

      const res = await nexus.addReplica(replica);

      expect(res.uri).to.equal(uri);
      expect(res.state).to.equal('CHILD_DEGRADED');
      sinon.assert.calledOnce(callStub);
      sinon.assert.calledWith(callStub, 'addChildNexus', {
        uuid: UUID,
        uri: 'iscsi://' + UUID,
        norebuild: false
      });
      expect(nexus.children).to.have.lengthOf(3);
      // should be sorted according to uri
      expect(nexus.children[0].uri).to.equal('bdev:///' + UUID);
      expect(nexus.children[1].uri).to.equal('iscsi://' + UUID);
      expect(nexus.children[2].uri).to.equal('nvmf://' + UUID);
      sinon.assert.calledOnce(eventSpy);
      sinon.assert.calledWith(eventSpy, 'nexus', {
        eventType: 'mod',
        object: nexus
      });
    });

    it('should not add replica to nexus if grpc fails', async () => {
      const replica = new Replica({
        uuid: UUID,
        uri: 'iscsi://' + UUID
      });
      callStub.rejects(new GrpcError(grpcCode.INTERNAL, 'Test failure'));

      await shouldFailWith(grpcCode.INTERNAL, async () => {
        await nexus.addReplica(replica);
      });

      sinon.assert.calledOnce(callStub);
      sinon.assert.calledWith(callStub, 'addChildNexus', {
        uuid: UUID,
        uri: 'iscsi://' + UUID,
        norebuild: false
      });
      expect(nexus.children).to.have.lengthOf(2);
      expect(nexus.children[0].uri).to.equal('bdev:///' + UUID);
      expect(nexus.children[1].uri).to.equal('nvmf://' + UUID);
      sinon.assert.notCalled(eventSpy);
    });

    it('should remove replica from nexus', async () => {
      const replica = new Replica({
        uuid: UUID,
        uri: 'nvmf://' + UUID
      });
      callStub.resolves({});

      await nexus.removeReplica(replica.uri);

      sinon.assert.calledOnce(callStub);
      sinon.assert.calledWith(callStub, 'removeChildNexus', {
        uuid: UUID,
        uri: 'nvmf://' + UUID
      });
      expect(nexus.children).to.have.lengthOf(1);
      expect(nexus.children[0].uri).to.equal('bdev:///' + UUID);
      sinon.assert.calledOnce(eventSpy);
      sinon.assert.calledWith(eventSpy, 'nexus', {
        eventType: 'mod',
        object: nexus
      });
    });

    it('should not remove replica from nexus if grpc fails', async () => {
      const replica = new Replica({
        uuid: UUID,
        uri: 'nvmf://' + UUID
      });
      callStub.rejects(new GrpcError(grpcCode.INTERNAL, 'Test failure'));

      await shouldFailWith(grpcCode.INTERNAL, async () => {
        await nexus.removeReplica(replica.uri);
      });

      sinon.assert.calledOnce(callStub);
      sinon.assert.calledWith(callStub, 'removeChildNexus', {
        uuid: UUID,
        uri: 'nvmf://' + UUID
      });
      expect(nexus.children).to.have.lengthOf(2);
      expect(nexus.children[0].uri).to.equal('bdev:///' + UUID);
      expect(nexus.children[1].uri).to.equal('nvmf://' + UUID);
      sinon.assert.notCalled(eventSpy);
    });

    it('should destroy the nexus', async () => {
      callStub.resolves({});

      await nexus.destroy();

      sinon.assert.calledOnce(eventSpy);
      sinon.assert.calledWith(eventSpy, 'nexus', {
        eventType: 'del',
        object: nexus
      });
      sinon.assert.calledOnce(callStub);
      sinon.assert.calledWith(callStub, 'destroyNexus', { uuid: UUID });
      expect(nexus.node).to.be.undefined;
      expect(node.nexus).to.have.lengthOf(0);
    });

    it('should not remove the nexus if grpc fails', async () => {
      callStub.rejects(new GrpcError(grpcCode.INTERNAL, 'Test failure'));

      await shouldFailWith(grpcCode.INTERNAL, async () => {
        await nexus.destroy();
      });

      sinon.assert.calledOnce(callStub);
      sinon.assert.calledWith(callStub, 'destroyNexus', { uuid: UUID });
      sinon.assert.notCalled(eventSpy);
      expect(nexus.node).to.equal(node);
      expect(node.nexus).to.have.lengthOf(1);
    });

    it('should fake the destroy if the node is offline', async () => {
      callStub.rejects(new GrpcError(grpcCode.INTERNAL, 'Not connected'));
      isSyncedStub.returns(false);

      await nexus.destroy();

      sinon.assert.calledOnce(eventSpy);
      sinon.assert.calledWith(eventSpy, 'nexus', {
        eventType: 'del',
        object: nexus
      });
      sinon.assert.notCalled(callStub);
      expect(nexus.node).to.be.undefined;
      expect(node.nexus).to.have.lengthOf(0);
    });
  });
};
