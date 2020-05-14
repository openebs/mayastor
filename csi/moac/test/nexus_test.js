// Unit tests for the nexus object

'use strict';

const _ = require('lodash');
const expect = require('chai').expect;
const sinon = require('sinon');
const Node = require('../node');
const Replica = require('../replica');
const Nexus = require('../nexus');
const { shouldFailWith } = require('./utils');
const { GrpcCode, GrpcError } = require('../grpc_client');

const UUID = 'ba5e39e9-0c0e-4973-8a3a-0dccada09cbb';

module.exports = function () {
  var props = {
    uuid: UUID,
    size: 100,
    devicePath: '',
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
          expect(nexus.node).to.be.null();
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
    var node, eventSpy, nexus, newProps;

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

    it('should emit event upon change of devicePath property', () => {
      newProps.devicePath = '/dev/nbd0';
      nexus.merge(newProps);

      // First event is new nexus event
      sinon.assert.calledTwice(eventSpy);
      sinon.assert.calledWith(eventSpy.secondCall, 'nexus', {
        eventType: 'mod',
        object: nexus
      });
      expect(nexus.devicePath).to.equal('/dev/nbd0');
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
    var node, nexus, eventSpy, callStub;

    // Create a sample nexus bound to a node
    beforeEach((done) => {
      node = new Node('node');
      nexus = new Nexus(props);
      node.once('nexus', (ev) => {
        expect(ev.eventType).to.equal('new');
        eventSpy = sinon.spy(node, 'emit');
        callStub = sinon.stub(node, 'call');
        done();
      });
      node._registerNexus(nexus);
    });

    afterEach(() => {
      eventSpy.resetHistory();
      callStub.reset();
    });

    it('should not publish the nexus with whatever protocol', async () => {
      callStub.resolves({ devicePath: '/dev/whatever0' });
      callStub.rejects(new GrpcError(GrpcCode.NOT_FOUND, 'Test failure'));

      await shouldFailWith(GrpcCode.NOT_FOUND, async () => {
        await nexus.publish('whatever');
      });

      sinon.assert.notCalled(callStub);
    });

    it('should publish the nexus with iscsi protocol', async () => {
      callStub.resolves({ devicePath: '/dev/iscsi' });

      await nexus.publish('iscsi');

      sinon.assert.calledOnce(callStub);
      sinon.assert.calledWith(callStub, 'publishNexus', {
        uuid: UUID,
        key: '',
        share: 2
      });
      expect(nexus.devicePath).to.equal('/dev/iscsi');
      sinon.assert.calledOnce(eventSpy);
      sinon.assert.calledWith(eventSpy, 'nexus', {
        eventType: 'mod',
        object: nexus
      });
    });

    it('should publish the nexus with nvmf protocol', async () => {
      callStub.resolves({ devicePath: '/dev/nvme0' });

      await nexus.publish('nvmf');

      sinon.assert.calledOnce(callStub);
      sinon.assert.calledWith(callStub, 'publishNexus', {
        uuid: UUID,
        key: '',
        share: 1
      });
      expect(nexus.devicePath).to.equal('/dev/nvme0');
      sinon.assert.calledOnce(eventSpy);
      sinon.assert.calledWith(eventSpy, 'nexus', {
        eventType: 'mod',
        object: nexus
      });
    });

    it('should publish the nexus with nbd protocol', async () => {
      callStub.resolves({ devicePath: '/dev/nbd0' });

      await nexus.publish('nbd');

      sinon.assert.calledOnce(callStub);
      sinon.assert.calledWith(callStub, 'publishNexus', {
        uuid: UUID,
        key: '',
        share: 0 // Nbd for now
      });
      expect(nexus.devicePath).to.equal('/dev/nbd0');
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
      expect(nexus.devicePath).to.equal('');
      sinon.assert.calledOnce(eventSpy);
      sinon.assert.calledWith(eventSpy, 'nexus', {
        eventType: 'mod',
        object: nexus
      });
    });

    it('should add replica to nexus', async () => {
      const replica = new Replica({
        uuid: UUID,
        uri: 'iscsi://' + UUID
      });
      callStub.resolves({});

      await nexus.addReplica(replica);

      sinon.assert.calledOnce(callStub);
      sinon.assert.calledWith(callStub, 'addChildNexus', {
        uuid: UUID,
        uri: 'iscsi://' + UUID,
        rebuild: true
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
      callStub.rejects(new GrpcError(GrpcCode.INTERNAL, 'Test failure'));

      await shouldFailWith(GrpcCode.INTERNAL, async () => {
        await nexus.addReplica(replica);
      });

      sinon.assert.calledOnce(callStub);
      sinon.assert.calledWith(callStub, 'addChildNexus', {
        uuid: UUID,
        uri: 'iscsi://' + UUID,
        rebuild: true
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
      callStub.rejects(new GrpcError(GrpcCode.INTERNAL, 'Test failure'));

      await shouldFailWith(GrpcCode.INTERNAL, async () => {
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
      expect(nexus.node).to.be.null();
      expect(node.nexus).to.have.lengthOf(0);
    });

    it('should not remove the nexus if grpc fails', async () => {
      callStub.rejects(new GrpcError(GrpcCode.INTERNAL, 'Test failure'));

      await shouldFailWith(GrpcCode.INTERNAL, async () => {
        await nexus.destroy();
      });

      sinon.assert.calledOnce(callStub);
      sinon.assert.calledWith(callStub, 'destroyNexus', { uuid: UUID });
      sinon.assert.notCalled(eventSpy);
      expect(nexus.node).to.equal(node);
      expect(node.nexus).to.have.lengthOf(1);
    });

    it('should ignore NOT_FOUND error when destroying the nexus', async () => {
      callStub.resolves({});

      await nexus.destroy();

      sinon.assert.calledOnce(eventSpy);
      sinon.assert.calledWith(eventSpy, 'nexus', {
        eventType: 'del',
        object: nexus
      });
      sinon.assert.calledOnce(callStub);
      sinon.assert.calledWith(callStub, 'destroyNexus', { uuid: UUID });
      expect(nexus.node).to.be.null();
      expect(node.nexus).to.have.lengthOf(0);
    });
  });
};
