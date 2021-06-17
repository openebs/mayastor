// Unit tests for the replica object

'use strict';

/* eslint-disable no-unused-expressions */

const _ = require('lodash');
const expect = require('chai').expect;
const sinon = require('sinon');
const { Node } = require('../dist/node');
const { Pool } = require('../dist/pool');
const { Replica } = require('../dist/replica');
const { shouldFailWith } = require('./utils');
const { grpcCode, GrpcError } = require('../dist/grpc_client');

const UUID = 'ba5e39e9-0c0e-4973-8a3a-0dccada09cbb';

module.exports = function () {
  const poolProps = {
    name: 'pool',
    disks: ['/dev/sda'],
    state: 'POOL_ONLINE',
    capacity: 100,
    used: 4
  };
  const props = {
    uuid: UUID,
    pool: 'pool',
    size: 100,
    share: 'REPLICA_NONE',
    uri: 'bdev:///' + UUID + '?uuid=1'
  };

  describe('mod event', () => {
    let node, eventSpy, replica, pool, newProps;

    beforeEach(() => {
      node = new Node('node');
      eventSpy = sinon.spy(node, 'emit');
      pool = new Pool(poolProps);
      node._registerPool(pool);
      replica = new Replica(props);
      pool.registerReplica(replica);
      newProps = _.clone(props);
    });

    it('should ignore change of pool property', () => {
      newProps.pool = 'some-other-pool';
      replica.merge(newProps);

      // First two events are new pool and new replica events
      sinon.assert.calledTwice(eventSpy);
      sinon.assert.calledWith(eventSpy.firstCall, 'pool', {
        eventType: 'new',
        object: pool
      });
      sinon.assert.calledWith(eventSpy.secondCall, 'replica', {
        eventType: 'new',
        object: replica
      });
      expect(replica.pool).to.equal(pool);
      expect(replica.pool.name).to.equal('pool');
    });

    it('should emit event upon change of size property', () => {
      newProps.size = 1000;
      replica.merge(newProps);

      // First two events are new pool and new replica events
      sinon.assert.calledThrice(eventSpy);
      sinon.assert.calledWith(eventSpy.lastCall, 'replica', {
        eventType: 'mod',
        object: replica
      });
      expect(replica.size).to.equal(1000);
    });

    it('should emit event upon change of share and uri property', () => {
      newProps.share = 'REPLICA_NVMF';
      newProps.uri = 'nvmf://blabla';
      replica.merge(newProps);

      // First two events are new pool and new replica events
      sinon.assert.calledThrice(eventSpy);
      sinon.assert.calledWith(eventSpy.lastCall, 'replica', {
        eventType: 'mod',
        object: replica
      });
      expect(replica.share).to.equal('REPLICA_NVMF');
      expect(replica.uri).to.equal('nvmf://blabla');
    });
  });

  it('should bind the replica to pool and then unbind it', (done) => {
    const node = new Node('node');
    const pool = new Pool(poolProps);
    node._registerPool(pool);
    const replica = new Replica(props);

    node.once('replica', (ev) => {
      expect(ev.eventType).to.equal('new');
      expect(ev.object).to.equal(replica);
      expect(replica.pool).to.equal(pool);

      node.once('replica', (ev) => {
        expect(ev.eventType).to.equal('del');
        expect(ev.object).to.equal(replica);
        setTimeout(() => {
          expect(replica.pool).to.be.undefined;
          done();
        }, 0);
      });
      replica.unbind();
    });
    replica.bind(pool);
  });

  it('should offline the replica', (done) => {
    const node = new Node('node');
    const pool = new Pool(poolProps);
    node._registerPool(pool);
    const replica = new Replica(props);
    pool.registerReplica(replica);

    node.once('replica', (ev) => {
      expect(ev.eventType).to.equal('mod');
      expect(ev.object).to.equal(replica);
      expect(replica.isOffline()).to.be.true;
      done();
    });
    replica.offline();
  });

  it('should share the replica', async () => {
    const node = new Node('node');
    const stub = sinon.stub(node, 'call');
    stub.resolves({ uri: 'nvmf://blabla' });
    const pool = new Pool(poolProps);
    node._registerPool(pool);
    const replica = new Replica(props);
    pool.registerReplica(replica);

    const uri = await replica.setShare('REPLICA_NVMF');

    sinon.assert.calledOnce(stub);
    sinon.assert.calledWith(stub, 'shareReplica', {
      uuid: UUID,
      share: 'REPLICA_NVMF'
    });
    expect(uri).to.equal('nvmf://blabla');
    expect(replica.share).to.equal('REPLICA_NVMF');
    expect(replica.uri).to.equal('nvmf://blabla');
  });

  it('should throw if grpc fails during sharing', async () => {
    const node = new Node('node');
    const stub = sinon.stub(node, 'call');
    stub.rejects(new GrpcError(grpcCode.INTERNAL, 'Test failure'));
    const pool = new Pool(poolProps);
    node._registerPool(pool);
    const replica = new Replica(props);
    pool.registerReplica(replica);

    await shouldFailWith(grpcCode.INTERNAL, async () => {
      await replica.setShare('REPLICA_NVMF');
    });
    expect(replica.share).to.equal('REPLICA_NONE');
  });

  it('should destroy the replica', (done) => {
    const node = new Node('node');
    const callStub = sinon.stub(node, 'call');
    callStub.resolves({});
    const isSyncedStub = sinon.stub(node, 'isSynced');
    isSyncedStub.returns(true);
    const pool = new Pool(poolProps);
    node._registerPool(pool);
    const replica = new Replica(props);
    pool.registerReplica(replica);

    node.once('replica', (ev) => {
      expect(ev.eventType).to.equal('del');
      expect(ev.object).to.equal(replica);
      sinon.assert.calledOnce(callStub);
      sinon.assert.calledWith(callStub, 'destroyReplica', { uuid: UUID });
      setTimeout(() => {
        expect(replica.pool).to.be.undefined;
        expect(pool.replicas).to.have.lengthOf(0);
        done();
      }, 0);
    });
    replica.destroy();
  });

  it('should not remove the replica if grpc fails', async () => {
    const node = new Node('node');
    const callStub = sinon.stub(node, 'call');
    callStub.rejects(new GrpcError(grpcCode.INTERNAL, 'Test failure'));
    const isSyncedStub = sinon.stub(node, 'isSynced');
    isSyncedStub.returns(true);
    const eventSpy = sinon.spy(node, 'emit');
    const pool = new Pool(poolProps);
    node._registerPool(pool);
    const replica = new Replica(props);
    pool.registerReplica(replica);

    await shouldFailWith(grpcCode.INTERNAL, async () => {
      await replica.destroy();
    });

    sinon.assert.calledOnce(callStub);
    sinon.assert.calledWith(callStub, 'destroyReplica', { uuid: UUID });
    // it is called when creating the pool and replica
    sinon.assert.calledTwice(eventSpy);
    sinon.assert.calledWith(eventSpy.firstCall, 'pool', {
      eventType: 'new',
      object: pool
    });
    sinon.assert.calledWith(eventSpy.secondCall, 'replica', {
      eventType: 'new',
      object: replica
    });
    expect(replica.pool).to.equal(pool);
    expect(pool.replicas).to.have.lengthOf(1);
  });

  it('should fake the destroy of the replica if the node is offline', (done) => {
    const node = new Node('node');
    const callStub = sinon.stub(node, 'call');
    callStub.rejects(new GrpcError(grpcCode.INTERNAL, 'Node is offline'));
    const isSyncedStub = sinon.stub(node, 'isSynced');
    isSyncedStub.returns(false);
    const pool = new Pool(poolProps);
    node._registerPool(pool);
    const replica = new Replica(props);
    pool.registerReplica(replica);

    node.once('replica', (ev) => {
      expect(ev.eventType).to.equal('del');
      expect(ev.object).to.equal(replica);
      sinon.assert.notCalled(callStub);
      setTimeout(() => {
        expect(replica.pool).to.be.undefined;
        expect(pool.replicas).to.have.lengthOf(0);
        done();
      }, 0);
    });
    replica.destroy();
  });
};
