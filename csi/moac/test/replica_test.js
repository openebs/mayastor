// Unit tests for the replica object

'use strict';

const _ = require('lodash');
const expect = require('chai').expect;
const sinon = require('sinon');
const Node = require('../node');
const Pool = require('../pool');
const Replica = require('../replica');
const { shouldFailWith } = require('./utils');
const { GrpcCode, GrpcError } = require('../grpc_client');

const UUID = 'ba5e39e9-0c0e-4973-8a3a-0dccada09cbb';

module.exports = function() {
  var poolProps = {
    name: 'pool',
    disks: ['/dev/sda'],
    state: 'ONLINE',
    capacity: 100,
    used: 4,
  };
  var props = {
    uuid: UUID,
    pool: 'pool',
    size: 100,
    share: 'REPLICA_NONE',
    uri: 'bdev:///' + UUID,
    state: 'ONLINE',
  };

  describe('mod event', () => {
    var node, eventSpy, replica, pool, newProps;

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
        object: pool,
      });
      sinon.assert.calledWith(eventSpy.secondCall, 'replica', {
        eventType: 'new',
        object: replica,
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
        object: replica,
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
        object: replica,
      });
      expect(replica.share).to.equal('REPLICA_NVMF');
      expect(replica.uri).to.equal('nvmf://blabla');
    });

    it('should emit event upon change of state property', () => {
      newProps.state = 'DEGRADED';
      replica.merge(newProps);

      // First two events are new pool and new replica events
      sinon.assert.calledThrice(eventSpy);
      sinon.assert.calledWith(eventSpy.lastCall, 'replica', {
        eventType: 'mod',
        object: replica,
      });
      expect(replica.state).to.equal('DEGRADED');
    });
  });

  it('should bind the replica to pool and then unbind it', done => {
    let node = new Node('node');
    let pool = new Pool(poolProps);
    node._registerPool(pool);
    let replica = new Replica(props);

    node.once('replica', ev => {
      expect(ev.eventType).to.equal('new');
      expect(ev.object).to.equal(replica);
      expect(replica.pool).to.equal(pool);

      node.once('replica', ev => {
        expect(ev.eventType).to.equal('del');
        expect(ev.object).to.equal(replica);
        setTimeout(() => {
          // jshint ignore:start
          expect(replica.pool).to.be.null;
          // jshint ignore:end
          done();
        }, 0);
      });
      replica.unbind();
    });
    replica.bind(pool);
  });

  it('should offline the replica', done => {
    let node = new Node('node');
    let pool = new Pool(poolProps);
    node._registerPool(pool);
    let replica = new Replica(props);
    pool.registerReplica(replica);

    node.once('replica', ev => {
      expect(ev.eventType).to.equal('mod');
      expect(ev.object).to.equal(replica);
      expect(replica.state).to.equal('OFFLINE');
      done();
    });
    replica.offline();
  });

  it('should share the replica', async () => {
    let node = new Node('node');
    let stub = sinon.stub(node, 'call');
    stub.resolves({ uri: 'nvmf://blabla' });
    let pool = new Pool(poolProps);
    node._registerPool(pool);
    let replica = new Replica(props);
    pool.registerReplica(replica);

    let uri = await replica.setShare('REPLICA_NVMF');

    sinon.assert.calledOnce(stub);
    sinon.assert.calledWith(stub, 'shareReplica', {
      uuid: UUID,
      share: 'REPLICA_NVMF',
    });
    expect(uri).to.equal('nvmf://blabla');
    expect(replica.share).to.equal('REPLICA_NVMF');
    expect(replica.uri).to.equal('nvmf://blabla');
  });

  it('should throw if grpc fails during sharing', async () => {
    let node = new Node('node');
    let stub = sinon.stub(node, 'call');
    stub.rejects(new GrpcError(GrpcCode.INTERNAL, 'Test failure'));
    let pool = new Pool(poolProps);
    node._registerPool(pool);
    let replica = new Replica(props);
    pool.registerReplica(replica);

    await shouldFailWith(GrpcCode.INTERNAL, async () => {
      await replica.setShare('REPLICA_NVMF');
    });
    expect(replica.share).to.equal('REPLICA_NONE');
  });

  it('should destroy the replica', done => {
    let node = new Node('node');
    let stub = sinon.stub(node, 'call');
    stub.resolves({});
    let pool = new Pool(poolProps);
    node._registerPool(pool);
    let replica = new Replica(props);
    pool.registerReplica(replica);

    node.once('replica', ev => {
      expect(ev.eventType).to.equal('del');
      expect(ev.object).to.equal(replica);
      sinon.assert.calledOnce(stub);
      sinon.assert.calledWith(stub, 'destroyReplica', { uuid: UUID });
      setTimeout(() => {
        // jshint ignore:start
        expect(replica.pool).to.be.null;
        // jshint ignore:end
        expect(pool.replicas).to.have.lengthOf(0);
        done();
      }, 0);
    });
    replica.destroy();
  });

  it('should not remove the replica if grpc fails', async () => {
    let node = new Node('node');
    let eventSpy = sinon.spy(node, 'emit');
    let stub = sinon.stub(node, 'call');
    stub.rejects(new GrpcError(GrpcCode.INTERNAL, 'Test failure'));
    let pool = new Pool(poolProps);
    node._registerPool(pool);
    let replica = new Replica(props);
    pool.registerReplica(replica);

    await shouldFailWith(GrpcCode.INTERNAL, async () => {
      await replica.destroy();
    });

    sinon.assert.calledOnce(stub);
    sinon.assert.calledWith(stub, 'destroyReplica', { uuid: UUID });
    // it is called when creating the pool and replica
    sinon.assert.calledTwice(eventSpy);
    sinon.assert.calledWith(eventSpy.firstCall, 'pool', {
      eventType: 'new',
      object: pool,
    });
    sinon.assert.calledWith(eventSpy.secondCall, 'replica', {
      eventType: 'new',
      object: replica,
    });
    expect(replica.pool).to.equal(pool);
    expect(pool.replicas).to.have.lengthOf(1);
  });

  it('should ignore NOT_FOUND error when destroying the replica', done => {
    let node = new Node('node');
    let stub = sinon.stub(node, 'call');
    stub.rejects(new GrpcError(GrpcCode.NOT_FOUND, 'not found test failure'));
    let pool = new Pool(poolProps);
    node._registerPool(pool);
    let replica = new Replica(props);
    pool.registerReplica(replica);

    node.once('replica', ev => {
      expect(ev.eventType).to.equal('del');
      expect(ev.object).to.equal(replica);
      sinon.assert.calledOnce(stub);
      sinon.assert.calledWith(stub, 'destroyReplica', { uuid: UUID });
      setTimeout(() => {
        // jshint ignore:start
        expect(replica.pool).to.be.null;
        // jshint ignore:end
        expect(pool.replicas).to.have.lengthOf(0);
        done();
      }, 0);
    });
    replica.destroy();
  });
};
