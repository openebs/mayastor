// Unit tests for the pool object

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

module.exports = function () {
  const props = {
    name: 'pool',
    disks: ['io_uring:///dev/sda'],
    state: 'POOL_ONLINE',
    capacity: 100,
    used: 4
  };

  describe('should emit event upon change of volatile property', () => {
    let node, eventSpy, pool, newProps;

    beforeEach(() => {
      node = new Node('node');
      eventSpy = sinon.spy(node, 'emit');
      pool = new Pool(props);
      node._registerPool(pool);
      newProps = _.clone(props);
    });

    it('state', () => {
      newProps.state = 'POOL_DEGRADED';
      pool.merge(newProps, []);

      // First call is new-pool event upon registering the pool
      sinon.assert.calledTwice(eventSpy);
      sinon.assert.calledWith(eventSpy.lastCall, 'pool', {
        eventType: 'mod',
        object: pool
      });
      expect(pool.state).to.equal('POOL_DEGRADED');
    });

    it('capacity', () => {
      newProps.capacity = 101;
      pool.merge(newProps, []);

      sinon.assert.calledTwice(eventSpy);
      sinon.assert.calledWith(eventSpy.lastCall, 'pool', {
        eventType: 'mod',
        object: pool
      });
      expect(pool.capacity).to.equal(101);
    });

    it('used', () => {
      newProps.used = 99;
      pool.merge(newProps, []);

      sinon.assert.calledTwice(eventSpy);
      sinon.assert.calledWith(eventSpy.lastCall, 'pool', {
        eventType: 'mod',
        object: pool
      });
      expect(pool.used).to.equal(99);
    });

    it('disk protocol', () => {
      newProps.disks = ['aio:///dev/sda'];
      pool.merge(newProps, []);

      sinon.assert.calledTwice(eventSpy);
      sinon.assert.calledWith(eventSpy.lastCall, 'pool', {
        eventType: 'mod',
        object: pool
      });
      expect(pool.disks[0]).to.equal('aio:///dev/sda');
    });

    it('disk device', () => {
      newProps.disks = ['aio:///dev/sdb'];
      pool.merge(newProps, []);

      sinon.assert.calledTwice(eventSpy);
      sinon.assert.calledWith(eventSpy.lastCall, 'pool', {
        eventType: 'mod',
        object: pool
      });
      expect(pool.disks[0]).to.equal('aio:///dev/sdb');
    });
  });

  it('should not emit event if nothing changed', () => {
    const node = new Node('node');
    const spy = sinon.spy(node, 'emit');
    const pool = new Pool(props);
    node._registerPool(pool);
    const newProps = _.clone(props);

    pool.merge(newProps, []);

    // Create pool event is expected, but no other.
    sinon.assert.calledOnce(spy);
    sinon.assert.calledWithMatch(spy, 'pool', { eventType: 'new' });
  });

  it('should properly merge replicas from the pool', () => {
    const node = new Node('node');
    const spy = sinon.spy(node, 'emit');
    const pool = new Pool(props);
    const modReplica = new Replica({ uuid: 'to-modify', uri: 'bdev:///to-modify?uuid=1' });
    const delReplica = new Replica({ uuid: 'to-delete', uri: 'bdev:///to-delete?uuid=2' });
    node._registerPool(pool);
    pool.registerReplica(modReplica);
    pool.registerReplica(delReplica);

    pool.merge(props, [{ uuid: 'to-create', uri: 'bdev:///to-create?uuid=3' }, { uuid: 'to-modify', uri: 'bdev:///to-modify?uuid=1', size: 10 }]);

    expect(pool.replicas).to.have.lengthOf(2);
    // first 3 events are for pool create and initial two replicas
    expect(spy.callCount).to.equal(6);
    sinon.assert.calledWithMatch(spy.getCall(0), 'pool', { eventType: 'new' });
    sinon.assert.calledWith(spy.getCall(1), 'replica', {
      eventType: 'new',
      object: modReplica
    });
    sinon.assert.calledWith(spy.getCall(2), 'replica', {
      eventType: 'new',
      object: delReplica
    });
    // now come the events we want to test
    sinon.assert.calledWithMatch(spy.getCall(3), 'replica', {
      eventType: 'new',
      object: { uuid: 'to-create' }
    });
    sinon.assert.calledWith(spy.getCall(4), 'replica', {
      eventType: 'mod',
      object: modReplica
    });
    sinon.assert.calledWith(spy.getCall(5), 'replica', {
      eventType: 'del',
      object: delReplica
    });
  });

  it('should print the pool name with a node name', () => {
    const node = new Node('node');
    const pool = new Pool(props);
    node._registerPool(pool);
    expect(pool.toString()).to.equal('pool@node');
  });

  it('should print the pool name without node name if not bound', () => {
    const pool = new Pool(props);
    expect(pool.toString()).to.equal('pool@nowhere');
  });

  it('should bind the pool to node and then unbind it', (done) => {
    const node = new Node('node');
    const pool = new Pool(props);
    node.once('pool', (ev) => {
      expect(ev.eventType).to.equal('new');
      expect(ev.object).to.equal(pool);
      expect(pool.node).to.equal(node);

      node.once('pool', (ev) => {
        expect(ev.eventType).to.equal('del');
        expect(ev.object).to.equal(pool);
        setTimeout(() => {
          expect(pool.node).to.be.undefined;
          done();
        }, 0);
      });
      pool.unbind();
    });
    pool.bind(node);
  });

  it('should unregister replica from the pool', () => {
    const node = new Node('node');
    const pool = new Pool(props);
    const replica = new Replica({ uuid: 'uuid', uri: 'bdev:///uuid?uuid=1' });
    node._registerPool(pool);
    pool.registerReplica(replica);
    expect(pool.replicas).to.have.lengthOf(1);
    pool.unregisterReplica(replica);
    expect(pool.replicas).to.have.lengthOf(0);
  });

  it('should destroy the pool with replica', async () => {
    const node = new Node('node');
    const eventSpy = sinon.spy(node, 'emit');
    const stub = sinon.stub(node, 'call');
    stub.resolves({});
    const pool = new Pool(props);
    node._registerPool(pool);
    const replica = new Replica({ uuid: 'uuid', uri: 'bdev:///uuid?uuid=1' });
    pool.registerReplica(replica);

    await pool.destroy();

    sinon.assert.calledOnce(stub);
    sinon.assert.calledWithMatch(stub, 'destroyPool', { name: 'pool' });
    expect(node.pools).to.be.empty;
    // first two events are for the new pool and new replica
    expect(eventSpy.callCount).to.equal(4);
    sinon.assert.calledWith(eventSpy.getCall(2), 'replica', {
      eventType: 'del',
      object: replica
    });
    sinon.assert.calledWith(eventSpy.getCall(3), 'pool', {
      eventType: 'del',
      object: pool
    });
  });

  it('should offline the pool with replica', () => {
    const node = new Node('node');
    const eventSpy = sinon.spy(node, 'emit');
    const pool = new Pool(props);
    node._registerPool(pool);
    const replica = new Replica({ uuid: 'uuid', uri: 'bdev:///uuid?uuid=1' });
    pool.registerReplica(replica);

    pool.offline();

    expect(pool.state).to.equal('POOL_OFFLINE');
    expect(replica.isOffline()).to.be.true;

    // first two events are for the new pool and new replica
    expect(eventSpy.callCount).to.equal(4);
    sinon.assert.calledWith(eventSpy.getCall(2), 'replica', {
      eventType: 'mod',
      object: replica
    });
    sinon.assert.calledWith(eventSpy.getCall(3), 'pool', {
      eventType: 'mod',
      object: pool
    });
  });

  it('should create replica on the pool', async () => {
    const node = new Node('node');
    const stub = sinon.stub(node, 'call');
    stub.resolves({
      uuid: 'uuid',
      pool: 'pool',
      size: 100,
      thin: false,
      share: 'REPLICA_NONE',
      uri: 'bdev://blabla?uuid=blabla'
    });
    const pool = new Pool(props);
    node._registerPool(pool);

    const repl = await pool.createReplica('uuid', 100);

    sinon.assert.calledOnce(stub);
    sinon.assert.calledWithMatch(stub, 'createReplica', {
      uuid: 'uuid',
      pool: 'pool',
      size: 100,
      thin: false,
      share: 'REPLICA_NONE'
    });
    expect(pool.replicas).to.have.lengthOf(1);
    expect(repl.uuid).to.equal('uuid');
  });

  it('should throw internal error if createReplica grpc fails', async () => {
    const node = new Node('node');
    const stub = sinon.stub(node, 'call');
    stub.rejects(new GrpcError(grpcCode.INTERNAL, 'Test failure'));
    const pool = new Pool(props);
    node._registerPool(pool);

    await shouldFailWith(grpcCode.INTERNAL, async () => {
      await pool.createReplica('uuid', 100);
    });

    expect(pool.replicas).to.have.lengthOf(0);
    sinon.assert.calledOnce(stub);
    sinon.assert.calledWithMatch(stub, 'createReplica', {
      uuid: 'uuid',
      pool: 'pool',
      size: 100,
      thin: false,
      share: 'REPLICA_NONE'
    });
  });

  it('should correctly indicate if pool is accessible or not', () => {
    const poolProps = _.clone(props);
    poolProps.state = 'POOL_ONLINE';
    let pool = new Pool(poolProps);
    expect(pool.isAccessible()).to.be.true;

    poolProps.state = 'POOL_FAULTED';
    pool = new Pool(poolProps);
    expect(pool.isAccessible()).to.be.false;

    poolProps.state = 'POOL_DEGRADED';
    pool = new Pool(poolProps);
    expect(pool.isAccessible()).to.be.true;
  });

  it('should return free space in the pool', () => {
    const pool = new Pool(props);
    expect(pool.freeBytes()).to.equal(96);
  });
};
