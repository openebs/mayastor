// Unit tests for the pool object

'use strict';

const _ = require('lodash');
const expect = require('chai').expect;
const sinon = require('sinon');
const Node = require('../node');
const Pool = require('../pool');
const Replica = require('../replica');
const { shouldFailWith } = require('./utils');
const { GrpcCode, GrpcError } = require('../grpc_client');

module.exports = function() {
  let props = {
    name: 'pool',
    disks: ['/dev/sda'],
    state: 'ONLINE',
    capacity: 100,
    used: 4,
  };

  describe('should emit event upon change of volatile property', () => {
    var node, eventSpy, pool, newProps;

    beforeEach(() => {
      node = new Node('node');
      eventSpy = sinon.spy(node, 'emit');
      pool = new Pool(props);
      node._registerPool(pool);
      newProps = _.clone(props);
    });

    it('state', () => {
      newProps.state = 'DEGRADED';
      pool.merge(newProps, []);

      // First call is new-pool event upon registering the pool
      sinon.assert.calledTwice(eventSpy);
      sinon.assert.calledWith(eventSpy.lastCall, 'pool', {
        eventType: 'mod',
        object: pool,
      });
      expect(pool.state).to.equal('DEGRADED');
    });

    it('capacity', () => {
      newProps.capacity = 101;
      pool.merge(newProps, []);

      sinon.assert.calledTwice(eventSpy);
      sinon.assert.calledWith(eventSpy.lastCall, 'pool', {
        eventType: 'mod',
        object: pool,
      });
      expect(pool.capacity).to.equal(101);
    });

    it('used', () => {
      newProps.used = 99;
      pool.merge(newProps, []);

      sinon.assert.calledTwice(eventSpy);
      sinon.assert.calledWith(eventSpy.lastCall, 'pool', {
        eventType: 'mod',
        object: pool,
      });
      expect(pool.used).to.equal(99);
    });
  });

  it('should not emit event upon change of non-volatile property', () => {
    let node = new Node('node');
    let eventSpy = sinon.spy(node, 'emit');
    let pool = new Pool(props);
    node._registerPool(pool);
    let newProps = _.clone(props);
    newProps.disks = ['/dev/sdb'];
    let node2 = new Node('node2');

    pool.merge(newProps, []);

    // Create pool event is expected, but no other.
    sinon.assert.calledOnce(eventSpy);
    sinon.assert.calledWithMatch(eventSpy, 'pool', { eventType: 'new' });
  });

  it('should not emit event if nothing changed', () => {
    let node = new Node('node');
    let spy = sinon.spy(node, 'emit');
    let pool = new Pool(props);
    node._registerPool(pool);
    let newProps = _.clone(props);

    pool.merge(newProps, []);

    // Create pool event is expected, but no other.
    sinon.assert.calledOnce(spy);
    sinon.assert.calledWithMatch(spy, 'pool', { eventType: 'new' });
  });

  it('should properly merge replicas from the pool', () => {
    let node = new Node('node');
    let spy = sinon.spy(node, 'emit');
    let pool = new Pool(props);
    let modReplica = new Replica({ uuid: 'to-modify' });
    let delReplica = new Replica({ uuid: 'to-delete' });
    node._registerPool(pool);
    pool.registerReplica(modReplica);
    pool.registerReplica(delReplica);

    pool.merge(props, [{ uuid: 'to-create' }, { uuid: 'to-modify', size: 10 }]);

    expect(pool.replicas).to.have.lengthOf(2);
    // first 3 events are for pool create and initial two replicas
    expect(spy.callCount).to.equal(6);
    sinon.assert.calledWithMatch(spy.getCall(0), 'pool', { eventType: 'new' });
    sinon.assert.calledWith(spy.getCall(1), 'replica', {
      eventType: 'new',
      object: modReplica,
    });
    sinon.assert.calledWith(spy.getCall(2), 'replica', {
      eventType: 'new',
      object: delReplica,
    });
    // now come the events we want to test
    sinon.assert.calledWithMatch(spy.getCall(3), 'replica', {
      eventType: 'new',
      object: { uuid: 'to-create' },
    });
    sinon.assert.calledWith(spy.getCall(4), 'replica', {
      eventType: 'mod',
      object: modReplica,
    });
    sinon.assert.calledWith(spy.getCall(5), 'replica', {
      eventType: 'del',
      object: delReplica,
    });
  });

  it('should print the pool name with a node name', () => {
    let node = new Node('node');
    let pool = new Pool(props);
    node._registerPool(pool);
    expect(pool.toString()).to.equal('pool@node');
  });

  it('should print the pool name without node name if not bound', () => {
    let pool = new Pool(props);
    expect(pool.toString()).to.equal('pool@nowhere');
  });

  it('should bind the pool to node and then unbind it', done => {
    let node = new Node('node');
    let pool = new Pool(props);
    node.once('pool', ev => {
      expect(ev.eventType).to.equal('new');
      expect(ev.object).to.equal(pool);
      expect(pool.node).to.equal(node);

      node.once('pool', ev => {
        expect(ev.eventType).to.equal('del');
        expect(ev.object).to.equal(pool);
        setTimeout(() => {
          // jshint ignore:start
          expect(pool.node).to.be.null;
          // jshint ignore:end
          done();
        }, 0);
      });
      pool.unbind();
    });
    pool.bind(node);
  });

  it('should unregister replica from the pool', () => {
    let node = new Node('node');
    let pool = new Pool(props);
    let replica = new Replica({ uuid: 'uuid' });
    node._registerPool(pool);
    pool.registerReplica(replica);
    expect(pool.replicas).to.have.lengthOf(1);
    pool.unregisterReplica(replica);
    expect(pool.replicas).to.have.lengthOf(0);
  });

  it('should destroy the pool with replica', async () => {
    let node = new Node('node');
    let eventSpy = sinon.spy(node, 'emit');
    let stub = sinon.stub(node, 'call');
    stub.resolves({});
    let pool = new Pool(props);
    node._registerPool(pool);
    let replica = new Replica({ uuid: 'uuid' });
    pool.registerReplica(replica);

    await pool.destroy();

    sinon.assert.calledOnce(stub);
    sinon.assert.calledWithMatch(stub, 'destroyPool', { name: 'pool' });
    // jshint ignore:start
    expect(node.pools).to.be.empty;
    // jshint ignore:end
    // first two events are for the new pool and new replica
    expect(eventSpy.callCount).to.equal(4);
    sinon.assert.calledWith(eventSpy.getCall(2), 'replica', {
      eventType: 'del',
      object: replica,
    });
    sinon.assert.calledWith(eventSpy.getCall(3), 'pool', {
      eventType: 'del',
      object: pool,
    });
  });

  it('should ignore NOT_FOUND error when destroying the pool', async () => {
    let node = new Node('node');
    let stub = sinon.stub(node, 'call');
    stub.rejects({ code: 5 });
    let pool = new Pool(props);
    node._registerPool(pool);

    await pool.destroy();

    sinon.assert.calledOnce(stub);
    sinon.assert.calledWithMatch(stub, 'destroyPool', { name: 'pool' });
    // jshint ignore:start
    expect(node.pools).to.be.empty;
    // jshint ignore:end
  });

  it('should offline the pool with replica', () => {
    let node = new Node('node');
    let eventSpy = sinon.spy(node, 'emit');
    let pool = new Pool(props);
    node._registerPool(pool);
    let replica = new Replica({ uuid: 'uuid' });
    pool.registerReplica(replica);

    pool.offline();

    expect(pool.state).to.equal('OFFLINE');
    expect(pool.reason).to.equal('mayastor does not run on the node "node"');
    expect(replica.state).to.equal('OFFLINE');

    // first two events are for the new pool and new replica
    expect(eventSpy.callCount).to.equal(4);
    sinon.assert.calledWith(eventSpy.getCall(2), 'replica', {
      eventType: 'mod',
      object: replica,
    });
    sinon.assert.calledWith(eventSpy.getCall(3), 'pool', {
      eventType: 'mod',
      object: pool,
    });
  });

  it('should update state of the pool', () => {
    let node = new Node('node');
    let pool = new Pool(props);
    node._registerPool(pool);

    pool.setState('PENDING', 'reason');

    expect(pool.state).to.equal('PENDING');
    expect(pool.reason).to.equal('reason');
  });

  it('should create replica on the pool', async () => {
    let node = new Node('node');
    let stub = sinon.stub(node, 'call');
    stub.onCall(0).resolves({});
    stub.onCall(1).resolves({
      replicas: [
        {
          uuid: 'uuid',
          pool: 'pool',
          size: 100,
          thin: false,
          share: 'REPLICA_NONE',
          state: 'ONLINE',
          uri: 'bdev://blabla',
        },
      ],
    });
    let pool = new Pool(props);
    node._registerPool(pool);

    let repl = await pool.createReplica('uuid', 100);

    sinon.assert.calledTwice(stub);
    sinon.assert.calledWithMatch(stub.firstCall, 'createReplica', {
      uuid: 'uuid',
      pool: 'pool',
      size: 100,
      thin: false,
      share: 'REPLICA_NONE',
    });
    sinon.assert.calledWithMatch(stub.secondCall, 'listReplicas', {});
    expect(pool.replicas).to.have.lengthOf(1);
    expect(repl.uuid).to.equal('uuid');
    expect(repl.state).to.equal('ONLINE');
  });

  it('should throw internal error if createReplica grpc fails', async () => {
    let node = new Node('node');
    let stub = sinon.stub(node, 'call');
    stub.onCall(0).rejects(new GrpcError(GrpcCode.INTERNAL, 'Test failure'));
    stub.onCall(1).resolves({
      replicas: [
        {
          uuid: 'uuid',
          pool: 'pool',
          size: 100,
          thin: false,
          share: 'REPLICA_NONE',
          state: 'ONLINE',
          uri: 'bdev://blabla',
        },
      ],
    });
    let pool = new Pool(props);
    node._registerPool(pool);

    await shouldFailWith(GrpcCode.INTERNAL, async () => {
      await pool.createReplica('uuid', 100);
    });

    expect(pool.replicas).to.have.lengthOf(0);
    sinon.assert.calledOnce(stub);
    sinon.assert.calledWithMatch(stub, 'createReplica', {
      uuid: 'uuid',
      pool: 'pool',
      size: 100,
      thin: false,
      share: 'REPLICA_NONE',
    });
  });

  it('should throw internal error if listReplicas grpc fails', async () => {
    let node = new Node('node');
    let stub = sinon.stub(node, 'call');
    stub.onCall(0).resolves({});
    stub.onCall(1).rejects(new Error('list call failed'));
    let pool = new Pool(props);
    node._registerPool(pool);

    await shouldFailWith(GrpcCode.INTERNAL, async () => {
      await pool.createReplica('uuid', 100);
    });

    expect(pool.replicas).to.have.lengthOf(0);
    sinon.assert.calledTwice(stub);
  });

  it('should correctly indicate if pool is accessible or not', () => {
    let poolProps = _.clone(props);
    poolProps.state = 'ONLINE';
    let pool = new Pool(poolProps);
    // jshint ignore:start
    expect(pool.isAccessible()).to.be.true;
    // jshint ignore:end

    poolProps.state = 'PENDING';
    pool = new Pool(poolProps);
    // jshint ignore:start
    expect(pool.isAccessible()).to.be.false;
    // jshint ignore:end

    poolProps.state = 'DEGRADED';
    pool = new Pool(poolProps);
    // jshint ignore:start
    expect(pool.isAccessible()).to.be.true;
    // jshint ignore:end
  });

  it('should return free space in the pool', () => {
    let pool = new Pool(props);
    expect(pool.freeBytes()).to.equal(96);
  });
};
