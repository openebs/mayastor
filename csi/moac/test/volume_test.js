// Unit tests for the volume object
//
// The tests for more complex volume methods are in volumes_test.js mainly
// because volumes.js takes care of routing registry events to the volume
// and it makes sense to test this together.

'use strict';

const EventEmitter = require('events');
const expect = require('chai').expect;
const sinon = require('sinon');
const { Nexus } = require('../nexus');
const { Node } = require('../node');
const { Pool } = require('../pool');
const Registry = require('../registry');
const { Replica } = require('../replica');
const { Volume } = require('../volume');
const { shouldFailWith } = require('./utils');
const { GrpcCode } = require('../grpc_client');

const UUID = 'ba5e39e9-0c0e-4973-8a3a-0dccada09cbb';

const defaultOpts = {
  replicaCount: 1,
  preferredNodes: [],
  requiredNodes: [],
  requiredBytes: 100,
  limitBytes: 100
};

// Repeating code that is extracted to a function.
function createFakeVolume (state) {
  const registry = new Registry();
  const volume = new Volume(UUID, registry, new EventEmitter(), defaultOpts, state, 100);
  const fsaStub = sinon.stub(volume, '_fsa');
  fsaStub.resolves();
  const node = new Node('node');
  const replica = new Replica({ uuid: UUID, size: 100, share: 'REPLICA_NONE', uri: `bdev:///${UUID}` });
  const pool = new Pool({ name: 'pool', disks: [] });
  pool.bind(node);
  replica.bind(pool);
  volume.newReplica(replica);
  return [volume, node];
}

module.exports = function () {
  it('should stringify volume name', () => {
    const registry = new Registry();
    const volume = new Volume(UUID, registry, new EventEmitter(), defaultOpts);
    expect(volume.toString()).to.equal(UUID);
  });

  it('should get name of the node where the volume has been published', () => {
    const registry = new Registry();
    const volume = new Volume(UUID, registry, new EventEmitter(), defaultOpts, 'degraded', 100, 'node');
    expect(volume.getNodeName()).to.equal('node');
    expect(volume.state).to.equal('degraded');
  });

  it('should get zero size of a volume that has not been created yet', () => {
    const registry = new Registry();
    const volume = new Volume(UUID, registry, new EventEmitter(), defaultOpts);
    expect(volume.getSize()).to.equal(0);
  });

  it('should get the right size of a volume that has been imported', () => {
    const registry = new Registry();
    const volume = new Volume(UUID, registry, new EventEmitter(), defaultOpts, 'healthy', 100);
    expect(volume.getSize()).to.equal(100);
    expect(volume.state).to.equal('healthy');
  });

  it('should set the preferred nodes for the volume', () => {
    let modified = false;
    const registry = new Registry();
    const emitter = new EventEmitter();
    emitter.on('volume', (ev) => {
      if (ev.eventType === 'mod') {
        modified = true;
      }
    });
    const volume = new Volume(UUID, registry, emitter, defaultOpts);
    expect(volume.preferredNodes).to.have.lengthOf(0);
    volume.update({ preferredNodes: ['node1', 'node2'] });
    expect(modified).to.equal(true);
    expect(volume.preferredNodes).to.have.lengthOf(2);
  });

  it('should not publish volume that is known to be broken', async () => {
    const registry = new Registry();
    const volume = new Volume(UUID, registry, new EventEmitter(), defaultOpts, 'faulted', 100);
    const node = new Node('node');
    const stub = sinon.stub(node, 'call');
    stub.onCall(0).resolves({});
    stub.onCall(1).resolves({ deviceUri: 'nvmf://host/nqn' });

    shouldFailWith(GrpcCode.INTERNAL, async () => {
      await volume.publish('nvmf');
    });
    sinon.assert.notCalled(stub);
  });
};
