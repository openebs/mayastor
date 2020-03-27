// Unit tests for the volume object
//
// Volume ensure tests are in volumes_test.js.

'use strict';

const expect = require('chai').expect;
const sinon = require('sinon');
const Nexus = require('../nexus');
const Node = require('../node');
const Pool = require('../pool');
const Registry = require('../registry');
const Replica = require('../replica');
const Volume = require('../volume');

const UUID = 'ba5e39e9-0c0e-4973-8a3a-0dccada09cbb';

const defaultOpts = {
  replicaCount: 1,
  preferredNodes: [],
  requiredNodes: [],
  requiredBytes: 100,
  limitBytes: 100,
};

module.exports = function() {
  it('should stringify volume name', () => {
    let registry = new Registry();
    let volume = new Volume(UUID, registry, defaultOpts);
    expect(volume.toString()).to.equal(UUID);
  });

  it('should get name of the node where the volume is accessible from', () => {
    let registry = new Registry();
    let volume = new Volume(UUID, registry, defaultOpts);
    let node = new Node('node');
    let nexus = new Nexus({ uuid: UUID });
    nexus.bind(node);
    volume.newNexus(nexus);
    expect(volume.getNodeName()).to.equal('node');
  });

  it('should get zero size of a volume that has not been created yet', () => {
    let registry = new Registry();
    let volume = new Volume(UUID, registry, defaultOpts);
    expect(volume.getSize()).to.equal(0);
  });

  it('should set the preferred nodes for the volume', () => {
    let registry = new Registry();
    let volume = new Volume(UUID, registry, defaultOpts);
    expect(volume.preferredNodes).to.have.lengthOf(0);
    let updated = volume.update({ preferredNodes: ['node1', 'node2'] });
    expect(updated).to.equal(true);
    expect(volume.preferredNodes).to.have.lengthOf(2);
  });

  it('should publish and unpublish the volume', async () => {
    let registry = new Registry();
    let volume = new Volume(UUID, registry, defaultOpts);
    let node = new Node('node');
    let nexus = new Nexus({ uuid: UUID });
    let stub = sinon.stub(node, 'call');
    nexus.bind(node);
    volume.newNexus(nexus);

    stub.resolves({ devicePath: '/dev/nbd0' });
    await volume.publish();
    expect(nexus.devicePath).to.equal('/dev/nbd0');

    stub.resolves({});
    await volume.unpublish();
    expect(nexus.devicePath).to.equal('');
  });

  it('should destroy a volume with 3 replicas', async () => {
    let registry = new Registry();
    let volume = new Volume(UUID, registry, defaultOpts);
    let node1 = new Node('node1');
    let node2 = new Node('node2');
    let node3 = new Node('node3');
    let pool1 = new Pool({ name: 'pool1', disks: [] });
    let pool2 = new Pool({ name: 'pool2', disks: [] });
    let pool3 = new Pool({ name: 'pool3', disks: [] });
    let nexus = new Nexus({ uuid: UUID });
    let replica1 = new Replica({ uuid: UUID });
    let replica2 = new Replica({ uuid: UUID });
    let replica3 = new Replica({ uuid: UUID });
    let stub1 = sinon.stub(node1, 'call');
    let stub2 = sinon.stub(node2, 'call');
    let stub3 = sinon.stub(node3, 'call');
    stub1.resolves({});
    stub2.resolves({});
    stub3.resolves({});
    node1._registerNexus(nexus);
    node1._registerPool(pool1);
    node2._registerPool(pool2);
    node3._registerPool(pool3);
    pool1.registerReplica(replica1);
    pool2.registerReplica(replica2);
    pool3.registerReplica(replica3);

    volume.newNexus(nexus);
    volume.newReplica(replica1);
    volume.newReplica(replica2);
    volume.newReplica(replica3);

    await volume.destroy();

    sinon.assert.calledTwice(stub1);
    sinon.assert.calledWith(stub1.firstCall, 'destroyNexus', { uuid: UUID });
    sinon.assert.calledWith(stub1.secondCall, 'destroyReplica', { uuid: UUID });
    sinon.assert.calledOnce(stub2);
    sinon.assert.calledOnce(stub3);
  });
};
