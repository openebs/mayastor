// Unit tests for the volumes operator

'use strict';

const assert = require('chai').assert;
const EventEmitter = require('events');
const grpc = require('grpc-uds');
const sleep = require('sleep-promise');
const { MayastorServer, STAT_DELTA } = require('./mayastor_mock');
const { NodeOperatorMock } = require('./nodes');
const volumesMod = require('./volumes');
const { shouldFailWith, waitUntil } = require('./test_utils');

const EGRESS_ENDPOINT = '127.0.0.1:1235';
const VolumeOperator = volumesMod.VolumeOperator;

const UUID = 'ba5e39e9-0c0e-4973-8a3a-0dccada09cbb';

function startMayastorServer(pools, replicas, nexus) {
  return new MayastorServer(EGRESS_ENDPOINT, pools, replicas, nexus).start();
}

function mockedVolumeOperator(nodeOperator) {
  let volumeOperator = new VolumeOperator(nodeOperator);
}

module.exports = function() {
  var mayastorSrv;
  var volumeOperator;

  afterEach(async () => {
    if (volumeOperator) {
      await volumeOperator.stop();
      volumeOperator = null;
    }
    if (mayastorSrv) {
      mayastorSrv.stop();
      mayastorSrv = null;
    }
  });

  it('should create a nexus with replica', async () => {
    mayastorSrv = startMayastorServer([
      {
        name: 'pool',
        disks: ['/dev/sda'],
        state: 0,
        capacity: 100,
        used: 50,
      },
    ]);
    let nodeOperator = new NodeOperatorMock([
      {
        node: 'node',
        endpoint: EGRESS_ENDPOINT,
      },
    ]);
    volumeOperator = new VolumeOperator(nodeOperator);
    await volumeOperator.createReplica('node', 'pool', UUID, 10);

    let repls = volumeOperator.getReplica();
    assert.lengthOf(repls, 1);
    assert.equal(repls[0].uuid, UUID);
    assert.equal(repls[0].size, 10);
    assert.equal(repls[0].node, 'node');
    assert.equal(repls[0].pool, 'pool');

    await volumeOperator.createNexus('node', UUID, 10, ['bdev:///' + UUID]);

    let nexus = volumeOperator.getNexus();
    assert.lengthOf(nexus, 1);
    assert.equal(nexus[0].uuid, UUID);
    assert.equal(nexus[0].size, 10);
    assert.equal(nexus[0].node, 'node');
    assert.lengthOf(nexus[0].children, 1);
    assert.equal(nexus[0].children[0], 'bdev:///' + UUID);
  });

  it('should create neither replica nor nexus if grpc fails', async () => {
    let nodeOperator = new NodeOperatorMock([
      {
        node: 'node',
        endpoint: EGRESS_ENDPOINT,
      },
    ]);
    volumeOperator = new VolumeOperator(nodeOperator);
    await shouldFailWith(grpc.status.INTERNAL, () =>
      volumeOperator.createReplica('node', 'pool', UUID, 10)
    );
    await shouldFailWith(grpc.status.INTERNAL, () =>
      volumeOperator.createNexus('node', UUID, 10, ['bdev:///' + UUID])
    );
  });

  it('should publish and unpublish the nexus', async () => {
    mayastorSrv = startMayastorServer(
      [],
      [],
      [
        {
          uuid: UUID,
          size: 10,
          state: 'online',
          children: [
            {
              uri: 'bdev:///' + UUID,
              state: 'offline',
            },
          ],
        },
      ]
    );
    let nodeOperator = new NodeOperatorMock([
      {
        node: 'node',
        endpoint: EGRESS_ENDPOINT,
      },
    ]);
    volumeOperator = new VolumeOperator(nodeOperator);
    await volumeOperator.start();

    let devicePath = await volumeOperator.publishNexus(UUID);
    assert.equal(devicePath, '/dev/nbd0');
    let n = volumeOperator.getNexus()[0];
    assert.equal(n.devicePath, '/dev/nbd0');

    await volumeOperator.unpublishNexus(UUID);
    assert.isNull(n.devicePath);
  });

  it('should not publish nexus if grpc fails', async () => {
    let nodeOperator = new NodeOperatorMock([
      {
        node: 'node',
        endpoint: EGRESS_ENDPOINT,
      },
    ]);
    volumeOperator = new VolumeOperator(nodeOperator);
    await volumeOperator.start();

    await shouldFailWith(grpc.status.INTERNAL, () =>
      volumeOperator.publishNexus(UUID)
    );
  });

  it('should destroy the nexus and replica', async () => {
    mayastorSrv = startMayastorServer(
      [
        {
          name: 'pool',
          disks: ['/dev/sda'],
          state: 0,
          capacity: 100,
          used: 50,
        },
      ],
      [
        {
          uuid: UUID,
          pool: 'pool',
          size: 10,
          thin: false,
        },
      ],
      [
        {
          uuid: UUID,
          size: 10,
          state: 'online',
          children: [
            {
              uri: 'bdev:///' + UUID,
              state: 'online',
            },
          ],
        },
      ]
    );
    let nodeOperator = new NodeOperatorMock([
      {
        node: 'node',
        endpoint: EGRESS_ENDPOINT,
      },
    ]);
    volumeOperator = new VolumeOperator(nodeOperator);
    await volumeOperator.start();
    assert.lengthOf(volumeOperator.getReplica(), 1);
    await volumeOperator.destroyReplica('node', UUID);
    assert.lengthOf(volumeOperator.getReplica(), 0);
    assert.lengthOf(volumeOperator.getNexus(), 1);
    await volumeOperator.destroyNexus('node', UUID);
    assert.lengthOf(volumeOperator.getNexus(), 0);
  });

  it('should destroy neither replica nor nexus if grpc fails', async () => {
    let nodeOperator = new NodeOperatorMock([
      {
        node: 'node',
        endpoint: EGRESS_ENDPOINT,
      },
    ]);
    volumeOperator = new VolumeOperator(nodeOperator);
    await shouldFailWith(grpc.status.INTERNAL, () =>
      volumeOperator.destroyReplica('node', UUID)
    );
    await shouldFailWith(grpc.status.INTERNAL, () =>
      volumeOperator.destroyNexus('node', UUID)
    );
  });

  it('should stat replicas even if one of grpc call fails', async () => {
    mayastorSrv = startMayastorServer(
      [
        {
          name: 'pool',
          disks: ['/dev/sda'],
          state: 0,
          capacity: 100,
          used: 50,
        },
      ],
      [
        {
          uuid: UUID,
          pool: 'pool',
          size: 10,
          thin: false,
        },
      ]
    );
    let nodeOperator = new NodeOperatorMock([
      {
        node: 'node',
        endpoint: EGRESS_ENDPOINT,
      },
      {
        node: 'unreachable-node',
        endpoint: '127.0.0.1:12358',
      },
    ]);
    volumeOperator = new VolumeOperator(nodeOperator);
    await volumeOperator.start();

    let stats = await volumeOperator.getStats();
    assert.lengthOf(stats, 1);
    assert.equal(stats[0].volume, UUID);
    assert.equal(stats[0].pool, 'pool');
    ['num_read_ops', 'num_write_ops', 'bytes_read', 'bytes_written'].forEach(
      name => {
        assert.equal(stats[0].stats[name], STAT_DELTA);
      }
    );
    stats = await volumeOperator.getStats();
    assert.lengthOf(stats, 1);
    assert.equal(stats[0].volume, UUID);
    assert.equal(stats[0].pool, 'pool');
    ['num_read_ops', 'num_write_ops', 'bytes_read', 'bytes_written'].forEach(
      name => {
        assert.equal(stats[0].stats[name], 2 * STAT_DELTA);
      }
    );
  });

  it('should sync volumes if new storage node is added', async () => {
    mayastorSrv = startMayastorServer(
      [
        {
          name: 'pool',
          disks: ['/dev/sda'],
          state: 0,
          capacity: 100,
          used: 50,
        },
      ],
      [
        {
          uuid: UUID,
          pool: 'pool',
          size: 10,
          thin: false,
        },
      ],
      [
        {
          uuid: UUID,
          size: 10,
          state: 'online',
          children: [
            {
              uri: 'bdev:///' + UUID,
              state: 'online',
            },
          ],
        },
      ]
    );
    let nodeOperator = new NodeOperatorMock();
    volumeOperator = new VolumeOperator(nodeOperator);
    await volumeOperator.start();
    nodeOperator.addNode('node', EGRESS_ENDPOINT);

    await waitUntil(
      () => volumeOperator.getReplica().length == 1,
      1500,
      'replica'
    );
    let r = volumeOperator.getReplica()[0];
    assert.equal(r.uuid, UUID);
    assert.equal(r.node, 'node');
    assert.equal(r.pool, 'pool');
    assert.equal(r.size, 10);

    await waitUntil(() => volumeOperator.getNexus().length == 1, 1500, 'nexus');
    let n = volumeOperator.getNexus()[0];
    assert.equal(n.uuid, UUID);
    assert.equal(n.node, 'node');
    assert.equal(n.size, 10);
    assert.equal(n.state, 'online');
    assert.lengthOf(n.children, 1);
    assert.equal(n.children[0], 'bdev:///' + UUID);
  });

  it('should sync volumes upon start', async () => {
    mayastorSrv = startMayastorServer(
      [
        {
          name: 'pool',
          disks: ['/dev/sda'],
          state: 0,
          capacity: 100,
          used: 50,
        },
      ],
      [
        {
          uuid: UUID,
          pool: 'pool',
          size: 10,
          thin: false,
        },
      ],
      [
        {
          uuid: UUID,
          size: 10,
          state: 'online',
          children: [
            {
              uri: 'bdev:///' + UUID,
              state: 'online',
            },
          ],
        },
      ]
    );
    let nodeOperator = new NodeOperatorMock([
      {
        node: 'node',
        endpoint: EGRESS_ENDPOINT,
      },
    ]);
    volumeOperator = new VolumeOperator(nodeOperator);
    await volumeOperator.start();

    let repls = volumeOperator.getReplica();
    assert.lengthOf(repls, 1);
    assert.equal(repls[0].uuid, UUID);
    assert.equal(repls[0].node, 'node');
    assert.equal(repls[0].pool, 'pool');
    assert.equal(repls[0].size, 10);

    let nexus = volumeOperator.getNexus();
    assert.lengthOf(nexus, 1);
    assert.equal(nexus[0].uuid, UUID);
    assert.equal(nexus[0].node, 'node');
    assert.equal(nexus[0].size, 10);
    assert.equal(nexus[0].state, 'online');
    assert.lengthOf(nexus[0].children, 1);
    assert.equal(nexus[0].children[0], 'bdev:///' + UUID);
  });

  it('should retry sync of volumes after failure', async () => {
    // change retry interval to 1s not to wait so long
    volumesMod.retrySyncInterval = 1000;

    let nodeOperator = new NodeOperatorMock([
      {
        node: 'node',
        endpoint: EGRESS_ENDPOINT,
      },
    ]);
    volumeOperator = new VolumeOperator(nodeOperator);
    await volumeOperator.start();
    assert.lengthOf(volumeOperator.getReplica(), 0);
    assert.lengthOf(volumeOperator.getNexus(), 0);

    mayastorSrv = startMayastorServer(
      [
        {
          name: 'pool',
          disks: ['/dev/sda'],
          state: 0,
          capacity: 100,
          used: 50,
        },
      ],
      [
        {
          uuid: UUID,
          pool: 'pool',
          size: 10,
          thin: false,
        },
      ],
      [
        {
          uuid: UUID,
          size: 10,
          state: 'online',
          children: [
            {
              uri: 'bdev:///' + UUID,
              state: 'online',
            },
          ],
        },
      ]
    );
    await waitUntil(
      () => volumeOperator.getReplica().length == 1,
      1500,
      'replica'
    );
    let r = volumeOperator.getReplica()[0];
    assert.equal(r.uuid, UUID);
    assert.equal(r.node, 'node');
    assert.equal(r.pool, 'pool');
    assert.equal(r.size, 10);

    await waitUntil(() => volumeOperator.getNexus().length == 1, 1500, 'nexus');
    let n = volumeOperator.getNexus()[0];
    assert.equal(n.uuid, UUID);
    assert.equal(n.node, 'node');
    assert.equal(n.size, 10);
    assert.equal(n.state, 'online');
    assert.lengthOf(n.children, 1);
    assert.equal(n.children[0], 'bdev:///' + UUID);
  });
};
