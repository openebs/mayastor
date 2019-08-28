// Unit tests for the volumes operator

'use strict';

const assert = require('chai').assert;
const EventEmitter = require('events');
const grpc = require('grpc-uds');
const sleep = require('sleep-promise');
const { MayastorServer, STAT_DELTA } = require('./mayastor_mock');
const { NodeOperatorMock } = require('./nodes');
const volumesMod = require('./volumes');
const { waitUntil } = require('./test_utils');

const EGRESS_ENDPOINT = '127.0.0.1:1235';
const VolumeOperator = volumesMod.VolumeOperator;

const UUID = 'ba5e39e9-0c0e-4973-8a3a-0dccada09cbb';

function startMayastorServer(pools, replicas) {
  return new MayastorServer(EGRESS_ENDPOINT, pools, replicas).start();
}

function mockedVolumeOperator(nodeOperator) {
  let volumeOperator = new VolumeOperator(nodeOperator);
}

// Check that the test callback which should return a future fails with
// given grpc error code.
async function shouldFailWith(code, test) {
  try {
    await test();
  } catch (err) {
    if (err.code != code) {
      throw err;
    }
    return;
  }
  throw new Error('Expected error');
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

  it('should create the volume', async () => {
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
    await volumeOperator.create('node', 'pool', UUID, 10);

    let vols = volumeOperator.snapshot();
    assert.lengthOf(vols, 1);
    assert.equal(vols[0].volumeId, UUID);
    assert.equal(vols[0].capacityBytes, 10);
    assert.equal(
      vols[0].accessibleTopology[0].segments['kubernetes.io/hostname'],
      'node'
    );
  });

  it('should not create volume if grpc fails', async () => {
    let nodeOperator = new NodeOperatorMock([
      {
        node: 'node',
        endpoint: EGRESS_ENDPOINT,
      },
    ]);
    volumeOperator = new VolumeOperator(nodeOperator);
    await shouldFailWith(grpc.status.INTERNAL, () =>
      volumeOperator.create('node', 'pool', UUID, 10)
    );
  });

  it('should destroy the volume', async () => {
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
    ]);
    volumeOperator = new VolumeOperator(nodeOperator);
    await volumeOperator.start();
    assert.lengthOf(volumeOperator.snapshot(), 1);
    await volumeOperator.destroy('node', UUID);
    assert.lengthOf(volumeOperator.snapshot(), 0);
  });

  it('should not destroy volume if grpc fails', async () => {
    let nodeOperator = new NodeOperatorMock([
      {
        node: 'node',
        endpoint: EGRESS_ENDPOINT,
      },
    ]);
    volumeOperator = new VolumeOperator(nodeOperator);
    await shouldFailWith(grpc.status.INTERNAL, () =>
      volumeOperator.destroy('node', UUID)
    );
  });

  it('should stat volumes even if one of grpc call fails', async () => {
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
      ]
    );
    let nodeOperator = new NodeOperatorMock();
    volumeOperator = new VolumeOperator(nodeOperator);
    await volumeOperator.start();
    let promise = waitUntil(
      () => volumeOperator.snapshot().length == 1,
      1500,
      'volume'
    );
    nodeOperator.addNode('node', EGRESS_ENDPOINT);
    await promise;
    let vol = volumeOperator.snapshot()[0];
    assert.equal(vol.volumeId, UUID);
    assert.equal(vol.capacityBytes, 10);
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
    let vols = volumeOperator.snapshot();
    assert.lengthOf(vols, 1);
    assert.equal(vols[0].volumeId, UUID);
    assert.equal(vols[0].capacityBytes, 10);
    assert.equal(
      vols[0].accessibleTopology[0].segments['kubernetes.io/hostname'],
      'node'
    );
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
    assert.lengthOf(volumeOperator.snapshot(), 0);

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
    await waitUntil(
      () => volumeOperator.snapshot().length == 1,
      1500,
      'volume'
    );
    let vol = volumeOperator.snapshot()[0];
    assert.equal(vol.volumeId, UUID);
    assert.equal(vol.capacityBytes, 10);
  });
};
