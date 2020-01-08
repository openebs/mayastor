// Unit tests for the volumes operator

'use strict';

const assert = require('chai').assert;
const grpc = require('grpc-uds');
const { MayastorServer, STAT_DELTA } = require('./mayastor_mock');
const { NodeOperatorMock } = require('./nodes');
const volumesMod = require('./volumes');
const { shouldFailWith, waitUntil } = require('./test_utils');

const MS_ENDPOINT1 = '127.0.0.1:1235';
const MS_ENDPOINT2 = '127.0.0.1:1236';
const VolumeOperator = volumesMod.VolumeOperator;

const UUID = 'ba5e39e9-0c0e-4973-8a3a-0dccada09cbb';

function mockedVolumeOperator(nodeOperator) {
  let volumeOperator = new VolumeOperator(nodeOperator);
}

module.exports = function() {
  var mayastorSrvs = [];
  var volumeOperator;

  function startMayastorServer(endpoint, pools, replicas, nexus) {
    let s = new MayastorServer(endpoint, pools, replicas, nexus).start();
    mayastorSrvs.push(s);
  }

  function stopMayastorServers() {
    mayastorSrvs.forEach(s => s.stop());
    mayastorSrvs = [];
  }

  afterEach(async () => {
    if (volumeOperator) {
      await volumeOperator.stop();
      volumeOperator = null;
    }
    stopMayastorServers();
  });

  it('should create a nexus with two replicas', async () => {
    startMayastorServer(MS_ENDPOINT1, [
      {
        name: 'pool',
        disks: ['/dev/sda'],
        state: 0,
        capacity: 100,
        used: 50,
      },
    ]);
    startMayastorServer(MS_ENDPOINT2, [
      {
        name: 'other-pool',
        disks: ['/dev/sda'],
        state: 0,
        capacity: 100,
        used: 50,
      },
    ]);
    let nodeOperator = new NodeOperatorMock([
      {
        node: 'node',
        endpoint: MS_ENDPOINT1,
      },
      {
        node: 'other-node',
        endpoint: MS_ENDPOINT2,
      },
    ]);
    volumeOperator = new VolumeOperator(nodeOperator);
    await volumeOperator.createReplica('node', 'pool', UUID, 10);
    await volumeOperator.createReplica('other-node', 'other-pool', UUID, 20);
    await volumeOperator.shareReplica('other-node', UUID, 'NVMF');

    let repls = volumeOperator.getReplicaSet(UUID);
    assert.lengthOf(repls, 2);
    assert.equal(repls[0].uuid, UUID);
    assert.equal(repls[0].size, 10);
    assert.equal(repls[0].node, 'node');
    assert.equal(repls[0].pool, 'pool');
    assert.equal(repls[0].share, 'NONE');
    assert.match(repls[0].uri, /^bdev:\/\/\//);
    assert.equal(repls[1].uuid, UUID);
    assert.equal(repls[1].size, 20);
    assert.equal(repls[1].node, 'other-node');
    assert.equal(repls[1].pool, 'other-pool');
    assert.equal(repls[1].share, 'NVMF');
    assert.match(repls[1].uri, /^nvmf:\/\//);

    await volumeOperator.createNexus('node', UUID, 10, [
      'bdev:///' + UUID,
      'nvmf://192.168.0.1:4444/' + UUID,
    ]);

    let nexus = volumeOperator.getNexus();
    assert.lengthOf(nexus, 1);
    assert.equal(nexus[0].uuid, UUID);
    assert.equal(nexus[0].size, 10);
    assert.equal(nexus[0].node, 'node');
    assert.lengthOf(nexus[0].children, 2);
    assert.equal(nexus[0].children[0], 'bdev:///' + UUID);
    assert.equal(nexus[0].children[1], 'nvmf://192.168.0.1:4444/' + UUID);
  });

  it('should create neither replica nor nexus if grpc fails', async () => {
    let nodeOperator = new NodeOperatorMock([
      {
        node: 'node',
        endpoint: MS_ENDPOINT1,
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
    startMayastorServer(
      MS_ENDPOINT1,
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
        endpoint: MS_ENDPOINT1,
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
    startMayastorServer(
      MS_ENDPOINT1,
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
        endpoint: MS_ENDPOINT1,
      },
    ]);
    volumeOperator = new VolumeOperator(nodeOperator);
    await volumeOperator.start();
    stopMayastorServers();

    await shouldFailWith(grpc.status.INTERNAL, () =>
      volumeOperator.publishNexus(UUID)
    );
  });

  it('should destroy the nexus and a replica', async () => {
    startMayastorServer(
      MS_ENDPOINT1,
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
          share: 'NONE',
          uri: 'bdev:///' + UUID,
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
    startMayastorServer(
      MS_ENDPOINT2,
      [
        {
          name: 'other-pool',
          disks: ['/dev/sda'],
          state: 0,
          capacity: 100,
          used: 50,
        },
      ],
      [
        {
          uuid: UUID,
          pool: 'other-pool',
          size: 10,
          thin: false,
          share: 'NONE',
          uri: 'bdev:///' + UUID,
        },
      ]
    );
    let nodeOperator = new NodeOperatorMock([
      {
        node: 'node',
        endpoint: MS_ENDPOINT1,
      },
      {
        node: 'other-node',
        endpoint: MS_ENDPOINT2,
      },
    ]);
    volumeOperator = new VolumeOperator(nodeOperator);
    await volumeOperator.start();
    assert.lengthOf(volumeOperator.getReplicaSet(UUID), 2);
    await volumeOperator.destroyReplica('node', UUID);
    let rs = volumeOperator.getReplicaSet(UUID);
    assert.lengthOf(rs, 1);
    assert.equal(rs[0].node, 'other-node');
    assert.lengthOf(volumeOperator.getNexus(), 1);
    await volumeOperator.destroyNexus('node', UUID);
    assert.lengthOf(volumeOperator.getNexus(), 0);
  });

  it('should destroy neither replica nor nexus if grpc fails', async () => {
    let nodeOperator = new NodeOperatorMock([
      {
        node: 'node',
        endpoint: MS_ENDPOINT1,
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
    startMayastorServer(
      MS_ENDPOINT1,
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
          share: 'NONE',
          uri: 'bdev:///' + UUID,
        },
      ]
    );
    let nodeOperator = new NodeOperatorMock([
      {
        node: 'node',
        endpoint: MS_ENDPOINT1,
      },
      {
        node: 'unreachable-node',
        endpoint: MS_ENDPOINT2,
      },
    ]);
    volumeOperator = new VolumeOperator(nodeOperator);
    await volumeOperator.start();

    let stats = await volumeOperator.getStats();
    assert.lengthOf(stats, 1);
    assert.equal(stats[0].uuid, UUID);
    assert.equal(stats[0].node, 'node');
    assert.equal(stats[0].pool, 'pool');
    ['num_read_ops', 'num_write_ops', 'bytes_read', 'bytes_written'].forEach(
      name => {
        assert.equal(stats[0][name], STAT_DELTA);
      }
    );
    stats = await volumeOperator.getStats();
    assert.lengthOf(stats, 1);
    assert.equal(stats[0].uuid, UUID);
    assert.equal(stats[0].node, 'node');
    assert.equal(stats[0].pool, 'pool');
    ['num_read_ops', 'num_write_ops', 'bytes_read', 'bytes_written'].forEach(
      name => {
        assert.equal(stats[0][name], 2 * STAT_DELTA);
      }
    );
  });

  it('should unshare the replica', async () => {
    startMayastorServer(
      MS_ENDPOINT1,
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
          share: 'NVMF',
          uri: 'nvmf://192.168.0.1:4444/' + UUID,
        },
      ]
    );
    let nodeOperator = new NodeOperatorMock([
      {
        node: 'node',
        endpoint: MS_ENDPOINT1,
      },
    ]);
    volumeOperator = new VolumeOperator(nodeOperator);
    await volumeOperator.start();

    await volumeOperator.shareReplica('node', UUID, 'NONE');
    let rs = volumeOperator.getReplicaSet();
    assert.lengthOf(rs, 1);
    assert.equal(rs[0].uuid, UUID);
    assert.equal(rs[0].node, 'node');
    assert.equal(rs[0].share, 'NONE');
    assert.match(rs[0].uri, /^bdev:\/\/\//);
  });

  it('should not unshare the replica if grpc fails', async () => {
    startMayastorServer(
      MS_ENDPOINT1,
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
          share: 'NVMF',
          uri: 'nvmf://192.168.0.1:4444/' + UUID,
        },
      ]
    );
    let nodeOperator = new NodeOperatorMock([
      {
        node: 'node',
        endpoint: MS_ENDPOINT1,
      },
    ]);
    volumeOperator = new VolumeOperator(nodeOperator);
    await volumeOperator.start();
    stopMayastorServers();

    await shouldFailWith(grpc.status.INTERNAL, () =>
      volumeOperator.shareReplica('node', UUID, 'NONE')
    );
  });

  it('should add child to nexus', async () => {
    startMayastorServer(
      MS_ENDPOINT1,
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
          share: 'NVMF',
          uri: 'nvmf://192.168.0.1:4444/' + UUID,
        },
      ],
      [
        {
          uuid: UUID,
          size: 10,
          state: 'online',
          children: [
            {
              uri: 'nvmf://192.168.0.1:4444/' + UUID,
              state: 'online',
            },
          ],
        },
      ]
    );
    let nodeOperator = new NodeOperatorMock([
      {
        node: 'node',
        endpoint: MS_ENDPOINT1,
      },
    ]);
    volumeOperator = new VolumeOperator(nodeOperator);
    await volumeOperator.start();

    await volumeOperator.addChildNexus(UUID, 'bdev:///' + UUID);
    let nlist = volumeOperator.getNexus();
    assert.lengthOf(nlist, 1);
    assert.equal(nlist[0].uuid, UUID);
    assert.deepEqual(nlist[0].children, [
      'nvmf://192.168.0.1:4444/' + UUID,
      'bdev:///' + UUID,
    ]);
  });

  it('should not add child to nexus if grpc fails', async () => {
    startMayastorServer(
      MS_ENDPOINT1,
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
          share: 'NVMF',
          uri: 'nvmf://192.168.0.1:4444/' + UUID,
        },
      ],
      [
        {
          uuid: UUID,
          size: 10,
          state: 'online',
          children: [
            {
              uri: 'nvmf://192.168.0.1:4444/' + UUID,
              state: 'online',
            },
          ],
        },
      ]
    );
    let nodeOperator = new NodeOperatorMock([
      {
        node: 'node',
        endpoint: MS_ENDPOINT1,
      },
    ]);
    volumeOperator = new VolumeOperator(nodeOperator);
    await volumeOperator.start();
    stopMayastorServers();

    await shouldFailWith(grpc.status.INTERNAL, () =>
      volumeOperator.addChildNexus(UUID, 'bdev:///' + UUID)
    );
  });

  it('should remove child from nexus', async () => {
    startMayastorServer(
      MS_ENDPOINT1,
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
          share: 'NVMF',
          uri: 'nvmf://192.168.0.1:4444/' + UUID,
        },
      ],
      [
        {
          uuid: UUID,
          size: 10,
          state: 'online',
          children: [
            {
              uri: 'nvmf://192.168.0.1:4444/' + UUID,
              state: 'online',
            },
          ],
        },
      ]
    );
    let nodeOperator = new NodeOperatorMock([
      {
        node: 'node',
        endpoint: MS_ENDPOINT1,
      },
    ]);
    volumeOperator = new VolumeOperator(nodeOperator);
    await volumeOperator.start();

    await volumeOperator.removeChildNexus(
      UUID,
      'nvmf://192.168.0.1:4444/' + UUID
    );
    let nlist = volumeOperator.getNexus();
    assert.lengthOf(nlist, 1);
    assert.equal(nlist[0].uuid, UUID);
    assert.equal(nlist[0].children.length, 0);
  });

  it('should not remove child from nexus if grpc fails', async () => {
    startMayastorServer(
      MS_ENDPOINT1,
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
          share: 'NVMF',
          uri: 'nvmf://192.168.0.1:4444/' + UUID,
        },
      ],
      [
        {
          uuid: UUID,
          size: 10,
          state: 'online',
          children: [
            {
              uri: 'nvmf://192.168.0.1:4444/' + UUID,
              state: 'online',
            },
          ],
        },
      ]
    );
    let nodeOperator = new NodeOperatorMock([
      {
        node: 'node',
        endpoint: MS_ENDPOINT1,
      },
    ]);
    volumeOperator = new VolumeOperator(nodeOperator);
    await volumeOperator.start();
    stopMayastorServers();

    await shouldFailWith(grpc.status.INTERNAL, () =>
      volumeOperator.removeChildNexus(UUID, 'nvmf://192.168.0.1:4444/' + UUID)
    );
  });

  it('should sync volumes if new storage node is added', async () => {
    startMayastorServer(
      MS_ENDPOINT1,
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
          share: 'NVMF',
          uri: 'nvmf://192.168.0.1:4444/' + UUID,
        },
      ],
      [
        {
          uuid: UUID,
          size: 10,
          state: 'online',
          children: [
            {
              uri: 'nvmf://192.168.0.1:4444/' + UUID,
              state: 'online',
            },
          ],
        },
      ]
    );
    startMayastorServer(
      MS_ENDPOINT2,
      [
        {
          name: 'other-pool',
          disks: ['/dev/sda'],
          state: 0,
          capacity: 100,
          used: 50,
        },
      ],
      [
        {
          uuid: UUID,
          pool: 'other-pool',
          size: 10,
          thin: false,
          share: 'NONE',
          uri: 'bdev:///' + UUID,
        },
      ]
    );
    let nodeOperator = new NodeOperatorMock([
      {
        node: 'other-node',
        endpoint: MS_ENDPOINT2,
      },
    ]);
    volumeOperator = new VolumeOperator(nodeOperator);
    await volumeOperator.start();
    nodeOperator.addNode('node', MS_ENDPOINT1);

    await waitUntil(
      () => volumeOperator.getReplicaSet().length == 2,
      1500,
      'replica'
    );
    let r = volumeOperator.getReplicaSet()[0];
    assert.equal(r.uuid, UUID);
    assert.equal(r.node, 'other-node');
    assert.equal(r.pool, 'other-pool');
    assert.equal(r.size, 10);
    assert.equal(r.share, 'NONE');
    assert.match(r.uri, /^bdev:\/\/\//);
    r = volumeOperator.getReplicaSet()[1];
    assert.equal(r.uuid, UUID);
    assert.equal(r.node, 'node');
    assert.equal(r.pool, 'pool');
    assert.equal(r.size, 10);
    assert.equal(r.share, 'NVMF');
    assert.match(r.uri, /^nvmf:\/\//);

    await waitUntil(() => volumeOperator.getNexus().length == 1, 1500, 'nexus');
    let n = volumeOperator.getNexus()[0];
    assert.equal(n.uuid, UUID);
    assert.equal(n.node, 'node');
    assert.equal(n.size, 10);
    assert.equal(n.state, 'online');
    assert.lengthOf(n.children, 1);
    assert.equal(n.children[0], 'nvmf://192.168.0.1:4444/' + UUID);
  });

  it('should sync volumes upon start', async () => {
    startMayastorServer(
      MS_ENDPOINT1,
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
          share: 'ISCSI',
          uri: 'iscsi://192.168.0.1:3333/' + UUID,
        },
      ],
      [
        {
          uuid: UUID,
          size: 10,
          state: 'online',
          children: [
            {
              uri: 'iscsi://192.168.0.1:3333/' + UUID,
              state: 'online',
            },
          ],
        },
      ]
    );
    let nodeOperator = new NodeOperatorMock([
      {
        node: 'node',
        endpoint: MS_ENDPOINT1,
      },
    ]);
    volumeOperator = new VolumeOperator(nodeOperator);
    await volumeOperator.start();

    let repls = volumeOperator.getReplicaSet();
    assert.lengthOf(repls, 1);
    assert.equal(repls[0].uuid, UUID);
    assert.equal(repls[0].node, 'node');
    assert.equal(repls[0].pool, 'pool');
    assert.equal(repls[0].size, 10);
    assert.equal(repls[0].share, 'ISCSI');
    assert.match(repls[0].uri, /^iscsi:\/\//);

    let nexus = volumeOperator.getNexus();
    assert.lengthOf(nexus, 1);
    assert.equal(nexus[0].uuid, UUID);
    assert.equal(nexus[0].node, 'node');
    assert.equal(nexus[0].size, 10);
    assert.equal(nexus[0].state, 'online');
    assert.lengthOf(nexus[0].children, 1);
    assert.equal(nexus[0].children[0], 'iscsi://192.168.0.1:3333/' + UUID);
  });

  it('should retry sync of volumes after failure', async () => {
    // change retry interval to 1s not to wait so long
    volumesMod.retrySyncInterval = 1000;

    let nodeOperator = new NodeOperatorMock([
      {
        node: 'node',
        endpoint: MS_ENDPOINT1,
      },
    ]);
    volumeOperator = new VolumeOperator(nodeOperator);
    await volumeOperator.start();
    assert.lengthOf(volumeOperator.getReplicaSet(), 0);
    assert.lengthOf(volumeOperator.getNexus(), 0);

    startMayastorServer(
      MS_ENDPOINT1,
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
          share: 'NONE',
          uri: 'bdev:///' + UUID,
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
      () => volumeOperator.getReplicaSet().length == 1,
      1500,
      'replica'
    );
    let r = volumeOperator.getReplicaSet()[0];
    assert.equal(r.uuid, UUID);
    assert.equal(r.node, 'node');
    assert.equal(r.pool, 'pool');
    assert.equal(r.size, 10);
    assert.equal(r.share, 'NONE');
    assert.match(r.uri, /^bdev:\/\/\//);

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
