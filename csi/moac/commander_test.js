// Unit tests for the commander

'use strict';

const assert = require('chai').assert;
const grpc = require('grpc-uds');
const { PoolOperatorMock } = require('./pools');
const { VolumeOperatorMock } = require('./volumes');
const { shouldFailWith, waitUntil } = require('./test_utils');
const { Commander } = require('./commander');

const UUID = 'ba5e39e9-0c0e-4973-8a3a-0dccada09cbb';

function mockedCommander(pools, nexus, replicas) {
  let poolOperator = new PoolOperatorMock(pools);
  let volumeOperator = new VolumeOperatorMock(nexus, replicas);
  return new Commander(poolOperator, volumeOperator);
}

module.exports = function() {
  describe('ensure supposedly existing volume', function() {
    it('should ensure volume that does exist', async () => {
      let commander = mockedCommander(
        [
          {
            name: 'pool1',
            node: 'node1',
            disks: ['/dev/sda'],
            state: 'ONLINE',
            capacity: 100,
            used: 50,
          },
          {
            name: 'pool2',
            node: 'node2',
            disks: ['/dev/sda'],
            state: 'ONLINE',
            capacity: 100,
            used: 50,
          },
        ],
        [
          {
            uuid: UUID,
            node: 'node1',
            size: 10,
            state: 'online',
            children: ['bdev:///' + UUID, 'nvmf://192.168.0.1:1234/' + UUID],
          },
        ],
        [
          {
            uuid: UUID,
            pool: 'pool1',
            node: 'node1',
            size: 10,
            thin: false,
            share: 'NONE',
            uri: 'bdev:///' + UUID,
          },
          {
            uuid: UUID,
            pool: 'pool2',
            node: 'node2',
            size: 10,
            thin: false,
            share: 'NVMF',
            uri: 'nvmf://192.168.0.1:1234/' + UUID,
          },
        ]
      );
      let nexus = await commander.ensureVolume(UUID);
      assert.lengthOf(nexus.children, 2);
      let rs = commander.volumes.getReplicaSet();
      assert.equal(rs.length, 2);
      let nlist = commander.volumes.getNexus();
      assert.equal(nlist.length, 1);
      assert.deepEqual(nexus, nlist[0]);
    });

    // There are two replicas with different size so the nexus size should
    // be the smaller of the two and it should be created on the node
    // with local replica access even if the replica there is less preferred.
    it('should ensure volume with missing nexus', async () => {
      let commander = mockedCommander(
        [
          {
            name: 'pool1',
            node: 'node1',
            disks: ['/dev/sda'],
            state: 'ONLINE',
            capacity: 100,
            used: 50,
          },
          {
            name: 'pool2',
            node: 'node2',
            disks: ['/dev/sda'],
            state: 'DEGRADED',
            capacity: 100,
            used: 70,
          },
        ],
        [],
        [
          {
            uuid: UUID,
            pool: 'pool1',
            node: 'node1',
            size: 10,
            thin: false,
            share: 'NVMF',
            uri: 'nvmf://192.168.0.1:5555/' + UUID,
          },
          {
            uuid: UUID,
            pool: 'pool2',
            node: 'node2',
            size: 20,
            thin: false,
            share: 'NONE',
            uri: 'bdev:///' + UUID,
          },
        ]
      );
      let nexus = await commander.ensureVolume(UUID);
      let rs = commander.volumes.getReplicaSet();
      assert.equal(rs.length, 2);
      assert.equal(nexus.uuid, UUID);
      assert.equal(nexus.node, 'node2');
      assert.equal(nexus.size, 10);
      assert.equal(nexus.state, 'online');
      assert.equal(nexus.children.length, 2);
    });

    // nexus has two children so ensure should recreate them
    it('should ensure volume with all missing replicas', async () => {
      let commander = mockedCommander(
        [
          {
            name: 'pool1',
            node: 'node1',
            disks: ['/dev/sda'],
            state: 'ONLINE',
            capacity: 100,
            used: 50,
          },
          {
            name: 'pool2',
            node: 'node2',
            disks: ['/dev/sda'],
            state: 'DEGRADED',
            capacity: 100,
            used: 70,
          },
        ],
        [
          {
            uuid: UUID,
            node: 'node1',
            size: 10,
            state: 'online',
            children: ['bdev:///blabla', 'nvmf://blabla2'],
          },
        ],
        []
      );
      let nexus = await commander.ensureVolume(UUID);
      assert.equal(nexus.uuid, UUID);
      assert.equal(nexus.children.length, 2);
      assert.match(nexus.children[0], /^bdev:\/\/\//);
      assert.match(nexus.children[1], /^nvmf:\/\//);
      let rs = commander.volumes.getReplicaSet();
      assert.equal(rs.length, 2);
      assert.equal(rs[0].uuid, UUID);
      assert.equal(rs[0].node, 'node1');
      assert.equal(rs[0].pool, 'pool1');
      assert.equal(rs[0].size, 10);
      assert.equal(rs[0].share, 'NONE');
      assert.match(rs[0].uri, /^bdev:\/\/\//);
      assert.equal(rs[1].uuid, UUID);
      assert.equal(rs[1].node, 'node2');
      assert.equal(rs[1].pool, 'pool2');
      assert.equal(rs[1].size, 10);
      assert.equal(rs[1].share, 'NVMF');
      assert.match(rs[1].uri, /^nvmf:\/\//);
    });

    it('should ensure volume with a missing replica', async () => {
      let commander = mockedCommander(
        [
          {
            name: 'pool1',
            node: 'node1',
            disks: ['/dev/sda'],
            state: 'ONLINE',
            capacity: 100,
            used: 85,
          },
          {
            name: 'pool2',
            node: 'node2',
            disks: ['/dev/sda'],
            state: 'DEGRADED',
            capacity: 100,
            used: 70,
          },
        ],
        [
          {
            uuid: UUID,
            node: 'node2',
            size: 10,
            state: 'online',
            children: ['bdev:///' + UUID, 'nvmf://blabla'],
          },
        ],
        [
          {
            uuid: UUID,
            pool: 'pool2',
            node: 'node2',
            size: 20,
            thin: false,
            share: 'NONE',
            uri: 'bdev:///' + UUID,
          },
        ]
      );
      await commander.ensureVolume(UUID);
      let nlist = commander.volumes.getNexus();
      assert.equal(nlist.length, 1);
      assert.equal(nlist[0].uuid, UUID);
      assert.equal(nlist[0].children.length, 2);
      assert.match(nlist[0].children[0], /^bdev:\/\/\//);
      assert.match(nlist[0].children[1], /^nvmf:\/\//);
      let rs = commander.volumes.getReplicaSet();
      assert.equal(rs.length, 2);
      assert.equal(rs[0].uuid, UUID);
      assert.equal(rs[0].node, 'node2');
      assert.equal(rs[0].pool, 'pool2');
      assert.equal(rs[0].size, 20);
      assert.equal(rs[0].share, 'NONE');
      assert.match(rs[0].uri, /^bdev:\/\/\//);
      assert.equal(rs[1].uuid, UUID);
      assert.equal(rs[1].node, 'node1');
      assert.equal(rs[1].pool, 'pool1');
      assert.equal(rs[1].size, 15);
      assert.equal(rs[1].share, 'NVMF');
      assert.match(rs[1].uri, /^nvmf:\/\//);
    });

    it('should ensure volume with superfluous replica', async () => {
      let commander = mockedCommander(
        [
          {
            name: 'pool1',
            node: 'node1',
            disks: ['/dev/sda'],
            state: 'ONLINE',
            capacity: 100,
            used: 85,
          },
          {
            name: 'pool2',
            node: 'node2',
            disks: ['/dev/sda'],
            state: 'DEGRADED',
            capacity: 100,
            used: 70,
          },
        ],
        [
          {
            uuid: UUID,
            node: 'node2',
            size: 10,
            state: 'online',
            children: ['bdev:///' + UUID],
          },
        ],
        [
          {
            uuid: UUID,
            pool: 'pool2',
            node: 'node2',
            size: 20,
            thin: false,
            share: 'NONE',
            uri: 'bdev:///' + UUID,
          },
          {
            uuid: UUID,
            pool: 'pool1',
            node: 'node1',
            size: 10,
            thin: false,
            share: 'ISCSI',
            uri: 'iscsi://192.168.0.1:3333/' + UUID,
          },
        ]
      );
      await commander.ensureVolume(UUID);
      let nlist = commander.volumes.getNexus();
      assert.equal(nlist.length, 1);
      assert.equal(nlist[0].uuid, UUID);
      assert.equal(nlist[0].children.length, 1);
      let rs = commander.volumes.getReplicaSet();
      assert.equal(rs.length, 1);
      assert.equal(rs[0].uuid, UUID);
      assert.equal(rs[0].node, 'node2');
      assert.equal(rs[0].pool, 'pool2');
      assert.equal(rs[0].size, 20);
      assert.equal(rs[0].share, 'NONE');
      assert.match(rs[0].uri, /^bdev:\/\/\//);
    });

    it('should fail if ensuring volume which does not exist', async () => {
      let commander = mockedCommander([], [], []);
      await shouldFailWith(grpc.status.INTERNAL, () =>
        commander.ensureVolume(UUID)
      );
    });
  });

  describe('ensure new volumes', function() {
    it('should create volume with 3 replicas', async () => {
      let commander = mockedCommander([
        {
          name: 'pool1',
          node: 'node1',
          disks: ['/dev/sda'],
          state: 'ONLINE',
          capacity: 100,
          used: 50,
        },
        {
          name: 'pool2',
          node: 'node2',
          disks: ['/dev/sda'],
          state: 'ONLINE',
          capacity: 100,
          used: 50,
        },
        {
          name: 'pool3',
          node: 'node3',
          disks: ['/dev/sda'],
          state: 'ONLINE',
          capacity: 100,
          used: 50,
        },
      ]);
      await commander.ensureVolume(UUID, {
        requiredBytes: 10,
        limitBytes: 0,
        mustNodes: [],
        shouldNodes: [],
        count: 3,
      });
      let rs = commander.volumes.getReplicaSet(UUID);
      assert.equal(rs.length, 3);
      assert.equal(rs[0].uuid, UUID);
      assert.equal(rs[1].uuid, UUID);
      assert.equal(rs[2].uuid, UUID);
      let local = rs.filter(r => r.share == 'NONE');
      assert.equal(local.length, 1);
      let remote = rs.filter(r => r.share == 'NVMF');
      assert.equal(remote.length, 2);

      let n = commander.volumes.getNexus(UUID);
      assert.equal(n.uuid, UUID);
      assert.equal(n.size, 10);
      assert.equal(n.node, local[0].node);
      assert.equal(n.children.length, 3);
    });
  });

  describe('pool selection', function() {
    it('should prefer ONLINE pool', async () => {
      let commander = mockedCommander(
        [
          {
            name: 'online',
            node: 'node1',
            disks: ['/dev/sdb'],
            state: 'ONLINE',
            capacity: 100,
            used: 50,
          },
          {
            // this one has more free space but is degraded
            name: 'degraded',
            node: 'node2',
            disks: ['/dev/sda'],
            state: 'DEGRADED',
            capacity: 100,
            used: 0,
          },
        ],
        [],
        []
      );

      await commander.ensureVolume(UUID, {
        requiredBytes: 50,
        limitBytes: 0,
        mustNodes: [],
        shouldNodes: [],
        count: 1,
      });

      let repls = commander.volumes.getReplicaSet();
      assert.lengthOf(repls, 1);
      assert.equal(repls[0].uuid, UUID);
      assert.equal(repls[0].pool, 'online');
      assert.equal(repls[0].node, 'node1');
    });

    it('should prefer pool with fewer volumes', async () => {
      let uuidBusy = '7c2c6500-2289-4385-9421-be7cdd5a811b';
      let commander = mockedCommander(
        [
          {
            name: 'busy1',
            node: 'node1',
            disks: ['/dev/sda'],
            state: 'ONLINE',
            capacity: 100,
            used: 50,
          },
          {
            name: 'idle',
            node: 'node2',
            disks: ['/dev/sdb'],
            state: 'ONLINE',
            capacity: 100,
            used: 50,
          },
          {
            name: 'busy2',
            node: 'node3',
            disks: ['/dev/sda'],
            state: 'ONLINE',
            capacity: 100,
            used: 50,
          },
        ],
        [],
        [
          {
            uuid: uuidBusy,
            pool: 'busy1',
            node: 'node1',
            size: 50,
          },
          {
            uuid: uuidBusy,
            pool: 'busy2',
            node: 'node3',
            size: 50,
          },
        ]
      );

      await commander.ensureVolume(UUID, {
        requiredBytes: 10,
        limitBytes: 0,
        mustNodes: [],
        shouldNodes: [],
        count: 1,
      });

      let repls = commander.volumes.getReplicaSet();
      assert.lengthOf(repls, 3);
      let r = repls.find(r => r.uuid == UUID);
      assert.equal(r.pool, 'idle');
      assert.equal(r.node, 'node2');
    });

    it('should prefer pool with more free space', async () => {
      let commander = mockedCommander(
        [
          {
            name: 'most',
            node: 'node1',
            disks: ['/dev/sda'],
            state: 'DEGRADED',
            capacity: 100,
            used: 51,
          },
          {
            name: 'less',
            node: 'node2',
            disks: ['/dev/sdb'],
            state: 'DEGRADED',
            capacity: 100,
            used: 49,
          },
          {
            name: 'more',
            node: 'node3',
            disks: ['/dev/sda'],
            state: 'DEGRADED',
            capacity: 100,
            used: 50,
          },
        ],
        [],
        []
      );

      await commander.ensureVolume(UUID, {
        requiredBytes: 10,
        limitBytes: 0,
        mustNodes: [],
        shouldNodes: [],
        count: 1,
      });

      let repls = commander.volumes.getReplicaSet();
      assert.equal(repls[0].uuid, UUID);
      assert.equal(repls[0].pool, 'less');
      assert.equal(repls[0].node, 'node2');
    });

    it('should resize replica according to avail space in pool', async () => {
      let commander = mockedCommander(
        [
          {
            name: 'more',
            node: 'node1',
            disks: ['/dev/sda'],
            state: 'ONLINE',
            capacity: 100,
            used: 80,
          },
          {
            name: 'less',
            node: 'node2',
            disks: ['/dev/sdb'],
            state: 'ONLINE',
            capacity: 100,
            used: 83,
          },
        ],
        [],
        []
      );

      await commander.ensureVolume(UUID, {
        requiredBytes: 10,
        limitBytes: 20,
        mustNodes: [],
        shouldNodes: [],
        count: 2,
      });

      let repls = commander.volumes.getReplicaSet();
      assert.equal(repls.length, 2);
      assert.equal(repls[0].uuid, UUID);
      assert.equal(repls[0].size, 17);
      assert.equal(repls[1].uuid, UUID);
      assert.equal(repls[1].size, 17);

      let nexus = commander.volumes.getNexus();
      assert.equal(nexus.length, 1);
      assert.equal(nexus[0].uuid, UUID);
      assert.equal(nexus[0].size, 17);
    });

    it('should fail if no suitable pool', async () => {
      let commander = mockedCommander(
        [
          {
            name: 'bad',
            node: 'node1',
            disks: ['/dev/sda'],
            state: 'OFFLINE',
            capacity: 100,
            used: 4,
          },
          {
            name: 'full',
            node: 'node2',
            disks: ['/dev/sdb'],
            state: 'ONLINE',
            capacity: 100,
            used: 91,
          },
        ],
        [],
        []
      );

      await shouldFailWith(grpc.status.RESOURCE_EXHAUSTED, () =>
        commander.ensureVolume(UUID, {
          requiredBytes: 10,
          limitBytes: 0,
          mustNodes: [],
          shouldNodes: [],
          count: 1,
        })
      );
    });

    it('should not create replica set on the same node', async () => {
      let commander = mockedCommander(
        [
          {
            name: 'pool1',
            node: 'node',
            disks: ['/dev/sda'],
            state: 'ONLINE',
            capacity: 100,
            used: 50,
          },
          {
            name: 'pool2',
            node: 'node',
            disks: ['/dev/sdb'],
            state: 'ONLINE',
            capacity: 100,
            used: 50,
          },
        ],
        [],
        []
      );

      await shouldFailWith(grpc.status.RESOURCE_EXHAUSTED, () =>
        commander.ensureVolume(UUID, {
          requiredBytes: 10,
          limitBytes: 0,
          mustNodes: [],
          shouldNodes: [],
          count: 2,
        })
      );
    });

    it('should create replica on user requested nodes', async () => {
      let commander = mockedCommander([
        {
          name: 'pool1',
          node: 'node1',
          disks: ['/dev/sda'],
          state: 'ONLINE',
          capacity: 100,
          used: 50,
        },
        // normally this pool would be the most preferred
        {
          name: 'pool2',
          node: 'node2',
          disks: ['/dev/sda'],
          state: 'ONLINE',
          capacity: 100,
          used: 0,
        },
        {
          name: 'pool3',
          node: 'node3',
          disks: ['/dev/sda'],
          state: 'DEGRADED',
          capacity: 100,
          used: 80,
        },
      ]);
      await commander.ensureVolume(UUID, {
        requiredBytes: 10,
        limitBytes: 0,
        mustNodes: ['node1', 'node3'],
        shouldNodes: [],
        count: 2,
      });
      let rs = commander.volumes.getReplicaSet(UUID);
      assert.equal(rs.length, 2);
      rs.sort((a, b) => (a.node < b.node ? -1 : 1));
      assert.equal(rs[0].node, 'node1');
      assert.equal(rs[0].share, 'NONE');
      assert.equal(rs[1].node, 'node3');
      assert.equal(rs[1].share, 'NVMF');
      let n = commander.volumes.getNexus(UUID);
      assert.equal(n.uuid, UUID);
      assert.equal(n.size, 10);
      assert.equal(n.node, 'node1');
      assert.equal(n.children.length, 2);
    });

    it('should fail if user requested nodes are not suitable', async () => {
      let commander = mockedCommander([
        {
          name: 'pool1',
          node: 'node1',
          disks: ['/dev/sda'],
          state: 'ONLINE',
          capacity: 100,
          used: 50,
        },
        // normally this pool would be the most preferred
        {
          name: 'pool2',
          node: 'node2',
          disks: ['/dev/sda'],
          state: 'ONLINE',
          capacity: 100,
          used: 0,
        },
        {
          name: 'pool3',
          node: 'node3',
          disks: ['/dev/sda'],
          state: 'OFFLINE',
          capacity: 100,
          used: 80,
        },
      ]);
      await shouldFailWith(grpc.status.RESOURCE_EXHAUSTED, () =>
        commander.ensureVolume(UUID, {
          requiredBytes: 10,
          limitBytes: 0,
          mustNodes: ['node1', 'node3'],
          shouldNodes: [],
          count: 2,
        })
      );
    });

    it('should create replica on user preferred nodes', async () => {
      let commander = mockedCommander([
        {
          name: 'pool1',
          node: 'node1',
          disks: ['/dev/sda'],
          state: 'ONLINE',
          capacity: 100,
          used: 50,
        },
        {
          name: 'pool2',
          node: 'node2',
          disks: ['/dev/sda'],
          state: 'ONLINE',
          capacity: 100,
          used: 0,
        },
        // this pool would normally be the last option if not user preferred
        {
          name: 'pool3',
          node: 'node3',
          disks: ['/dev/sda'],
          state: 'DEGRADED',
          capacity: 100,
          used: 80,
        },
      ]);
      await commander.ensureVolume(UUID, {
        requiredBytes: 10,
        limitBytes: 0,
        mustNodes: [],
        shouldNodes: ['node3'],
        count: 2,
      });
      let rs = commander.volumes.getReplicaSet(UUID);
      assert.equal(rs.length, 2);
      rs.sort((a, b) => (a.node < b.node ? -1 : 1));
      assert.equal(rs[0].node, 'node2');
      assert.equal(rs[0].share, 'NVMF');
      assert.equal(rs[1].node, 'node3');
      assert.equal(rs[1].share, 'NONE');
      let n = commander.volumes.getNexus(UUID);
      assert.equal(n.uuid, UUID);
      assert.equal(n.size, 10);
      assert.equal(n.node, 'node3');
      assert.equal(n.children.length, 2);
    });

    it('should not fail if user preferred nodes are not suitable', async () => {
      let commander = mockedCommander([
        {
          name: 'pool1',
          node: 'node1',
          disks: ['/dev/sda'],
          state: 'ONLINE',
          capacity: 100,
          used: 50,
        },
        // normally this pool would be the most preferred
        {
          name: 'pool2',
          node: 'node2',
          disks: ['/dev/sda'],
          state: 'ONLINE',
          capacity: 100,
          used: 0,
        },
        {
          name: 'pool3',
          node: 'node3',
          disks: ['/dev/sda'],
          state: 'OFFLINE',
          capacity: 100,
          used: 80,
        },
      ]);
      await commander.ensureVolume(UUID, {
        requiredBytes: 10,
        limitBytes: 0,
        mustNodes: [],
        shouldNodes: ['node3'],
        count: 2,
      });
      let rs = commander.volumes.getReplicaSet(UUID);
      assert.equal(rs.length, 2);
      rs.sort((a, b) => (a.node < b.node ? -1 : 1));
      assert.equal(rs[0].node, 'node1');
      assert.equal(rs[0].share, 'NVMF');
      assert.equal(rs[1].node, 'node2');
      assert.equal(rs[1].share, 'NONE');
      let n = commander.volumes.getNexus(UUID);
      assert.equal(n.uuid, UUID);
      assert.equal(n.size, 10);
      assert.equal(n.node, 'node2');
      assert.equal(n.children.length, 2);
    });
  });

  // we create artificial inconsistency and wait for the rescan to fix it
  describe('rescan', function() {
    let commander;

    before(() => {
      commander = mockedCommander(
        [
          {
            name: 'pool1',
            node: 'node1',
            disks: ['/dev/sda'],
            state: 'ONLINE',
            capacity: 100,
            used: 50,
          },
          {
            name: 'pool2',
            node: 'node2',
            disks: ['/dev/sda'],
            state: 'ONLINE',
            capacity: 100,
            used: 0,
          },
        ],
        [
          // nexus's second replica does not exist - should be recreated
          {
            uuid: UUID,
            node: 'node1',
            size: 10,
            state: 'online',
            children: ['bdev:///' + UUID, 'nvmf://blabla'],
          },
        ],
        [
          {
            uuid: UUID,
            pool: 'pool1',
            node: 'node1',
            size: 20,
            thin: false,
            share: 'NONE',
            uri: 'bdev:///' + UUID,
          },
        ]
      );
    });

    after(() => {
      if (commander) {
        commander.stop();
      }
    });

    it('should rescan volumes', async () => {
      var rescanDone = false;
      commander.once('rescan-done', () => (rescanDone = true));
      // rescan volumes in one second interval
      commander.start(1);
      await waitUntil(() => rescanDone, 1500, 'replica');
      let rs = commander.volumes.getReplicaSet(UUID);
      assert.equal(rs.length, 2);
      assert.equal(rs[0].node, 'node1');
      assert.equal(rs[0].size, 20);
      assert.equal(rs[0].share, 'NONE');
      assert.equal(rs[1].node, 'node2');
      assert.equal(rs[1].pool, 'pool2');
      assert.equal(rs[1].share, 'NVMF');
      assert.equal(rs[1].size, 20);
      let n = commander.volumes.getNexus(UUID);
      assert.equal(n.uuid, UUID);
      assert.equal(n.size, 10);
      assert.equal(n.node, 'node1');
      assert.equal(n.children.length, 2);
      assert.equal(n.children[0], 'bdev:///' + UUID);
      assert.match(n.children[1], /^nvmf:\/\/[1-9]/);
    });
  });
};
