// Unit tests for the CSI controller

'use strict';

const assert = require('chai').assert;
const fs = require('fs').promises;
const grpc = require('grpc-uds');
const grpc_promise = require('grpc-promise');
const { CsiServer, csi } = require('./csi');
const { GrpcError } = require('./grpc_client');
const { VolumeOperatorMock } = require('./volumes');
const { PoolOperatorMock } = require('./pools');
const { Commander } = require('./commander');
const { shouldFailWith } = require('./test_utils');

const SOCKPATH = '/tmp/csi_controller_test.sock';
// uuid used whenever we need some uuid and don't care about which one
const UUID = 'd01b8bfb-0116-47b0-a03a-447fcbdc0e99';

// Return gRPC CSI client for given csi service
function getCsiClient(svc) {
  let client = new csi[svc](SOCKPATH, grpc.credentials.createInsecure());
  assert(client);
  grpc_promise.promisifyAll(client);
  return client;
}

module.exports = function() {
  it('should start even if there is stale socket file', async () => {
    await fs.writeFile(SOCKPATH, 'blabla');
    var server = new CsiServer(SOCKPATH);
    await server.start();
    await server.stop();
    try {
      await fs.stat(SOCKPATH);
    } catch (err) {
      if (err.code == 'ENOENT') {
        return;
      }
      throw err;
    }
    throw new Error('Server did not clean up the socket file');
  });

  describe('identity', function() {
    var server;
    var client;

    // create csi server and client
    before(async () => {
      server = new CsiServer(SOCKPATH);
      await server.start();
      client = getCsiClient('Identity');
    });

    after(async () => {
      if (server) {
        await server.stop();
      }
      if (client) {
        client.close();
      }
    });

    it('get plugin info', async () => {
      let res = await client.getPluginInfo().sendMessage({});
      // If you need to change any value of properties below, you will
      // need to change source code of csi node server too!
      assert.strictEqual(res.name, 'io.openebs.csi-mayastor');
      assert.strictEqual(res.vendorVersion, '0.1');
      assert.lengthOf(Object.keys(res.manifest), 0);
    });

    it('get plugin capabilities', async () => {
      let res = await client.getPluginCapabilities().sendMessage({});
      // If you need to change any capabilities below, you will
      // need to change source code of csi node server too!
      assert.lengthOf(res.capabilities, 2);
      assert.strictEqual(
        res.capabilities[0].service.type,
        'CONTROLLER_SERVICE'
      );
      assert.strictEqual(
        res.capabilities[1].service.type,
        'VOLUME_ACCESSIBILITY_CONSTRAINTS'
      );
    });

    it('probe not ready', async () => {
      let res = await client.probe().sendMessage({});
      assert.propertyVal(res.ready, 'value', false);
    });

    it('probe ready', async () => {
      server.makeReady({}, {});
      let res = await client.probe().sendMessage({});
      assert.propertyVal(res.ready, 'value', true);
    });
  });

  describe('controller', function() {
    var client;

    async function mockedServer(pools, replicas, nexus) {
      var server = new CsiServer(SOCKPATH);
      await server.start();
      var poolOper = new PoolOperatorMock(pools || []);
      var volOper = new VolumeOperatorMock(nexus, replicas);
      var commander = new Commander(poolOper, volOper);
      server.makeReady(poolOper, volOper, commander);
      return server;
    }

    // create csi server and client
    before(() => {
      client = getCsiClient('Controller');
    });

    after(() => {
      if (client) {
        client.close();
        client = null;
      }
    });

    describe('generic', function() {
      var server;

      afterEach(async () => {
        if (server) {
          await server.stop();
          server = null;
        }
      });

      it('should get controller capabilities', async () => {
        server = await mockedServer();
        let res = await client.controllerGetCapabilities().sendMessage({});
        let caps = res.capabilities;
        assert.lengthOf(caps, 4);
        assert.equal(caps[0].rpc.type, 'CREATE_DELETE_VOLUME');
        assert.equal(caps[1].rpc.type, 'PUBLISH_UNPUBLISH_VOLUME');
        assert.equal(caps[2].rpc.type, 'LIST_VOLUMES');
        assert.equal(caps[3].rpc.type, 'GET_CAPACITY');
      });

      it('should not get controller capabilities if not ready', async () => {
        server = await mockedServer();
        server.undoReady();
        await shouldFailWith(grpc.status.UNAVAILABLE, () =>
          client.controllerGetCapabilities().sendMessage({})
        );
      });

      it('should return unimlemented error for CreateSnapshot', async () => {
        server = await mockedServer();
        await shouldFailWith(grpc.status.UNIMPLEMENTED, () =>
          client.createSnapshot().sendMessage({
            sourceVolumeId: 'd01b8bfb-0116-47b0-a03a-447fcbdc0e99',
            name: 'blabla2',
          })
        );
      });

      it('should return unimlemented error for DeleteSnapshot', async () => {
        server = await mockedServer();
        await shouldFailWith(grpc.status.UNIMPLEMENTED, () =>
          client.deleteSnapshot().sendMessage({ snapshotId: 'blabla' })
        );
      });

      it('should return unimlemented error for ListSnapshots', async () => {
        server = await mockedServer();
        await shouldFailWith(grpc.status.UNIMPLEMENTED, () =>
          client.listSnapshots().sendMessage({})
        );
      });

      it('should return unimlemented error for ControllerExpandVolume', async () => {
        server = await mockedServer();
        await shouldFailWith(grpc.status.UNIMPLEMENTED, () =>
          client.controllerExpandVolume().sendMessage({
            volumeId: UUID,
            capacityRange: {
              requiredBytes: 200,
              limitBytes: 500,
            },
          })
        );
      });
    });

    describe('CreateVolume', function() {
      var server;

      afterEach(async () => {
        if (server) {
          await server.stop();
          server = null;
        }
      });

      it('should fail if topology requirement other than hostname', async () => {
        server = await mockedServer();
        await shouldFailWith(grpc.status.INVALID_ARGUMENT, () =>
          client.createVolume().sendMessage({
            name: 'pvc-' + UUID,
            capacityRange: {
              requiredBytes: 10,
              limitBytes: 20,
            },
            volumeCapabilities: [
              {
                accessMode: { mode: 'SINGLE_NODE_WRITER' },
                block: {},
              },
            ],
            accessibilityRequirements: {
              requisite: [{ segments: { rack: 'some-rack-info' } }],
              preferred: [],
            },
          })
        );
      });

      it('should fail if volume source', async () => {
        server = await mockedServer();
        await shouldFailWith(grpc.status.INVALID_ARGUMENT, () =>
          client.createVolume().sendMessage({
            name: 'pvc-' + UUID,
            volumeContentSource: { volume: { volumeId: UUID } },
            capacityRange: {
              requiredBytes: 10,
              limitBytes: 20,
            },
            volumeCapabilities: [
              {
                accessMode: { mode: 'SINGLE_NODE_WRITER' },
                block: {},
              },
            ],
          })
        );
      });

      it('should fail if capability other than SINGLE_NODE_WRITER', async () => {
        server = await mockedServer();
        await shouldFailWith(grpc.status.INVALID_ARGUMENT, () =>
          client.createVolume().sendMessage({
            name: 'pvc-' + UUID,
            capacityRange: {
              requiredBytes: 10,
              limitBytes: 20,
            },
            volumeCapabilities: [
              {
                accessMode: { mode: 'SINGLE_NODE_READER_ONLY' },
                block: {},
              },
            ],
          })
        );
      });

      it('should fail if there are no suitable pools', async () => {
        server = await mockedServer([
          {
            name: 'untouched',
            node: 'node',
            disks: ['/dev/sda'],
          },
          {
            name: 'pending',
            node: 'node',
            disks: ['/dev/sda'],
            state: 'PENDING',
          },
          {
            // could be used but is too small
            name: 'online',
            node: 'node',
            disks: ['/dev/sda'],
            state: 'ONLINE',
            capacity: 100,
            used: 50,
          },
          {
            // could be used but is offline
            name: 'offline',
            node: 'node',
            disks: ['/dev/sda'],
            state: 'OFFLINE',
            capacity: 100,
            used: 0,
          },
        ]);
        await shouldFailWith(grpc.status.RESOURCE_EXHAUSTED, () =>
          client.createVolume().sendMessage({
            name: 'pvc-' + UUID,
            capacityRange: {
              requiredBytes: 100,
              limitBytes: 100,
            },
            volumeCapabilities: [
              {
                accessMode: { mode: 'SINGLE_NODE_WRITER' },
                filesystem: {},
              },
            ],
          })
        );
      });

      it('should fail if backend grpc call fails', async () => {
        server = await mockedServer([
          {
            name: 'online',
            node: 'node',
            disks: ['/dev/sda'],
            state: 'ONLINE',
            capacity: 100,
            used: 50,
          },
        ]);
        server.volumes.injectError(
          new GrpcError(grpc.status.INTERNAL, 'Something went wrong')
        );
        await shouldFailWith(grpc.status.INTERNAL, () =>
          client.createVolume().sendMessage({
            name: 'pvc-' + UUID,
            capacityRange: {
              requiredBytes: 10,
              limitBytes: 20,
            },
            volumeCapabilities: [
              {
                accessMode: { mode: 'SINGLE_NODE_WRITER' },
                filesystem: {},
              },
            ],
          })
        );
      });

      it('should fail if volume name is not in expected form', async () => {
        server = await mockedServer();
        await shouldFailWith(grpc.status.INVALID_ARGUMENT, () =>
          client.createVolume().sendMessage({
            name: UUID, // missing pvc- prefix
            capacityRange: {
              requiredBytes: 10,
              limitBytes: 20,
            },
            volumeCapabilities: [
              {
                accessMode: { mode: 'SINGLE_NODE_WRITER' },
                filesystem: {},
              },
            ],
          })
        );
      });

      it('should create volume on specified node', async () => {
        let uuid2 = 'd9a2645e-cc3f-4e62-87ce-94c14a553e1d';

        server = await mockedServer([
          {
            // by all measures this one would normally be preferred
            name: 'online',
            node: 'node-other',
            disks: ['/dev/sdb'],
            state: 'ONLINE',
            capacity: 100,
            used: 0,
          },
          {
            name: 'degraded',
            node: 'node',
            disks: ['/dev/sda'],
            state: 'DEGRADED',
            capacity: 100,
            used: 50,
          },
        ]);

        await client.createVolume().sendMessage({
          name: 'pvc-' + UUID,
          capacityRange: {
            requiredBytes: 50,
            limitBytes: 0,
          },
          volumeCapabilities: [
            {
              accessMode: { mode: 'SINGLE_NODE_WRITER' },
              filesystem: {},
            },
          ],
          accessibilityRequirements: {
            requisite: [{ segments: { 'kubernetes.io/hostname': 'node' } }],
          },
        });
        let repls = server.volumes.getReplicaSet();
        assert.lengthOf(repls, 1);
        assert.equal(repls[0].uuid, UUID);
        assert.equal(repls[0].pool, 'degraded');
        assert.equal(repls[0].node, 'node');
        let nexus = server.volumes.getNexus();
        assert.lengthOf(nexus, 1);
        assert.equal(nexus[0].uuid, UUID);
        assert.equal(nexus[0].node, 'node');
        assert.lengthOf(nexus[0].children, 1);
        assert.equal(nexus[0].children[0], 'bdev:///' + UUID);

        // simulate pool sync
        server.pools.once('sync', () => {
          server.pools.pools[1].used += 50;
        });

        // The second attempt should fail as the pool is already full
        await shouldFailWith(grpc.status.RESOURCE_EXHAUSTED, () =>
          client.createVolume().sendMessage({
            name: 'pvc-' + uuid2,
            capacityRange: {
              requiredBytes: 50,
              limitBytes: 50,
            },
            volumeCapabilities: [
              {
                accessMode: { mode: 'SINGLE_NODE_WRITER' },
                filesystem: {},
              },
            ],
            accessibilityRequirements: {
              requisite: [{ segments: { 'kubernetes.io/hostname': 'node' } }],
            },
          })
        );
      });

      it('should create volume on preferred node', async () => {
        server = await mockedServer([
          {
            // by all measures this one would normally be preferred
            name: 'online',
            node: 'node-other',
            disks: ['/dev/sdb'],
            state: 'ONLINE',
            capacity: 100,
            used: 0,
          },
          {
            name: 'degraded',
            node: 'node',
            disks: ['/dev/sda'],
            state: 'DEGRADED',
            capacity: 100,
            used: 50,
          },
        ]);

        await client.createVolume().sendMessage({
          name: 'pvc-' + UUID,
          capacityRange: {
            requiredBytes: 50,
            limitBytes: 50,
          },
          volumeCapabilities: [
            {
              accessMode: { mode: 'SINGLE_NODE_WRITER' },
              block: {},
            },
          ],
          accessibilityRequirements: {
            preferred: [
              {
                segments: {
                  // should ignore unknown segment if preferred
                  rack: 'some-rack-info',
                  'kubernetes.io/hostname': 'node',
                },
              },
            ],
          },
        });
        let repls = server.volumes.getReplicaSet();
        assert.lengthOf(repls, 1);
        assert.equal(repls[0].uuid, UUID);
        assert.equal(repls[0].pool, 'degraded');
        assert.equal(repls[0].node, 'node');
        let nexus = server.volumes.getNexus();
        assert.lengthOf(nexus, 1);
        assert.equal(nexus[0].uuid, UUID);
        assert.equal(nexus[0].node, 'node');
        assert.lengthOf(nexus[0].children, 1);
        assert.equal(nexus[0].children[0], 'bdev:///' + UUID);
      });

      it('should create volume with specified number of replicas', async () => {
        server = await mockedServer([
          {
            name: 'pool1',
            node: 'node1',
            disks: ['/dev/sdb'],
            state: 'ONLINE',
            capacity: 100,
            used: 0,
          },
          {
            name: 'pool2',
            node: 'node2',
            disks: ['/dev/sda'],
            state: 'DEGRADED',
            capacity: 100,
            used: 50,
          },
          {
            name: 'pool3',
            node: 'node3',
            disks: ['/dev/sda'],
            state: 'ONLINE',
            capacity: 100,
            used: 10,
          },
        ]);

        await client.createVolume().sendMessage({
          name: 'pvc-' + UUID,
          capacityRange: {
            requiredBytes: 50,
            limitBytes: 70,
          },
          volumeCapabilities: [
            {
              accessMode: { mode: 'SINGLE_NODE_WRITER' },
              block: {},
            },
          ],
          parameters: { repl: '3' },
        });
        let rs = server.volumes.getReplicaSet(UUID);
        assert.lengthOf(rs, 3);
        rs.sort((a, b) => (a.node < b.node ? -1 : 1));
        assert.equal(rs[0].uuid, UUID);
        assert.equal(rs[0].pool, 'pool1');
        assert.equal(rs[0].node, 'node1');
        assert.equal(rs[0].size, 50);
        assert.equal(rs[1].uuid, UUID);
        assert.equal(rs[1].pool, 'pool2');
        assert.equal(rs[1].node, 'node2');
        assert.equal(rs[1].size, 50);
        assert.equal(rs[2].uuid, UUID);
        assert.equal(rs[2].pool, 'pool3');
        assert.equal(rs[2].node, 'node3');
        assert.equal(rs[2].size, 50);
        let nexus = server.volumes.getNexus();
        assert.lengthOf(nexus, 1);
        assert.equal(nexus[0].uuid, UUID);
        assert.equal(nexus[0].node, 'node1');
        assert.lengthOf(nexus[0].children, 3);
        assert.equal(nexus[0].children[0], 'bdev:///' + UUID);
        assert.match(nexus[0].children[1], /^nvmf:\/\//);
        assert.match(nexus[0].children[2], /^nvmf:\/\//);
      });

      it('should not create volume with zero size', async () => {
        server = await mockedServer([
          {
            name: 'online',
            node: 'node',
            disks: ['/dev/sda'],
            state: 'ONLINE',
            capacity: 100,
            used: 50,
          },
        ]);

        await shouldFailWith(grpc.status.INVALID_ARGUMENT, () =>
          client.createVolume().sendMessage({
            name: 'pvc-' + UUID,
            capacityRange: {
              requiredBytes: 0,
              limitBytes: 0,
            },
            volumeCapabilities: [
              {
                accessMode: { mode: 'SINGLE_NODE_WRITER' },
                filesystem: {},
              },
            ],
          })
        );
      });

      it('should create volume with max size', async () => {
        server = await mockedServer([
          {
            name: 'online',
            node: 'node',
            disks: ['/dev/sda'],
            state: 'ONLINE',
            capacity: 100,
            used: 50,
          },
        ]);

        await client.createVolume().sendMessage({
          name: 'pvc-' + UUID,
          capacityRange: {
            requiredBytes: 10,
            limitBytes: 50,
          },
          volumeCapabilities: [
            {
              accessMode: { mode: 'SINGLE_NODE_WRITER' },
              block: {},
            },
          ],
        });
        let repls = server.volumes.getReplicaSet();
        assert.lengthOf(repls, 1);
        assert.equal(repls[0].uuid, UUID);
        assert.equal(repls[0].pool, 'online');
        assert.equal(repls[0].node, 'node');
        assert.equal(repls[0].size, 50);
        let nexus = server.volumes.getNexus();
        assert.lengthOf(nexus, 1);
        assert.equal(nexus[0].uuid, UUID);
        assert.equal(nexus[0].node, 'node');
        assert.equal(nexus[0].size, 50);
        assert.lengthOf(nexus[0].children, 1);
        assert.equal(nexus[0].children[0], 'bdev:///' + UUID);
      });

      it('should create volume with min size', async () => {
        server = await mockedServer([
          {
            name: 'online',
            node: 'node',
            disks: ['/dev/sda'],
            state: 'ONLINE',
            capacity: 100,
            used: 50,
          },
        ]);

        await client.createVolume().sendMessage({
          name: 'pvc-' + UUID,
          capacityRange: {
            requiredBytes: 50,
            limitBytes: 100,
          },
          volumeCapabilities: [
            {
              accessMode: { mode: 'SINGLE_NODE_WRITER' },
              block: {},
            },
          ],
        });
        let repls = server.volumes.getReplicaSet();
        assert.lengthOf(repls, 1);
        assert.equal(repls[0].uuid, UUID);
        assert.equal(repls[0].pool, 'online');
        assert.equal(repls[0].node, 'node');
        assert.equal(repls[0].size, 50);
        let nexus = server.volumes.getNexus();
        assert.lengthOf(nexus, 1);
        assert.equal(nexus[0].uuid, UUID);
        assert.equal(nexus[0].node, 'node');
        assert.equal(nexus[0].size, 50);
        assert.lengthOf(nexus[0].children, 1);
        assert.equal(nexus[0].children[0], 'bdev:///' + UUID);
      });

      it('should not fail if it already exists', async () => {
        server = await mockedServer([
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
            used: 40,
          },
        ]);

        await client.createVolume().sendMessage({
          name: 'pvc-' + UUID,
          capacityRange: {
            requiredBytes: 10,
            limitBytes: 50,
          },
          volumeCapabilities: [
            {
              accessMode: { mode: 'SINGLE_NODE_WRITER' },
              filesystem: {},
            },
          ],
        });
        let repls = server.volumes.getReplicaSet();
        assert.lengthOf(repls, 1);
        assert.equal(repls[0].uuid, UUID);
        assert.equal(repls[0].pool, 'pool2');
        assert.equal(repls[0].node, 'node');
        assert.equal(repls[0].size, 50);
        let nexus = server.volumes.getNexus();
        assert.lengthOf(nexus, 1);
        assert.equal(nexus[0].uuid, UUID);
        assert.equal(nexus[0].node, 'node');
        assert.equal(nexus[0].size, 50);
        assert.lengthOf(nexus[0].children, 1);
        assert.equal(nexus[0].children[0], 'bdev:///' + UUID);

        // note that the capacity is different but it is compatible
        await client.createVolume().sendMessage({
          name: 'pvc-' + UUID,
          capacityRange: {
            requiredBytes: 50,
            limitBytes: 60,
          },
          volumeCapabilities: [
            {
              accessMode: { mode: 'SINGLE_NODE_WRITER' },
              filesystem: {},
            },
          ],
        });
        repls = server.volumes.getReplicaSet();
        assert.lengthOf(repls, 1);
        assert.equal(repls[0].uuid, UUID);
        assert.equal(repls[0].pool, 'pool2');
        assert.equal(repls[0].node, 'node');
        assert.equal(repls[0].size, 50);
        nexus = server.volumes.getNexus();
        assert.lengthOf(nexus, 1);
        assert.equal(nexus[0].uuid, UUID);
        assert.equal(nexus[0].node, 'node');
        assert.equal(nexus[0].size, 50);
        assert.lengthOf(nexus[0].children, 1);
        assert.equal(nexus[0].children[0], 'bdev:///' + UUID);
      });

      it('should fail if it exists but is incompatible', async () => {
        server = await mockedServer([
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
            used: 40,
          },
        ]);

        await client.createVolume().sendMessage({
          name: 'pvc-' + UUID,
          capacityRange: {
            requiredBytes: 10,
            limitBytes: 50,
          },
          volumeCapabilities: [
            {
              accessMode: { mode: 'SINGLE_NODE_WRITER' },
              filesystem: {},
            },
          ],
        });
        let repls = server.volumes.getReplicaSet();
        assert.lengthOf(repls, 1);
        assert.equal(repls[0].uuid, UUID);
        assert.equal(repls[0].pool, 'pool2');
        assert.equal(repls[0].node, 'node');
        assert.equal(repls[0].size, 50);
        let nexus = server.volumes.getNexus();
        assert.lengthOf(nexus, 1);
        assert.equal(nexus[0].uuid, UUID);
        assert.equal(nexus[0].node, 'node');
        assert.equal(nexus[0].size, 50);
        assert.lengthOf(nexus[0].children, 1);
        assert.equal(nexus[0].children[0], 'bdev:///' + UUID);

        // note that the capacity is different and incompatible
        await shouldFailWith(grpc.status.ALREADY_EXISTS, () =>
          client.createVolume().sendMessage({
            name: 'pvc-' + UUID,
            capacityRange: {
              requiredBytes: 51,
              limitBytes: 0,
            },
            volumeCapabilities: [
              {
                accessMode: { mode: 'SINGLE_NODE_WRITER' },
                filesystem: {},
              },
            ],
          })
        );
        // note that the limit bytes is incompatible
        await shouldFailWith(grpc.status.ALREADY_EXISTS, () =>
          client.createVolume().sendMessage({
            name: 'pvc-' + UUID,
            capacityRange: {
              requiredBytes: 20,
              limitBytes: 49,
            },
            volumeCapabilities: [
              {
                accessMode: { mode: 'SINGLE_NODE_WRITER' },
                filesystem: {},
              },
            ],
          })
        );
      });
    });

    describe('DeleteVolume', function() {
      var server;

      afterEach(async () => {
        if (server) {
          await server.stop();
          server = null;
        }
      });

      it('should delete volume with multiple replicas', async () => {
        server = await mockedServer(
          [
            {
              name: 'pool1',
              node: 'node1',
              disks: ['/dev/sdb'],
              state: 'ONLINE',
              capacity: 100,
              used: 0,
            },
            {
              name: 'pool2',
              node: 'node2',
              disks: ['/dev/sda'],
              state: 'DEGRADED',
              capacity: 100,
              used: 50,
            },
            {
              name: 'pool3',
              node: 'node3',
              disks: ['/dev/sda'],
              state: 'ONLINE',
              capacity: 100,
              used: 10,
            },
          ],
          [
            {
              uuid: UUID,
              pool: 'pool1',
              node: 'node1',
              size: 50,
              share: 'NONE',
              uri: 'bdev:///' + UUID,
            },
            {
              uuid: UUID,
              pool: 'pool2',
              node: 'node2',
              size: 50,
              share: 'NVMF',
              uri: 'nvmf://192.168.0.2:8420/' + UUID,
            },
            {
              uuid: UUID,
              pool: 'pool3',
              node: 'node3',
              size: 50,
              share: 'NVMF',
              uri: 'nvmf://192.168.0.3:8420/' + UUID,
            },
          ],
          [
            {
              uuid: UUID,
              node: 'node1',
              size: 50,
              state: 'online',
              children: [
                'bdev:///' + UUID,
                'nvmf://192.168.0.2:8420/' + UUID,
                'nvmf://192.168.0.3:8420/' + UUID,
              ],
            },
          ]
        );

        assert.lengthOf(server.volumes.getReplicaSet(), 3);
        assert.lengthOf(server.volumes.getNexus(), 1);
        await client.deleteVolume().sendMessage({ volumeId: UUID });
        assert.lengthOf(server.volumes.getReplicaSet(), 0);
        assert.lengthOf(server.volumes.getNexus(), 0);
      });

      it('should not fail if not found', async () => {
        server = await mockedServer([
          {
            name: 'pool',
            node: 'node',
            disks: ['/dev/sda'],
            state: 'ONLINE',
            capacity: 100,
            used: 50,
          },
        ]);

        assert.lengthOf(server.volumes.getReplicaSet(), 0);
        await client.deleteVolume().sendMessage({ volumeId: UUID });
        assert.lengthOf(server.volumes.getReplicaSet(), 0);
      });

      it('should fail if backend grpc call fails', async () => {
        server = await mockedServer(
          [
            {
              name: 'pool',
              node: 'node',
              disks: ['/dev/sda'],
              state: 'ONLINE',
              capacity: 100,
              used: 50,
            },
          ],
          [
            {
              uuid: UUID,
              pool: 'pool',
              node: 'node',
              size: 50,
            },
          ]
        );

        server.volumes.injectError(
          new GrpcError(grpc.status.INTERNAL, 'Something went wrong')
        );

        await shouldFailWith(grpc.status.INTERNAL, () =>
          client.deleteVolume().sendMessage({ volumeId: UUID })
        );
      });
    });

    describe('ListVolumes', function() {
      var server;
      // uuid except the last two digits
      var uuidBase = '4334cc8a-2fed-45ed-866f-3716639db5';

      // Create army of volumes (100) equally distributed across 10 nodes.
      // On each node is one pool.
      before(async () => {
        var pools = [];
        var replicas = [];
        var nexus = [];

        for (let i = 0; i < 10; i++) {
          pools.push({
            name: 'pool' + i,
            node: 'node' + i,
            disks: ['/dev/sda'],
            state: 'ONLINE',
            capacity: 100,
            used: 50,
          });
          for (let j = 0; j < 10; j++) {
            replicas.push({
              uuid: uuidBase + i + j,
              pool: 'pool' + i,
              node: 'node' + i,
              size: 10,
            });
            nexus.push({
              uuid: uuidBase + i + j,
              node: 'node' + i,
              size: 10,
              state: 'online',
              children: ['bdev:///' + UUID],
            });
          }
        }
        server = await mockedServer(pools, replicas, nexus);
      });

      after(async () => {
        if (server) {
          await server.stop();
          server = null;
        }
      });

      it('should list all volumes', async () => {
        let resp = await client.listVolumes().sendMessage({});
        assert(!resp.nextToken);
        let vols = resp.entries.map(ent => ent.volume);
        assert.lengthOf(vols, 100);
        for (let i = 0; i < 10; i++) {
          for (let j = 0; j < 10; j++) {
            assert.equal(vols[10 * i + j].volumeId, uuidBase + i + j);
          }
        }
      });

      it('should list volumes page by page', async () => {
        let pageSize = 17;
        let next;
        let allVols = [];

        do {
          let resp = await client.listVolumes().sendMessage({
            maxEntries: pageSize,
            startingToken: next,
          });
          let vols = resp.entries.map(ent => ent.volume);
          next = resp.nextToken;
          if (next) {
            assert.lengthOf(vols, pageSize);
          } else {
            assert.lengthOf(vols, 100 % pageSize);
          }
          allVols = allVols.concat(vols);
        } while (next);

        assert.lengthOf(allVols, 100);
        for (let i = 0; i < 10; i++) {
          for (let j = 0; j < 10; j++) {
            assert.equal(allVols[10 * i + j].volumeId, uuidBase + i + j);
          }
        }
      });

      it('should fail if starting token is unknown', async () => {
        await shouldFailWith(grpc.status.INVALID_ARGUMENT, () =>
          client.listVolumes().sendMessage({ startingToken: 'asdfquwer' })
        );
      });
    });

    describe('ControllerPublishVolume', function() {
      var server;
      var unknownUuid = '86705387-a323-4632-9faa-5e4f2162c142';

      before(async () => {
        server = await mockedServer(
          [
            {
              name: 'pool',
              node: 'node',
              disks: ['/dev/sda'],
              state: 'DEGRADED',
              capacity: 100,
              used: 50,
            },
          ],
          [
            {
              uuid: UUID,
              pool: 'pool',
              node: 'node',
              size: 10,
            },
          ],
          [
            {
              uuid: UUID,
              node: 'node',
              size: 50,
              state: 'online',
              children: ['bdev:///' + UUID],
              devicePath: null,
            },
          ]
        );
      });

      after(async () => {
        if (server) {
          await server.stop();
          server = null;
        }
      });

      it('should publish volume', async () => {
        let nexus = server.volumes.getNexus();
        assert.lengthOf(nexus, 1);
        assert.equal(nexus[0].uuid, UUID);
        assert.isNull(nexus[0].devicePath);

        await client.controllerPublishVolume().sendMessage({
          volumeId: UUID,
          nodeId: 'mayastor://node/10.244.2.15:10124',
          readonly: false,
          volumeCapability: {
            accessMode: { mode: 'SINGLE_NODE_WRITER' },
            mount: {
              fsType: 'xfs',
              mount_flags: 'ro',
            },
          },
        });
        nexus = server.volumes.getNexus();
        assert.equal(nexus[0].devicePath, '/dev/nbd0');
      });

      it('should not publish volume if it does not exist', async () => {
        await shouldFailWith(grpc.status.NOT_FOUND, () =>
          client.controllerPublishVolume().sendMessage({
            volumeId: unknownUuid,
            nodeId: 'mayastor://node/10.244.2.15:10124',
            readonly: false,
            volumeCapability: {
              accessMode: { mode: 'SINGLE_NODE_WRITER' },
              mount: {
                fsType: 'xfs',
                mount_flags: 'ro',
              },
            },
          })
        );
      });

      it('should not publish volume on a different node', async () => {
        await shouldFailWith(grpc.status.INVALID_ARGUMENT, () =>
          client.controllerPublishVolume().sendMessage({
            volumeId: UUID,
            nodeId: 'mayastor://another-node/10.244.2.15:10124',
            readonly: false,
            volumeCapability: {
              accessMode: { mode: 'SINGLE_NODE_WRITER' },
              mount: {
                fsType: 'xfs',
                mount_flags: 'ro',
              },
            },
          })
        );
      });

      it('should not publish readonly volume', async () => {
        await shouldFailWith(grpc.status.INVALID_ARGUMENT, () =>
          client.controllerPublishVolume().sendMessage({
            volumeId: UUID,
            nodeId: 'mayastor://node/10.244.2.15:10124',
            readonly: true,
            volumeCapability: {
              accessMode: { mode: 'SINGLE_NODE_WRITER' },
              mount: {
                fsType: 'xfs',
                mount_flags: 'ro',
              },
            },
          })
        );
      });

      it('should not publish volume with unsupported capability', async () => {
        await shouldFailWith(grpc.status.INVALID_ARGUMENT, () =>
          client.controllerPublishVolume().sendMessage({
            volumeId: UUID,
            nodeId: 'mayastor://node/10.244.2.15:10124',
            readonly: false,
            volumeCapability: {
              accessMode: { mode: 'SINGLE_NODE_READER_ONLY' },
              mount: {
                fsType: 'xfs',
                mount_flags: 'ro',
              },
            },
          })
        );
      });

      it('should not publish volume on node with invalid ID', async () => {
        await shouldFailWith(grpc.status.INVALID_ARGUMENT, () =>
          client.controllerPublishVolume().sendMessage({
            volumeId: UUID,
            nodeId: 'mayastor2://node/10.244.2.15:10124',
            readonly: false,
            volumeCapability: {
              accessMode: { mode: 'SINGLE_NODE_WRITER' },
              mount: {
                fsType: 'xfs',
                mount_flags: 'ro',
              },
            },
          })
        );
      });
    });

    describe('ControllerUnpublishVolume', function() {
      var unknownUuid = '86705387-a323-4632-9faa-5e4f2162c142';
      var server;

      before(async () => {
        server = await mockedServer(
          [
            {
              name: 'pool',
              node: 'node',
              disks: ['/dev/sda'],
              state: 'DEGRADED',
              capacity: 100,
              used: 50,
            },
          ],
          [
            {
              uuid: UUID,
              pool: 'pool',
              node: 'node',
              size: 10,
            },
          ],
          [
            {
              uuid: UUID,
              node: 'node',
              size: 50,
              state: 'online',
              children: ['bdev:///' + UUID],
            },
          ]
        );
      });

      after(async () => {
        if (server) {
          await server.stop();
          server = null;
        }
      });

      // make the volume published before each test case
      beforeEach(() => {
        server.volumes.publishNexus(UUID);
      });

      it('should not unpublish volume if it does not exist', async () => {
        await shouldFailWith(grpc.status.NOT_FOUND, () =>
          client.controllerUnpublishVolume().sendMessage({
            volumeId: unknownUuid,
            nodeId: 'mayastor://node/10.244.2.15:10124',
          })
        );
      });

      it('should not unpublish volume on pool with invalid ID', async () => {
        await shouldFailWith(grpc.status.INVALID_ARGUMENT, () =>
          client.controllerUnpublishVolume().sendMessage({
            volumeId: UUID,
            nodeId: 'mayastor2://node/10.244.2.15:10124',
          })
        );
      });

      it('should unpublish volume', async () => {
        let nexus = server.volumes.getNexus();
        assert.lengthOf(nexus, 1);
        assert.equal(nexus[0].uuid, UUID);
        assert.equal(nexus[0].devicePath, '/dev/nbd0');
        await client.controllerUnpublishVolume().sendMessage({
          volumeId: UUID,
          nodeId: 'mayastor://node/10.244.2.15:10124',
        });
        nexus = server.volumes.getNexus();
        assert.lengthOf(nexus, 1);
        assert.equal(nexus[0].uuid, UUID);
        assert.isNull(nexus[0].devicePath);
      });

      it('should unpublish volume even if on a different node', async () => {
        client.controllerUnpublishVolume().sendMessage({
          volumeId: UUID,
          nodeId: 'mayastor://another-node/10.244.2.15:10124',
        });
      });
    });

    describe('ValidateVolumeCapabilities', function() {
      var server;

      before(async () => {
        server = await mockedServer(
          [
            {
              name: 'pool',
              node: 'node',
              disks: ['/dev/sda'],
              state: 'DEGRADED',
              capacity: 100,
              used: 50,
            },
          ],
          [
            {
              uuid: UUID,
              pool: 'pool',
              node: 'node',
              size: 10,
            },
          ],
          [
            {
              uuid: UUID,
              node: 'node',
              size: 50,
              state: 'online',
              children: ['bdev:///' + UUID],
            },
          ]
        );
      });

      after(async () => {
        if (server) {
          await server.stop();
          server = null;
        }
      });

      it('should report SINGLE_NODE_WRITER cap as valid', async () => {
        var caps = [
          'SINGLE_NODE_WRITER',
          'SINGLE_NODE_READER_ONLY',
          'MULTI_NODE_READER_ONLY',
          'MULTI_NODE_SINGLE_WRITER',
          'MULTI_NODE_MULTI_WRITER',
        ];
        var resp = await client.validateVolumeCapabilities().sendMessage({
          volumeId: UUID,
          volumeCapabilities: caps.map(c => {
            return {
              accessMode: { mode: c },
              block: {},
            };
          }),
        });
        assert.lengthOf(resp.confirmed.volumeCapabilities, 1);
        assert.equal(
          resp.confirmed.volumeCapabilities[0].accessMode.mode,
          'SINGLE_NODE_WRITER'
        );
        assert(!resp.message);
      });

      it('should report other caps than SINGLE_NODE_WRITER as invalid', async () => {
        var caps = [
          'SINGLE_NODE_READER_ONLY',
          'MULTI_NODE_READER_ONLY',
          'MULTI_NODE_SINGLE_WRITER',
          'MULTI_NODE_MULTI_WRITER',
        ];
        var resp = await client.validateVolumeCapabilities().sendMessage({
          volumeId: UUID,
          volumeCapabilities: caps.map(c => {
            return {
              accessMode: { mode: c },
              block: {},
            };
          }),
        });
        assert(!resp.confirmed);
        assert.match(resp.message, /SINGLE_NODE_WRITER/);
      });
    });

    describe('GetCapacity', function() {
      var server;

      before(async () => {
        server = await mockedServer([
          {
            name: 'pool1a',
            node: 'node1',
            disks: ['/dev/sda'],
            state: 'DEGRADED',
            capacity: 100,
            used: 50,
          },
          {
            name: 'pool1b',
            node: 'node1',
            disks: ['/dev/sda'],
            state: 'ONLINE',
            capacity: 100,
            used: 75,
          },
          {
            // this one should not be counted because it's offline
            name: 'pool2a',
            node: 'node2',
            disks: ['/dev/sda'],
            state: 'OFFLINE',
            capacity: 100,
            used: 80,
          },
          {
            name: 'pool2b',
            node: 'node2',
            disks: ['/dev/sda'],
            state: 'ONLINE',
            capacity: 100,
            used: 95,
          },
        ]);
      });

      after(async () => {
        if (server) {
          await server.stop();
          server = null;
        }
      });

      it('should get capacity of a single node with multiple pools', async () => {
        var resp = await client.getCapacity().sendMessage({
          accessibleTopology: {
            segments: {
              'kubernetes.io/hostname': 'node1',
            },
          },
        });
        assert.equal(resp.availableCapacity, 75);

        resp = await client.getCapacity().sendMessage({
          accessibleTopology: {
            segments: {
              'kubernetes.io/hostname': 'node2',
            },
          },
        });
        assert.equal(resp.availableCapacity, 5);
      });

      it('should get capacity of all pools on all nodes', async () => {
        var resp = await client.getCapacity().sendMessage({});
        assert.equal(resp.availableCapacity, 80);
      });
    });
  });
};
