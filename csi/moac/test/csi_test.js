// Unit tests for the CSI controller

'use strict';

const expect = require('chai').expect;
const fs = require('fs').promises;
const grpc = require('grpc-uds');
const grpc_promise = require('grpc-promise');
const sinon = require('sinon');
const { CsiServer, csi } = require('../csi');
const { GrpcError, GrpcCode } = require('../grpc_client');
const Registry = require('../registry');
const Volume = require('../volume');
const Volumes = require('../volumes');
const { shouldFailWith } = require('./utils');

const SOCKPATH = '/tmp/csi_controller_test.sock';
// uuid used whenever we need some uuid and don't care about which one
const UUID = 'd01b8bfb-0116-47b0-a03a-447fcbdc0e99';

// Return gRPC CSI client for given csi service
function getCsiClient(svc) {
  let client = new csi[svc](SOCKPATH, grpc.credentials.createInsecure());
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
      expect(res.name).to.equal('io.openebs.csi-mayastor');
      expect(res.vendorVersion).to.equal('0.1');
      expect(Object.keys(res.manifest)).to.have.lengthOf(0);
    });

    it('get plugin capabilities', async () => {
      let res = await client.getPluginCapabilities().sendMessage({});
      // If you need to change any capabilities below, you will
      // need to change source code of csi node server too!
      expect(res.capabilities).to.have.lengthOf(2);
      expect(res.capabilities[0].service.type).to.equal('CONTROLLER_SERVICE');
      expect(res.capabilities[1].service.type).to.equal(
        'VOLUME_ACCESSIBILITY_CONSTRAINTS'
      );
    });

    it('probe not ready', async () => {
      let res = await client.probe().sendMessage({});
      expect(res.ready).to.have.property('value', false);
    });

    it('probe ready', async () => {
      server.makeReady({}, {});
      let res = await client.probe().sendMessage({});
      expect(res.ready).to.have.property('value', true);
    });
  });

  describe('controller', function() {
    var client;
    var registry, volumes;
    var getCapacityStub, createVolumeStub, getVolumesStub, destroyVolumeStub;

    async function mockedServer(pools, replicas, nexus) {
      var server = new CsiServer(SOCKPATH);
      await server.start();
      registry = new Registry();
      volumes = new Volumes(registry);
      server.makeReady(registry, volumes);
      getCapacityStub = sinon.stub(registry, 'getCapacity');
      createVolumeStub = sinon.stub(volumes, 'createVolume');
      getVolumesStub = sinon.stub(volumes, 'get');
      destroyVolumeStub = sinon.stub(volumes, 'destroyVolume');
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
        expect(caps).to.have.lengthOf(4);
        expect(caps[0].rpc.type).to.equal('CREATE_DELETE_VOLUME');
        expect(caps[1].rpc.type).to.equal('PUBLISH_UNPUBLISH_VOLUME');
        expect(caps[2].rpc.type).to.equal('LIST_VOLUMES');
        expect(caps[3].rpc.type).to.equal('GET_CAPACITY');
      });

      it('should not get controller capabilities if not ready', async () => {
        server = await mockedServer();
        server.undoReady();
        await shouldFailWith(GrpcCode.UNAVAILABLE, () =>
          client.controllerGetCapabilities().sendMessage({})
        );
      });

      it('should return unimlemented error for CreateSnapshot', async () => {
        server = await mockedServer();
        await shouldFailWith(GrpcCode.UNIMPLEMENTED, () =>
          client.createSnapshot().sendMessage({
            sourceVolumeId: 'd01b8bfb-0116-47b0-a03a-447fcbdc0e99',
            name: 'blabla2',
          })
        );
      });

      it('should return unimlemented error for DeleteSnapshot', async () => {
        server = await mockedServer();
        await shouldFailWith(GrpcCode.UNIMPLEMENTED, () =>
          client.deleteSnapshot().sendMessage({ snapshotId: 'blabla' })
        );
      });

      it('should return unimlemented error for ListSnapshots', async () => {
        server = await mockedServer();
        await shouldFailWith(GrpcCode.UNIMPLEMENTED, () =>
          client.listSnapshots().sendMessage({})
        );
      });

      it('should return unimlemented error for ControllerExpandVolume', async () => {
        server = await mockedServer();
        await shouldFailWith(GrpcCode.UNIMPLEMENTED, () =>
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
      // place-holder for return value from createVolume when we don't care
      // about the data (i.e. when testing error cases).
      var returnedVolume = new Volume(UUID, registry, {
        replicaCount: 1,
        preferredNodes: [],
        requiredNodes: [],
        requiredBytes: 10,
        limitBytes: 20,
      });

      beforeEach(async () => {
        server = await mockedServer();
      });

      afterEach(async () => {
        if (server) {
          await server.stop();
          server = null;
        }
      });

      it('should fail if topology requirement other than hostname', async () => {
        createVolumeStub.resolves(returnedVolume);
        await shouldFailWith(GrpcCode.INVALID_ARGUMENT, () =>
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
        createVolumeStub.resolves(returnedVolume);
        await shouldFailWith(GrpcCode.INVALID_ARGUMENT, () =>
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
        createVolumeStub.resolves(returnedVolume);
        await shouldFailWith(GrpcCode.INVALID_ARGUMENT, () =>
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

      it('should fail if grpc exception is thrown', async () => {
        createVolumeStub.rejects(
          new GrpcError(GrpcCode.INTERNAL, 'Something went wrong')
        );
        await shouldFailWith(GrpcCode.INTERNAL, () =>
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
        createVolumeStub.resolves(returnedVolume);
        await shouldFailWith(GrpcCode.INVALID_ARGUMENT, () =>
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
        createVolumeStub.resolves(returnedVolume);
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
        sinon.assert.calledWith(createVolumeStub, UUID, {
          replicaCount: 1,
          preferredNodes: [],
          requiredNodes: ['node'],
          requiredBytes: 50,
          limitBytes: 0,
        });
      });

      it('should create volume on preferred node', async () => {
        createVolumeStub.resolves(returnedVolume);
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
        sinon.assert.calledWith(createVolumeStub, UUID, {
          replicaCount: 1,
          preferredNodes: ['node'],
          requiredNodes: [],
          requiredBytes: 50,
          limitBytes: 50,
        });
      });

      it('should create volume with specified number of replicas', async () => {
        createVolumeStub.resolves(returnedVolume);
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
        sinon.assert.calledWith(createVolumeStub, UUID, {
          replicaCount: 3,
          preferredNodes: [],
          requiredNodes: [],
          requiredBytes: 50,
          limitBytes: 70,
        });
      });

      it('should fail if number of replicas is not a number', async () => {
        createVolumeStub.resolves(returnedVolume);
        await shouldFailWith(GrpcCode.INVALID_ARGUMENT, () =>
          client.createVolume().sendMessage({
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
            parameters: { repl: 'bla2' },
          })
        );
      });
    });

    describe('DeleteVolume', function() {
      var server;

      beforeEach(async () => {
        server = await mockedServer();
      });

      afterEach(async () => {
        if (server) {
          await server.stop();
          server = null;
        }
      });

      it('should delete volume with multiple replicas', async () => {
        destroyVolumeStub.resolves();

        await client.deleteVolume().sendMessage({ volumeId: UUID });

        sinon.assert.calledOnce(destroyVolumeStub);
        sinon.assert.calledWith(destroyVolumeStub, UUID);
      });

      it('should fail if backend grpc call fails', async () => {
        destroyVolumeStub.rejects(
          new GrpcError(GrpcCode.INTERNAL, 'Something went wrong')
        );

        await shouldFailWith(GrpcCode.INTERNAL, () =>
          client.deleteVolume().sendMessage({ volumeId: UUID })
        );

        sinon.assert.calledOnce(destroyVolumeStub);
      });
    });

    describe('ListVolumes', function() {
      var server;
      // uuid except the last two digits
      var uuidBase = '4334cc8a-2fed-45ed-866f-3716639db5';

      // Create army of volumes (100)
      before(async () => {
        var vols = [];
        for (let i = 0; i < 10; i++) {
          for (let j = 0; j < 10; j++) {
            let vol = new Volume(uuidBase + i + j, registry, {});
            let getSizeStub = sinon.stub(vol, 'getSize');
            getSizeStub.returns(100);
            let getNodeName = sinon.stub(vol, 'getNodeName');
            getNodeName.returns('node');
            vols.push(vol);
          }
        }
        server = await mockedServer();
        getVolumesStub.returns(vols);
      });

      after(async () => {
        if (server) {
          await server.stop();
          server = null;
        }
      });

      it('should list all volumes', async () => {
        let resp = await client.listVolumes().sendMessage({});
        // jshint ignore:start
        expect(resp.nextToken).to.be.empty;
        // jshint ignore:end
        let vols = resp.entries.map(ent => ent.volume);
        expect(vols).to.have.lengthOf(100);
        for (let i = 0; i < 10; i++) {
          for (let j = 0; j < 10; j++) {
            expect(vols[10 * i + j].volumeId).to.equal(uuidBase + i + j);
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
            expect(vols).to.have.lengthOf(pageSize);
          } else {
            expect(vols).to.have.lengthOf(100 % pageSize);
          }
          allVols = allVols.concat(vols);
        } while (next);

        expect(allVols).to.have.lengthOf(100);
        for (let i = 0; i < 10; i++) {
          for (let j = 0; j < 10; j++) {
            expect(allVols[10 * i + j].volumeId).to.equal(uuidBase + i + j);
          }
        }
      });

      it('should fail if starting token is unknown', async () => {
        await shouldFailWith(GrpcCode.INVALID_ARGUMENT, () =>
          client.listVolumes().sendMessage({ startingToken: 'asdfquwer' })
        );
      });
    });

    describe('ControllerPublishVolume', function() {
      var server;

      before(async () => {
        server = await mockedServer();
      });

      after(async () => {
        if (server) {
          await server.stop();
          server = null;
        }
      });

      afterEach(() => {
        getVolumesStub.reset();
      });

      it('should publish volume', async () => {
        let volume = new Volume(UUID, registry, {});
        let publishStub = sinon.stub(volume, 'publish');
        publishStub.resolves();
        let getNodeNameStub = sinon.stub(volume, 'getNodeName');
        getNodeNameStub.returns('node');
        getVolumesStub.returns(volume);

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

        sinon.assert.calledOnce(getVolumesStub);
        sinon.assert.calledWith(getVolumesStub, UUID);
        sinon.assert.calledOnce(publishStub);
      });

      it('should not publish volume if it does not exist', async () => {
        getVolumesStub.returns();

        await shouldFailWith(GrpcCode.NOT_FOUND, () =>
          client.controllerPublishVolume().sendMessage({
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
          })
        );
        sinon.assert.calledOnce(getVolumesStub);
        sinon.assert.calledWith(getVolumesStub, UUID);
      });

      it('should not publish volume on a different node', async () => {
        let volume = new Volume(UUID, registry, {});
        let publishStub = sinon.stub(volume, 'publish');
        publishStub.resolves();
        let getNodeNameStub = sinon.stub(volume, 'getNodeName');
        getNodeNameStub.returns('another-node');
        getVolumesStub.returns(volume);

        await shouldFailWith(GrpcCode.INVALID_ARGUMENT, () =>
          client.controllerPublishVolume().sendMessage({
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
          })
        );
        sinon.assert.calledOnce(getVolumesStub);
        sinon.assert.calledWith(getVolumesStub, UUID);
        sinon.assert.notCalled(publishStub);
      });

      it('should not publish readonly volume', async () => {
        let volume = new Volume(UUID, registry, {});
        let publishStub = sinon.stub(volume, 'publish');
        publishStub.resolves();
        let getNodeNameStub = sinon.stub(volume, 'getNodeName');
        getNodeNameStub.returns('node');
        getVolumesStub.returns(volume);

        await shouldFailWith(GrpcCode.INVALID_ARGUMENT, () =>
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
        let volume = new Volume(UUID, registry, {});
        let publishStub = sinon.stub(volume, 'publish');
        publishStub.resolves();
        let getNodeNameStub = sinon.stub(volume, 'getNodeName');
        getNodeNameStub.returns('node');
        getVolumesStub.returns(volume);

        await shouldFailWith(GrpcCode.INVALID_ARGUMENT, () =>
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
        let volume = new Volume(UUID, registry, {});
        let publishStub = sinon.stub(volume, 'publish');
        publishStub.resolves();
        let getNodeNameStub = sinon.stub(volume, 'getNodeName');
        getNodeNameStub.returns('node');
        getVolumesStub.returns(volume);

        await shouldFailWith(GrpcCode.INVALID_ARGUMENT, () =>
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
      var server;

      before(async () => {
        server = await mockedServer();
      });

      after(async () => {
        if (server) {
          await server.stop();
          server = null;
        }
      });

      afterEach(() => {
        getVolumesStub.reset();
      });

      it('should not unpublish volume if it does not exist', async () => {
        getVolumesStub.returns(null);

        await shouldFailWith(GrpcCode.NOT_FOUND, () =>
          client.controllerUnpublishVolume().sendMessage({
            volumeId: UUID,
            nodeId: 'mayastor://node/10.244.2.15:10124',
          })
        );
      });

      it('should not unpublish volume on pool with invalid ID', async () => {
        let volume = new Volume(UUID, registry, {});
        let unpublishStub = sinon.stub(volume, 'unpublish');
        unpublishStub.resolves();
        let getNodeNameStub = sinon.stub(volume, 'getNodeName');
        getNodeNameStub.returns('node');
        getVolumesStub.returns(volume);

        await shouldFailWith(GrpcCode.INVALID_ARGUMENT, () =>
          client.controllerUnpublishVolume().sendMessage({
            volumeId: UUID,
            nodeId: 'mayastor2://node/10.244.2.15:10124',
          })
        );
      });

      it('should unpublish volume', async () => {
        let volume = new Volume(UUID, registry, {});
        let unpublishStub = sinon.stub(volume, 'unpublish');
        unpublishStub.resolves();
        let getNodeNameStub = sinon.stub(volume, 'getNodeName');
        getNodeNameStub.returns('node');
        getVolumesStub.returns(volume);

        await client.controllerUnpublishVolume().sendMessage({
          volumeId: UUID,
          nodeId: 'mayastor://node/10.244.2.15:10124',
        });

        sinon.assert.calledOnce(getVolumesStub);
        sinon.assert.calledWith(getVolumesStub, UUID);
        sinon.assert.calledOnce(unpublishStub);
      });

      it('should unpublish volume even if on a different node', async () => {
        let volume = new Volume(UUID, registry, {});
        let unpublishStub = sinon.stub(volume, 'unpublish');
        unpublishStub.resolves();
        let getNodeNameStub = sinon.stub(volume, 'getNodeName');
        getNodeNameStub.returns('node');
        getVolumesStub.returns(volume);

        await client.controllerUnpublishVolume().sendMessage({
          volumeId: UUID,
          nodeId: 'mayastor://another-node/10.244.2.15:10124',
        });

        sinon.assert.calledOnce(getVolumesStub);
        sinon.assert.calledWith(getVolumesStub, UUID);
        sinon.assert.calledOnce(unpublishStub);
      });
    });

    describe('ValidateVolumeCapabilities', function() {
      var server;

      before(async () => {
        server = await mockedServer();
      });

      after(async () => {
        if (server) {
          await server.stop();
          server = null;
        }
      });

      it('should report SINGLE_NODE_WRITER cap as valid', async () => {
        var volume = new Volume(UUID, registry, {});
        getVolumesStub.returns(volume);
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
        expect(resp.confirmed.volumeCapabilities).to.have.lengthOf(1);
        expect(resp.confirmed.volumeCapabilities[0].accessMode.mode).to.equal(
          'SINGLE_NODE_WRITER'
        );
        expect(resp.message).to.have.lengthOf(0);
      });

      it('should report other caps than SINGLE_NODE_WRITER as invalid', async () => {
        var volume = new Volume(UUID, registry, {});
        getVolumesStub.returns(volume);
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
        // jshint ignore:start
        expect(resp.confirmed).to.be.null;
        // jshint ignore:end
        expect(resp.message).to.match(/SINGLE_NODE_WRITER/);
      });

      it('should return error if volume does not exist', async () => {
        var volume = new Volume(UUID, registry, {});
        getVolumesStub.returns(null);
        await shouldFailWith(GrpcCode.NOT_FOUND, () =>
          client.validateVolumeCapabilities().sendMessage({
            volumeId: UUID,
            volumeCapabilities: [
              {
                accessMode: { mode: 'SINGLE_NODE_WRITER' },
                block: {},
              },
            ],
          })
        );
      });
    });

    describe('GetCapacity', function() {
      var server;

      before(async () => {
        server = await mockedServer();
      });

      after(async () => {
        if (server) {
          await server.stop();
          server = null;
        }
      });

      afterEach(() => {
        getCapacityStub.reset();
      });

      it('should get capacity of a single node with multiple pools', async () => {
        getCapacityStub.returns(75);
        var resp = await client.getCapacity().sendMessage({
          accessibleTopology: {
            segments: {
              'kubernetes.io/hostname': 'node1',
            },
          },
        });
        expect(resp.availableCapacity).to.equal(75);
        sinon.assert.calledOnce(getCapacityStub);
        sinon.assert.calledWith(getCapacityStub, 'node1');
      });

      it('should get capacity of all pools on all nodes', async () => {
        getCapacityStub.returns(80);
        var resp = await client.getCapacity().sendMessage({});
        expect(resp.availableCapacity).to.equal(80);
        sinon.assert.calledOnce(getCapacityStub);
        sinon.assert.calledWith(getCapacityStub, undefined);
      });
    });
  });
};
