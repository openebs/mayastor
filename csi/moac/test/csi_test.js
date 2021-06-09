// Unit tests for the CSI controller

'use strict';

/* eslint-disable no-unused-expressions */

const _ = require('lodash');
const expect = require('chai').expect;
const fs = require('fs').promises;
const grpc = require('@grpc/grpc-js');
const sinon = require('sinon');
const sleep = require('sleep-promise');
const EventEmitter = require('events');
const { CsiServer, csi } = require('../dist/csi');
const { GrpcError, grpcCode } = require('../dist/grpc_client');
const { Registry } = require('../dist/registry');
const { Volume } = require('../dist/volume');
const { Volumes } = require('../dist/volumes');
const { shouldFailWith } = require('./utils');

const SOCKPATH = '/tmp/csi_controller_test.sock';
// uuid used whenever we need some uuid and don't care about which one
const UUID = 'd01b8bfb-0116-47b0-a03a-447fcbdc0e99';
const UUID2 = 'a01b8bfb-0116-47b0-a03a-447fcbdc0e92';
const YAML_TRUE_VALUE = [
  'y', 'Y', 'yes', 'Yes', 'YES',
  'true', 'True', 'TRUE',
  'on', 'On', 'ON'
];

// Return gRPC CSI client for given csi service
function getCsiClient (svc) {
  const client = new csi[svc]('unix://' + SOCKPATH, grpc.credentials.createInsecure());
  // promisifying wrapper for calling api methods
  client.pcall = (method, args) => {
    return new Promise((resolve, reject) => {
      client[method](args, (err, res) => {
        if (err) reject(err);
        else resolve(res);
      });
    });
  };
  return client;
}

module.exports = function () {
  it('should start even if there is stale socket file', async () => {
    await fs.writeFile(SOCKPATH, 'blabla');
    const server = new CsiServer(SOCKPATH);
    await server.start();
    await server.stop();
    try {
      await fs.stat(SOCKPATH);
    } catch (err) {
      if (err.code === 'ENOENT') {
        return;
      }
      throw err;
    }
    throw new Error('Server did not clean up the socket file');
  });

  describe('identity', function () {
    let server;
    let client;

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
      const res = await client.pcall('getPluginInfo', {});
      // If you need to change any value of properties below, you will
      // need to change source code of csi node server too!
      expect(res.name).to.equal('io.openebs.csi-mayastor');
      expect(res.vendorVersion).to.equal('0.1');
      expect(Object.keys(res.manifest)).to.have.lengthOf(0);
    });

    it('get plugin capabilities', async () => {
      const res = await client.pcall('getPluginCapabilities', {});
      // If you need to change any capabilities below, you will
      // need to change source code of csi node server too!
      expect(res.capabilities).to.have.lengthOf(2);
      expect(res.capabilities[0].service.type).to.equal('CONTROLLER_SERVICE');
      expect(res.capabilities[1].service.type).to.equal(
        'VOLUME_ACCESSIBILITY_CONSTRAINTS'
      );
    });

    it('probe not ready', async () => {
      const res = await client.pcall('probe', {});
      expect(res.ready).to.have.property('value', false);
    });

    it('probe ready', async () => {
      server.makeReady({}, {});
      const res = await client.pcall('probe', {});
      expect(res.ready).to.have.property('value', true);
    });
  });

  describe('controller', function () {
    let client;
    let registry, volumes;
    let getCapacityStub, createVolumeStub, listVolumesStub, getVolumesStub, destroyVolumeStub;
    const volumeArgs = {
      replicaCount: 1,
      local: false,
      preferredNodes: [],
      requiredNodes: [],
      requiredBytes: 100,
      limitBytes: 20,
      protocol: 'nvmf'
    };

    async function mockedServer (pools, replicas, nexus) {
      const server = new CsiServer(SOCKPATH);
      await server.start();
      registry = new Registry({});
      volumes = new Volumes(registry);
      server.makeReady(registry, volumes);
      getCapacityStub = sinon.stub(registry, 'getCapacity');
      createVolumeStub = sinon.stub(volumes, 'createVolume');
      listVolumesStub = sinon.stub(volumes, 'list');
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

    describe('generic', function () {
      let server;

      afterEach(async () => {
        if (server) {
          await server.stop();
          server = null;
        }
      });

      it('should get controller capabilities', async () => {
        server = await mockedServer();
        const res = await client.pcall('controllerGetCapabilities', {});
        const caps = res.capabilities;
        expect(caps).to.have.lengthOf(4);
        expect(caps[0].rpc.type).to.equal('CREATE_DELETE_VOLUME');
        expect(caps[1].rpc.type).to.equal('PUBLISH_UNPUBLISH_VOLUME');
        expect(caps[2].rpc.type).to.equal('LIST_VOLUMES');
        expect(caps[3].rpc.type).to.equal('GET_CAPACITY');
      });

      it('should not get controller capabilities if not ready', async () => {
        server = await mockedServer();
        server.undoReady();
        await shouldFailWith(grpcCode.UNAVAILABLE, () =>
          client.pcall('controllerGetCapabilities', {})
        );
      });

      it('should return unimplemented error for CreateSnapshot', async () => {
        server = await mockedServer();
        await shouldFailWith(grpcCode.UNIMPLEMENTED, () =>
          client.pcall('createSnapshot', {
            sourceVolumeId: 'd01b8bfb-0116-47b0-a03a-447fcbdc0e99',
            name: 'blabla2'
          })
        );
      });

      it('should return unimplemented error for DeleteSnapshot', async () => {
        server = await mockedServer();
        await shouldFailWith(grpcCode.UNIMPLEMENTED, () =>
          client.pcall('deleteSnapshot', { snapshotId: 'blabla' })
        );
      });

      it('should return unimplemented error for ListSnapshots', async () => {
        server = await mockedServer();
        await shouldFailWith(grpcCode.UNIMPLEMENTED, () =>
          client.pcall('listSnapshots', {})
        );
      });

      it('should return unimplemented error for ControllerExpandVolume', async () => {
        server = await mockedServer();
        await shouldFailWith(grpcCode.UNIMPLEMENTED, () =>
          client.pcall('controllerExpandVolume', {
            volumeId: UUID,
            capacityRange: {
              requiredBytes: 200,
              limitBytes: 500
            }
          })
        );
      });
    });

    describe('CreateVolume', function () {
      let server;
      const defaultParams = { protocol: 'nvmf', repl: '1' };

      // place-holder for return value from createVolume when we don't care
      // if the input matches the output data (i.e. when testing error cases).
      function returnedVolume (params) {
        const vol = new Volume(UUID, registry, new EventEmitter(), {
          replicaCount: parseInt(params.repl) || 1,
          local: YAML_TRUE_VALUE.indexOf(params.local) >= 0,
          preferredNodes: [],
          requiredNodes: [],
          requiredBytes: 10,
          limitBytes: 20,
          protocol: params.protocol
        });
        sinon.stub(vol, 'getSize').returns(20);
        sinon.stub(vol, 'getNodeName').returns('some-node');
        sinon.stub(vol, 'getReplicas').callsFake(() => {
          const replicas = [];
          for (let i = 1; i <= vol.spec.replicaCount; i++) {
            // poor approximation of replica object, but it works
            replicas.push({
              pool: { node: { name: `node${i}` } }
            });
          }
          return replicas;
        });
        return vol;
      }

      beforeEach(async () => {
        server = await mockedServer();
      });

      afterEach(async () => {
        if (server) {
          await server.stop();
          server = null;
        }
      });

      it('should create a volume and return parameters in volume context', async () => {
        const parameters = { protocol: 'iscsi', repl: 3, local: 'true', blah: 'again' };
        createVolumeStub.resolves(returnedVolume(parameters));
        const result = await client.pcall('createVolume', {
          name: 'pvc-' + UUID,
          capacityRange: {
            requiredBytes: 10,
            limitBytes: 20
          },
          volumeCapabilities: [
            {
              accessMode: { mode: 'SINGLE_NODE_WRITER' },
              block: {}
            }
          ],
          parameters
        });
        // volume context is a of type map<string><string>
        const expected = {};
        for (const key in parameters) {
          expected[key] = parameters[key].toString();
        }
        expect(result.volume.volumeId).to.equal(UUID);
        expect(result.volume.capacityBytes).to.equal(20);
        expect(result.volume.volumeContext).to.eql(expected);
        expect(result.volume.accessibleTopology).to.have.lengthOf(3);
        sinon.assert.calledWith(createVolumeStub, UUID, {
          replicaCount: 3,
          local: true,
          preferredNodes: [],
          requiredNodes: [],
          requiredBytes: 10,
          limitBytes: 20,
          protocol: 'iscsi'
        });
      });

      it('should fail if topology requirement other than hostname', async () => {
        createVolumeStub.resolves(returnedVolume(defaultParams));
        await shouldFailWith(grpcCode.INVALID_ARGUMENT, () =>
          client.pcall('createVolume', {
            name: 'pvc-' + UUID,
            capacityRange: {
              requiredBytes: 10,
              limitBytes: 20
            },
            volumeCapabilities: [
              {
                accessMode: { mode: 'SINGLE_NODE_WRITER' },
                block: {}
              }
            ],
            accessibilityRequirements: {
              requisite: [{ segments: { rack: 'some-rack-info' } }],
              preferred: []
            },
            parameters: { protocol: 'nvmf' }
          })
        );
      });

      it('should fail if volume source', async () => {
        createVolumeStub.resolves(returnedVolume(defaultParams));
        await shouldFailWith(grpcCode.INVALID_ARGUMENT, () =>
          client.pcall('createVolume', {
            name: 'pvc-' + UUID,
            volumeContentSource: { volume: { volumeId: UUID } },
            capacityRange: {
              requiredBytes: 10,
              limitBytes: 20
            },
            volumeCapabilities: [
              {
                accessMode: { mode: 'SINGLE_NODE_WRITER' },
                block: {}
              }
            ],
            parameters: { protocol: 'nvmf' }
          })
        );
      });

      it('should fail if capability other than SINGLE_NODE_WRITER', async () => {
        createVolumeStub.resolves(returnedVolume(defaultParams));
        await shouldFailWith(grpcCode.INVALID_ARGUMENT, () =>
          client.pcall('createVolume', {
            name: 'pvc-' + UUID,
            capacityRange: {
              requiredBytes: 10,
              limitBytes: 20
            },
            volumeCapabilities: [
              {
                accessMode: { mode: 'SINGLE_NODE_READER_ONLY' },
                block: {}
              }
            ],
            parameters: { protocol: 'nvmf' }
          })
        );
      });

      it('should fail if grpc exception is thrown', async () => {
        createVolumeStub.rejects(
          new GrpcError(grpcCode.INTERNAL, 'Something went wrong')
        );
        await shouldFailWith(grpcCode.INTERNAL, () =>
          client.pcall('createVolume', {
            name: 'pvc-' + UUID,
            capacityRange: {
              requiredBytes: 10,
              limitBytes: 20
            },
            volumeCapabilities: [
              {
                accessMode: { mode: 'SINGLE_NODE_WRITER' },
                filesystem: {}
              }
            ],
            parameters: { protocol: 'nvmf' }
          })
        );
      });

      it('should fail if volume name is not in expected form', async () => {
        createVolumeStub.resolves(returnedVolume(defaultParams));
        await shouldFailWith(grpcCode.INVALID_ARGUMENT, () =>
          client.pcall('createVolume', {
            name: UUID, // missing pvc- prefix
            capacityRange: {
              requiredBytes: 10,
              limitBytes: 20
            },
            volumeCapabilities: [
              {
                accessMode: { mode: 'SINGLE_NODE_WRITER' },
                filesystem: {}
              }
            ],
            parameters: { protocol: 'nvmf' }
          })
        );
      });

      it('should fail if ioTimeout is used with protocol other than nvmf', async () => {
        const parameters = { protocol: 'iscsi', ioTimeout: '30' };
        createVolumeStub.resolves(returnedVolume(parameters));
        await shouldFailWith(grpcCode.INVALID_ARGUMENT, () =>
          client.pcall('createVolume', {
            name: 'pvc-' + UUID,
            capacityRange: {
              requiredBytes: 10,
              limitBytes: 20
            },
            volumeCapabilities: [
              {
                accessMode: { mode: 'SINGLE_NODE_WRITER' },
                filesystem: {}
              }
            ],
            parameters: {
              protocol: 'iscsi',
              ioTimeout: 30
            }
          })
        );
      });

      it('should fail if ioTimeout has invalid value', async () => {
        const parameters = { protocol: 'nvmf', ioTimeout: 'bla' };
        createVolumeStub.resolves(returnedVolume(parameters));
        await shouldFailWith(grpcCode.INVALID_ARGUMENT, () =>
          client.pcall('createVolume', {
            name: 'pvc-' + UUID,
            capacityRange: {
              requiredBytes: 10,
              limitBytes: 20
            },
            volumeCapabilities: [
              {
                accessMode: { mode: 'SINGLE_NODE_WRITER' },
                filesystem: {}
              }
            ],
            parameters: {
              protocol: 'nvmf',
              ioTimeout: 'non-sense'
            }
          })
        );
      });

      it('should fail if share protocol is not specified', async () => {
        const params = { ioTimeout: '30', local: 'On' };
        createVolumeStub.resolves(returnedVolume(params));
        await shouldFailWith(grpcCode.INVALID_ARGUMENT, () =>
          client.pcall('createVolume', {
            name: 'pvc-' + UUID,
            capacityRange: {
              requiredBytes: 10,
              limitBytes: 20
            },
            volumeCapabilities: [
              {
                accessMode: { mode: 'SINGLE_NODE_WRITER' },
                filesystem: {}
              }
            ],
            parameters: { ioTimeout: '60' }
          })
        );
      });

      it('should create volume on specified node', async () => {
        const params = { protocol: 'nvmf', local: 'Y' };
        createVolumeStub.resolves(returnedVolume(params));
        const result = await client.pcall('createVolume', {
          name: 'pvc-' + UUID,
          capacityRange: {
            requiredBytes: 50,
            limitBytes: 0
          },
          volumeCapabilities: [
            {
              accessMode: { mode: 'SINGLE_NODE_WRITER' },
              filesystem: {}
            }
          ],
          accessibilityRequirements: {
            requisite: [{ segments: { 'kubernetes.io/hostname': 'node' } }]
          },
          parameters: params
        });
        expect(result.volume.volumeId).to.equal(UUID);
        expect(result.volume.accessibleTopology).to.have.lengthOf(1);
        expect(result.volume.accessibleTopology[0].segments['kubernetes.io/hostname']).to.equal('node1');
        sinon.assert.calledWith(createVolumeStub, UUID, {
          replicaCount: 1,
          local: true,
          preferredNodes: [],
          requiredNodes: ['node'],
          requiredBytes: 50,
          limitBytes: 0,
          protocol: 'nvmf'
        });
      });

      it('should create volume on preferred node', async () => {
        const params = { protocol: 'nvmf', local: 'No' };
        createVolumeStub.resolves(returnedVolume(params));
        await client.pcall('createVolume', {
          name: 'pvc-' + UUID,
          capacityRange: {
            requiredBytes: 50,
            limitBytes: 50
          },
          volumeCapabilities: [
            {
              accessMode: { mode: 'SINGLE_NODE_WRITER' },
              block: {}
            }
          ],
          accessibilityRequirements: {
            preferred: [
              {
                segments: {
                  // should ignore unknown segment if preferred
                  rack: 'some-rack-info',
                  'kubernetes.io/hostname': 'node'
                }
              }
            ]
          },
          parameters: params
        });
        sinon.assert.calledWith(createVolumeStub, UUID, {
          replicaCount: 1,
          local: false,
          preferredNodes: ['node'],
          requiredNodes: [],
          requiredBytes: 50,
          limitBytes: 50,
          protocol: 'nvmf'
        });
      });

      it('should create volume with specified number of replicas', async () => {
        const params = { repl: '3', protocol: 'nvmf' };
        createVolumeStub.resolves(returnedVolume(params));
        await client.pcall('createVolume', {
          name: 'pvc-' + UUID,
          capacityRange: {
            requiredBytes: 50,
            limitBytes: 70
          },
          volumeCapabilities: [
            {
              accessMode: { mode: 'SINGLE_NODE_WRITER' },
              block: {}
            }
          ],
          parameters: params
        });
        sinon.assert.calledWith(createVolumeStub, UUID, {
          replicaCount: 3,
          local: false,
          preferredNodes: [],
          requiredNodes: [],
          requiredBytes: 50,
          limitBytes: 70,
          protocol: 'nvmf'
        });
      });

      it('should fail if number of replicas is not a number', async () => {
        createVolumeStub.resolves(returnedVolume(defaultParams));
        await shouldFailWith(grpcCode.INVALID_ARGUMENT, () =>
          client.pcall('createVolume', {
            name: 'pvc-' + UUID,
            capacityRange: {
              requiredBytes: 50,
              limitBytes: 70
            },
            volumeCapabilities: [
              {
                accessMode: { mode: 'SINGLE_NODE_WRITER' },
                block: {}
              }
            ],
            parameters: { repl: 'bla2', protocol: 'nvmf' }
          })
        );
      });

      it('should serialize all requests and detect duplicates', (done) => {
        // We must sleep in the stub. Otherwise reply is sent before the second
        // request comes in.
        this.timeout(1000);
        const delay = 50;
        createVolumeStub.callsFake(async () => {
          await sleep(delay);
          return returnedVolume(defaultParams);
        });
        const create1 = client.pcall('createVolume', {
          name: 'pvc-' + UUID,
          capacityRange: {
            requiredBytes: 50,
            limitBytes: 70
          },
          volumeCapabilities: [
            {
              accessMode: { mode: 'SINGLE_NODE_WRITER' },
              block: {}
            }
          ],
          parameters: { repl: '3', protocol: 'nvmf' }
        });
        const create2 = client.pcall('createVolume', {
          name: 'pvc-' + UUID2,
          capacityRange: {
            requiredBytes: 50,
            limitBytes: 70
          },
          volumeCapabilities: [
            {
              accessMode: { mode: 'SINGLE_NODE_WRITER' },
              block: {}
            }
          ],
          parameters: { repl: '3', protocol: 'nvmf' }
        });
        const create3 = client.pcall('createVolume', {
          name: 'pvc-' + UUID,
          capacityRange: {
            requiredBytes: 50,
            limitBytes: 70
          },
          volumeCapabilities: [
            {
              accessMode: { mode: 'SINGLE_NODE_WRITER' },
              block: {}
            }
          ],
          parameters: { repl: '3', protocol: 'nvmf' }
        });
        const start = new Date();
        Promise.all([create1, create2, create3]).then((results) => {
          expect(results).to.have.lengthOf(3);
          expect(results[0].volume.volumeId).to.equal(UUID);
          expect(results[1].volume.volumeId).to.equal(UUID2);
          expect(results[2].volume.volumeId).to.equal(UUID);
          sinon.assert.calledTwice(createVolumeStub);
          expect(new Date() - start).to.be.above(2 * delay - 1);
          done();
        });
      });
    });

    describe('DeleteVolume', function () {
      let server;

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

        await client.pcall('deleteVolume', { volumeId: UUID });

        sinon.assert.calledOnce(destroyVolumeStub);
        sinon.assert.calledWith(destroyVolumeStub, UUID);
      });

      it('should fail if backend grpc call fails', async () => {
        destroyVolumeStub.rejects(
          new GrpcError(grpcCode.INTERNAL, 'Something went wrong')
        );

        await shouldFailWith(grpcCode.INTERNAL, () =>
          client.pcall('deleteVolume', { volumeId: UUID })
        );

        sinon.assert.calledOnce(destroyVolumeStub);
      });

      it('should detect duplicate delete volume request', (done) => {
        // We must sleep in the stub. Otherwise reply is sent before the second
        // request comes in.
        destroyVolumeStub.callsFake(async () => {
          await sleep(10);
        });
        const delete1 = client.pcall('deleteVolume', { volumeId: UUID });
        const delete2 = client.pcall('deleteVolume', { volumeId: UUID });
        Promise.all([delete1, delete2]).then((results) => {
          sinon.assert.calledOnce(destroyVolumeStub);
          expect(results).to.have.lengthOf(2);
          done();
        });
      });
    });

    describe('ListVolumes', function () {
      let server;
      // uuid except the last two digits
      const uuidBase = '4334cc8a-2fed-45ed-866f-3716639db5';

      // Create army of volumes (100)
      before(async () => {
        const vols = [];
        for (let i = 0; i < 10; i++) {
          for (let j = 0; j < 10; j++) {
            const vol = new Volume(uuidBase + i + j, registry, new EventEmitter(), {
              replicaCount: 3,
              local: false,
              preferredNodes: [],
              requiredNodes: [],
              requiredBytes: 100,
              limitBytes: 20,
              protocol: 'nvmf'
            });
            const getSizeStub = sinon.stub(vol, 'getSize');
            getSizeStub.returns(100);
            const getNodeName = sinon.stub(vol, 'getNodeName');
            getNodeName.returns('node');
            vols.push(vol);
          }
        }
        server = await mockedServer();
        listVolumesStub.returns(vols);
      });

      after(async () => {
        if (server) {
          await server.stop();
          server = null;
        }
      });

      it('should list all volumes', async () => {
        const resp = await client.pcall('listVolumes', {});
        expect(resp.nextToken).to.be.empty;
        const vols = resp.entries.map((ent) => ent.volume);
        expect(vols).to.have.lengthOf(100);
        for (let i = 0; i < 10; i++) {
          for (let j = 0; j < 10; j++) {
            expect(vols[10 * i + j].volumeId).to.equal(uuidBase + i + j);
          }
        }
      });

      it('should list volumes page by page', async () => {
        const pageSize = 17;
        let next;
        let allVols = [];

        do {
          const resp = await client.pcall('listVolumes', {
            maxEntries: pageSize,
            startingToken: next
          });
          const vols = resp.entries.map((ent) => ent.volume);
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
        await shouldFailWith(grpcCode.INVALID_ARGUMENT, () =>
          client.pcall('listVolumes', { startingToken: 'asdfquwer' })
        );
      });
    });

    describe('ControllerPublishVolume', function () {
      let server;

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
        const nvmfUri = `nvmf://host/nqn-${UUID}`;
        const volume = new Volume(UUID, registry, new EventEmitter(), volumeArgs);
        const publishStub = sinon.stub(volume, 'publish');
        publishStub.resolves(nvmfUri);
        const getNodeNameStub = sinon.stub(volume, 'getNodeName');
        getNodeNameStub.returns('node');
        getVolumesStub.returns(volume);

        const reply = await client.pcall('controllerPublishVolume', {
          volumeId: UUID,
          nodeId: 'mayastor://node2',
          readonly: false,
          volumeCapability: {
            accessMode: { mode: 'SINGLE_NODE_WRITER' },
            mount: {
              fsType: 'xfs',
              mount_flags: 'ro'
            }
          },
          volumeContext: {
            protocol: 'nvmf',
            ioTimeout: 0
          }
        });
        expect(reply.publishContext.uri).to.equal(nvmfUri);
        expect(reply.publishContext.ioTimeout).to.equal('0');
        sinon.assert.calledOnce(getVolumesStub);
        sinon.assert.calledWith(getVolumesStub, UUID);
        sinon.assert.calledOnce(publishStub);
        sinon.assert.calledWith(publishStub, 'node2');
      });

      it('should serialize all requests and detect duplicates', (done) => {
        const delay = 50;
        const iscsiUri = `iscsi://host/iqn-${UUID}`;
        const publishArgs = {
          volumeId: UUID,
          nodeId: 'mayastor://node2',
          readonly: false,
          volumeCapability: {
            accessMode: { mode: 'SINGLE_NODE_WRITER' },
            mount: {
              fsType: 'xfs',
              mount_flags: 'ro'
            }
          },
          volumeContext: { protocol: 'iscsi' }
        };
        const publishArgs2 = _.clone(publishArgs);
        publishArgs.volumeId = UUID2;
        const volume = new Volume(UUID, registry, new EventEmitter(), volumeArgs);
        const publishStub = sinon.stub(volume, 'publish');
        // We must sleep in the stub. Otherwise reply is sent before the second
        // request comes in.
        publishStub.callsFake(async () => {
          await sleep(delay);
          return iscsiUri;
        });
        const getNodeNameStub = sinon.stub(volume, 'getNodeName');
        getNodeNameStub.returns('node');
        getVolumesStub.returns(volume);

        const publish1 = client.pcall('controllerPublishVolume', publishArgs);
        const publish2 = client.pcall('controllerPublishVolume', publishArgs2);
        const publish3 = client.pcall('controllerPublishVolume', publishArgs);
        const start = new Date();
        Promise.all([publish1, publish2, publish3]).then((results) => {
          expect(results).to.have.lengthOf(3);
          expect(results[0].publishContext.uri).to.equal(iscsiUri);
          expect(results[1].publishContext.uri).to.equal(iscsiUri);
          expect(results[2].publishContext.uri).to.equal(iscsiUri);
          sinon.assert.calledTwice(publishStub);
          expect(new Date() - start).to.be.above(2 * delay - 1);
          done();
        });
      });

      it('should not publish volume if it does not exist', async () => {
        getVolumesStub.returns();

        await shouldFailWith(grpcCode.NOT_FOUND, () =>
          client.pcall('controllerPublishVolume', {
            volumeId: UUID,
            nodeId: 'mayastor://node',
            readonly: false,
            volumeCapability: {
              accessMode: { mode: 'SINGLE_NODE_WRITER' },
              mount: {
                fsType: 'xfs',
                mount_flags: 'ro'
              }
            },
            volumeContext: { protocol: 'nvmf' }
          })
        );
        sinon.assert.calledOnce(getVolumesStub);
        sinon.assert.calledWith(getVolumesStub, UUID);
      });

      it('should not publish readonly volume', async () => {
        const volume = new Volume(UUID, registry, new EventEmitter(), volumeArgs);
        const publishStub = sinon.stub(volume, 'publish');
        publishStub.resolves();
        const getNodeNameStub = sinon.stub(volume, 'getNodeName');
        getNodeNameStub.returns('node');
        getVolumesStub.returns(volume);

        await shouldFailWith(grpcCode.INVALID_ARGUMENT, () =>
          client.pcall('controllerPublishVolume', {
            volumeId: UUID,
            nodeId: 'mayastor://node',
            readonly: true,
            volumeCapability: {
              accessMode: { mode: 'SINGLE_NODE_WRITER' },
              mount: {
                fsType: 'xfs',
                mount_flags: 'ro'
              }
            },
            volumeContext: { protocol: 'nvmf' }
          })
        );
      });

      it('should not publish volume with unsupported capability', async () => {
        const volume = new Volume(UUID, registry, new EventEmitter(), volumeArgs);
        const publishStub = sinon.stub(volume, 'publish');
        publishStub.resolves();
        const getNodeNameStub = sinon.stub(volume, 'getNodeName');
        getNodeNameStub.returns('node');
        getVolumesStub.returns(volume);

        await shouldFailWith(grpcCode.INVALID_ARGUMENT, () =>
          client.pcall('controllerPublishVolume', {
            volumeId: UUID,
            nodeId: 'mayastor://node',
            readonly: false,
            volumeCapability: {
              accessMode: { mode: 'SINGLE_NODE_READER_ONLY' },
              mount: {
                fsType: 'xfs',
                mount_flags: 'ro'
              }
            },
            volumeContext: { protocol: 'nvmf' }
          })
        );
      });

      it('should not publish volume on node with invalid ID', async () => {
        const volume = new Volume(UUID, registry, new EventEmitter(), volumeArgs);
        const publishStub = sinon.stub(volume, 'publish');
        publishStub.resolves();
        const getNodeNameStub = sinon.stub(volume, 'getNodeName');
        getNodeNameStub.returns('node');
        getVolumesStub.returns(volume);

        await shouldFailWith(grpcCode.INVALID_ARGUMENT, () =>
          client.pcall('controllerPublishVolume', {
            volumeId: UUID,
            nodeId: 'mayastor2://node/10.244.2.15:10124',
            readonly: false,
            volumeCapability: {
              accessMode: { mode: 'SINGLE_NODE_WRITER' },
              mount: {
                fsType: 'xfs',
                mount_flags: 'ro'
              }
            },
            volumeContext: { protocol: 'nvmf' }
          })
        );
      });
    });

    describe('ControllerUnpublishVolume', function () {
      let server;

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

      it('should not return an error on unpublish volume if it does not exist', async () => {
        getVolumesStub.returns(null);

        const error = await client.pcall('controllerUnpublishVolume', {
          volumeId: UUID,
          nodeId: 'mayastor://node'
        });

        expect(error).is.empty;
      });

      it('should not unpublish volume on pool with invalid ID', async () => {
        const volume = new Volume(UUID, registry, new EventEmitter(), volumeArgs);
        const unpublishStub = sinon.stub(volume, 'unpublish');
        unpublishStub.resolves();
        const getNodeNameStub = sinon.stub(volume, 'getNodeName');
        getNodeNameStub.returns('node');
        getVolumesStub.returns(volume);

        await shouldFailWith(grpcCode.INVALID_ARGUMENT, () =>
          client.pcall('controllerUnpublishVolume', {
            volumeId: UUID,
            nodeId: 'mayastor2://node/10.244.2.15:10124'
          })
        );
      });

      it('should unpublish volume', async () => {
        const volume = new Volume(UUID, registry, new EventEmitter(), volumeArgs);
        const unpublishStub = sinon.stub(volume, 'unpublish');
        unpublishStub.resolves();
        const getNodeNameStub = sinon.stub(volume, 'getNodeName');
        getNodeNameStub.returns('node');
        getVolumesStub.returns(volume);

        await client.pcall('controllerUnpublishVolume', {
          volumeId: UUID,
          nodeId: 'mayastor://node'
        });

        sinon.assert.calledOnce(getVolumesStub);
        sinon.assert.calledWith(getVolumesStub, UUID);
        sinon.assert.calledOnce(unpublishStub);
      });

      it('should unpublish volume even if on a different node', async () => {
        const volume = new Volume(UUID, registry, new EventEmitter(), volumeArgs);
        const unpublishStub = sinon.stub(volume, 'unpublish');
        unpublishStub.resolves();
        const getNodeNameStub = sinon.stub(volume, 'getNodeName');
        getNodeNameStub.returns('node');
        getVolumesStub.returns(volume);

        await client.pcall('controllerUnpublishVolume', {
          volumeId: UUID,
          nodeId: 'mayastor://another-node'
        });

        sinon.assert.calledOnce(getVolumesStub);
        sinon.assert.calledWith(getVolumesStub, UUID);
        sinon.assert.calledOnce(unpublishStub);
      });

      it('should detect duplicate unpublish volume request', (done) => {
        const unpublishArgs = {
          volumeId: UUID,
          nodeId: 'mayastor://another-node'
        };
        const volume = new Volume(UUID, registry, new EventEmitter(), volumeArgs);
        const unpublishStub = sinon.stub(volume, 'unpublish');
        // We must sleep in the stub. Otherwise reply is sent before the second
        // request comes in.
        unpublishStub.callsFake(async () => {
          await sleep(10);
        });
        const getNodeNameStub = sinon.stub(volume, 'getNodeName');
        getNodeNameStub.returns('node');
        getVolumesStub.returns(volume);

        const unpublish1 = client.pcall('controllerUnpublishVolume', unpublishArgs);
        const unpublish2 = client.pcall('controllerUnpublishVolume', unpublishArgs);
        Promise.all([unpublish1, unpublish2]).then((results) => {
          sinon.assert.calledOnce(unpublishStub);
          expect(results).to.have.lengthOf(2);
          done();
        });
      });
    });

    describe('ValidateVolumeCapabilities', function () {
      let server;

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
        const volume = new Volume(UUID, registry, new EventEmitter(), volumeArgs);
        getVolumesStub.returns(volume);
        const caps = [
          'SINGLE_NODE_WRITER',
          'SINGLE_NODE_READER_ONLY',
          'MULTI_NODE_READER_ONLY',
          'MULTI_NODE_SINGLE_WRITER',
          'MULTI_NODE_MULTI_WRITER'
        ];
        const resp = await client.pcall('validateVolumeCapabilities', {
          volumeId: UUID,
          volumeCapabilities: caps.map((c) => {
            return {
              accessMode: { mode: c },
              block: {}
            };
          })
        });
        expect(resp.confirmed.volumeCapabilities).to.have.lengthOf(1);
        expect(resp.confirmed.volumeCapabilities[0].accessMode.mode).to.equal(
          'SINGLE_NODE_WRITER'
        );
        expect(resp.message).to.have.lengthOf(0);
      });

      it('should report other caps than SINGLE_NODE_WRITER as invalid', async () => {
        const volume = new Volume(UUID, registry, new EventEmitter(), volumeArgs);
        getVolumesStub.returns(volume);
        const caps = [
          'SINGLE_NODE_READER_ONLY',
          'MULTI_NODE_READER_ONLY',
          'MULTI_NODE_SINGLE_WRITER',
          'MULTI_NODE_MULTI_WRITER'
        ];
        const resp = await client.pcall('validateVolumeCapabilities', {
          volumeId: UUID,
          volumeCapabilities: caps.map((c) => {
            return {
              accessMode: { mode: c },
              block: {}
            };
          })
        });
        expect(resp.confirmed).to.be.null;
        expect(resp.message).to.match(/SINGLE_NODE_WRITER/);
      });

      it('should return error if volume does not exist', async () => {
        getVolumesStub.returns(null);
        await shouldFailWith(grpcCode.NOT_FOUND, () =>
          client.pcall('validateVolumeCapabilities', {
            volumeId: UUID,
            volumeCapabilities: [
              {
                accessMode: { mode: 'SINGLE_NODE_WRITER' },
                block: {}
              }
            ]
          })
        );
      });
    });

    describe('GetCapacity', function () {
      let server;

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
        const resp = await client.pcall('getCapacity', {
          accessibleTopology: {
            segments: {
              'kubernetes.io/hostname': 'node1'
            }
          }
        });
        expect(resp.availableCapacity).to.equal(75);
        sinon.assert.calledOnce(getCapacityStub);
        sinon.assert.calledWith(getCapacityStub, 'node1');
      });

      it('should get capacity of all pools on all nodes', async () => {
        getCapacityStub.returns(80);
        const resp = await client.pcall('getCapacity', {});
        expect(resp.availableCapacity).to.equal(80);
        sinon.assert.calledOnce(getCapacityStub);
        sinon.assert.calledWith(getCapacityStub, undefined);
      });
    });
  });
};
