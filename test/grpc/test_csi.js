// Test CSI gRPC services of mayastor.
//
// It used to be possible to start this test suite against external mayastor
// instance to verify it. But later we dropped this feature because stage and
// publish volume tests became really unsuitable for this type of operation.
// We could split the test suite in future if we want this functionality at
// least for some tests where it is possible to do.
//
// It is a mess to work with nbd devices. If nbd device is attached to kernel
// then detached and immediately attached again we see all kinds of issues.
// That's why we use a different nbd device for each stage operation so that
// we don't confuse the kernel :-(

'use strict';

const URL = require('url').URL;
const assert = require('chai').assert;
const async = require('async');
const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');
const protoLoader = require('@grpc/proto-loader');
// we can't use grpc-kit because we need to connect to UDS and that's currently
// possible only with grpc-uds.
const grpc = require('grpc-uds');
const common = require('./test_common');
const enums = require('./grpc_enums');

const csiSock = common.CSI_ENDPOINT;

// One big malloc bdev which we put lvol store on.
const CONFIG = `
sync_disable: true
base_bdevs:
  - uri: "malloc:///malloc0?size_mb=64&uuid=11111111-0000-0000-0000-000000000000&blk_size=4096"
  - uri: "malloc:///malloc1?size_mb=64&uuid=11111111-0000-0000-0000-000000000001&blk_size=4096"
  - uri: "malloc:///malloc2?size_mb=64&uuid=11111111-0000-0000-0000-000000000002&blk_size=4096"
  - uri: "malloc:///malloc3?size_mb=64&uuid=11111111-0000-0000-0000-000000000003&blk_size=4096"
  - uri: "malloc:///malloc4?size_mb=64&uuid=11111111-0000-0000-0000-000000000004&blk_size=4096"
`;
// uuid without the last digit
const BASE_UUID = '11111111-0000-0000-0000-00000000000';
// used UUID aliases
const UUID1 = BASE_UUID + '0';
const UUID2 = BASE_UUID + '1';
const UUID3 = BASE_UUID + '2';
const UUID4 = BASE_UUID + '3';
const UUID5 = BASE_UUID + '4';

function createCsiClient (service) {
  const pkgDef = grpc.loadPackageDefinition(
    protoLoader.loadSync(
      path.join(__dirname, '..', '..', 'csi', 'proto', 'csi.proto'),
      {
        // this is to load google/descriptor.proto
        includeDirs: ['./node_modules/protobufjs'],
        keepCase: true,
        longs: String,
        enums: String,
        defaults: true,
        oneofs: true
      }
    )
  );
  const proto = pkgDef.csi.v1;
  return new proto[service](csiSock, grpc.credentials.createInsecure());
}

function cleanPublishDir (mountTarget, done) {
  const proc = common.runAsRoot('umount', ['-f', mountTarget]);
  proc.once('close', (code, signal) => {
    try {
      fs.rmdirSync(mountTarget);
    } catch (err) {}

    done();
  });
}

function createPublishDir (mountTarget) {
  fs.mkdirSync(mountTarget);
}

function cleanBlockMount (blockfile, done) {
  common.execAsRoot('umount', ['-f', blockfile], () => {
    common.execAsRoot('rm', ['-f', blockfile], () => {
      done();
    });
  });
}

function cleanupiSCSISession (tp, iqn, done) {
  common.execAsRoot('iscsiadm', ['--mode', 'node', '--targetname', iqn, '--portal', tp, '--logout'], () => {
    common.execAsRoot('iscsiadm', ['-m', 'node', '-o', 'delete', '-T', iqn], () => {
      done();
    });
  });
}

function cleanupNvmfSession (nqn, done) {
  common.execAsRoot('nvme', ['disconnect', nqn], () => {
    done();
  });
}

function cleanupNexusSession (url, done) {
  if (url.protocol === 'iscsi:') {
    const tp = url.host;
    const iqn = url.pathname.split('/')[1];
    cleanupiSCSISession(tp, iqn, done);
  } else if (url.protocol === 'nvmf:') {
    const nqn = url.pathname.split('/')[1];
    cleanupNvmfSession(nqn, done);
  } else {
    done();
  }
}

// Returns a callback which verifies that method ended with given grpc error.
function shouldFailWith (code, done) {
  return function (err, res) {
    if (err) {
      assert.equal(err.code, code);
      done();
    } else {
      done(new Error('Succeeded but expected to fail with ' + code));
    }
  };
}

// Get filesystem type for given mount point.
function getFsType (mp) {
  const lines = execSync('mount')
    .toString()
    .trim()
    .split('\n');
  for (let i = 0; i < lines.length; i++) {
    const cols = lines[i].split(' ');
    if (mp === cols[2]) {
      return cols[4];
    }
  }
}

describe('csi', function () {
  this.timeout(10000); // for network tests we need long timeouts

  // Start mayastor and create the lvol configuration needed for testing.
  // NOTE: Don't use mayastor in setup - we test CSI interface and we don't want
  // to depend on correct function of mayastor iface in order to test CSI.
  before((done) => {
    common.startMayastor(CONFIG);
    common.startMayastorCsi();

    const client = common.createGrpcClient();

    async.series(
      [
        (next) => {
          common.waitFor((pingDone) => {
            // fix the perms now - we can't do that before because it takes
            // time to csi-agent to create it ..
            common.fixSocketPerms((err) => {
              if (err) {
                return pingDone(err);
              }
              // use harmless method to test if the mayastor is up and running
              client.listPools({}, pingDone);
            });
          }, next);
        },
        (next) => {
          async.times(
            5,
            function (n, next) {
              const uuid = BASE_UUID + n;
              client.createNexus(
                {
                  uuid: uuid,
                  size: 64 * 1024 * 1024,
                  children: ['bdev:///malloc' + n]
                },
                next
              );
            },
            next
          );
        }
      ], (err) => {
        client.close();
        done(err);
      });
  });

  // stop mayastor server if it was started by us
  after((done) => {
    async.series(
      [
        (next) => {
          common.stopAll(next);
        },
        (next) => {
          common.stopAll(next);
        }
      ],
      done
    );
  });

  describe('general', function () {
    it('should start even if there is a stale csi socket file', (done) => {
      const client = createCsiClient('Identity');

      async.series(
        [
          (next) => {
            common.restartMayastorCsi((pingDone) => {
            // fix the perms now - we can't do that before because it takes
            // time to csi-agent to create it ..
              common.fixSocketPerms((err) => {
                if (err) {
                  return pingDone(err);
                }
                // use harmless method to test if it is up and running
                client.probe({}, pingDone);
              });
            }, next);
          }
        ],
        done
      );
    });
  });

  describe('identity', function () {
    let client;

    before(() => {
      client = createCsiClient('Identity');
    });

    after(() => {
      if (client != null) {
        client.close();
      }
    });

    it('probe', (done) => {
      client.probe({}, (err, res) => {
        if (err) return done(err);
        assert.equal(res.ready.value, true);
        done();
      });
    });

    it('get plugin info', (done) => {
      client.getPluginInfo({}, (err, res) => {
        if (err) return done(err);
        // If you need to change values of any properties here,
        // you must change the moac's csi server code as well!
        assert.equal(res.name, 'io.openebs.csi-mayastor');
        assert.equal(res.vendor_version, '0.2');
        assert.lengthOf(Object.keys(res.manifest), 0);
        done();
      });
    });

    it('get plugin capabilities', (done) => {
      client.getPluginCapabilities({}, (err, res) => {
        if (err) return done(err);
        // If you need to change any capabilities here,
        // you must change the moac's csi server code as well!
        assert.lengthOf(res.capabilities, 2);
        assert.equal(res.capabilities[0].service.type, 'CONTROLLER_SERVICE');
        assert.equal(
          res.capabilities[1].service.type,
          'VOLUME_ACCESSIBILITY_CONSTRAINTS'
        );
        done();
      });
    });
  });

  describe('node', function () {
    let client;

    before(() => {
      client = createCsiClient('Node');
    });

    after(() => {
      if (client != null) {
        client.close();
      }
    });

    it('get info', (done) => {
      client.nodeGetInfo({}, (err, res) => {
        if (err) return done(err);
        assert.equal(
          res.node_id,
          'mayastor://' + common.CSI_ID
        );

        assert.isAbove(
          parseInt(res.max_volumes_per_node, 10),
          1,
          'number of nbd devices should be above 1'
        );
        done();
      });
    });

    it('get capabilities', (done) => {
      client.nodeGetCapabilities({}, (err, res) => {
        if (err) return done(err);
        assert.lengthOf(res.capabilities, 1);
        assert.equal(res.capabilities[0].type, 'rpc');
        assert.equal(res.capabilities[0].rpc.type, 'STAGE_UNSTAGE_VOLUME');
        done();
      });
    });
  });

  csiProtocolTest('NBD', enums.NEXUS_NBD, 10000);
  csiProtocolTest('iSCSI', enums.NEXUS_ISCSI, 120000);
  csiProtocolTest('NVMF', enums.NEXUS_NVMF, 120000);
});

function csiProtocolTest (protoname, shareType, timeoutMillis) {
  describe(protoname, function () {
    this.timeout(timeoutMillis); // for network tests we need long timeouts
    const publishedUris = {};

    // Start mayastor and create the lvol configuration needed for testing.
    // NOTE: Don't use mayastor in setup - we test CSI interface and we don't want
    // to depend on correct function of mayastor iface in order to test CSI.
    before((done) => {
      const client = common.createGrpcClient();
      async.times(
        5,
        (n, next) => {
          const uuid = BASE_UUID + n;
          client.publishNexus(
            {
              uuid: uuid,
              key: '',
              share: shareType
            },
            next
          );
        },
        (err, results) => {
          client.close();
          if (err) {
            return done(err);
          }
          for (const n in results) {
            const uuid = BASE_UUID + n;
            // stash the published URIs in a map indexed
            // on the uuid of the volume.
            publishedUris[uuid] = { uri: results[n].device_uri };
          }
          done();
        }
      );
    });

    // stop mayastor server if it was started by us
    after((done) => {
      const client = common.createGrpcClient();
      async.times(
        5,
        function (n, next) {
          const uuid = BASE_UUID + n;
          cleanupNexusSession(new URL(publishedUris[uuid].uri), function () {
            client.unpublishNexus({ uuid: uuid }, next);
          });
        },
        function () {
          client.close();
          done();
        }
      );
    });

    describe('stage and unstage xfs volume', function () {
      let client;
      const mountTarget = '/tmp/target0';

      // get default args for stage op with xfs fs
      function getDefaultArgs () {
        return {
          volume_id: UUID1,
          publish_context: publishedUris[UUID1],
          staging_target_path: mountTarget,
          volume_capability: {
            access_mode: {
              mode: 'MULTI_NODE_READER_ONLY'
            },
            mount: {
              fs_type: 'xfs'
            }
          },
          readonly: false,
          secrets: {},
          volume_context: {}
        };
      }

      before((done) => {
        client = createCsiClient('Node');
        cleanPublishDir(mountTarget, () => {
          createPublishDir(mountTarget);
          done();
        });
      });

      after((done) => {
        if (client != null) {
          client.close();
        }
        cleanPublishDir(mountTarget, done);
      });

      it('should be able to stage volume (xfs)', (done) => {
        client.nodeStageVolume(getDefaultArgs(), (err) => {
          if (err) return done(err);
          assert.equal(getFsType(mountTarget), 'xfs');
          done();
        });
      });

      it('get volume stats', (done) => {
        client.nodeGetVolumeStats(
          {
            volume_id: UUID1,
            volume_path: mountTarget
          },
          shouldFailWith(grpc.status.UNIMPLEMENTED, done)
        );
      });

      it('staging the same volume again should return ok (idempotent)', (done) => {
        client.nodeStageVolume(getDefaultArgs(), done);
      });

      it('staging a volume with the same staging path but with a different bdev should fail', (done) => {
        const args = getDefaultArgs();
        args.volume_id = UUID2;
        args.publish_context = publishedUris[UUID2];

        client.nodeStageVolume(
          args,
          shouldFailWith(grpc.status.ALREADY_EXISTS, done)
        );
      });

      it('should fail to stage a volume with the bdev using a different target path', (done) => {
        const args = getDefaultArgs();
        args.staging_target_path = '/tmp/hello_world';
        client.nodeStageVolume(
          args,
          shouldFailWith(grpc.status.ALREADY_EXISTS, done)
        );
      });

      it('should fail to stage a volume with a missing volume ID', (done) => {
        const args = getDefaultArgs();
        delete args.volume_id;
        client.nodeStageVolume(
          args,
          shouldFailWith(grpc.status.INVALID_ARGUMENT, done)
        );
      });

      it('should fail to stage a volume with a missing stage target path', (done) => {
        const args = getDefaultArgs();
        delete args.staging_target_path;
        client.nodeStageVolume(
          args,
          shouldFailWith(grpc.status.INVALID_ARGUMENT, done)
        );
      });

      it('should fail to stage a volume with missing access type', (done) => {
        const args = getDefaultArgs();
        delete args.volume_capability.mount;
        client.nodeStageVolume(
          args,
          shouldFailWith(grpc.status.INVALID_ARGUMENT, done)
        );
      });

      it('should fail to stage a volume with missing access mode', (done) => {
        const args = getDefaultArgs();
        args.volume_capability.access_mode = {};
        client.nodeStageVolume(
          args,
          shouldFailWith(grpc.status.INVALID_ARGUMENT, done)
        );
      });

      it('should fail to stage a volume with missing volume_capability section', (done) => {
        const args = getDefaultArgs();
        delete args.volume_capability;
        client.nodeStageVolume(
          args,
          shouldFailWith(grpc.status.INVALID_ARGUMENT, done)
        );
      });

      it('should be able to unstage volume (xfs)', (done) => {
        client.nodeUnstageVolume(
          {
            volume_id: UUID1,
            staging_target_path: mountTarget
          },
          (err) => {
            if (err) return done(err);
            assert.isUndefined(getFsType(mountTarget));
            done();
          }
        );
      });
    });

    describe('stage and unstage ext4 volume', function () {
      let client;
      const mountTarget = '/tmp/target1';

      before((done) => {
        client = createCsiClient('Node');
        cleanPublishDir(mountTarget, () => {
          createPublishDir(mountTarget);
          done();
        });
      });

      after((done) => {
        if (client != null) {
          client.close();
        }
        cleanPublishDir(mountTarget, done);
      });

      it('should be able to stage volume (ext4)', (done) => {
        client.nodeStageVolume(
          {
            volume_id: UUID2,
            publish_context: publishedUris[UUID2],
            staging_target_path: mountTarget,
            volume_capability: {
              access_mode: {
                mode: 'MULTI_NODE_READER_ONLY'
              },
              mount: {
                fs_type: 'ext4'
              }
            },
            readonly: false,
            secrets: {},
            volume_context: {}
          },
          (err) => {
            if (err) return done(err);
            assert.equal(getFsType(mountTarget), 'ext4');
            done();
          }
        );
      });

      it('should be able to unstage volume (ext4)', (done) => {
        client.nodeUnstageVolume(
          {
            volume_id: UUID2,
            staging_target_path: mountTarget
          },
          (err) => {
            if (err) return done(err);
            assert.isUndefined(getFsType(mountTarget));
            done();
          }
        );
      });
    });

    describe('stage misc', function () {
      let client;
      const mountTarget = '/tmp/target2';

      before((done) => {
        client = createCsiClient('Node');
        cleanPublishDir(mountTarget, () => {
          createPublishDir(mountTarget);
          done();
        });
      });

      after((done) => {
        if (client != null) {
          client.close();
        }
        cleanPublishDir(mountTarget, done);
      });

      it('should fail to stage unsupported fs', (done) => {
        const args = {
          volume_id: UUID3,
          publish_context: publishedUris[UUID3],
          staging_target_path: mountTarget,
          volume_capability: {
            access_mode: {
              mode: 'MULTI_NODE_READER_ONLY'
            },
            mount: {
              fs_type: 'ext3'
            }
          }
        };
        client.nodeStageVolume(
          args,
          shouldFailWith(grpc.status.INVALID_ARGUMENT, done)
        );
      });
    });

    // The combinations of ro/rw and access mode flags are quite confusing.
    // See the source code for more info on how this should work.
    describe('publish and unpublish', function () {
      let client;

      before(() => {
        client = createCsiClient('Node');
      });

      after(() => {
        if (client != null) {
          client.close();
        }
      });

      describe('MULTI_NODE_READER_ONLY staged volume', function () {
        const mountTarget = '/tmp/target3';
        const bindTarget1 = '/tmp/bind1';
        const bindTarget2 = '/tmp/bind2';

        before((done) => {
          const stageArgs = {
            volume_id: UUID4,
            publish_context: publishedUris[UUID4],
            staging_target_path: mountTarget,
            volume_capability: {
              access_mode: {
                mode: 'MULTI_NODE_READER_ONLY'
              },
              mount: {
                fs_type: 'xfs'
              }
            },
            readonly: false,
            secrets: {},
            volume_context: {}
          };

          cleanPublishDir(mountTarget, () => {
            createPublishDir(mountTarget);
            client.nodeStageVolume(stageArgs, done);
          });
        });

        after((done) => {
          async.series(
            [
              (next) => {
                client.nodeUnstageVolume(
                  {
                    volume_id: UUID4,
                    staging_target_path: mountTarget
                  },
                  next
                );
              },
              (next) => {
                cleanPublishDir(mountTarget, next);
              },
              (next) => {
                cleanPublishDir(bindTarget1, next);
              },
              (next) => {
                cleanPublishDir(bindTarget2, next);
              }
            ],
            done
          );
        });

        it('should publish a volume in ro mode and test it is an idempotent op', (done) => {
          const args = {
            volume_id: UUID4,
            publish_context: publishedUris[UUID4],
            staging_target_path: mountTarget,
            target_path: bindTarget1,
            volume_capability: {
              access_mode: {
                mode: 'MULTI_NODE_READER_ONLY'
              },
              mount: {
                fs_type: 'xfs'
              }
            },
            readonly: true
          };

          client.nodePublishVolume(args, (err) => {
            if (err) return done(err);
            assert.equal(getFsType(bindTarget1), 'xfs');
            // re-publish should succeed (idempotent)
            client.nodePublishVolume(args, done);
          });
        });

        it('should fail when re-publishing with a different staging path', (done) => {
          const args = {
            volume_id: UUID4,
            publish_context: publishedUris[UUID4],
            staging_target_path: '/invalid_staging_path',
            target_path: bindTarget1,
            volume_capability: {
              access_mode: {
                mode: 'MULTI_NODE_READER_ONLY'
              },
              mount: {
                fs_type: 'xfs'
              }
            }
          };

          client.nodePublishVolume(
            args,
            shouldFailWith(grpc.status.INVALID_ARGUMENT, done)
          );
        });

        it('should fail with a missing target path', (done) => {
          const args = {
            volume_id: UUID4,
            publish_context: publishedUris[UUID4],
            staging_target_path: mountTarget,
            volume_capability: {
              access_mode: {
                mode: 'MULTI_NODE_READER_ONLY'
              },
              mount: {
                fs_type: 'xfs'
              }
            }
          };

          client.nodePublishVolume(
            args,
            shouldFailWith(grpc.status.INVALID_ARGUMENT, done)
          );
        });

        it('should fail to publish the volume as rw', (done) => {
          const args = {
            volume_id: UUID4,
            publish_context: publishedUris[UUID4],
            staging_target_path: mountTarget,
            target_path: bindTarget2,
            volume_capability: {
              access_mode: {
                mode: 'MULTI_NODE_READER_ONLY'
              },
              mount: {
                fs_type: 'xfs',
                mnt_flags: []
              }
            },
            readonly: false
          };

          client.nodePublishVolume(
            args,
            shouldFailWith(grpc.status.INVALID_ARGUMENT, (err) => {
              if (err) return done(err);
              assert.isUndefined(getFsType(bindTarget2));
              done();
            })
          );
        });

        it('should be able to unpublish ro volume', (done) => {
          client.nodeUnpublishVolume(
            {
              volume_id: UUID4,
              target_path: bindTarget2
            },
            (err) => {
              if (err) return done(err);
              assert.isUndefined(getFsType(bindTarget2));
              done();
            }
          );
        });

        it('should be able to unpublish rw volume', (done) => {
          client.nodeUnpublishVolume(
            {
              volume_id: UUID4,
              target_path: bindTarget1
            },
            (err) => {
              if (err) return done(err);
              // we cannot assert because the fs is lazily unmounted
              // assert.isUndefined(getFsType(bindTarget1));
              done();
            }
          );
        });
      });

      describe('MULTI_NODE_SINGLE_WRITER staged volume', function () {
        const mountTarget = '/tmp/target4';
        const bindTarget1 = '/tmp/bind1';
        const bindTarget2 = '/tmp/bind2';

        before((done) => {
          const stageArgs = {
            volume_id: UUID5,
            publish_context: publishedUris[UUID5],
            staging_target_path: mountTarget,
            volume_capability: {
              access_mode: {
                mode: 'MULTI_NODE_SINGLE_WRITER'
              },
              mount: {
                fs_type: 'ext4'
              }
            },
            secrets: {},
            volume_context: {}
          };

          cleanPublishDir(mountTarget, () => {
            createPublishDir(mountTarget);
            client.nodeStageVolume(stageArgs, done);
          });
        });

        after((done) => {
          async.series(
            [
              (next) => {
                client.nodeUnstageVolume(
                  {
                    volume_id: UUID5,
                    staging_target_path: mountTarget
                  },
                  next
                );
              },
              (next) => {
                cleanPublishDir(mountTarget, next);
              },
              (next) => {
                cleanPublishDir(bindTarget1, next);
              },
              (next) => {
                cleanPublishDir(bindTarget2, next);
              }
            ],
            done
          );
        });

        it('should publish ro volume', (done) => {
          const args = {
            volume_id: UUID5,
            publish_context: publishedUris[UUID5],
            staging_target_path: mountTarget,
            target_path: bindTarget1,
            readonly: true,
            volume_capability: {
              access_mode: {
                mode: 'MULTI_NODE_SINGLE_WRITER'
              },
              mount: {
                fs_type: 'ext4',
                mnt_flags: ['ro']
              }
            }
          };

          client.nodePublishVolume(args, (err) => {
            if (err) return done(err);
            assert.equal(getFsType(bindTarget1), 'ext4');
            // re-publish should succeed (idempotent)
            client.nodePublishVolume(args, done);
          });
        });

        it('should publish rw volume', (done) => {
          const args = {
            volume_id: UUID5,
            publish_context: publishedUris[UUID5],
            staging_target_path: mountTarget,
            target_path: bindTarget2,
            volume_capability: {
              access_mode: {
                mode: 'MULTI_NODE_SINGLE_WRITER'
              },
              mount: {
                fs_type: 'ext4'
              }
            }
          };

          client.nodePublishVolume(args, (err) => {
            if (err) return done(err);
            assert.equal(getFsType(bindTarget2), 'ext4');
            done();
          });
        });

        it('should be able to unpublish ro volume', (done) => {
          client.nodeUnpublishVolume(
            {
              volume_id: UUID5,
              target_path: bindTarget1
            },
            (err) => {
              if (err) return done(err);
              // we cannot assert because the fs is lazily unmounted
              // assert.isUndefined(getFsType(bindTarget1));
              done();
            }
          );
        });

        it('should be able to unpublish rw volume', (done) => {
          client.nodeUnpublishVolume(
            {
              volume_id: UUID5,
              target_path: bindTarget2
            },
            (err) => {
              if (err) return done(err);
              assert.isUndefined(getFsType(bindTarget2));
              done();
            }
          );
        });
      });
    });

    describe('stage and unstage block volume', function () {
      let client;
      const mountTarget = '/tmp/target2';

      before((done) => {
        client = createCsiClient('Node');
        cleanPublishDir(mountTarget, () => {
          createPublishDir(mountTarget);
          done();
        });
      });

      after((done) => {
        if (client != null) {
          client.close();
        }
        cleanPublishDir(mountTarget, done);
      });

      it('should be able to stage block volume', (done) => {
        client.nodeStageVolume(
          {
            volume_id: UUID3,
            publish_context: publishedUris[UUID3],
            staging_target_path: mountTarget,
            volume_capability: {
              access_mode: {
                mode: 'MULTI_NODE_READER_ONLY'
              },
              block: {
              }
            },
            readonly: false,
            secrets: {},
            volume_context: {}
          },
          (err) => {
            if (err) return done(err);
            assert.isUndefined(getFsType(mountTarget));
            done();
          }
        );
      });

      it('should be able to unstage block volume', (done) => {
        client.nodeUnstageVolume(
          {
            volume_id: UUID3,
            staging_target_path: mountTarget
          },
          (err) => {
            if (err) return done(err);
            assert.isUndefined(getFsType(mountTarget));
            done();
          }
        );
      });
    });

    // The combinations of ro/rw and access mode flags are quite confusing.
    // See the source code for more info on how this should work.
    describe('publish and unpublish block volumes', function () {
      let client;

      before(() => {
        client = createCsiClient('Node');
      });

      after(() => {
        if (client != null) {
          client.close();
        }
      });

      describe('MULTI_NODE_READER_ONLY staged volume', function () {
        const stagingPath = '/tmp/target3';
        const stagingPath2 = '/tmp/target4';
        const publishPath1 = '/tmp/blockvol1';
        const publishPath2 = '/tmp/blockvol2';

        before((done) => {
          const stageArgs = {
            volume_id: UUID4,
            publish_context: publishedUris[UUID4],
            staging_target_path: stagingPath,
            volume_capability: {
              access_mode: {
                mode: 'MULTI_NODE_READER_ONLY'
              },
              block: {
              }
            },
            readonly: false,
            secrets: {},
            volume_context: {}
          };

          const stageArgs2 = {
            volume_id: UUID5,
            publish_context: publishedUris[UUID5],
            staging_target_path: stagingPath2,
            volume_capability: {
              access_mode: {
                mode: 'MULTI_NODE_READER_ONLY'
              },
              block: {
              }
            },
            readonly: false,
            secrets: {},
            volume_context: {}
          };

          async.series(
            [
              (next) => {
                cleanBlockMount(publishPath1, next);
              },
              (next) => {
                cleanBlockMount(publishPath2, next);
              },
              (next) => {
                cleanPublishDir(stagingPath, () => {
                  client.nodeStageVolume(stageArgs, next);
                });
              },
              (next) => {
                cleanPublishDir(stagingPath2, () => {
                  client.nodeStageVolume(stageArgs2, next);
                });
              }
            ],
            done
          );
        });

        after((done) => {
          async.series(
            [
              (next) => {
                client.nodeUnstageVolume(
                  {
                    volume_id: UUID4,
                    staging_target_path: stagingPath
                  },
                  next
                );
              },
              (next) => {
                client.nodeUnstageVolume(
                  {
                    volume_id: UUID5,
                    staging_target_path: stagingPath2
                  },
                  next
                );
              },
              (next) => {
                cleanPublishDir(stagingPath, next);
              },
              (next) => {
                cleanPublishDir(stagingPath2, next);
              },
              (next) => {
                cleanBlockMount(publishPath1, next);
              },
              (next) => {
                cleanBlockMount(publishPath2, next);
              }
            ],
            done
          );
        });

        it('should publish a block volume in ro mode and test it is an idempotent op', (done) => {
          const args = {
            volume_id: UUID4,
            publish_context: publishedUris[UUID4],
            staging_target_path: stagingPath,
            target_path: publishPath1,
            volume_capability: {
              access_mode: {
                mode: 'MULTI_NODE_READER_ONLY'
              },
              block: {
              }
            },
            readonly: true
          };

          client.nodePublishVolume(args, (err) => {
            if (err) return done(err);
            assert.equal(fs.existsSync(publishPath1), true);
            assert.equal(getFsType(publishPath1), 'devtmpfs');
            // re-publish should succeed (idempotent)
            client.nodePublishVolume(args, done);
          });
        });

        it('should fail when publishing another volume on the same target path', (done) => {
          const args = {
            volume_id: UUID5,
            publish_context: publishedUris[UUID5],
            staging_target_path: stagingPath2,
            target_path: publishPath1,
            volume_capability: {
              access_mode: {
                mode: 'MULTI_NODE_READER_ONLY'
              },
              block: {
              }
            },
            readonly: true
          };

          client.nodePublishVolume(
            args,
            shouldFailWith(grpc.status.INTERNAL, done)
          );
        });

        it('should fail when re-publishing with a different staging path', (done) => {
          const args = {
            volume_id: UUID4,
            publish_context: publishedUris[UUID4],
            staging_target_path: '/invalid_staging_path',
            target_path: publishPath1,
            volume_capability: {
              access_mode: {
                mode: 'MULTI_NODE_READER_ONLY'
              },
              block: {
              }
            }
          };

          client.nodePublishVolume(
            args,
            shouldFailWith(grpc.status.INVALID_ARGUMENT, done)
          );
        });

        it('should fail with a missing target path', (done) => {
          const args = {
            volume_id: UUID4,
            publish_context: publishedUris[UUID4],
            staging_target_path: stagingPath,
            volume_capability: {
              access_mode: {
                mode: 'MULTI_NODE_READER_ONLY'
              },
              block: {
              }
            }
          };

          client.nodePublishVolume(
            args,
            shouldFailWith(grpc.status.INVALID_ARGUMENT, done)
          );
        });

        it('should fail to publish the block volume as rw', (done) => {
          const args = {
            volume_id: UUID4,
            publish_context: publishedUris[UUID4],
            staging_target_path: stagingPath,
            target_path: publishPath2,
            volume_capability: {
              access_mode: {
                mode: 'MULTI_NODE_READER_ONLY'
              },
              block: {
              }
            },
            readonly: false
          };

          client.nodePublishVolume(
            args,
            shouldFailWith(grpc.status.INVALID_ARGUMENT, (err) => {
              if (err) return done(err);
              assert.equal(fs.existsSync(publishPath2), false);
              assert.isUndefined(getFsType(publishPath2));
              done();
            })
          );
        });

        it('should be able to unpublish ro block volume', (done) => {
          client.nodeUnpublishVolume(
            {
              volume_id: UUID4,
              target_path: publishPath2
            },
            (err) => {
              if (err) return done(err);
              assert.equal(fs.existsSync(publishPath2), false);
              assert.isUndefined(getFsType(publishPath2));
              done();
            }
          );
        });

        it('should be able to unpublish rw block volume', (done) => {
          client.nodeUnpublishVolume(
            {
              volume_id: UUID4,
              target_path: publishPath1
            },
            (err) => {
              if (err) return done(err);
              assert.equal(fs.existsSync(publishPath1), false);
              assert.isUndefined(getFsType(publishPath1));
              done();
            }
          );
        });
      });

      describe('MULTI_NODE_SINGLE_WRITER staged volume', function () {
        const stagingPath = '/tmp/target4';
        const publishPath1 = '/tmp/blockvol1';
        const publishPath2 = '/tmp/blockvol2';

        before((done) => {
          const stageArgs = {
            volume_id: UUID5,
            publish_context: publishedUris[UUID5],
            staging_target_path: stagingPath,
            volume_capability: {
              access_mode: {
                mode: 'MULTI_NODE_SINGLE_WRITER'
              },
              block: {
              }
            },
            secrets: {},
            volume_context: {}
          };

          async.series(
            [
              (next) => {
                cleanBlockMount(publishPath1, next);
              },
              (next) => {
                cleanBlockMount(publishPath2, next);
              },
              (next) => {
                cleanPublishDir(stagingPath, () => {
                  client.nodeStageVolume(stageArgs, next);
                });
              }
            ],
            done
          );
        });

        after((done) => {
          async.series(
            [
              (next) => {
                client.nodeUnstageVolume(
                  {
                    volume_id: UUID5,
                    staging_target_path: stagingPath
                  },
                  next
                );
              },
              (next) => {
                cleanPublishDir(stagingPath, next);
              },
              (next) => {
                cleanBlockMount(publishPath1, next);
              },
              (next) => {
                cleanBlockMount(publishPath2, next);
              }
            ],
            done
          );
        });

        it('should publish ro volume', (done) => {
          const args = {
            volume_id: UUID5,
            publish_context: publishedUris[UUID5],
            staging_target_path: stagingPath,
            target_path: publishPath1,
            readonly: true,
            volume_capability: {
              access_mode: {
                mode: 'MULTI_NODE_SINGLE_WRITER'
              },
              block: {
              }
            }
          };

          client.nodePublishVolume(args, (err) => {
            if (err) return done(err);
            assert.equal(fs.existsSync(publishPath1), true);
            assert.equal(getFsType(publishPath1), 'devtmpfs');
            // re-publish should succeed (idempotent)
            client.nodePublishVolume(args, done);
          });
        });

        it('should publish rw volume', (done) => {
          const args = {
            volume_id: UUID5,
            publish_context: publishedUris[UUID5],
            staging_target_path: stagingPath,
            target_path: publishPath2,
            volume_capability: {
              access_mode: {
                mode: 'MULTI_NODE_SINGLE_WRITER'
              },
              block: {
              }
            }
          };

          client.nodePublishVolume(args, (err) => {
            if (err) return done(err);
            assert.equal(fs.existsSync(publishPath2), true);
            assert.equal(getFsType(publishPath2), 'devtmpfs');
            done();
          });
        });

        it('should be able to unpublish ro volume', (done) => {
          client.nodeUnpublishVolume(
            {
              volume_id: UUID5,
              target_path: publishPath1
            },
            (err) => {
              if (err) return done(err);
              assert.isUndefined(getFsType(publishPath1));
              assert.equal(fs.existsSync(publishPath1), false);
              done();
            }
          );
        });

        it('should be able to unpublish rw volume', (done) => {
          client.nodeUnpublishVolume(
            {
              volume_id: UUID5,
              target_path: publishPath2
            },
            (err) => {
              if (err) return done(err);
              assert.isUndefined(getFsType(publishPath2));
              assert.equal(fs.existsSync(publishPath2), false);
              done();
            }
          );
        });
      });
    });
  });
}
