// Unit tests for nexus snapshot grpc api.

'use strict';

const assert = require('chai').assert;
const async = require('async');
const fs = require('fs');
const common = require('./test_common');
const enums = require('./grpc_enums');
const UUID = 'dbe4d7eb-118a-4d15-b789-a18d9af6ff21';

const replicaUuid = '00000000-76b6-4fcf-864d-1027d4038756';
const poolName = 'pool1';
const pool2Name = 'pool2';
// backend file for pool
const poolFile = '/tmp/pool1-backend';
const pool2File = '/tmp/pool2-backend';
// 128MB is the size of pool
const diskSize = 128 * 1024 * 1024;
// 64MB is the size of replica
const replicaSize = 64 * 1024 * 1024;

// The config just for nvmf target which cannot run in the same process as
// the nvmf initiator (SPDK limitation).
const config = `
sync_disable: true
nexus_opts:
  nvmf_enable: true
  nvmf_discovery_enable: true
  nvmf_nexus_port: 8440
  nvmf_replica_port: 8430
  iscsi_enable: false
  iscsi_nexus_port: 3260
  iscsi_replica_port: 3262
pools:
  - name: pool2
    disks:
      - aio:///tmp/pool2-backend
    replicas: []
`;

let client, client2;
let disks, disks2;

// URI of Nexus published over NVMf
let nexusUri;

describe('snapshot', function () {
  this.timeout(10000); // for network tests we need long timeouts

  before((done) => {
    client = common.createGrpcClient();
    if (!client) {
      return done(new Error('Failed to initialize grpc client'));
    }
    client2 = common.createGrpcClient('127.0.0.1:10125');
    if (!client2) {
      return done(new Error('Failed to initialize grpc client for 2nd Mayastor instance'));
    }
    disks = [poolFile];
    disks2 = [pool2File];

    async.series(
      [
        (next) => {
          fs.writeFile(poolFile, '', next);
        },
        (next) => {
          fs.truncate(poolFile, diskSize, next);
        },
        (next) => {
          fs.writeFile(pool2File, '', next);
        },
        (next) => {
          fs.truncate(pool2File, diskSize, next);
        },
        // start this as early as possible to avoid mayastor getting connection refused.
        (next) => {
          // Start another mayastor instance for the remote nvmf target of the
          // shared replica.
          // SPDK hangs if nvme initiator and target are in the same instance.
          //
          // Use -s option to limit hugepage allocation.
          common.startMayastor(config, [
            '-r',
            '/tmp/target.sock',
            '-s',
            '128',
            '-g',
            '127.0.0.1:10125'
          ],
          null,
          '_tgt');
          common.waitFor((pingDone) => {
            // use harmless method to test if the mayastor is up and running
            client2.listPools({}, pingDone);
          }, next);
        },
        (next) => {
          common.startMayastor(null, ['-r', common.SOCK, '-g', common.grpcEndpoint, '-s', 384]);

          common.waitFor((pingDone) => {
            // use harmless method to test if the mayastor is up and running
            client.listPools({}, pingDone);
          }, next);
        }
      ],
      done
    );
  });

  after((done) => {
    async.series(
      [
        common.stopAll,
        (next) => {
          fs.unlink(poolFile, (err) => {
            if (err) console.log('unlink failed:', poolFile, err);
            next();
          });
        },
        (next) => {
          fs.unlink(pool2File, (err) => {
            if (err) console.log('unlink failed:', pool2File, err);
            next();
          });
        }
      ],
      (err) => {
        if (client2 != null) {
          client2.close();
        }
        if (client != null) {
          client.close();
        }
        done(err);
      }
    );
  });

  it('should destroy the pool loaded from yaml', (done) => {
    client2.destroyPool(
      { name: pool2Name },
      (err, res) => {
        if (err) return done(err);
        done();
      }
    );
  });

  it('should create a local pool with aio bdevs', (done) => {
    // explicitly specify aio as that always works
    client.createPool(
      { name: poolName, disks: disks.map((d) => `aio://${d}`) },
      (err, res) => {
        if (err) return done(err);
        assert.equal(res.name, poolName);
        assert.equal(res.used, 0);
        assert.equal(res.state, 'POOL_ONLINE');
        assert.equal(res.disks.length, disks.length);
        for (let i = 0; i < res.disks.length; ++i) {
          assert.equal(res.disks[i].includes(disks[i]), true);
        }
        done();
      }
    );
  });

  it('should create a remote pool with aio bdevs', (done) => {
    client2.createPool(
      { name: pool2Name, disks: disks2.map((d) => `aio://${d}`) },
      (err, res) => {
        if (err) return done(err);
        assert.equal(res.name, pool2Name);
        assert.equal(res.used, 0);
        assert.equal(res.state, 'POOL_ONLINE');
        assert.equal(res.disks.length, disks2.length);
        for (let i = 0; i < res.disks.length; ++i) {
          assert.equal(res.disks[i].includes(disks2[i]), true);
        }
        done();
      }
    );
  });

  it('should create a local replica', (done) => {
    client.createReplica(
      {
        uuid: replicaUuid,
        pool: poolName,
        thin: true,
        share: 'REPLICA_NONE',
        size: replicaSize
      },
      (err, res) => {
        if (err) return done(err);
        assert.match(res.uri, /^bdev:\/\//);
        done();
      }
    );
  });

  it('should create a remote replica exported over nvmf', (done) => {
    client2.createReplica(
      {
        uuid: replicaUuid,
        pool: pool2Name,
        thin: true,
        share: 'REPLICA_NVMF',
        size: replicaSize
      },
      (err, res) => {
        if (err) return done(err);
        assert.match(res.uri, /^nvmf:\/\//);
        done();
      }
    );
  });

  it('should create a nexus with a local replica and 1 remote nvmf replica', (done) => {
    const args = {
      uuid: UUID,
      size: 131072,
      children: ['loopback:///' + replicaUuid,
        'nvmf://' + common.getMyIp() + ':8430/nqn.2019-05.io.openebs:' + replicaUuid]
    };

    client.createNexus(args, (err) => {
      if (err) return done(err);
      done();
    });
  });

  it('should list the created nexus', (done) => {
    client.listNexus({}, (err, res) => {
      if (err) return done(err);
      assert.lengthOf(res.nexus_list, 1);
      const nexus = res.nexus_list[0];

      const expectedChildren = 2;
      assert.equal(nexus.uuid, UUID);
      assert.equal(nexus.state, 'NEXUS_ONLINE');
      assert.lengthOf(nexus.children, expectedChildren);
      done();
    });
  });

  it('should publish the nexus on nvmf', (done) => {
    client.publishNexus(
      {
        uuid: UUID,
        share: enums.NEXUS_NVMF
      },
      (err, res) => {
        if (err) done(err);
        assert(res.device_uri);
        nexusUri = res.device_uri;
        done();
      }
    );
  });

  it('should create a snapshot on the nexus', (done) => {
    const args = { uuid: UUID };
    client.createSnapshot(args, (err) => {
      if (err) return done(err);
      done();
    });
  });

  it('should list the snapshot as a local replica', (done) => {
    client.listReplicas({}, (err, res) => {
      if (err) return done(err);

      res = res.replicas.filter((ent) => ent.pool === poolName);
      assert.lengthOf(res, 2);
      res = res[1];

      assert.equal(res.uuid.startsWith(replicaUuid + '-snap-'), true);
      assert.equal(res.share, 'REPLICA_NONE');
      assert.match(res.uri, /^bdev:\/\/\//);
      done();
    });
  });

  it('should list the snapshot as a remote replica', (done) => {
    client2.listReplicas({}, (err, res) => {
      if (err) return done(err);

      res = res.replicas.filter((ent) => ent.pool === pool2Name);
      assert.lengthOf(res, 2);
      res = res[1];

      assert.equal(res.uuid.startsWith(replicaUuid + '-snap-'), true);
      assert.equal(res.share, 'REPLICA_NONE');
      assert.match(res.uri, /^bdev:\/\/\//);
      // Wait 1 second so that the 2nd snapshot has a different name and can
      // be created successfully
      setTimeout(done, 1000);
    });
  });

  it('should take snapshot on nvmf-published nexus', (done) => {
    common.execAsRoot(
      common.getCmdPath('initiator'),
      [nexusUri, 'create-snapshot'],
      done
    );
  });

  it('should list the 2 snapshots as local replicas', (done) => {
    client.listReplicas({}, (err, res) => {
      if (err) return done(err);

      res = res.replicas.filter((ent) => ent.pool === poolName);
      assert.lengthOf(res, 3);
      let i;
      for (i = 1; i < 3; i++) {
        assert.equal(res[i].uuid.startsWith(replicaUuid + '-snap-'), true);
        assert.equal(res[i].share, 'REPLICA_NONE');
        assert.match(res[i].uri, /^bdev:\/\/\//);
      }
      done();
    });
  });

  it('should list the 2 snapshots as remote replicas', (done) => {
    client2.listReplicas({}, (err, res) => {
      if (err) return done(err);

      res = res.replicas.filter((ent) => ent.pool === pool2Name);
      assert.lengthOf(res, 3);
      let i;
      for (i = 1; i < 3; i++) {
        assert.equal(res[i].uuid.startsWith(replicaUuid + '-snap-'), true);
        assert.equal(res[i].share, 'REPLICA_NONE');
        assert.match(res[i].uri, /^bdev:\/\/\//);
      }
      done();
    });
  });

  it('should remove the nexus', (done) => {
    const args = { uuid: UUID };

    client.destroyNexus(args, (err) => {
      if (err) return done(err);
      done();
    });
  });
});
