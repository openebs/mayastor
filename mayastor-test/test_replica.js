'use strict';

// Test gRPC interface of mayastor (other than CSI services)
//
// The tests can be run as they are or the test suite can be pointed against
// running mayastor instance in order to validate it. Use MAYASTOR_ENDPOINT
// and MAYASTOR_DISKS environment variables for that.

const assert = require('chai').assert;
const async = require('async');
const fs = require('fs');
const path = require('path');
const { exec } = require('child_process');
const { createClient } = require('grpc-kit');
const grpc = require('grpc');
const common = require('./test_common');

const POOL = 'tpool';
const DISK_FILE = '/tmp/mayastor_test_disk';
// arbitrary uuid used for creating a replica
const UUID = 'dbe4d7eb-118a-4d15-b789-a18d9af6ff21';
// uuid without the last digit for generating a set of uuids
const BASE_UUID = 'c35fa4dd-d527-4b7b-9cf0-436b8bb0ba7';
// regexps for testing nvmf and iscsi URIs
const NVMF_URI = /^nvmf:\/\/(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}):\d{1,5}\/nqn.2019-05.io.openebs:/;
const ISCSI_URI = /^iscsi:\/\/(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}):\d{1,5}\/iqn.2019-05.io.openebs:/;

// tunables of the test suite
var endpoint = process.env.MAYASTOR_ENDPOINT;
var disks = process.env.MAYASTOR_DISKS;

var remote; // true if the test suite is run against a remote grpc server
var implicitDisk;

// Create fake disk device used for testing (size 100M)
function createTestDisk(done) {
  exec('truncate -s 100m ' + DISK_FILE, (err, stdout, stderr) => {
    if (err) return done(stderr);

    common.execAsRoot(
      'losetup',
      // Explicitly set blksiz to 512 to be different from the
      // default in mayastor (4096) to test it.
      ['--show', '-b', '512', '-f', DISK_FILE],
      (err, stdout) => done(err, stdout ? stdout.trim() : '')
    );
  });
}

// Destroy the fake disk used for testing (disregard any error).
function destroyTestDisk(done) {
  if (implicitDisk != null) {
    common.execAsRoot('losetup', ['-d', implicitDisk], err => {
      fs.unlink(DISK_FILE, err => done());
    });
  } else {
    done();
  }
}

function createGrpcClient(service) {
  return createClient(
    {
      protoPath: path.join(
        __dirname,
        '..',
        'rpc',
        'proto',
        'mayastor_service.proto'
      ),
      packageName: 'mayastor_service',
      serviceName: 'Mayastor',
      options: {
        keepCase: true,
        longs: String,
        enums: String,
        defaults: true,
        oneofs: true,
      },
    },
    endpoint
  );
}

describe('replica', function() {
  var client;

  this.timeout(10000); // for network tests we need long timeouts

  // Destroy test pool if it exists (ignore errors as the test pool may not
  // exist).
  function ensureNoTestPool(done) {
    if (client == null) {
      return done();
    }
    client.destroyPool({ name: POOL }, err => done());
  }

  // start mayastor if needed
  before(() => {
    // if no explicit gRPC endpoint given then create one by starting
    // mayastor and grpc server
    if (!endpoint) {
      remote = false;
      endpoint = common.endpoint;
      common.startMayastor();
      common.startMayastorGrpc();
    } else {
      remote = true;
    }
  });

  // stop mayastor server if it was started by us
  after(common.stopAll);

  before(done => {
    client = createGrpcClient('MayaStor');
    if (!client) {
      return done(new Error('Failed to initialize grpc client'));
    }

    async.series(
      [
        next => {
          if (!disks) {
            createTestDisk((err, newDisk) => {
              if (err) return next(err);

              implicitDisk = newDisk;
              disks = [newDisk];
              next();
            });
          } else {
            disks = disks
              .trim()
              .split(' ')
              .filter(e => !!e);
            next();
          }
        },
        next => {
          common.waitFor(pingDone => {
            // use harmless method to test if the mayastor is up and running
            client.listPools({}, pingDone);
          }, next);
        },
        ensureNoTestPool,
        common.ensureNbdWritable,
      ],
      done
    );
  });

  after(done => {
    async.series(
      [
        ensureNoTestPool,
        next => {
          if (!implicitDisk) {
            next();
          } else {
            destroyTestDisk(next);
          }
        },
        common.restoreNbdPerms,
      ],
      err => {
        if (client != null) {
          client.close();
        }
        done(err);
      }
    );
  });

  it('should not support multiple disks for a pool', done => {
    client.createPool(
      {
        name: POOL,
        disks: ['/dev/somethingA', '/dev/somethingB'],
      },
      (err, res) => {
        assert.equal(err.code, grpc.status.INVALID_ARGUMENT);
        done();
      }
    );
  });

  it('should not create a pool with invalid block size', done => {
    client.createPool(
      { name: POOL, disks: disks, block_size: 1238513 },
      (err, res) => {
        assert.equal(err.code, grpc.status.INVALID_ARGUMENT);
        done();
      }
    );
  });

  it('should create a pool', done => {
    client.createPool({ name: POOL, disks: disks }, (err, res) => {
      if (err) return done(err);
      assert.lengthOf(Object.keys(res), 0);
      done();
    });
  });

  it('should return error from create when the pool exists', done => {
    client.createPool({ name: POOL, disks: disks }, (err, res) => {
      assert.equal(err.code, grpc.status.ALREADY_EXISTS);
      done();
    });
  });

  it('should list the pool', done => {
    client.listPools({}, (err, res) => {
      if (err) return done(err);

      res = res.pools.filter(ent => ent.name == POOL);
      assert.lengthOf(res, 1);
      res = res[0];

      assert.equal(res.name, POOL);
      // we don't know size of external disks ..
      if (implicitDisk) {
        // 4MB (one cluster) is eaten by super block
        assert.equal(Math.floor(res.capacity / (1024 * 1024)), 96);
      }
      assert.equal(res.used, 0);
      assert.equal(res.state, 'ONLINE');
      assert.deepEqual(res.disks, disks);
      done();
    });
  });

  it('should create replica exported over iscsi', done => {
    client.createReplica(
      {
        uuid: UUID,
        pool: POOL,
        thin: true,
        share: 'REPLICA_ISCSI',
        size: 8 * (1024 * 1024), // keep this multiple of cluster size (4MB)
      },
      (err, res) => {
        if (err) return done(err);
        assert.hasAllKeys(res, ['uri']);
        assert.match(res.uri, ISCSI_URI);
        assert.equal(res.uri.match(ISCSI_URI)[1], common.getMyIp());
        done();
      }
    );
  });

  it('should list iscsi replica and do io', done => {
    client.listReplicas({}, (err, res) => {
      if (err) return done(err);
      res = res.replicas.filter(ent => {
        return ent.uuid == UUID;
      });
      assert.lengthOf(res, 1);
      res = res[0];
      assert.equal(res.pool, POOL);
      assert.equal(res.thin, true);
      assert.equal(res.size, 8 * 1024 * 1024);
      assert.equal(res.share, 'REPLICA_ISCSI');
      assert.match(res.uri, ISCSI_URI);
      assert.equal(res.uri.match(ISCSI_URI)[1], common.getMyIp());

      // runs the perf test for 1 second
      exec('iscsi-perf -t 1 ' + res.uri + '/0', (error, stdout, stderr) => {
        if (error) {
          done(stderr);
        } else {
          done();
        }
      });
    });
  });

  it('should destroy iscsi replica', done => {
    client.destroyReplica({ uuid: UUID }, (err, res) => {
      if (err) return done(err);
      assert.lengthOf(Object.keys(res), 0);
      done();
    });
  });

  it('should create unexported replica', done => {
    client.createReplica(
      {
        uuid: UUID,
        pool: POOL,
        thin: true,
        share: 'NONE',
        size: 8 * (1024 * 1024), // keep this multiple of cluster size (4MB)
      },
      (err, res) => {
        if (err) return done(err);
        assert.hasAllKeys(res, ['uri']);
        assert.match(res.uri, /^bdev:\/\/\//);
        done();
      }
    );
  });

  it('should fail if creating replica which already exists', done => {
    client.createReplica(
      {
        uuid: UUID,
        pool: POOL,
        thin: true,
        share: 'NONE',
        size: 8 * (1024 * 1024), // keep this multiple of cluster size (4MB)
      },
      (err, res) => {
        assert.equal(err.code, grpc.status.ALREADY_EXISTS);
        done();
      }
    );
  });

  it('should list the replica', done => {
    client.listReplicas({}, (err, res) => {
      if (err) return done(err);
      res = res.replicas.filter(ent => {
        return ent.uuid == UUID;
      });
      assert.lengthOf(res, 1);
      res = res[0];
      assert.equal(res.pool, POOL);
      assert.equal(res.thin, true);
      assert.equal(res.size, 8 * 1024 * 1024);
      assert.equal(res.share, 'REPLICA_NONE');
      assert.match(res.uri, /^bdev:\/\/\//);
      done();
    });
  });

  it('should share the unexported replica', done => {
    client.shareReplica(
      {
        uuid: UUID,
        share: 'REPLICA_NVMF',
      },
      (err, res) => {
        if (err) return done(err);
        assert.match(res.uri, NVMF_URI);
        assert.equal(res.uri.match(NVMF_URI)[1], common.getMyIp());

        client.listReplicas({}, (err, res) => {
          if (err) return done(err);
          res = res.replicas.filter(ent => {
            return ent.uuid == UUID;
          });
          assert.lengthOf(res, 1);
          res = res[0];
          assert.equal(res.share, 'REPLICA_NVMF');
          assert.match(res.uri, NVMF_URI);
          done();
        });
      }
    );
  });

  it('should not fail if shared again using the same protocol', done => {
    client.shareReplica(
      {
        uuid: UUID,
        share: 'REPLICA_NVMF',
      },
      (err, res) => {
        if (err) return done(err);
        assert.match(res.uri, NVMF_URI);
        done();
      }
    );
  });

  it('should share the replica under a different protocol', done => {
    client.shareReplica(
      {
        uuid: UUID,
        share: 'REPLICA_ISCSI',
      },
      (err, res) => {
        if (err) return done(err);
        assert.match(res.uri, ISCSI_URI);
        assert.equal(res.uri.match(ISCSI_URI)[1], common.getMyIp());

        client.listReplicas({}, (err, res) => {
          if (err) return done(err);
          res = res.replicas.filter(ent => {
            return ent.uuid == UUID;
          });
          assert.lengthOf(res, 1);
          res = res[0];
          assert.equal(res.share, 'REPLICA_ISCSI');
          assert.match(res.uri, ISCSI_URI);
          done();
        });
      }
    );
  });

  it('should unshare the replica', done => {
    client.shareReplica(
      {
        uuid: UUID,
        share: 'NONE',
      },
      (err, res) => {
        if (err) return done(err);
        assert.match(res.uri, /^bdev:\/\/\//);

        client.listReplicas({}, (err, res) => {
          if (err) return done(err);
          res = res.replicas.filter(ent => {
            return ent.uuid == UUID;
          });
          assert.lengthOf(res, 1);
          res = res[0];
          assert.equal(res.share, 'REPLICA_NONE');
          assert.match(res.uri, /^bdev:\/\/\//);
          done();
        });
      }
    );
  });

  it('should get stats for the replica', done => {
    client.statReplicas({}, (err, res) => {
      if (err) return done(err);

      res = res.replicas.filter(ent => {
        return ent.uuid == UUID;
      });
      assert.lengthOf(res, 1);
      res = res[0];
      assert.equal(res.pool, POOL);
      // new bdevs are not written (unless they are lvols or so)
      assert.isAbove(parseInt(res.stats.num_read_ops), 0);
      assert.equal(parseInt(res.stats.num_write_ops), 0);
      assert.isAbove(parseInt(res.stats.bytes_read), 0);
      assert.equal(parseInt(res.stats.bytes_written), 0);
      done();
    });
  });

  it('should return NotFound for a non existing replica', done => {
    let unknownUuid = 'c35fa4dd-d527-4b7b-9cf0-436b8bb0ba77';
    client.destroyReplica({ uuid: unknownUuid }, (err, res) => {
      assert.equal(err.code, grpc.status.NOT_FOUND);
      done();
    });
  });

  it('should destroy the replica', done => {
    client.destroyReplica({ uuid: UUID }, (err, res) => {
      if (err) return done(err);
      assert.lengthOf(Object.keys(res), 0);
      done();
    });
  });

  it('should not list the replica', done => {
    client.listReplicas({}, (err, res) => {
      if (err) return done(err);

      res = res.replicas.filter(ent => {
        return ent.uuid == UUID;
      });
      assert.lengthOf(res, 0);
      done();
    });
  });

  it('should create 5 replicas', done => {
    async.times(
      5,
      function(n, next) {
        client.createReplica(
          {
            uuid: BASE_UUID + n,
            pool: POOL,
            thin: true,
            share: 'NONE',
            size: 8 * (1024 * 1024), // keep this multiple of cluster size (4MB)
          },
          next
        );
      },
      done
    );
  });

  it('should destroy the pool', done => {
    client.destroyPool({ name: POOL }, (err, res) => {
      if (err) return done(err);
      assert.lengthOf(Object.keys(res), 0);
      done();
    });
  });

  it('should not destroy a pool which does not exist', done => {
    client.destroyPool({ name: POOL }, (err, res) => {
      if (err) {
        assert.equal(err.code, grpc.status.NOT_FOUND);
        done();
      } else {
        done(new Error('Expected error and did not get any'));
      }
    });
  });

  it('should not list the pool', done => {
    client.listPools({}, (err, res) => {
      if (err) return done(err);

      res = res.pools.filter(ent => ent.name == POOL);
      assert.lengthOf(res, 0);
      done();
    });
  });

  describe('nvmf', function() {
    let uri; // URI of the created nvmf replica
    let blockFile = '/tmp/test_block'; // file with contents of a data block

    // run unlink as root because the file was created by root
    function rmBlockFile(done) {
      common.execAsRoot('rm', ['-f', blockFile], err => {
        // ignore unlink error
        done();
      });
    }

    before(done => {
      const buf = Buffer.alloc(4096, 'm');

      async.series(
        [
          next => rmBlockFile(next),
          next => fs.writeFile(blockFile, buf, next),
          next => client.createPool({ name: POOL, disks: disks }, next),
        ],
        done
      );
    });

    after(done => {
      rmBlockFile(() => {
        client.destroyPool({ name: POOL }, done);
      });
    });

    it('should create replica exported over nvmf', done => {
      client.createReplica(
        {
          uuid: UUID,
          pool: POOL,
          thin: true,
          share: 'REPLICA_NVMF',
          // Keep this multiple of cluster size (4MB).
          // Fill the entire pool so that we can test data reset
          // upon replica recreate.
          size: 96 * (1024 * 1024),
        },
        (err, res) => {
          if (err) return done(err);
          assert.hasAllKeys(res, ['uri']);
          assert.match(res.uri, NVMF_URI);
          assert.equal(res.uri.match(NVMF_URI)[1], common.getMyIp());
          uri = res.uri;
          done();
        }
      );
    });

    it('should list nvmf replica', done => {
      client.listReplicas({}, (err, res) => {
        if (err) return done(err);
        res = res.replicas.filter(ent => {
          return ent.uuid == UUID;
        });
        assert.lengthOf(res, 1);
        res = res[0];
        assert.equal(res.pool, POOL);
        assert.equal(res.thin, true);
        assert.equal(res.size, 96 * 1024 * 1024);
        assert.equal(res.share, 'REPLICA_NVMF');
        assert.equal(res.uri, uri);
        done();
      });
    });

    it('should write to nvmf replica', done => {
      common.execAsRoot(
        common.getCmdPath('initiator'),
        ['--offset=4096', uri, 'write', blockFile],
        done
      );
    });

    it('should read from nvmf replica', done => {
      async.series(
        [
          next => {
            // remove the file we used for writing just to be sure that what we read
            // really comes from the replica
            fs.unlink(blockFile, next);
          },
          next => {
            common.execAsRoot(
              common.getCmdPath('initiator'),
              ['--offset=4096', uri, 'read', blockFile],
              next
            );
          },
          next => {
            fs.readFile(blockFile, (err, data) => {
              if (err) return done(err);
              data = data.toString();
              assert.lengthOf(data, 4096);
              for (let i = 0; i < data.length; i++) {
                if (data[i] != 'm') {
                  next(new Error(`Invalid char '${data[i]}' at offset ${i}`));
                  return;
                }
              }
              next();
            });
          },
        ],
        done
      );
    });

    it('should destroy nvmf replica', done => {
      client.destroyReplica({ uuid: UUID }, (err, res) => {
        if (err) return done(err);
        assert.lengthOf(Object.keys(res), 0);
        done();
      });
    });

    it('should create the replica again', done => {
      client.createReplica(
        {
          uuid: UUID,
          pool: POOL,
          thin: true,
          share: 'REPLICA_NVMF',
          size: 8 * (1024 * 1024), // keep this multiple of cluster size (4MB)
        },
        (err, res) => {
          if (err) return done(err);
          assert.hasAllKeys(res, ['uri']);
          assert.match(res.uri, NVMF_URI);
          assert.equal(res.uri.match(NVMF_URI)[1], common.getMyIp());
          uri = res.uri;
          done();
        }
      );
    });

    it('the old data should have been reset', done => {
      async.series(
        [
          next => {
            common.execAsRoot(
              common.getCmdPath('initiator'),
              ['--offset=4096', uri, 'read', blockFile],
              next
            );
          },
          next => {
            fs.readFile(blockFile, (err, data) => {
              if (err) return done(err);
              assert.lengthOf(data, 4096);
              for (let i = 0; i < data.length; i++) {
                if (data[i] != 0) {
                  next(new Error(`Invalid char '${data[i]}' at offset ${i}`));
                  return;
                }
              }
              next();
            });
          },
        ],
        done
      );
    });

    it('should destroy nvmf replica', done => {
      client.destroyReplica({ uuid: UUID }, (err, res) => {
        if (err) return done(err);
        assert.lengthOf(Object.keys(res), 0);
        done();
      });
    });
  });

  describe('import', function() {
    before(() => {
      // For testing import we need to restart mayastor which is possible only
      // if testing local instance of mayastor.
      if (remote) {
        this.skip();
      }
    });

    it('should create the pool first time', done => {
      client.createPool({ name: POOL, disks: disks }, done);
    });

    it('should not list the created pool after restart', done => {
      async.series(
        [
          // restart mayastor
          next => {
            common.restartMayastor(pingDone => {
              // use harmless method to test if the mayastor is up and running
              client.listPools({}, pingDone);
            }, next);
          },
          next =>
            client.listPools({}, (err, res) => {
              res = res.pools.filter(ent => ent.name == POOL);
              if (res.length > 0) {
                next(new Error("Found pool which hasn't been imported yet"));
              } else {
                next();
              }
            }),
        ],
        done
      );
    });

    it('should import the pool', done => {
      async.series(
        [
          // import the pool created by previous mayastor instance
          next => client.createPool({ name: POOL, disks: disks }, next),
          next =>
            client.listPools({}, (err, res) => {
              if (err) return next(err);
              res = res.pools.filter(ent => ent.name == POOL);
              assert.lengthOf(res, 1);
              next();
            }),
        ],
        done
      );
    });

    it('should not import a pool which does not exist on device', done => {
      client.createPool({ name: 'non-existing', disks: disks }, (err, res) => {
        if (!err) {
          done(
            new Error('Expected error when importing a pool with wrong name')
          );
        } else {
          assert.equal(err.code, grpc.status.INVALID_ARGUMENT);
          done();
        }
      });
    });
  });
});
