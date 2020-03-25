// Unit tests for rebuild tasks

'use strict';

const { createClient } = require('grpc-kit');
const async = require('async');
const fs = require('fs');
const common = require('./test_common');
const path = require('path');
const assert = require('chai').assert;

// backend file for aio bdev
const child1 = '/tmp/child1';
const child2 = '/tmp/child2';
// 64MB is the size of nexus and replicas
const diskSize = 64 * 1024 * 1024;
// nexus UUID
const UUID = 'dbe4d7eb-118a-4d15-b789-a18d9af6ff21';
// external IP address detected by common lib
const externIp = common.getMyIp();

// Instead of using mayastor grpc methods to create replicas we use a config
// file to create them. Advantage is that we don't depend on bugs in replica
// code (the nexus tests are more independent). Disadvantage is that we don't
// test the nexus with implementation of replicas which are used in the
// production.
const configNexus = `
[Malloc]
  NumberOfLuns 2
  LunSizeInMB  64
  BlockSize    4096

[iSCSI]
  NodeBase "iqn.2019-05.io.openebs"
  # Socket I/O timeout sec. (0 is infinite)
  Timeout 30
  DiscoveryAuthMethod None
  DefaultTime2Wait 2
  DefaultTime2Retain 60
  ImmediateData Yes
  ErrorRecoveryLevel 0
  # Reduce mem requirements for iSCSI
  MaxSessions 1
  MaxConnectionsPerSession 1

[PortalGroup1]
  Portal GR1 0.0.0.0:3261

[InitiatorGroup1]
  InitiatorName Any
  Netmask ${externIp}/24

[TargetNode0]
  TargetName "iqn.2019-05.io.openebs:disk1"
  TargetAlias "Backend Malloc1"
  Mapping PortalGroup1 InitiatorGroup1
  AuthMethod None
  UseDigest Auto
  LUN0 Malloc1
  QueueDepth 1
`;

// The config just for nvmf target which cannot run in the same process as
// the nvmf initiator (SPDK limitation).
const configNvmfTarget = `
[Malloc]
  NumberOfLuns 1
  LunSizeInMB  64
  BlockSize    4096

[Nvmf]
  AcceptorPollRate 10000
  ConnectionScheduler RoundRobin

[Transport]
  Type TCP
  # reduce memory requirements
  NumSharedBuffers 32

[Subsystem1]
  NQN nqn.2019-05.io.openebs:disk2
  Listen TCP 127.0.0.1:8420
  AllowAnyHost Yes
  SN MAYASTOR0000000001
  MN NEXUSController1
  MaxNamespaces 1
  Namespace Malloc0 1

# although not used we still have to reduce mem requirements for iSCSI
[iSCSI]
  MaxSessions 1
  MaxConnectionsPerSession 1
`;

const nexusArgs = {
  uuid: UUID,
  size: 131072,
  children: [`aio:///${child1}?blk_size=4096`],
};

const rebuildArgs = {
  uuid: UUID,
  uri: `aio:///${child2}?blk_size=4096`,
};

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
    common.endpoint
  );
}

describe('rebuild tests', function() {
  var client;

  var ObjectType = {
    NEXUS: 0,
    SOURCE_CHILD: 1,
    DESTINATION_CHILD: 2,
  };

  function checkState(childType, expectedState) {
    client.ListNexus({}, (err, res, done) => {
      if (err) return done(err);
      assert.lengthOf(res.nexus_list, 1);

      let nexus = res.nexus_list[0];
      assert.equal(nexus.uuid, UUID);

      if (childType == ObjectType.NEXUS) {
        assert.equal(nexus.state, expectedState);
      } else if (childType == ObjectType.SOURCE_CHILD) {
        assert.equal(nexus.children[0].state, expectedState);
      } else if (childType == ObjectType.DESTINATION_CHILD) {
        assert.equal(nexus.children[1].state, expectedState);
      }
    });
  }

  function checkNumRebuilds(expected) {
    client.ListNexus({}, (err, res, done) => {
      if (err) return done(err);
      assert.lengthOf(res.nexus_list, 1);

      let nexus = res.nexus_list[0];
      assert.equal(nexus.uuid, UUID);

      assert.equal(nexus.rebuilds, expected);
    });
  }

  function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  const createNexus = args => {
    return new Promise((resolve, reject) => {
      client.createNexus(args, (err, data) => {
        if (err) return reject(err);
        resolve(data);
      });
    });
  };

  const destroyNexus = args => {
    return new Promise((resolve, reject) => {
      client.destroyNexus(args, (err, data) => {
        if (err) return reject(err);
        resolve(data);
      });
    });
  };

  const addChild = args => {
    return new Promise((resolve, reject) => {
      client.addChildNexus(args, (err, data) => {
        if (err) return reject(err);
        resolve(data);
      });
    });
  };

  const removeChild = args => {
    return new Promise((resolve, reject) => {
      client.removeChildNexus(args, (err, data) => {
        if (err) return reject(err);
        resolve(data);
      });
    });
  };

  const startRebuild = args => {
    return new Promise((resolve, reject) => {
      client.startRebuild(args, (err, data) => {
        if (err) return reject(err);
        resolve(data);
      });
    });
  };

  const stopRebuild = args => {
    return new Promise((resolve, reject) => {
      client.stopRebuild(args, (err, data) => {
        if (err) return reject(err);
        resolve(data);
      });
    });
  };

  const rebuildState = args => {
    return new Promise((resolve, reject) => {
      client.getRebuildState(args, (err, data) => {
        if (err) return reject(err);
        resolve(data);
      });
    });
  };

  before(done => {
    client = createGrpcClient('MayaStor');
    if (!client) {
      return done(new Error('Failed to initialize grpc client'));
    }

    async.series(
      [
        common.ensureNbdWritable,
        next => {
          fs.writeFile(child1, '', next);
        },
        next => {
          fs.truncate(child1, diskSize, next);
        },
        next => {
          fs.writeFile(child2, '', next);
        },
        next => {
          fs.truncate(child2, diskSize, next);
        },
        next => {
          common.startMayastor(configNexus, ['-r', common.SOCK, '-s', 386]);
          common.startMayastorGrpc();
          common.waitFor(pingDone => {
            // use harmless method to test if the mayastor is up and running
            client.listPools({}, pingDone);
          }, next);
        },
        next => {
          createNexus(nexusArgs)
            .then(() => {
              next();
            })
            .catch(err => {
              assert(err);
            })
            .catch(done);
        },
      ],
      done
    );
  });

  after(done => {
    async.series(
      [
        common.stopAll,
        common.restoreNbdPerms,
        next => {
          fs.unlink(child1, err => next());
        },
        next => {
          fs.unlink(child2, err => next());
        },
        next => {
          destroyNexus({ uuid: UUID })
            .then(() => {
              next();
            })
            .catch(err => {
              done();
            })
            .catch(done);
        },
      ],
      err => {
        if (client != null) {
          client.close();
        }
        done(err);
      }
    );
  });

  describe('running rebuild', function() {
    beforeEach(async () => {
      await addChild(rebuildArgs);
      await startRebuild(rebuildArgs);
    });

    afterEach(async () => {
      await stopRebuild(rebuildArgs);
      await removeChild(rebuildArgs);
    });

    it('check nexus state', () => {
      checkState(ObjectType.NEXUS, 'degraded');
    });

    it('check source state', () => {
      checkState(ObjectType.SOURCE_CHILD, 'open');
    });

    it('check destination state', () => {
      checkState(ObjectType.DESTINATION_CHILD, 'faulted');
    });
  });

  describe('stopping rebuild', function() {
    beforeEach(async () => {
      await addChild(rebuildArgs);
      await startRebuild(rebuildArgs);
      await stopRebuild(rebuildArgs);
      // TODO: Check for rebuild stop rather than sleeping
      await sleep(1000); // Give time for the rebuild to stop
    });

    afterEach(async () => {
      await removeChild(rebuildArgs);
    });

    it('check nexus state', () => {
      checkState(ObjectType.NEXUS, 'degraded');
    });

    it('check source state', () => {
      checkState(ObjectType.SOURCE_CHILD, 'open');
    });

    it('check destination state', () => {
      checkState(ObjectType.DESTINATION_CHILD, 'faulted');
    });

    it('get rebuild state', done => {
      // Expect to fail to get rebuild state because
      // after stopping there is no rebuild task
      rebuildState(client, nexusArgs)
        .then(() => {
          done(new Error('Expected to fail to get rebuild state.'));
        })
        .catch(err => {
          assert.isDefined(err);
        })
        .catch(done);
      done();
    });

    it('get number of rebuilds', done => {
      checkNumRebuilds('0');
      done();
    });
  });
});
