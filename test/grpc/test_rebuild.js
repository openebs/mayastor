// Unit tests for rebuild tasks

'use strict';

const async = require('async');
const fs = require('fs');
const common = require('./test_common');
const path = require('path');
const assert = require('chai').assert;
const sleep = require('sleep-promise');
const grpc = require('grpc');
const grpcPromise = require('grpc-promise');
const protoLoader = require('@grpc/proto-loader');

// backend file for aio bdev
const child1 = '/tmp/child1';
const child2 = '/tmp/child2';
// 100MB is the size of nexus and replicas
const diskSize = 100 * 1024 * 1024;
// nexus UUID
const UUID = 'dbe4d7eb-118a-4d15-b789-a18d9af6ff21';

const nexusArgs = {
  uuid: UUID,
  size: 104857600, // Size in bytes
  children: [`aio://${child1}?blk_size=4096`]
};

const rebuildArgs = {
  uuid: UUID,
  uri: `aio://${child2}?blk_size=4096`
};

const addChildArgs = {
  uuid: UUID,
  uri: `aio://${child2}?blk_size=4096`,
  norebuild: true
};

const childOnlineArgs = {
  uuid: UUID,
  uri: `aio://${child2}?blk_size=4096`,
  action: 1
};

const childOfflineArgs = {
  uuid: UUID,
  uri: `aio://${child2}?blk_size=4096`,
  action: 0
};

function createGrpcClient () {
  const PROTO_PATH = path.join(__dirname, '/../../rpc/mayastor-api/protobuf/mayastor.proto');

  // Load mayastor proto file with mayastor service
  const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
    keepCase: false,
    longs: Number,
    enums: String,
    defaults: true,
    oneofs: true
  });

  const mayastor = grpc.loadPackageDefinition(packageDefinition).mayastor;

  const client = new mayastor.Mayastor(
    common.grpcEndpoint,
    grpc.credentials.createInsecure()
  );
  grpcPromise.promisifyAll(client);
  return client;
}

describe('rebuild tests', function () {
  let client;

  this.timeout(10000); // for network tests we need long timeouts

  const ObjectType = {
    NEXUS: 0,
    SOURCE_CHILD: 1,
    DESTINATION_CHILD: 2
  };

  async function checkState (childType, expectedState) {
    const res = await client.listNexus().sendMessage();
    assert.lengthOf(res.nexusList, 1);

    const nexus = res.nexusList[0];
    assert.equal(nexus.uuid, UUID);

    if (childType === ObjectType.NEXUS) {
      assert.equal(nexus.state, expectedState);
    } else if (childType === ObjectType.SOURCE_CHILD) {
      assert.equal(nexus.children[0].state, expectedState);
    } else if (childType === ObjectType.DESTINATION_CHILD) {
      assert.equal(nexus.children[1].state, expectedState);
    }
  }

  async function checkNumRebuilds (expected) {
    const res = await client.listNexus().sendMessage();
    assert.lengthOf(res.nexusList, 1);

    const nexus = res.nexusList[0];
    assert.equal(nexus.uuid, UUID);
    assert.equal(nexus.rebuilds, expected);
  }

  async function checkRebuildState (expected) {
    try {
      const res = await client.getRebuildState().sendMessage(rebuildArgs);
      assert.equal(res.state, expected);
    } catch (e) {
      if (expected === 'stopped' && e.code === grpc.status.NOT_FOUND) {
        // this is ok, when stopped it gets removed
      } else {
        throw e;
      }
    }
  }

  async function untilChildStatus (state, sleepMs = 200, retries = 10) {
    const promise = () => checkState(ObjectType.DESTINATION_CHILD, state);
    await retryPromiseFn(promise, sleepMs, retries);
  }

  async function untilRebuildState (state, sleepMs = 200, retries = 10) {
    const promise = () => checkRebuildState(state);
    await retryPromiseFn(promise, sleepMs, retries);
  }

  async function retryPromiseFn (promiseFn, sleepMs, retries) {
    try {
      return await promiseFn();
    } catch (e) {
      if (retries > 0) {
        await sleep(sleepMs);
        return await retryPromiseFn(promiseFn, sleepMs, retries - 1);
      }
      throw e;
    }
  }

  async function checkRebuildStats () {
    const stats = await client.getRebuildStats().sendMessage(rebuildArgs);
    assert.isTrue(stats.blocksTotal > 0);
    assert.isTrue(stats.blocksRecovered > 0);
    assert.isTrue(stats.progress > 0);
    assert.isTrue(stats.segmentSizeBlks > 0);
    assert.isTrue(stats.blockSize === 4096);
    assert.isTrue(stats.tasksTotal > 0);
    assert.isTrue(stats.tasksActive === 0);
  }

  function pingMayastor (done) {
    // use harmless method to test if the mayastor is up and running
    client
      .listPools()
      .sendMessage()
      .then(() => {
        done();
      })
      .catch((err) => {
        return done(err);
      });
  }

  before((done) => {
    client = createGrpcClient();
    if (!client) {
      return done(new Error('Failed to initialize grpc client'));
    }

    async.series(
      [
        common.ensureNbdWritable,
        (next) => {
          fs.writeFile(child1, '', next);
        },
        (next) => {
          fs.truncate(child1, diskSize, next);
        },
        (next) => {
          fs.writeFile(child2, '', next);
        },
        (next) => {
          fs.truncate(child2, diskSize, next);
        },
        (next) => {
          common.startMayastor(null, ['-r', common.SOCK, '-g', common.grpcEndpoint, '-s', 384]);
          common.waitFor((pingDone) => {
            pingMayastor(pingDone);
          }, next);
        },
        (next) => {
          client
            .createNexus()
            .sendMessage(nexusArgs)
            .then(() => {
              next();
            })
            .catch(done);
        }
      ],
      done
    );
  });

  after((done) => {
    async.series(
      [
        common.stopAll,
        common.restoreNbdPerms,
        (next) => {
          fs.unlink(child1, () => next());
        },
        (next) => {
          fs.unlink(child2, () => next());
        },
        (next) => {
          client
            .destroyNexus()
            .sendMessage({ uuid: UUID })
            .then(() => {
              next();
            })
            .catch(() => {
              done();
            })
            .catch(done);
        }
      ],
      (err) => {
        if (client != null) {
          client.close();
        }
        done(err);
      }
    );
  });

  describe('running rebuild', function () {
    beforeEach(async () => {
      await client.addChildNexus().sendMessage(addChildArgs);
      await client.startRebuild().sendMessage(rebuildArgs);
    });

    afterEach(async () => {
      await client.stopRebuild().sendMessage(rebuildArgs);
      await client.removeChildNexus().sendMessage(rebuildArgs);
    });

    it('check nexus state', async () => {
      await checkState(ObjectType.NEXUS, 'NEXUS_DEGRADED');
    });

    it('check source state', async () => {
      await checkState(ObjectType.SOURCE_CHILD, 'CHILD_ONLINE');
    });

    it('check destination state', async () => {
      await checkState(ObjectType.DESTINATION_CHILD, 'CHILD_DEGRADED');
    });

    it('check rebuild state', async () => {
      await checkRebuildState('running');
    });

    it('check number of rebuilds', async () => {
      await checkNumRebuilds('1');
    });
  });

  describe('stopping rebuild', function () {
    beforeEach(async () => {
      await client.addChildNexus().sendMessage(addChildArgs);
      await client.startRebuild().sendMessage(rebuildArgs);
      await client.stopRebuild().sendMessage(rebuildArgs);
      await untilRebuildState('stopped');
    });

    afterEach(async () => {
      await client.removeChildNexus().sendMessage(rebuildArgs);
    });

    it('check nexus state', async () => {
      await checkState(ObjectType.NEXUS, 'NEXUS_DEGRADED');
    });

    it('check source state', async () => {
      await checkState(ObjectType.SOURCE_CHILD, 'CHILD_ONLINE');
    });

    it('check destination state', async () => {
      await checkState(ObjectType.DESTINATION_CHILD, 'CHILD_DEGRADED');
    });

    it('check rebuild state', async (done) => {
      // Expect to fail to get rebuild state because
      // after stopping there is no rebuild task
      client
        .getRebuildState()
        .sendMessage(rebuildArgs)
        .then(() => {
          done(new Error('Expected to fail to get rebuild state.'));
        })
        .catch((err) => {
          assert.isDefined(err);
        })
        .catch(done);
      done();
    });

    it('check number of rebuilds', async () => {
      await checkNumRebuilds('0');
    });
  });

  describe('pausing rebuild', function () {
    beforeEach(async () => {
      await client.addChildNexus().sendMessage(addChildArgs);
      await client.startRebuild().sendMessage(rebuildArgs);
      await client.pauseRebuild().sendMessage(rebuildArgs);
      await untilRebuildState('paused');
    });

    afterEach(async () => {
      await client.removeChildNexus().sendMessage(rebuildArgs);
    });

    it('check nexus state', async () => {
      await checkState(ObjectType.NEXUS, 'NEXUS_DEGRADED');
    });

    it('check source state', async () => {
      await checkState(ObjectType.SOURCE_CHILD, 'CHILD_ONLINE');
    });

    it('check destination state', async () => {
      await checkState(ObjectType.DESTINATION_CHILD, 'CHILD_DEGRADED');
    });

    it('check rebuild state', async () => {
      await checkRebuildState('paused');
    });

    it('check number of rebuilds', async () => {
      await checkNumRebuilds('1');
    });

    it('check stats', async () => {
      await checkRebuildStats();
    });
  });

  describe('resuming rebuild', function () {
    beforeEach(async () => {
      await client.addChildNexus().sendMessage(addChildArgs);
      await client.startRebuild().sendMessage(rebuildArgs);
      await client.pauseRebuild().sendMessage(rebuildArgs);
      await untilRebuildState('paused');
      await client.resumeRebuild().sendMessage(rebuildArgs);
    });

    afterEach(async () => {
      await client.removeChildNexus().sendMessage(rebuildArgs);
    });

    it('check nexus state', async () => {
      await checkState(ObjectType.NEXUS, 'NEXUS_DEGRADED');
    });

    it('check source state', async () => {
      await checkState(ObjectType.SOURCE_CHILD, 'CHILD_ONLINE');
    });

    it('check destination state', async () => {
      await checkState(ObjectType.DESTINATION_CHILD, 'CHILD_DEGRADED');
    });

    it('check rebuild state', async () => {
      await checkRebuildState('running');
    });

    it('check number of rebuilds', async () => {
      await checkNumRebuilds('1');
    });
  });

  describe('set child online', function () {
    beforeEach(async () => {
      await client.addChildNexus().sendMessage(addChildArgs);
      await client.childOperation().sendMessage(childOfflineArgs);
      await client.childOperation().sendMessage(childOnlineArgs);
    });

    afterEach(async () => {
      await client.removeChildNexus().sendMessage(rebuildArgs);
    });

    it('check nexus state', async () => {
      await checkState(ObjectType.NEXUS, 'NEXUS_DEGRADED');
    });

    it('check source state', async () => {
      await checkState(ObjectType.SOURCE_CHILD, 'CHILD_ONLINE');
    });

    it('check destination state', async () => {
      await checkState(ObjectType.DESTINATION_CHILD, 'CHILD_DEGRADED');
    });

    it('check rebuild state', async () => {
      await checkRebuildState('running');
    });

    it('check number of rebuilds', async () => {
      await checkNumRebuilds('1');
    });
  });

  describe('set child offline', function () {
    beforeEach(async () => {
      await client.addChildNexus().sendMessage(addChildArgs);
      await client.startRebuild().sendMessage(rebuildArgs);
      await untilRebuildState('running');
      await client.childOperation().sendMessage(childOfflineArgs);
      await untilChildStatus('CHILD_DEGRADED');
    });

    afterEach(async () => {
      await client.removeChildNexus().sendMessage(rebuildArgs);
    });

    it('check nexus state', async () => {
      await checkState(ObjectType.NEXUS, 'NEXUS_DEGRADED');
    });

    it('check source state', async () => {
      await checkState(ObjectType.SOURCE_CHILD, 'CHILD_ONLINE');
    });

    it('check destination state', async () => {
      await checkState(ObjectType.DESTINATION_CHILD, 'CHILD_DEGRADED');
    });

    it('check number of rebuilds', async () => {
      await checkNumRebuilds('0');
    });
  });
});
