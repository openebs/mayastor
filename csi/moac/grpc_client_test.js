// Unit tests for grpc utility functions.

'use strict';

const assert = require('chai').assert;
const grpc = require('grpc-uds');
const { MayastorServer } = require('./mayastor_mock');
const { NodeOperatorMock } = require('./nodes');
const { GrpcClient, GrpcError, GrpcHandle } = require('./grpc_client');
const { shouldFailWith, waitUntil } = require('./test_utils');

const EGRESS_ENDPOINT = '127.0.0.1:12345';

module.exports = function() {
  var srv;
  var client;

  // start a fake mayastor server and initialize client
  before(() => {
    let nodeOperator = new NodeOperatorMock([
      {
        node: 'node',
        endpoint: EGRESS_ENDPOINT,
      },
    ]);
    client = new GrpcClient(nodeOperator);

    let pools = [
      {
        name: 'pool',
        disks: ['/dev/sdb'],
        state: 0,
        capacity: 100,
        used: 4,
      },
    ];
    srv = new MayastorServer(EGRESS_ENDPOINT, pools).start();
  });

  after(() => {
    if (srv) {
      srv.stop();
    }
    srv = null;
  });

  it('should call a grpc method', async () => {
    let res = await client.with_handle('node', async handle => {
      return await handle.call('listPools', {});
    });
    assert.lengthOf(res.pools, 1);
    assert.equal(res.pools[0].name, 'pool');
  });

  it('should throw if target node is not known', async () => {
    await shouldFailWith(grpc.status.INTERNAL, async () => {
      await client.with_handle('unknown-node', async handle => {
        throw new Error('Should have thrown error');
      });
    });
  });

  it('should throw if grpc method fails', async () => {
    await client.with_handle('node', async handle => {
      await shouldFailWith(grpc.status.NOT_FOUND, async () => {
        await handle.call('destroyPool', { name: 'unknown-pool' });
      });
    });
  });

  it('should release the handle after use', async () => {
    var staleHandle;

    await client.with_handle('node', async handle => {
      staleHandle = handle;
    });
    try {
      await staleHandle.call('listPools', {});
    } catch (err) {
      return;
    }
    throw new Error('Expected to throw error');
  });
};
