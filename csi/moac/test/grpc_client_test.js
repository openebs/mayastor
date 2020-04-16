// Unit tests for grpc utility functions.

'use strict';

const expect = require('chai').expect;
const grpc = require('grpc-uds');
const { MayastorServer } = require('./mayastor_mock');
const { GrpcClient, GrpcCode, GrpcError } = require('../grpc_client');
const { shouldFailWith } = require('./utils');

const EGRESS_ENDPOINT = '127.0.0.1:12345';

module.exports = function() {
  var srv;
  var client;

  // start a fake mayastor server and initialize the client
  before(() => {
    client = new GrpcClient(EGRESS_ENDPOINT);
  });

  beforeEach(() => {
    if (!srv) {
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
    }
  });

  after(() => {
    if (client) client.close();
    if (srv) srv.stop();
    client = null;
    srv = null;
  });

  it('should provide grpc status codes', () => {
    expect(GrpcCode.NOT_FOUND).to.equal(5);
    expect(GrpcCode.INTERNAL).to.equal(13);
  });

  it('should call a grpc method', async () => {
    let res = await client.call('listPools', {});
    expect(res.pools).to.have.lengthOf(1);
    expect(res.pools[0].name).to.equal('pool');
  });

  it('should throw if grpc method fails', async () => {
    await shouldFailWith(grpc.status.NOT_FOUND, async () => {
      await client.call('destroyPool', { name: 'unknown-pool' });
    });
  });

  it('should throw if unable to connect to the server', async () => {
    srv.stop();
    srv = null;
    // 14 = UNAVAILABLE: GOAWAY received
    await shouldFailWith(14, async () => {
      await client.call('destroyPool', { name: 'unknown-pool' });
    });
  });

  it('should release the client after close', async () => {
    client.close();
    try {
      await client.call('listPools', {});
    } catch (err) {
      return;
    }
    throw new Error('Expected to throw error');
  });
};
