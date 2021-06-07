// Unit tests for grpc utility functions.

'use strict';

const expect = require('chai').expect;
const { MayastorServer } = require('./mayastor_mock');
const { GrpcClient, grpcCode } = require('../dist/grpc_client');
const { shouldFailWith } = require('./utils');

const MS_ENDPOINT = '127.0.0.1:12345';
const UUID = '88dba542-d187-11ea-87d0-0242ac130003';

module.exports = function () {
  let srv;
  let client;

  function startServer (replyDelay, done) {
    if (!srv) {
      const pools = [
        {
          name: 'pool',
          disks: ['/dev/sdb'],
          state: 0,
          capacity: 100,
          used: 4
        }
      ];
      srv = new MayastorServer(MS_ENDPOINT, pools, [], [], replyDelay);
      srv.start(done);
    } else {
      done();
    }
  }

  function stopServer () {
    if (srv) {
      srv.stop();
      srv = null;
    }
  }

  function createClient (timeout) {
    client = new GrpcClient(MS_ENDPOINT, timeout);
  }

  function destroyClient () {
    if (client) {
      client.close();
      client = null;
    }
  }

  describe('server without delay', () => {
    before((done) => {
      createClient();
      startServer(undefined, done);
    });

    after(() => {
      destroyClient();
      stopServer();
    });

    it('should provide grpc status codes', () => {
      expect(grpcCode.NOT_FOUND).to.equal(5);
      expect(grpcCode.INTERNAL).to.equal(13);
    });

    it('should call a grpc method', async () => {
      const res = await client.call('listPools', {});
      expect(res.pools).to.have.lengthOf(1);
      expect(res.pools[0].name).to.equal('pool');
    });

    it('should throw if grpc method fails', async () => {
      await shouldFailWith(
        grpcCode.NOT_FOUND,
        () => client.call('removeChildNexus', { uuid: UUID, uri: 'bdev://bbb' })
      );
    });

    // This must come after other tests using the server because it closes it.
    it('should throw if the server with connected client shuts down', async () => {
      stopServer();
      await shouldFailWith(
        grpcCode.CANCELLED,
        () => client.call('destroyPool', { name: 'unknown-pool' })
      );
    });

    // This must be the last test here because it closes the client handle.
    it('should release the client after close', async () => {
      client.close();
      try {
        await client.call('listPools', {});
      } catch (err) {
        return;
      }
      throw new Error('Expected to throw error');
    });
  });

  describe('server with delayed replies', () => {
    const delayMs = 20;

    before((done) => {
      startServer(delayMs, done);
    });

    after(() => {
      stopServer();
    });

    afterEach(destroyClient);

    it('should honor timeout set for the grpc call', async () => {
      createClient();
      await shouldFailWith(
        grpcCode.DEADLINE_EXCEEDED,
        () => client.call('listPools', {}, delayMs / 2)
      );
    });

    it('should honor the default timeout set for the grpc client', async () => {
      createClient(delayMs / 2);
      await shouldFailWith(
        grpcCode.DEADLINE_EXCEEDED,
        () => client.call('listPools', {})
      );
    });
  });

  describe('no server', () => {
    before(() => createClient());
    after(destroyClient);

    it('should throw if unable to connect to the server', async () => {
      await shouldFailWith(
        grpcCode.UNAVAILABLE,
        () => client.call('destroyPool', { name: 'unknown-pool' })
      );
    });
  });
};
