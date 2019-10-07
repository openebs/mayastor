// Unit tests for the REST API

'use strict';

const assert = require('chai').assert;
const http = require('http');
const { VolumeOperatorMock } = require('./volumes');
const { ApiServer } = require('./rest_api');

const PORT = 12312;
const STAT_COUNTER = 1000000; // feels good!
const UUID = '02de3df9-ce18-4164-89e1-b1cbf7a88e56';

module.exports = function() {
  var volumeOperator;
  var apiServer;

  before(() => {
    volumeOperator = new VolumeOperatorMock(
      [],
      [
        {
          uuid: UUID,
          pool: 'pool',
          node: 'node',
          size: 10,
        },
      ],
      STAT_COUNTER
    );
    apiServer = new ApiServer(volumeOperator);
    apiServer.start(PORT);
  });

  after(() => {
    apiServer.stop();
  });

  it('should get volume stats', done => {
    // TODO: Use user-friendly "request" lib when we have more tests
    http
      .get('http://127.0.0.1:' + PORT + '/stats', resp => {
        assert.equal(resp.statusCode, 200);

        let data = '';
        resp.on('data', chunk => {
          data += chunk;
        });
        resp.on('end', () => {
          let vols = JSON.parse(data);
          assert.lengthOf(vols, 1);
          assert.equal(vols[0].uuid, UUID);
          assert.equal(vols[0].pool, 'pool');
          assert.equal(vols[0].stats.num_read_ops, STAT_COUNTER);
          assert.equal(vols[0].stats.num_write_ops, STAT_COUNTER);
          assert.equal(vols[0].stats.bytes_read, STAT_COUNTER);
          assert.equal(vols[0].stats.bytes_written, STAT_COUNTER);
          done();
        });
      })
      .on('error', done);
  });
};
